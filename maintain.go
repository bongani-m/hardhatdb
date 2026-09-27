package persist

import (
	"context"
	"encoding/binary"
	"encoding/json"

	"github.com/dolthub/go-mysql-server/sql"
)

func indexMaintained(idx storedIndex) bool {
	switch sql.IndexConstraint(idx.Constraint) {
	case sql.IndexConstraint_Fulltext, sql.IndexConstraint_Spatial, sql.IndexConstraint_Vector:
		return false
	default:
		return true
	}
}

func indexIsUnique(idx storedIndex) bool {
	return sql.IndexConstraint(idx.Constraint) == sql.IndexConstraint_Unique
}

func putFormat(bucket *kvBucket) error {
	var raw [2]byte
	binary.BigEndian.PutUint16(raw[:], formatCurrent)
	return bucket.Put(keyFormat, raw[:])
}

func tableFormat(bucket *kvBucket) uint16 {
	raw := bucket.Get(keyFormat)
	if len(raw) < 2 {
		return 0
	}
	return binary.BigEndian.Uint16(raw)
}

func putUint64(bucket *kvBucket, key []byte, v uint64) error {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], v)
	return bucket.Put(key, raw[:])
}

func getUint64(bucket *kvBucket, key []byte) (uint64, bool) {
	raw := bucket.Get(key)
	if len(raw) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(raw), true
}

func decodeIndexes(raw []byte) ([]storedIndex, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var indexes []storedIndex
	if err := json.Unmarshal(raw, &indexes); err != nil {
		return nil, err
	}
	return indexes, nil
}

func indexesIn(bucket *kvBucket) ([]storedIndex, error) {
	return decodeIndexes(bucket.Get(keyIndexes))
}

func indexParent(bucket *kvBucket) (*kvBucket, error) {
	parent, err := bucket.CreateBucketIfNotExists(bucketIndex)
	return parent, err
}

func syncIndexBuckets(bucket *kvBucket, indexes []storedIndex) error {
	parent := bucket.Bucket(bucketIndex)
	if parent == nil {
		return nil
	}
	keep := make(map[string]struct{}, len(indexes))
	for _, idx := range indexes {
		if indexMaintained(idx) {
			keep[idx.Name] = struct{}{}
		}
	}
	var drop [][]byte
	err := parent.ForEach(func(k, v []byte) error {
		if v != nil {
			return nil
		}
		if _, ok := keep[string(k)]; ok {
			return nil
		}
		drop = append(drop, append([]byte(nil), k...))
		return nil
	})
	if err != nil {
		return err
	}
	for _, name := range drop {
		if err := parent.DeleteBucket(name); err != nil {
			return err
		}
	}
	return nil
}

func renameIndexBucket(bucket *kvBucket, from, to string) error {
	parent := bucket.Bucket(bucketIndex)
	if parent == nil {
		return nil
	}
	src := parent.Bucket([]byte(from))
	if src == nil {
		return nil
	}
	dst, err := parent.CreateBucket([]byte(to))
	if err != nil {
		return err
	}
	if err := copyBucket(src, dst); err != nil {
		return err
	}
	return parent.DeleteBucket([]byte(from))
}

func clearIndexData(bucket *kvBucket) error {
	if bucket.Bucket(bucketIndex) == nil {
		return nil
	}
	return bucket.DeleteBucket(bucketIndex)
}

func readStoredRows(ctx context.Context, rows *kvBucket, schema sql.Schema) ([]storedRow, error) {
	if rows == nil {
		return nil, nil
	}
	var out []storedRow
	err := rows.forEachRaw(func(k, v []byte) error {
		row, err := decodeRow(ctx, schema, v)
		if err != nil {
			return err
		}
		out = append(out, storedRow{key: append([]byte(nil), k...), row: row})
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = rows.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}
		row, err := decodeRow(ctx, schema, v)
		if err != nil {
			return err
		}
		out = append(out, storedRow{key: append([]byte(nil), k...), row: row})
		return nil
	})
	return out, err
}

func putIndexEntries(ctx context.Context, bucket *kvBucket, schema sql.Schema, indexes []storedIndex, row sql.Row, rowKey []byte) error {
	for _, idx := range indexes {
		if !indexMaintained(idx) {
			continue
		}
		if err := putOneIndex(ctx, bucket, schema, idx, row, rowKey); err != nil {
			return err
		}
	}
	return nil
}

func putOneIndex(ctx context.Context, bucket *kvBucket, schema sql.Schema, idx storedIndex, row sql.Row, rowKey []byte) error {
	fields, err := indexFields(schema, idx)
	if err != nil {
		return err
	}
	colKey, hasNull, err := encodeIndexColumns(ctx, fields, row)
	if err != nil {
		return err
	}
	parent, err := indexParent(bucket)
	if err != nil {
		return err
	}
	data, err := parent.CreateBucketIfNotExists([]byte(idx.Name))
	if err != nil {
		return err
	}
	key := indexEntryKey(colKey, rowKey, indexIsUnique(idx), hasNull)
	return data.PutRaw(key, rowKey)
}

func deleteIndexEntries(ctx context.Context, bucket *kvBucket, schema sql.Schema, indexes []storedIndex, row sql.Row, rowKey []byte) error {
	if row == nil {
		return nil
	}
	parent := bucket.Bucket(bucketIndex)
	if parent == nil {
		return nil
	}
	for _, idx := range indexes {
		if !indexMaintained(idx) {
			continue
		}
		fields, err := indexFields(schema, idx)
		if err != nil {
			return err
		}
		colKey, hasNull, err := encodeIndexColumns(ctx, fields, row)
		if err != nil {
			return err
		}
		data := parent.Bucket([]byte(idx.Name))
		if data == nil {
			continue
		}
		key := indexEntryKey(colKey, rowKey, indexIsUnique(idx), hasNull)
		if err := data.DeleteRaw(key); err != nil {
			return err
		}
	}
	return nil
}

func checkUniqueWrite(ctx context.Context, bucket *kvBucket, schema sql.Schema, indexes []storedIndex, row sql.Row, rowKey, ignore []byte) error {
	parent := bucket.Bucket(bucketIndex)
	rows := bucket.Bucket(bucketRows)
	for _, idx := range indexes {
		if !indexIsUnique(idx) || !indexMaintained(idx) {
			continue
		}
		fields, err := indexFields(schema, idx)
		if err != nil {
			return err
		}
		colKey, hasNull, err := encodeIndexColumns(ctx, fields, row)
		if err != nil || hasNull {
			if err != nil {
				return err
			}
			continue
		}
		if parent == nil {
			continue
		}
		data := parent.Bucket([]byte(idx.Name))
		if data == nil {
			continue
		}
		existing := data.GetRaw(colKey)
		if len(existing) == 0 || bytesEqual(existing, rowKey) || bytesEqual(existing, ignore) {
			continue
		}
		var existingRow sql.Row
		if rows != nil {
			if raw := rows.GetRaw(existing); len(raw) > 0 {
				existingRow, _ = decodeRow(ctx, schema, raw)
			}
		}
		return sql.NewUniqueKeyErr(idx.Name, false, existingRow)
	}
	return nil
}

// backfillIndexUnique writes one index. A second row with the same unique key
// fails the check, and the transaction drops the definition with it.
func backfillIndexUnique(ctx context.Context, bucket *kvBucket, schema sql.Schema, idx storedIndex) error {
	rows := bucket.Bucket(bucketRows)
	stored, err := readStoredRows(ctx, rows, schema)
	if err != nil {
		return err
	}
	for _, row := range stored {
		if err := checkUniqueWrite(ctx, bucket, schema, []storedIndex{idx}, row.row, row.key, nil); err != nil {
			return err
		}
		if err := putOneIndex(ctx, bucket, schema, idx, row.row, row.key); err != nil {
			return err
		}
	}
	return nil
}

func rebuildIndexes(ctx context.Context, bucket *kvBucket, schema sql.Schema, pk []int, rows []storedRow) error {
	if err := clearIndexData(bucket); err != nil {
		return err
	}
	indexes, err := indexesIn(bucket)
	if err != nil {
		return err
	}
	for _, row := range rows {
		key := row.key
		if len(pk) > 0 {
			key, err = primaryKey(ctx, schema, pk, row.row)
			if err != nil {
				return err
			}
		}
		if err := putIndexEntries(ctx, bucket, schema, indexes, row.row, key); err != nil {
			return err
		}
	}
	return nil
}

func countRows(rows *kvBucket) (n uint64, nbytes uint64, err error) {
	if rows == nil {
		return 0, 0, nil
	}
	err = rows.forEachRaw(func(_, v []byte) error {
		n++
		nbytes += uint64(len(v))
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	err = rows.ForEach(func(_, v []byte) error {
		if v == nil {
			return nil
		}
		n++
		nbytes += uint64(len(v))
		return nil
	})
	return n, nbytes, err
}

func loadCounts(bucket, rows *kvBucket) (uint64, uint64, error) {
	count, countOK := getUint64(bucket, keyRowCount)
	nbytes, bytesOK := getUint64(bucket, keyDataBytes)
	if countOK && bytesOK {
		return count, nbytes, nil
	}
	return countRows(rows)
}

func isProvisional(key []byte) bool {
	return len(key) == 10 && key[0] == 0xFE && key[1] == 0x00
}

func provisionalKey(n uint64) []byte {
	key := make([]byte, 10)
	key[0] = 0xFE
	key[1] = 0x00
	binary.BigEndian.PutUint64(key[2:], n)
	return key
}

func resolveProvisional(rows *kvBucket, edits []edit) ([]edit, error) {
	overlay := buildOverlay(edits)
	assigned := make(map[string][]byte)
	for _, entry := range overlay {
		if entry.tomb || !isProvisional(entry.key) {
			continue
		}
		seq, err := rows.NextSequence()
		if err != nil {
			return nil, err
		}
		assigned[string(entry.key)] = sequenceKey(seq)
	}
	if len(assigned) == 0 && !provisionalEdits(edits) {
		return edits, nil
	}
	out := make([]edit, 0, len(edits))
	for _, ed := range edits {
		if isProvisional(ed.key) {
			key, ok := assigned[string(ed.key)]
			if !ok {
				continue
			}
			ed.key = key
		}
		if isProvisional(ed.oldKey) {
			key, ok := assigned[string(ed.oldKey)]
			if !ok {
				continue
			}
			ed.oldKey = key
		}
		out = append(out, ed)
	}
	return out, nil
}

func provisionalEdits(edits []edit) bool {
	for _, ed := range edits {
		if isProvisional(ed.key) || isProvisional(ed.oldKey) {
			return true
		}
	}
	return false
}
