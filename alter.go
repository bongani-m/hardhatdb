package persist

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// AddColumn implements sql.AlterableTable. Existing rows gain a nil in the new position.
// The engine fills non-null defaults afterwards.
func (t *Table) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	if columnOrdinal(t.meta.schema, column.Name) >= 0 {
		return sql.ErrColumnExists.New(column.Name)
	}
	column.Source = t.name
	column.DatabaseSource = t.dbName
	pos := columnPosition(t.meta.schema, order)
	schema := make(sql.Schema, 0, len(t.meta.schema)+1)
	schema = append(schema, t.meta.schema[:pos]...)
	schema = append(schema, column)
	schema = append(schema, t.meta.schema[pos:]...)

	rows, err := t.visibleRows(ctx)
	if err != nil {
		return err
	}
	for i, row := range rows {
		widened := make(sql.Row, 0, len(schema))
		widened = append(widened, row[:pos]...)
		widened = append(widened, nil)
		widened = append(widened, row[pos:]...)
		rows[i] = widened
	}
	return t.commitSchema(ctx, schema, rows)
}

// DropColumn implements sql.AlterableTable.
func (t *Table) DropColumn(ctx *sql.Context, columnName string) error {
	ord := columnOrdinal(t.meta.schema, columnName)
	if ord < 0 {
		return sql.ErrColumnNotFound.New(columnName)
	}
	schema := make(sql.Schema, 0, len(t.meta.schema)-1)
	schema = append(schema, t.meta.schema[:ord]...)
	schema = append(schema, t.meta.schema[ord+1:]...)

	rows, err := t.visibleRows(ctx)
	if err != nil {
		return err
	}
	for i, row := range rows {
		if ord >= len(row) {
			continue
		}
		narrowed := make(sql.Row, 0, len(schema))
		narrowed = append(narrowed, row[:ord]...)
		narrowed = append(narrowed, row[ord+1:]...)
		rows[i] = narrowed
	}
	if err := t.commitSchema(ctx, schema, rows); err != nil {
		return err
	}
	return t.dropIndexesUsing(columnName)
}

// ModifyColumn implements sql.AlterableTable.
func (t *Table) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	ord := columnOrdinal(t.meta.schema, columnName)
	if ord < 0 {
		return sql.ErrColumnNotFound.New(columnName)
	}
	column.Source = t.name
	column.DatabaseSource = t.dbName
	// MODIFY COLUMN restates the type and attributes but never the key, so the
	// column keeps whatever primary key membership it already had.
	column.PrimaryKey = t.meta.schema[ord].PrimaryKey
	if column.PrimaryKey {
		column.Nullable = false
	}

	schema := t.meta.schema.Copy()
	schema[ord] = column
	from := ord
	to := from
	if order != nil && (order.First || order.AfterColumn != "") {
		to = columnPosition(schema, order)
		if to > from {
			to--
		}
		if to != from {
			moved := schema[from]
			schema = append(schema[:from], schema[from+1:]...)
			schema = append(schema[:to], append(sql.Schema{moved}, schema[to:]...)...)
		}
	}

	oldType := t.meta.schema[ord].Type
	rows, err := t.visibleRows(ctx)
	if err != nil {
		return err
	}
	for i, row := range rows {
		if ord >= len(row) {
			return fmt.Errorf("persist: row is missing column %s", columnName)
		}
		// The old type matters: converting an ENUM to a string has to go through
		// the enum's value list, which Type.Convert alone cannot see.
		converted, inRange, err := types.TypeAwareConversion(ctx, row[ord], oldType, column.Type)
		if err != nil {
			if sql.ErrNotMatchingSRID.Is(err) {
				err = sql.ErrNotMatchingSRIDWithColName.New(columnName, err)
			}
			return err
		}
		if inRange != sql.InRange {
			return sql.ErrValueOutOfRange.New(row[ord], column.Type)
		}
		next := row.Copy()
		next[ord] = converted
		if to != from {
			value := next[from]
			next = append(next[:from], next[from+1:]...)
			moved := make(sql.Row, 0, len(schema))
			moved = append(moved, next[:to]...)
			moved = append(moved, value)
			moved = append(moved, next[to:]...)
			next = moved
		}
		rows[i] = next
	}
	// Rename the index definitions before the rows are rewritten. writeSchema
	// skips an index whose column is no longer in the schema, which drops the
	// entries for the name this column is about to leave behind.
	if !strings.EqualFold(columnName, column.Name) {
		if err := t.renameIndexColumn(columnName, column.Name); err != nil {
			return err
		}
	}
	return t.commitSchema(ctx, schema, rows)
}

// CreatePrimaryKey implements sql.PrimaryKeyAlterableTable.
func (t *Table) CreatePrimaryKey(ctx *sql.Context, columns []sql.IndexColumn) error {
	for _, col := range t.meta.schema {
		if col.PrimaryKey {
			return sql.ErrMultiplePrimaryKeysDefined.New()
		}
	}
	schema := t.meta.schema.Copy()
	pk := make([]int, len(columns))
	for i, col := range columns {
		ord := columnOrdinal(schema, col.Name)
		if ord < 0 {
			return sql.ErrKeyColumnDoesNotExist.New(col.Name)
		}
		schema[ord].PrimaryKey = true
		schema[ord].Nullable = false
		pk[i] = ord
	}
	rows, err := t.visibleRows(ctx)
	if err != nil {
		return err
	}
	seen := make(map[string]sql.Row, len(rows))
	for _, row := range rows {
		for _, ord := range pk {
			if ord < len(row) && row[ord] == nil {
				return sql.ErrInsertIntoNonNullableProvidedNull.New(schema[ord].Name)
			}
		}
		key, err := primaryKey(ctx, schema, pk, row)
		if err != nil {
			return err
		}
		if existing, ok := seen[string(key)]; ok {
			return sql.NewUniqueKeyErr(pkString(pk, row), true, existing)
		}
		seen[string(key)] = row
	}
	return t.commitSchema(ctx, schema, rows)
}

// DropPrimaryKey implements sql.PrimaryKeyAlterableTable.
func (t *Table) DropPrimaryKey(ctx *sql.Context) error {
	if err := sql.ValidatePrimaryKeyDrop(ctx, t, t.PrimaryKeySchema(ctx)); err != nil {
		return err
	}
	schema := t.meta.schema.Copy()
	found := false
	for _, col := range schema {
		if col.PrimaryKey {
			col.PrimaryKey = false
			found = true
		}
	}
	if !found {
		return sql.ErrCantDropFieldOrKey.New("PRIMARY")
	}
	rows, err := t.visibleRows(ctx)
	if err != nil {
		return err
	}
	return t.commitSchema(ctx, schema, rows)
}

func columnPosition(schema sql.Schema, order *sql.ColumnOrder) int {
	if order == nil {
		return len(schema)
	}
	if order.First {
		return 0
	}
	if order.AfterColumn != "" {
		if ord := columnOrdinal(schema, order.AfterColumn); ord >= 0 {
			return ord + 1
		}
	}
	return len(schema)
}

func (t *Table) commitSchema(ctx *sql.Context, schema sql.Schema, rows []sql.Row) error {
	return t.writeSchema(ctx, schema, t.primaryKeyOrdinals(schema), rows)
}

func (t *Table) writeSchema(ctx *sql.Context, schema sql.Schema, pk []int, rows []sql.Row) error {
	rawSchema, err := encodeSchema(ctx, sql.NewPrimaryKeySchema(schema, pk...), t.meta.collation, t.meta.comment)
	if err != nil {
		return err
	}
	err = t.store.updateQuery(ctx, func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		if err := bucket.Put(keySchema, rawSchema); err != nil {
			return err
		}
		if err := clearIndexData(bucket); err != nil {
			return err
		}
		if err := bucket.DeleteBucket(bucketRows); err != nil {
			return err
		}
		rowsBucket, err := bucket.CreateBucket(bucketRows)
		if err != nil {
			return err
		}
		indexes, err := indexesIn(bucket)
		if err != nil {
			return err
		}
		var nbytes uint64
		written := make([]storedRow, 0, len(rows))
		for _, row := range rows {
			encoded, err := encodeRow(ctx, schema, row)
			if err != nil {
				return err
			}
			var key []byte
			if len(pk) == 0 {
				seq, err := rowsBucket.NextSequence()
				if err != nil {
					return err
				}
				key = sequenceKey(seq)
			} else {
				key, err = primaryKey(ctx, schema, pk, row)
				if err != nil {
					return err
				}
			}
			if err := rowsBucket.PutRaw(key, encoded); err != nil {
				return err
			}
			written = append(written, storedRow{key: key, row: row})
			nbytes += uint64(len(encoded))
		}
		for _, row := range written {
			kept := indexes[:0:0]
			for _, idx := range indexes {
				if !indexMaintained(idx) {
					continue
				}
				ok := true
				for _, name := range idx.Columns {
					if columnOrdinal(schema, name) < 0 {
						ok = false
						break
					}
				}
				if ok {
					kept = append(kept, idx)
				}
			}
			if err := putIndexEntries(ctx, bucket, schema, kept, row.row, row.key); err != nil {
				return err
			}
		}
		if err := putUint64(bucket, keyRowCount, uint64(len(rows))); err != nil {
			return err
		}
		if err := putUint64(bucket, keyDataBytes, nbytes); err != nil {
			return err
		}
		return putFormat(bucket)
	})
	if err != nil {
		return err
	}
	t.meta.schema = schema
	t.meta.pk = pk
	if sess, ok := sessionFrom(ctx); ok {
		sess.clear(t.ref())
	}
	return nil
}

// primaryKeyOrdinals locates the primary key columns of schema. A primary key
// can be declared in an order that does not match the column order, so the
// existing ordinals lead and only columns the alter newly marked as part of the
// key are appended.
func (t *Table) primaryKeyOrdinals(schema sql.Schema) []int {
	pk := make([]int, 0, len(t.meta.pk))
	seen := make(map[string]struct{}, len(t.meta.pk))
	for _, ord := range t.meta.pk {
		if ord < 0 || ord >= len(t.meta.schema) {
			continue
		}
		name := t.meta.schema[ord].Name
		next := columnOrdinal(schema, name)
		if next < 0 || !schema[next].PrimaryKey {
			continue
		}
		pk = append(pk, next)
		seen[strings.ToLower(name)] = struct{}{}
	}
	for i, col := range schema {
		if !col.PrimaryKey {
			continue
		}
		if _, ok := seen[strings.ToLower(col.Name)]; ok {
			continue
		}
		pk = append(pk, i)
	}
	return pk
}

func (t *Table) dropIndexesUsing(columnName string) error {
	indexes, err := t.readIndexes()
	if err != nil {
		return err
	}
	next := indexes[:0]
	for _, idx := range indexes {
		drop := false
		for _, col := range idx.Columns {
			if strings.EqualFold(col, columnName) {
				drop = true
				break
			}
		}
		if !drop {
			next = append(next, idx)
		}
	}
	if len(next) == len(indexes) {
		return nil
	}
	return t.writeIndexes(next)
}

func (t *Table) renameIndexColumn(from, to string) error {
	indexes, err := t.readIndexes()
	if err != nil {
		return err
	}
	changed := false
	for i, idx := range indexes {
		for j, col := range idx.Columns {
			if strings.EqualFold(col, from) {
				indexes[i].Columns[j] = to
				changed = true
			}
		}
	}
	if !changed {
		return nil
	}
	return t.writeIndexes(indexes)
}

// ModifyTargetRowSize implements sql.TargetRowSizeAlterableTable.
func (t *Table) ModifyTargetRowSize(ctx *sql.Context, sizeInBytes uint64) error {
	err := t.store.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		var raw [8]byte
		binary.BigEndian.PutUint64(raw[:], sizeInBytes)
		return bucket.Put(keyTargetRows, raw[:])
	})
	if err != nil {
		return err
	}
	t.meta.targetRowSize = sizeInBytes
	return nil
}

// HasTargetRowSize implements sql.TargetRowSizeTable.
func (t *Table) HasTargetRowSize() bool { return t.meta.targetRowSize != 0 }

// GetTargetRowSize implements sql.TargetRowSizeTable.
func (t *Table) GetTargetRowSize() uint64 { return t.meta.targetRowSize }

// GetChecks implements sql.CheckTable.
func (t *Table) GetChecks(ctx *sql.Context) ([]sql.CheckDefinition, error) {
	return t.readChecks()
}

// CreateCheck implements sql.CheckAlterableTable.
func (t *Table) CreateCheck(ctx *sql.Context, check *sql.CheckDefinition) error {
	checks, err := t.readChecks()
	if err != nil {
		return err
	}
	toInsert := *check
	if toInsert.Name == "" {
		toInsert.Name = t.generateCheckName(checks)
	}
	for _, existing := range checks {
		if strings.EqualFold(existing.Name, toInsert.Name) {
			return sql.ErrDuplicateCheckName.New(toInsert.Name)
		}
	}
	checks = append(checks, toInsert)
	return t.writeChecks(checks)
}

// DropCheck implements sql.CheckAlterableTable.
func (t *Table) DropCheck(ctx *sql.Context, chName string) error {
	checks, err := t.readChecks()
	if err != nil {
		return err
	}
	next := checks[:0]
	found := false
	for _, check := range checks {
		if strings.EqualFold(check.Name, chName) {
			found = true
			continue
		}
		next = append(next, check)
	}
	if !found {
		return fmt.Errorf("check '%s' was not found on the table", chName)
	}
	return t.writeChecks(next)
}

func (t *Table) generateCheckName(checks []sql.CheckDefinition) string {
	for i := 1; ; i++ {
		name := fmt.Sprintf("%s_chk_%d", t.name, i)
		taken := false
		for _, check := range checks {
			if strings.EqualFold(check.Name, name) {
				taken = true
				break
			}
		}
		if !taken {
			return name
		}
	}
}

func (t *Table) readChecks() ([]sql.CheckDefinition, error) {
	var raw []byte
	err := t.store.view(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		raw = append([]byte(nil), bucket.Get(keyChecks)...)
		return nil
	})
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var checks []sql.CheckDefinition
	if err := json.Unmarshal(raw, &checks); err != nil {
		return nil, err
	}
	return checks, nil
}

func (t *Table) writeChecks(checks []sql.CheckDefinition) error {
	raw, err := json.Marshal(checks)
	if err != nil {
		return err
	}
	return t.store.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		return bucket.Put(keyChecks, raw)
	})
}
