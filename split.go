package persist

import (
	"bytes"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// LSMBytes is the size of this store's on-disk tables. A split watcher
// compares it with GMS_RANGE_SPLIT_BYTES.
func (s *Store) LSMBytes() int64 {
	db := s.badgerDB()
	if db == nil {
		return 0
	}
	lsm, _ := db.Size()
	return lsm
}

// SplitKey picks a primary key that can be the left edge of a split.
// The key itself stays on the left. ok is false when the span has fewer
// than two distinct keys.
func (s *Store) SplitKey(ctx *sql.Context, dbName, tableName string, start, end []byte) ([]byte, bool, error) {
	var keys [][]byte
	err := s.view(func(tx *kvTx) error {
		rows := rowsBucket(tx, dbName, tableName)
		if rows == nil {
			return sql.ErrTableNotFound.New(tableName)
		}
		it := rows.rawIter(false)
		defer it.Close()
		if len(start) > 0 {
			it.Seek(start)
		} else {
			it.Rewind()
		}
		for ; it.Valid(); it.Next() {
			key := it.Key()
			if len(end) > 0 && bytes.Compare(key, end) >= 0 {
				break
			}
			keys = append(keys, append([]byte(nil), key...))
		}
		return nil
	})
	if err != nil || len(keys) < 2 {
		return nil, false, err
	}
	return keys[len(keys)/2-1], true, nil
}

// EnsureTable creates db.table on dst from src when dst does not have it.
func EnsureTable(ctx *sql.Context, src, dst *Store, dbName, tableName string) error {
	ddb, err := dst.Database(ctx, dbName)
	if err != nil {
		if err := dst.CreateDatabase(ctx, dbName); err != nil {
			return err
		}
		ddb, err = dst.Database(ctx, dbName)
		if err != nil {
			return err
		}
	}
	if _, ok, err := ddb.GetTableInsensitive(ctx, tableName); err != nil {
		return err
	} else if ok {
		return nil
	}
	sdb, err := src.Database(ctx, dbName)
	if err != nil {
		return err
	}
	st, ok, err := sdb.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrTableNotFound.New(tableName)
	}
	table := st.(*Table)
	creator, ok := ddb.(sql.TableCreator)
	if !ok {
		return fmt.Errorf("persist: %s cannot create tables", dbName)
	}
	return creator.CreateTable(ctx, table.Name(), table.PrimaryKeySchema(ctx), table.Collation(), table.Comment())
}

// CopyRows inserts into dst every row of src whose primary key is in
// [start, end). A row that is already on dst is left in place.
func CopyRows(ctx *sql.Context, srcTable, dstTable *Table, start, end []byte) error {
	rows, err := rowsInSpan(ctx, srcTable, start, end)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	var missing []sql.Row
	for _, row := range rows {
		pk, err := primaryKey(ctx, dstTable.meta.schema, dstTable.meta.pk, row)
		if err != nil {
			return err
		}
		have, err := rowPresent(dstTable, pk)
		if err != nil {
			return err
		}
		if !have {
			missing = append(missing, row)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return insertRowsStore(ctx, dstTable, missing...)
}

// DeleteRows removes rows whose primary key is in [start, end).
func DeleteRows(ctx *sql.Context, table *Table, start, end []byte) error {
	rows, err := rowsInSpan(ctx, table, start, end)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := deleteRowStore(ctx, table, row); err != nil {
			return err
		}
	}
	return nil
}

// SplitRange copies keys strictly above at from src to dst, publishes the
// two spans, then deletes the moved keys from src. at itself stays on src.
// A range already marked copying resumes the same cut.
func SplitRange(ctx *sql.Context, meta RangeCatalog, src, dst *Store, dbName, tableName string, at []byte, right KeyRange) error {
	if len(at) == 0 {
		return fmt.Errorf("persist: split key is empty")
	}
	if right.Group == "" || right.ID == "" {
		return fmt.Errorf("persist: split needs a new group")
	}
	list, err := meta.Ranges()
	if err != nil {
		return err
	}
	kr, ok := RangeHolding(list, dbName, tableName, at)
	if !ok {
		return fmt.Errorf("persist: no range holds the split key")
	}
	if kr.State == RangeCopying && len(kr.SplitKey) > 0 {
		at = append([]byte(nil), kr.SplitKey...)
		right.ID = kr.RightID
		right.Group = kr.RightGroup
		right.Peers = kr.RightPeers
	}
	bound := KeySuccessor(at)
	if kr.State != RangeCopying {
		if err := meta.ApplyRangeOp(RangeOp{
			Kind: RangeOpMark, ID: kr.ID, SplitKey: append([]byte(nil), at...), Right: right,
		}); err != nil {
			return err
		}
	}
	if err := EnsureTable(ctx, src, dst, dbName, tableName); err != nil {
		return err
	}
	srcTable, err := loadLocalTable(ctx, src, dbName, tableName)
	if err != nil {
		return err
	}
	dstTable, err := loadLocalTable(ctx, dst, dbName, tableName)
	if err != nil {
		return err
	}
	if err := CopyRows(ctx, srcTable, dstTable, bound, kr.End); err != nil {
		return err
	}
	right.DB = kr.DB
	right.Table = kr.Table
	right.Start = bound
	right.End = append([]byte(nil), kr.End...)
	right.State = RangeActive
	if err := meta.ApplyRangeOp(RangeOp{
		Kind: RangeOpPublish, ID: kr.ID, Bound: bound, Right: right,
	}); err != nil {
		return err
	}
	return DeleteRows(ctx, srcTable, bound, kr.End)
}

func loadLocalTable(ctx *sql.Context, store *Store, dbName, tableName string) (*Table, error) {
	db, err := store.Database(ctx, dbName)
	if err != nil {
		return nil, err
	}
	table, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(tableName)
	}
	return table.(*Table), nil
}

func rowsInSpan(ctx *sql.Context, table *Table, start, end []byte) ([]sql.Row, error) {
	var out []sql.Row
	err := table.store.view(func(tx *kvTx) error {
		rows := rowsBucket(tx, table.dbName, table.name)
		if rows == nil {
			return sql.ErrTableNotFound.New(table.name)
		}
		bucket := tableBucket(tx, table.dbName, table.name)
		meta, err := decodeSchema(append([]byte(nil), bucket.Get(keySchema)...), table.dbName, table.name)
		if err != nil {
			return err
		}
		it := rows.rawIter(false)
		defer it.Close()
		if len(start) > 0 {
			it.Seek(start)
		} else {
			it.Rewind()
		}
		for ; it.Valid(); it.Next() {
			key := it.Key()
			if len(start) > 0 && bytes.Compare(key, start) < 0 {
				continue
			}
			if len(end) > 0 && bytes.Compare(key, end) >= 0 {
				break
			}
			val, err := it.Value()
			if err != nil {
				return err
			}
			row, err := decodeRow(ctx, meta.schema, val)
			if err != nil {
				return err
			}
			out = append(out, row)
		}
		return nil
	})
	return out, err
}

func rowPresent(table *Table, pk []byte) (bool, error) {
	var have bool
	err := table.store.view(func(tx *kvTx) error {
		rows := rowsBucket(tx, table.dbName, table.name)
		if rows == nil {
			return nil
		}
		have = len(rows.GetRaw(pk)) > 0
		return nil
	})
	return have, err
}

func insertRowsStore(ctx *sql.Context, table *Table, rows ...sql.Row) error {
	inserter := table.Inserter(ctx)
	inserter.StatementBegin(ctx)
	for _, row := range rows {
		if err := inserter.Insert(ctx, row); err != nil {
			_ = inserter.DiscardChanges(ctx, err)
			_ = inserter.Close(ctx)
			return err
		}
	}
	if err := inserter.StatementComplete(ctx); err != nil {
		return err
	}
	return inserter.Close(ctx)
}

func deleteRowStore(ctx *sql.Context, table *Table, row sql.Row) error {
	deleter := table.Deleter(ctx)
	deleter.StatementBegin(ctx)
	if err := deleter.Delete(ctx, row); err != nil {
		_ = deleter.DiscardChanges(ctx, err)
		_ = deleter.Close(ctx)
		return err
	}
	if err := deleter.StatementComplete(ctx); err != nil {
		return err
	}
	return deleter.Close(ctx)
}
