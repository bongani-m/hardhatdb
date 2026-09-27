package persist

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.RewritableTable = (*Table)(nil)

// ShouldRewriteTable implements sql.RewritableTable.
//
// Every schema change is served by a rewrite. The alternative path calls
// AddColumn / DropColumn / ModifyColumn and then backfills through an ordinary
// UPDATE, which leaves the GetField indexes inside column default expressions
// pointing at the pre-alter column positions. The rewrite path hands us rows
// that the engine has already projected into the new schema, so ADD COLUMN ...
// FIRST with a default that references another column lands on the right value.
func (t *Table) ShouldRewriteTable(ctx *sql.Context, oldSchema, newSchema sql.PrimaryKeySchema, oldColumn, newColumn *sql.Column) bool {
	return true
}

// RewriteInserter implements sql.RewritableTable.
func (t *Table) RewriteInserter(
	ctx *sql.Context,
	oldSchema, newSchema sql.PrimaryKeySchema,
	oldColumn, newColumn *sql.Column,
	idxCols []sql.IndexColumn,
) (sql.RowInserter, error) {
	if len(oldSchema.PkOrdinals) > 0 && len(newSchema.PkOrdinals) == 0 {
		if err := sql.ValidatePrimaryKeyDrop(ctx, t, oldSchema); err != nil {
			return nil, err
		}
	}
	// A primary key column is never nullable, but MODIFY COLUMN does not restate
	// the key, so the new column description arrives with the DDL's (absent)
	// nullability rather than the one the key implies.
	newSchema.Schema = newSchema.Schema.Copy()
	for _, ord := range newSchema.PkOrdinals {
		if ord >= 0 && ord < len(newSchema.Schema) {
			newSchema.Schema[ord].PrimaryKey = true
			newSchema.Schema[ord].Nullable = false
		}
	}
	return &rewriter{table: t, schema: newSchema, oldColumn: oldColumn, newColumn: newColumn}, nil
}

// rewriter buffers the rows of a table rewrite. The table has to keep answering
// scans in the old schema while the rewrite runs, so nothing is written until
// Close.
type rewriter struct {
	table     *Table
	schema    sql.PrimaryKeySchema
	oldColumn *sql.Column
	newColumn *sql.Column
	rows      []sql.Row
	discarded bool
	closed    bool
}

var _ sql.RowInserter = (*rewriter)(nil)

func (r *rewriter) StatementBegin(*sql.Context) {}

func (r *rewriter) DiscardChanges(_ *sql.Context, _ error) error {
	r.discarded = true
	r.rows = nil
	return nil
}

func (r *rewriter) StatementComplete(*sql.Context) error { return nil }

func (r *rewriter) Insert(ctx *sql.Context, row sql.Row) error {
	if len(row) != len(r.schema.Schema) {
		return sql.ErrUnexpectedRowLength.New(len(r.schema.Schema), len(row))
	}
	r.rows = append(r.rows, row.Copy())
	return nil
}

func (r *rewriter) Close(ctx *sql.Context) error {
	if r.closed || r.discarded {
		return nil
	}
	r.closed = true
	if err := r.table.writeSchema(ctx, r.schema.Schema, r.schema.PkOrdinals, r.rows); err != nil {
		return err
	}
	if err := r.table.reconcileIndexes(ctx, r.oldColumn, r.newColumn); err != nil {
		return err
	}
	return r.table.rebuildFulltext(ctx)
}

// reconcileIndexes brings the stored index definitions back in line with the
// schema the rewrite just installed: a renamed column is renamed inside the
// definitions, and an index over a column that no longer exists is dropped.
// A dropped FULLTEXT index takes its pseudo-index tables with it.
func (t *Table) reconcileIndexes(ctx *sql.Context, oldColumn, newColumn *sql.Column) error {
	indexes, err := t.readIndexes()
	if err != nil || len(indexes) == 0 {
		return err
	}
	changed := false
	kept := make([]storedIndex, 0, len(indexes))
	var dropped []storedIndex
	for _, idx := range indexes {
		if sql.IndexConstraint(idx.Constraint) == sql.IndexConstraint_Fulltext {
			next, drop, idxChanged := trimFulltextColumns(idx, t.meta.schema, oldColumn, newColumn)
			if idxChanged {
				changed = true
			}
			if drop {
				dropped = append(dropped, idx)
				continue
			}
			kept = append(kept, next)
			continue
		}
		drop := false
		for i, name := range idx.Columns {
			if oldColumn != nil && newColumn != nil && strings.EqualFold(name, oldColumn.Name) {
				name = newColumn.Name
			}
			ord := columnOrdinal(t.meta.schema, name)
			if ord < 0 {
				drop = true
				break
			}
			if idx.Columns[i] != t.meta.schema[ord].Name {
				idx.Columns[i] = t.meta.schema[ord].Name
				changed = true
			}
		}
		if drop {
			changed = true
			continue
		}
		kept = append(kept, idx)
	}
	if !changed {
		return nil
	}
	if err := t.writeIndexes(kept); err != nil {
		return err
	}
	// writeSchema runs before the definitions are renamed, so an index on the
	// old column name was not rebuilt. Fill it now that the names match.
	if err := t.backfillIndexes(ctx, kept); err != nil {
		return err
	}
	return t.dropFulltextTables(ctx, dropped, kept)
}

func (t *Table) backfillIndexes(ctx *sql.Context, indexes []storedIndex) error {
	return t.store.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		for _, idx := range indexes {
			if !indexMaintained(idx) {
				continue
			}
			parent, err := indexParent(bucket)
			if err != nil {
				return err
			}
			if parent.Bucket([]byte(idx.Name)) != nil {
				if err := parent.DeleteBucket([]byte(idx.Name)); err != nil {
					return err
				}
			}
			if err := backfillIndexUnique(ctx, bucket, t.meta.schema, idx); err != nil {
				return err
			}
		}
		return nil
	})
}

// trimFulltextColumns drops index columns that the rewrite removed and renames
// the rest. The index itself is dropped only when every column is gone, which
// matches DropColumnFromTables.
func trimFulltextColumns(idx storedIndex, schema sql.Schema, oldColumn, newColumn *sql.Column) (storedIndex, bool, bool) {
	cols := make([]string, 0, len(idx.Columns))
	var lengths []uint16
	var descending []bool
	changed := false
	for i, name := range idx.Columns {
		if oldColumn != nil && newColumn != nil && strings.EqualFold(name, oldColumn.Name) {
			name = newColumn.Name
		}
		ord := columnOrdinal(schema, name)
		if ord < 0 {
			changed = true
			continue
		}
		canonical := schema[ord].Name
		if name != idx.Columns[i] || canonical != idx.Columns[i] {
			changed = true
		}
		cols = append(cols, canonical)
		if i < len(idx.Lengths) {
			lengths = append(lengths, idx.Lengths[i])
		}
		if i < len(idx.Descending) {
			descending = append(descending, idx.Descending[i])
		}
	}
	if len(cols) == 0 {
		return idx, true, true
	}
	if len(cols) != len(idx.Columns) {
		changed = true
	}
	idx.Columns = cols
	if len(idx.Lengths) > 0 {
		idx.Lengths = lengths
	}
	if len(idx.Descending) > 0 {
		idx.Descending = descending
	}
	return idx, false, changed
}
