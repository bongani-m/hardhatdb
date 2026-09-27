package persist

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// Table is one persisted MySQL table. A primary-key equality reads one row.
// Other scans walk the rows bucket in key order.
type Table struct {
	store  *Store
	dbName string
	name   string
	meta   tableMeta
}

var _ sql.Table = (*Table)(nil)
var _ sql.PrimaryKeyTable = (*Table)(nil)
var _ sql.InsertableTable = (*Table)(nil)
var _ sql.UpdatableTable = (*Table)(nil)
var _ sql.DeletableTable = (*Table)(nil)
var _ sql.TruncateableTable = (*Table)(nil)
var _ sql.CommentedTable = (*Table)(nil)
var _ sql.AutoIncrementTable = (*Table)(nil)
var _ sql.IndexAlterableTable = (*Table)(nil)
var _ sql.IndexAddressableTable = (*Table)(nil)
var _ sql.ForeignKeyTable = (*Table)(nil)
var _ sql.StatisticsTable = (*Table)(nil)
var _ sql.ReplaceableTable = (*Table)(nil)
var _ sql.AlterableTable = (*Table)(nil)
var _ sql.PrimaryKeyAlterableTable = (*Table)(nil)
var _ sql.CheckTable = (*Table)(nil)
var _ sql.CheckAlterableTable = (*Table)(nil)
var _ sql.TargetRowSizeTable = (*Table)(nil)
var _ sql.TargetRowSizeAlterableTable = (*Table)(nil)
var _ sql.ForeignKeyEditor = (*editor)(nil)
var _ sql.AutoIncrementSetter = (*editor)(nil)

type tableRef struct {
	db   string
	name string
}

func (t *Table) ref() tableRef {
	return tableRef{db: strings.ToLower(t.dbName), name: strings.ToLower(t.name)}
}

// Name implements sql.Nameable.
func (t *Table) Name() string { return t.name }

// String implements fmt.Stringer.
func (t *Table) String() string { return t.name }

// Schema implements sql.Table.
func (t *Table) Schema(*sql.Context) sql.Schema { return t.meta.schema }

// Collation implements sql.Table.
func (t *Table) Collation() sql.CollationID { return t.meta.collation }

// Comment implements sql.CommentedTable.
func (t *Table) Comment() string { return t.meta.comment }

// PrimaryKeySchema implements sql.PrimaryKeyTable.
func (t *Table) PrimaryKeySchema(*sql.Context) sql.PrimaryKeySchema {
	return sql.NewPrimaryKeySchema(t.meta.schema, t.meta.pk...)
}

// Partitions implements sql.Table. Every row lives in one partition.
func (t *Table) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(partition{key: []byte("rows")}), nil
}

// PartitionRows implements sql.Table.
func (t *Table) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return t.streamRows(ctx, nil, keySpan{}, false)
}

// Inserter implements sql.InsertableTable.
func (t *Table) Inserter(ctx *sql.Context) sql.RowInserter { return t.mustWriteEditor(ctx) }

// Updater implements sql.UpdatableTable.
func (t *Table) Updater(ctx *sql.Context) sql.RowUpdater { return t.mustWriteEditor(ctx) }

// Deleter implements sql.DeletableTable.
func (t *Table) Deleter(ctx *sql.Context) sql.RowDeleter { return t.mustWriteEditor(ctx) }

// Replacer implements sql.ReplaceableTable.
func (t *Table) Replacer(ctx *sql.Context) sql.RowReplacer { return t.mustWriteEditor(ctx) }

var _ sql.Lockable = (*Table)(nil)

// Lock implements sql.Lockable. LOCK TABLES takes the process-wide lock table
// at table granularity and holds it until UNLOCK TABLES or disconnect.
func (t *Table) Lock(ctx *sql.Context, write bool) error {
	sess, ok := sessionFrom(ctx)
	if !ok {
		return nil
	}
	return sess.lockTable(ctx, t.ref(), write)
}

// Unlock implements sql.Lockable.
func (t *Table) Unlock(ctx *sql.Context, _ uint32) error {
	sess, ok := sessionFrom(ctx)
	if !ok {
		return nil
	}
	sess.unlockTables()
	return nil
}

// Truncate implements sql.TruncateableTable. Like MySQL, truncate commits immediately.
func (t *Table) Truncate(ctx *sql.Context) (int, error) {
	if sess, ok := sessionFrom(ctx); ok {
		sess.clear(t.ref())
	}
	return t.store.truncate(t)
}

// refreshMeta reloads the stored schema. A table value carries the schema it was
// loaded with, which goes stale when one statement both changes the schema and
// then reads it back through a table resolved earlier.
func (t *Table) refreshMeta() error {
	meta, _, ok, err := t.store.loadTable(t.dbName, t.name)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrTableNotFound.New(t.name)
	}
	t.meta = meta
	return nil
}

func (t *Table) newEditor() *editor {
	return &editor{table: t, meta: t.meta}
}

// bind registers the editor so other open editors on this table can see its
// buffered rows before Close.
func (e *editor) bind(ctx *sql.Context) *editor {
	if sess, ok := sessionFrom(ctx); ok {
		sess.track(e)
	}
	return e
}

// visibleRows returns the committed rows overlaid with the session's
// uncommitted edits, plus any extra edits an open editor has buffered.
func (t *Table) visibleRows(ctx *sql.Context, extra ...edit) ([]sql.Row, error) {
	iter, err := t.streamRows(ctx, extra, keySpan{}, false)
	if err != nil {
		return nil, err
	}
	return sql.RowIterToRows(ctx, iter)
}

func (t *Table) streamRows(ctx *sql.Context, extra []edit, span keySpan, reverse bool) (sql.RowIter, error) {
	var base context.Context
	if ctx != nil {
		base = ctx
	} else {
		base = context.Background()
	}
	return t.store.mergeRows(base, t, t.editsFor(ctx, nil, extra), span, reverse)
}

// editsFor orders edits so the last one wins: session, then siblings, then
// this editor. Scans pass the editor's edits in extra instead.
func (t *Table) editsFor(ctx *sql.Context, self *editor, extra []edit) []edit {
	var out []edit
	if sess, ok := sessionFrom(ctx); ok {
		out = append(out, sess.edits(t.ref())...)
		if self != nil {
			out = append(out, sess.openEditsExcept(t.ref(), self)...)
		}
	}
	out = append(out, extra...)
	if self != nil {
		out = append(out, self.edits...)
	}
	return out
}

type partition struct {
	key []byte
}

func (p partition) Key() []byte { return p.key }

type rowIter struct {
	rows []sql.Row
	pos  int
}

func (it *rowIter) Next(*sql.Context) (sql.Row, error) {
	if it.pos >= len(it.rows) {
		return nil, io.EOF
	}
	row := it.rows[it.pos]
	it.pos++
	return row, nil
}

func (it *rowIter) Close(*sql.Context) error { return nil }

type editOp int

const (
	opInsert editOp = iota
	opUpdate
	opDelete
)

type edit struct {
	op     editOp
	key    []byte
	oldKey []byte
	row    sql.Row
	raw    []byte
	// expected is the on-disk row image this update or delete was based on.
	// Nil means the row existed only in this session's buffer, so apply does
	// not compare it with disk.
	expected []byte
}

// editor buffers one statement. Close writes it, unless the session is inside
// BEGIN, in which case CommitTransaction writes the whole transaction.
type editor struct {
	table     *Table
	meta      tableMeta
	edits     []edit
	mark      int
	discarded bool
	closed    bool
	nextProv  uint64
}

var _ sql.RowInserter = (*editor)(nil)
var _ sql.RowUpdater = (*editor)(nil)
var _ sql.RowDeleter = (*editor)(nil)
var _ sql.UniqueKeyConflictCheckingRowInserter = (*editor)(nil)

func (e *editor) StatementBegin(*sql.Context) { e.mark = len(e.edits) }

func (e *editor) DiscardChanges(_ *sql.Context, cause error) error {
	if _, ignore := cause.(sql.IgnorableError); ignore {
		e.edits = e.edits[:e.mark]
		return nil
	}
	e.discarded = true
	e.edits = nil
	return nil
}

func (e *editor) StatementComplete(*sql.Context) error { return nil }

func (e *editor) Close(ctx *sql.Context) error {
	if e.closed {
		return nil
	}
	e.closed = true
	if sess, ok := sessionFrom(ctx); ok {
		sess.untrack(e)
		if ctx == nil || !ctx.GetIgnoreAutoCommit() {
			defer sess.releaseLocks()
		}
	}
	edits := e.edits
	e.edits = nil
	if e.discarded || len(edits) == 0 {
		return nil
	}
	if ctx != nil && ctx.GetIgnoreAutoCommit() {
		if sess, ok := sessionFrom(ctx); ok {
			sess.add(e.table.ref(), edits)
			return nil
		}
	}
	statement := ""
	if ctx != nil {
		statement = ctx.Query()
	}
	if err := e.table.store.apply(e.table, edits, statement, sourceGTID(ctx)); err != nil {
		return err
	}
	if sess, ok := sessionFrom(ctx); ok {
		sess.refreshSnapshot()
	}
	return nil
}

func (e *editor) Insert(ctx *sql.Context, row sql.Row) error {
	if err := e.writable(ctx); err != nil {
		return err
	}
	if err := e.checkRow(ctx, row); err != nil {
		return err
	}
	row = row.Copy()
	key, err := e.insertKey(ctx, row)
	if err != nil {
		return err
	}
	if len(e.meta.pk) > 0 {
		if err := e.guard(ctx, key); err != nil {
			return err
		}
		existing, taken, err := e.occupiedPK(ctx, row, nil)
		if err != nil {
			return err
		}
		if taken {
			return sql.NewUniqueKeyErr(pkString(e.meta.pk, row), true, existing)
		}
	}
	if err := e.checkUniqueIndexes(ctx, row, nil); err != nil {
		return err
	}
	if err := e.noteAutoIncrement(ctx, row); err != nil {
		return err
	}
	raw, err := encodeRow(ctx, e.meta.schema, row)
	if err != nil {
		return err
	}
	e.edits = append(e.edits, edit{op: opInsert, key: key, row: row, raw: raw})
	return nil
}

func (e *editor) Update(ctx *sql.Context, oldRow, newRow sql.Row) error {
	if err := e.writable(ctx); err != nil {
		return err
	}
	if err := e.checkRow(ctx, newRow); err != nil {
		return err
	}
	oldKey, _, expected, err := e.locateLocked(ctx, oldRow)
	if err != nil {
		return err
	}
	newRow = newRow.Copy()
	var newKey []byte
	if len(e.meta.pk) == 0 {
		newKey = oldKey
	} else {
		newKey, err = primaryKey(ctx, e.meta.schema, e.meta.pk, newRow)
		if err != nil {
			return err
		}
		if !bytesEqual(oldKey, newKey) {
			if err := e.guard(ctx, newKey); err != nil {
				return err
			}
			existing, taken, err := e.occupiedPK(ctx, newRow, oldRow)
			if err != nil {
				return err
			}
			if taken {
				return sql.NewUniqueKeyErr(pkString(e.meta.pk, newRow), true, existing)
			}
		}
	}
	if err := e.checkUniqueIndexes(ctx, newRow, oldRow); err != nil {
		return err
	}
	if err := e.noteAutoIncrement(ctx, newRow); err != nil {
		return err
	}
	raw, err := encodeRow(ctx, e.meta.schema, newRow)
	if err != nil {
		return err
	}
	e.edits = append(e.edits, edit{op: opUpdate, key: newKey, oldKey: oldKey, row: newRow, raw: raw, expected: copyBytes(expected)})
	return nil
}

func (e *editor) Delete(ctx *sql.Context, row sql.Row) error {
	if err := e.writable(ctx); err != nil {
		return err
	}
	key, _, expected, err := e.locateLocked(ctx, row)
	if err != nil {
		return err
	}
	e.edits = append(e.edits, edit{op: opDelete, key: key, oldKey: key, expected: copyBytes(expected)})
	return nil
}

// HasUniqueKeyConflict implements sql.UniqueKeyConflictCheckingRowInserter.
func (e *editor) HasUniqueKeyConflict(ctx *sql.Context, row sql.Row, columns []string) (bool, error) {
	if len(e.meta.pk) > 0 && pkColumnsCovered(e.meta, columns) {
		key, err := primaryKey(ctx, e.meta.schema, e.meta.pk, row)
		if err != nil {
			return false, err
		}
		_, taken, err := e.lookup(ctx, key)
		return taken, err
	}
	indexes, err := e.table.readIndexes()
	if err != nil {
		return false, err
	}
	for _, idx := range indexes {
		if !indexIsUnique(idx) || !indexMaintained(idx) {
			continue
		}
		if len(columns) > 0 && !indexColumnsMatch(idx, columns) {
			continue
		}
		hit, _, err := e.uniqueHit(ctx, idx, row, nil)
		if err != nil || hit {
			return hit, err
		}
	}
	return false, nil
}

func (e *editor) pendingEdits(ctx *sql.Context) []edit {
	return e.table.editsFor(ctx, e, nil)
}

func (e *editor) uniqueHit(ctx *sql.Context, idx storedIndex, row, skip sql.Row) (bool, sql.Row, error) {
	fields, err := indexFields(e.meta.schema, idx)
	if err != nil {
		return false, nil, err
	}
	colKey, hasNull, err := encodeIndexColumns(ctx, fields, row)
	if err != nil || hasNull {
		return false, nil, err
	}
	for _, existing := range buildOverlay(e.pendingEdits(ctx)) {
		if existing.tomb {
			continue
		}
		if skip != nil {
			same, err := rowsEqual(ctx, e.meta.schema, existing.row, skip)
			if err != nil {
				return false, nil, err
			}
			if same {
				continue
			}
		}
		key, nulls, err := encodeIndexColumns(ctx, fields, existing.row)
		if err != nil {
			return false, nil, err
		}
		if nulls || !bytesEqual(key, colKey) {
			continue
		}
		return true, existing.row, nil
	}
	pk, err := e.table.store.indexGet(ctx, e.table, idx.Name, colKey)
	if err != nil || len(pk) == 0 {
		return false, nil, err
	}
	existing, ok, err := e.lookup(ctx, pk)
	if err != nil || !ok {
		return false, nil, err
	}
	if skip != nil {
		same, err := rowsEqual(ctx, e.meta.schema, existing, skip)
		if err != nil || same {
			return false, nil, err
		}
	}
	conflict, err := indexRowsConflict(ctx, e.meta.schema, idx, existing, row)
	if err != nil || !conflict {
		return false, nil, err
	}
	return true, existing, nil
}

// checkRow rejects a row the schema cannot hold. The engine converts values to
// the column types before handing them over, but a backend is still the last
// line of defence: without this an INSERT of bytes that are not valid for the
// column's character set would be stored verbatim.
func (e *editor) checkRow(ctx *sql.Context, row sql.Row) error {
	if len(row) != len(e.meta.schema) {
		return fmt.Errorf("persist: row has %d values for %d columns", len(row), len(e.meta.schema))
	}
	for i, value := range row {
		if !e.meta.schema[i].Check(ctx, value) {
			return sql.ErrInvalidType.New(value)
		}
	}
	return nil
}

func (e *editor) writable(ctx *sql.Context) error {
	if ctx == nil || ctx.GetTransaction() == nil {
		return nil
	}
	if ctx.GetTransaction().IsReadOnly() {
		return sql.ErrReadOnlyTransaction.New()
	}
	return nil
}

func (e *editor) insertKey(ctx *sql.Context, row sql.Row) ([]byte, error) {
	if len(e.meta.pk) == 0 {
		e.nextProv++
		return provisionalKey(e.nextProv), nil
	}
	return primaryKey(ctx, e.meta.schema, e.meta.pk, row)
}

// locateLocked locks the row, then reads the latest committed image so the
// edit applies to the current row rather than the transaction snapshot.
func (e *editor) locateLocked(ctx *sql.Context, row sql.Row) ([]byte, sql.Row, []byte, error) {
	if len(e.meta.pk) > 0 {
		key, err := primaryKey(ctx, e.meta.schema, e.meta.pk, row)
		if err != nil {
			return nil, nil, nil, err
		}
		if err := e.guard(ctx, key); err != nil {
			return nil, nil, nil, err
		}
		found, expected, ok, err := e.table.lookupLatest(ctx, key, e)
		if err != nil {
			return nil, nil, nil, err
		}
		if !ok {
			return nil, nil, nil, sql.ErrDeleteRowNotFound.New()
		}
		return key, found, expected, nil
	}
	key, found, expected, err := e.locate(ctx, row)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := e.guard(ctx, key); err != nil {
		return nil, nil, nil, err
	}
	return key, found, expected, nil
}

func (e *editor) guard(ctx *sql.Context, key []byte) error {
	sess, ok := sessionFrom(ctx)
	if !ok {
		return nil
	}
	return sess.lockExclusive(ctx, e.table.ref(), key)
}

func (e *editor) locate(ctx *sql.Context, row sql.Row) ([]byte, sql.Row, []byte, error) {
	if len(e.meta.pk) > 0 {
		key, err := primaryKey(ctx, e.meta.schema, e.meta.pk, row)
		if err != nil {
			return nil, nil, nil, err
		}
		found, expected, ok, err := e.table.lookupImage(ctx, key, e)
		if err != nil {
			return nil, nil, nil, err
		}
		if !ok {
			return nil, nil, nil, sql.ErrDeleteRowNotFound.New()
		}
		return key, found, expected, nil
	}
	rows, err := e.matchingRows(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, candidate := range rows {
		equal, err := rowsEqual(ctx, e.meta.schema, candidate.row, row)
		if err != nil {
			return nil, nil, nil, err
		}
		if equal {
			return candidate.key, candidate.row, candidate.raw, nil
		}
	}
	return nil, nil, nil, sql.ErrDeleteRowNotFound.New()
}

func (e *editor) lookup(ctx *sql.Context, key []byte) (sql.Row, bool, error) {
	return e.table.lookupKey(ctx, key, e)
}

// occupiedPK reports a row whose primary key compares equal, including a
// different spelling that shares a collation weight.
func (e *editor) occupiedPK(ctx *sql.Context, row, skip sql.Row) (sql.Row, bool, error) {
	if !pkUsesCollation(e.meta) {
		key, err := primaryKey(ctx, e.meta.schema, e.meta.pk, row)
		if err != nil {
			return nil, false, err
		}
		return e.lookupLatest(ctx, key)
	}
	prefix, err := pkSeekPrefix(ctx, e.meta, row)
	if err != nil {
		return nil, false, err
	}
	iter, err := e.table.store.mergeRows(ctx, e.table, e.pendingEdits(ctx), keySpan{start: prefix, end: prefixEnd(prefix)}, false)
	if err != nil {
		return nil, false, err
	}
	defer iter.Close(ctx)
	for {
		next, err := iter.Next(ctx)
		if err == io.EOF {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if skip != nil {
			same, err := rowsEqual(ctx, e.meta.schema, next, skip)
			if err != nil {
				return nil, false, err
			}
			if same {
				continue
			}
		}
		return next, true, nil
	}
}

func pkUsesCollation(meta tableMeta) bool {
	for _, ord := range meta.pk {
		if ord >= 0 && ord < len(meta.schema) && !byteOrderType(meta.schema[ord].Type) {
			return true
		}
	}
	return false
}

func pkSeekPrefix(ctx *sql.Context, meta tableMeta, row sql.Row) ([]byte, error) {
	var buf []byte
	for _, ord := range meta.pk {
		field := indexField{ordinal: ord, typ: meta.schema[ord].Type}
		var value interface{}
		if ord < len(row) {
			value = row[ord]
		}
		part, err := seekPart(ctx, field, value)
		if err != nil {
			return nil, err
		}
		buf = append(buf, part...)
	}
	return buf, nil
}

func (e *editor) lookupLatest(ctx *sql.Context, key []byte) (sql.Row, bool, error) {
	row, _, ok, err := e.table.lookupLatest(ctx, key, e)
	return row, ok, err
}

func (t *Table) lookupKey(ctx *sql.Context, key []byte, self *editor) (sql.Row, bool, error) {
	row, _, ok, err := t.lookupImage(ctx, key, self)
	return row, ok, err
}

// lookupImage returns the row and, when it was read from disk, the stored bytes.
// A row resolved from this session's buffer has a nil image.
func (t *Table) lookupImage(ctx *sql.Context, key []byte, self *editor) (sql.Row, []byte, bool, error) {
	if self != nil {
		if row, ok, decided := lookupEdits(self.edits, key); decided {
			return row, nil, ok, nil
		}
	}
	if sess, ok := sessionFrom(ctx); ok {
		if self != nil {
			if row, ok, decided := sess.lookupOpen(self, key); decided {
				return row, nil, ok, nil
			}
		}
		if row, ok, decided := lookupEdits(sess.edits(t.ref()), key); decided {
			return row, nil, ok, nil
		}
	}
	return t.store.getRowImage(ctx, t, key, wantsCurrentRead(ctx))
}

// lookupLatest is lookupImage against the latest commit, ignoring the snapshot.
func (t *Table) lookupLatest(ctx *sql.Context, key []byte, self *editor) (sql.Row, []byte, bool, error) {
	if self != nil {
		if row, ok, decided := lookupEdits(self.edits, key); decided {
			return row, nil, ok, nil
		}
	}
	if sess, ok := sessionFrom(ctx); ok {
		if self != nil {
			if row, ok, decided := sess.lookupOpen(self, key); decided {
				return row, nil, ok, nil
			}
		}
		if row, ok, decided := lookupEdits(sess.edits(t.ref()), key); decided {
			return row, nil, ok, nil
		}
	}
	return t.store.getRowImage(ctx, t, key, true)
}

func (e *editor) matchingRows(ctx *sql.Context) ([]storedRow, error) {
	var extra []edit
	if sess, ok := sessionFrom(ctx); ok {
		extra = sess.openEdits(e.table.ref())
	} else {
		extra = e.edits
	}
	iter, err := e.table.streamRows(ctx, extra, keySpan{}, false)
	if err != nil {
		return nil, err
	}
	defer iter.Close(ctx)
	merger, ok := iter.(*mergeIter)
	if !ok {
		return nil, fmt.Errorf("persist: scan is %T", iter)
	}
	var rows []storedRow
	for {
		row, err := merger.nextStored(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func lookupEdits(edits []edit, key []byte) (sql.Row, bool, bool) {
	for i := len(edits) - 1; i >= 0; i-- {
		ed := edits[i]
		if ed.op != opDelete && bytesEqual(ed.key, key) {
			return ed.row, true, true
		}
		if ed.op == opDelete && bytesEqual(ed.key, key) {
			return nil, false, true
		}
		if ed.op == opUpdate && bytesEqual(ed.oldKey, key) && !bytesEqual(ed.oldKey, ed.key) {
			return nil, false, true
		}
	}
	return nil, false, false
}

func mergeRows(base []storedRow, edits []edit) []sql.Row {
	merged := mergeStored(base, edits)
	rows := make([]sql.Row, 0, len(merged))
	for _, row := range merged {
		rows = append(rows, row.row.Copy())
	}
	return rows
}

func mergeStored(base []storedRow, edits []edit) []storedRow {
	index := make(map[string]int, len(base))
	rows := make([]storedRow, 0, len(base))
	for _, row := range base {
		index[string(row.key)] = len(rows)
		rows = append(rows, row)
	}
	for _, ed := range edits {
		switch ed.op {
		case opDelete:
			if i, ok := index[string(ed.key)]; ok {
				rows[i].row = nil
				delete(index, string(ed.key))
			}
		case opInsert, opUpdate:
			if ed.op == opUpdate && len(ed.oldKey) > 0 && !bytesEqual(ed.oldKey, ed.key) {
				if i, ok := index[string(ed.oldKey)]; ok {
					rows[i].row = nil
					delete(index, string(ed.oldKey))
				}
			}
			if i, ok := index[string(ed.key)]; ok {
				rows[i] = storedRow{key: ed.key, row: ed.row}
			} else {
				index[string(ed.key)] = len(rows)
				rows = append(rows, storedRow{key: ed.key, row: ed.row})
			}
		}
	}
	out := make([]storedRow, 0, len(rows))
	for _, row := range rows {
		if row.row != nil {
			out = append(out, row)
		}
	}
	return out
}

func rowsEqual(ctx *sql.Context, schema sql.Schema, left, right sql.Row) (bool, error) {
	if len(left) != len(right) || len(left) != len(schema) {
		return false, nil
	}
	for i, col := range schema {
		cmp, err := col.Type.Compare(ctx, left[i], right[i])
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func pkColumnsCovered(meta tableMeta, columns []string) bool {
	if len(columns) == 0 {
		return true
	}
	have := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		have[strings.ToLower(column)] = struct{}{}
	}
	for _, ord := range meta.pk {
		if _, ok := have[strings.ToLower(meta.schema[ord].Name)]; !ok {
			return false
		}
	}
	return true
}

func sessionFrom(ctx *sql.Context) (*Session, bool) {
	if ctx == nil || ctx.Session == nil {
		return nil, false
	}
	sess, ok := ctx.Session.(*Session)
	return sess, ok
}
