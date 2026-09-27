package persist

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type storedIndex struct {
	Name       string          `json:"name"`
	Columns    []string        `json:"columns"`
	Lengths    []uint16        `json:"lengths,omitempty"`
	Descending []bool          `json:"descending,omitempty"`
	Constraint byte            `json:"constraint"`
	Comment    string          `json:"comment,omitempty"`
	Fulltext   *storedFulltext `json:"fulltext,omitempty"`
}

// storedFulltext names the pseudo-index tables for one FULLTEXT index and the
// parent key those tables use to point back at a row.
type storedFulltext struct {
	Config       string `json:"config"`
	Position     string `json:"position"`
	DocCount     string `json:"docCount"`
	GlobalCount  string `json:"globalCount"`
	RowCount     string `json:"rowCount"`
	KeyName      string `json:"keyName,omitempty"`
	KeyType      byte   `json:"keyType"`
	KeyPositions []int  `json:"keyPositions,omitempty"`
}

// Index is one secondary index. Lookups filter a full scan with the same range
// expression the in-memory tables use, so results stay correct without a
// separate index b-tree.
type Index struct {
	store      *Store
	db         string
	table      string
	name       string
	columns    []string
	lengths    []uint16
	descending []bool
	schema     sql.Schema
	unique     bool
	spatial    bool
	full       bool
	vector     bool
	comment    string
	ft         *storedFulltext
}

func (idx *Index) ID() string                                 { return idx.name }
func (idx *Index) Database() string                           { return idx.db }
func (idx *Index) Table() string                              { return idx.table }
func (idx *Index) IsUnique() bool                             { return idx.unique }
func (idx *Index) IsSpatial() bool                            { return idx.spatial }
func (idx *Index) IsFullText() bool                           { return idx.full }
func (idx *Index) IsVector() bool                             { return idx.vector }
func (idx *Index) Comment() string                            { return idx.comment }
func (idx *Index) IndexType() string                          { return "BTREE" }
func (idx *Index) IsGenerated() bool                          { return false }
func (idx *Index) CanSupport(*sql.Context, ...sql.Range) bool { return true }

// Cardinality is the distinct count from the last ANALYZE TABLE.
func (idx *Index) Cardinality(ctx *sql.Context) int64 {
	if idx.store == nil {
		return 0
	}
	stat, ok := idx.store.GetStats(ctx, sql.NewStatQualifier(idx.db, "", idx.table, idx.name), idx.columns)
	if !ok || stat == nil {
		return 0
	}
	return int64(stat.DistinctCount())
}
func (idx *Index) CanSupportOrderBy(sql.Expression) bool { return false }

// Order is ascending for a btree index with no DESC column. A descending
// column is reported by ColumnOrders, and Order stays unordered so a caller
// that only understands one direction does not assume ascending.
func (idx *Index) Order(*sql.Context) sql.IndexOrder {
	if idx.full || idx.spatial || idx.vector {
		return sql.IndexOrderNone
	}
	desc := false
	asc := false
	for _, down := range idx.descending {
		if down {
			desc = true
		} else {
			asc = true
		}
	}
	if desc && !asc {
		return sql.IndexOrderDesc
	}
	if desc {
		return sql.IndexOrderNone
	}
	return sql.IndexOrderAsc
}

func (idx *Index) Reversible(*sql.Context) bool {
	return !idx.full && !idx.spatial && !idx.vector
}

func (idx *Index) ColumnOrders(*sql.Context) []sql.IndexColumnOrder {
	if len(idx.descending) == 0 {
		return nil
	}
	orders := make([]sql.IndexColumnOrder, len(idx.descending))
	any := false
	for i, down := range idx.descending {
		orders[i].Descending = down
		if down {
			any = true
		}
	}
	if !any {
		return nil
	}
	return orders
}

func (idx *Index) Expressions() []string {
	exprs := make([]string, len(idx.columns))
	for i, name := range idx.columns {
		exprs[i] = idx.table + "." + name
	}
	return exprs
}

func (idx *Index) PrefixLengths() []uint16 {
	for _, length := range idx.lengths {
		if length != 0 {
			return idx.lengths
		}
	}
	return nil
}

func (idx *Index) ColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	return expressionTypes(idx.Expressions(), idx.columnTypes(idx.columns))
}

func (idx *Index) CoversColumns(cols []string) bool {
	for _, col := range cols {
		found := false
		for _, name := range idx.columns {
			if strings.EqualFold(col, name) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (idx *Index) ExtendedExpressions(ctx *sql.Context) []string {
	names := idx.extendedNames()
	exprs := make([]string, len(names))
	for i, name := range names {
		exprs[i] = idx.table + "." + name
	}
	return exprs
}

func (idx *Index) ExtendedColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	names := idx.extendedNames()
	return expressionTypes(idx.ExtendedExpressions(ctx), idx.columnTypes(names))
}

func (idx *Index) extendedNames() []string {
	names := append([]string(nil), idx.columns...)
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		seen[strings.ToLower(name)] = struct{}{}
	}
	for _, col := range idx.schema {
		if !col.PrimaryKey {
			continue
		}
		if _, ok := seen[strings.ToLower(col.Name)]; ok {
			continue
		}
		names = append(names, col.Name)
	}
	return names
}

func (idx *Index) columnTypes(names []string) []sql.Type {
	types := make([]sql.Type, len(names))
	for i, name := range names {
		for _, col := range idx.schema {
			if strings.EqualFold(col.Name, name) {
				types[i] = col.Type
				break
			}
		}
	}
	return types
}

func (idx *Index) exprs() []sql.Expression {
	names := idx.extendedNames()
	exprs := make([]sql.Expression, len(names))
	for i, name := range names {
		for ord, col := range idx.schema {
			if !strings.EqualFold(col.Name, name) {
				continue
			}
			exprs[i] = expression.NewGetFieldWithTable(ord, 0, col.Type, idx.db, idx.table, col.Name, col.Nullable)
			break
		}
	}
	return exprs
}

func expressionTypes(exprs []string, colTypes []sql.Type) []sql.ColumnExpressionType {
	out := make([]sql.ColumnExpressionType, len(exprs))
	for i, expr := range exprs {
		out[i] = sql.ColumnExpressionType{Expression: expr, Type: colTypes[i]}
	}
	return out
}

func (t *Table) indexFromStored(stored storedIndex) *Index {
	return &Index{
		store:      t.store,
		db:         t.dbName,
		table:      t.name,
		name:       stored.Name,
		columns:    append([]string(nil), stored.Columns...),
		lengths:    append([]uint16(nil), stored.Lengths...),
		descending: append([]bool(nil), stored.Descending...),
		schema:     t.meta.schema,
		unique:     sql.IndexConstraint(stored.Constraint) == sql.IndexConstraint_Unique,
		spatial:    sql.IndexConstraint(stored.Constraint) == sql.IndexConstraint_Spatial,
		full:       sql.IndexConstraint(stored.Constraint) == sql.IndexConstraint_Fulltext,
		vector:     sql.IndexConstraint(stored.Constraint) == sql.IndexConstraint_Vector,
		comment:    stored.Comment,
		ft:         copyStoredFulltext(stored.Fulltext),
	}
}

func copyStoredFulltext(ft *storedFulltext) *storedFulltext {
	if ft == nil {
		return nil
	}
	copied := *ft
	copied.KeyPositions = append([]int(nil), ft.KeyPositions...)
	return &copied
}

func (t *Table) primaryIndex() *Index {
	if len(t.meta.pk) == 0 {
		return nil
	}
	columns := make([]string, len(t.meta.pk))
	for i, ord := range t.meta.pk {
		columns[i] = t.meta.schema[ord].Name
	}
	return &Index{
		store:   t.store,
		db:      t.dbName,
		table:   t.name,
		name:    "PRIMARY",
		columns: columns,
		schema:  t.meta.schema,
		unique:  true,
	}
}

// GetIndexes implements sql.IndexAddressable.
func (t *Table) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	stored, err := t.readIndexes()
	if err != nil {
		return nil, err
	}
	// Secondary indexes are reported in name order, which is the order MySQL uses
	// for SHOW CREATE TABLE and information_schema.statistics. The primary key
	// always leads.
	sort.Slice(stored, func(i, j int) bool { return stored[i].Name < stored[j].Name })
	indexes := make([]sql.Index, 0, len(stored)+1)
	if primary := t.primaryIndex(); primary != nil {
		indexes = append(indexes, primary)
	}
	for _, idx := range stored {
		indexes = append(indexes, t.indexFromStored(idx))
	}
	return indexes, nil
}

// IndexedAccess implements sql.IndexAddressable.
func (t *Table) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return &indexedTable{Table: t, lookup: lookup}
}

// PreciseMatch implements sql.IndexAddressable.
func (t *Table) PreciseMatch() bool { return true }

// CreateIndex implements sql.IndexAlterableTable.
func (t *Table) CreateIndex(ctx *sql.Context, indexDef sql.IndexDef) error {
	if indexDef.Constraint == sql.IndexConstraint_Fulltext {
		return fmt.Errorf("persist: FULLTEXT indexes are created through CreateFulltextIndex")
	}
	return t.appendStoredIndex(ctx, indexDef, nil)
}

// appendStoredIndex writes one secondary index. ft is set only for a FULLTEXT
// index, which also carries the pseudo-table names and key columns.
func (t *Table) appendStoredIndex(ctx *sql.Context, indexDef sql.IndexDef, ft *storedFulltext) error {
	if indexDef.Name == "" {
		return fmt.Errorf("persist: index name is empty")
	}
	// An index over an expression is created in two steps: the engine first adds a
	// hidden generated column for the expression, then indexes it. This table
	// value predates that column, so its cached schema has to be reloaded.
	if err := t.refreshMeta(); err != nil {
		return err
	}
	indexes, err := t.readIndexes()
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		if strings.EqualFold(idx.Name, indexDef.Name) {
			return sql.ErrDuplicateKey.New(indexDef.Name)
		}
	}
	if ft != nil {
		for _, idx := range indexes {
			if idx.Fulltext == nil {
				continue
			}
			if idx.Fulltext.Config != ft.Config {
				return fmt.Errorf("Full-Text config table name has been changed from `%s` to `%s`", idx.Fulltext.Config, ft.Config)
			}
		}
	}
	stored := storedIndex{
		Name:       indexDef.Name,
		Columns:    make([]string, len(indexDef.Columns)),
		Lengths:    make([]uint16, len(indexDef.Columns)),
		Constraint: byte(indexDef.Constraint),
		Comment:    indexDef.Comment,
	}
	descending := make([]bool, len(indexDef.Columns))
	anyDescending := false
	for i, col := range indexDef.Columns {
		if col.Name == "" {
			return fmt.Errorf("persist: index %s column %d has no name", indexDef.Name, i)
		}
		ord := columnOrdinal(t.meta.schema, col.Name)
		if ord < 0 {
			return sql.ErrColumnNotFound.New(col.Name)
		}
		// Store the column's own spelling rather than the one the statement used:
		// index expressions are matched against "table.column" by exact string
		// comparison when the schema is displayed.
		stored.Columns[i] = t.meta.schema[ord].Name
		if col.Length > 0 && col.Length <= math.MaxUint16 {
			stored.Lengths[i] = uint16(col.Length)
		}
		if col.Order != nil && col.Order.Descending {
			descending[i] = true
			anyDescending = true
		}
	}
	if anyDescending {
		stored.Descending = descending
	}
	if ft != nil {
		stored.Fulltext = copyStoredFulltext(ft)
	}
	indexes = append(indexes, stored)
	raw, err := json.Marshal(indexes)
	if err != nil {
		return err
	}
	return t.store.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		if err := bucket.Put(keyIndexes, raw); err != nil {
			return err
		}
		if err := syncIndexBuckets(bucket, indexes); err != nil {
			return err
		}
		if !indexMaintained(stored) {
			return nil
		}
		return backfillIndexUnique(ctx, bucket, t.meta.schema, stored)
	})
}

// DropIndex implements sql.IndexAlterableTable.
func (t *Table) DropIndex(ctx *sql.Context, indexName string) error {
	indexes, err := t.readIndexes()
	if err != nil {
		return err
	}
	next := indexes[:0]
	found := false
	for _, idx := range indexes {
		if strings.EqualFold(idx.Name, indexName) {
			found = true
			continue
		}
		next = append(next, idx)
	}
	if !found {
		return sql.ErrIndexNotFound.New(indexName)
	}
	return t.writeIndexes(next)
}

// RenameIndex implements sql.IndexAlterableTable.
func (t *Table) RenameIndex(ctx *sql.Context, fromIndexName, toIndexName string) error {
	if strings.EqualFold(fromIndexName, toIndexName) {
		return nil
	}
	indexes, err := t.readIndexes()
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		if strings.EqualFold(idx.Name, toIndexName) {
			return sql.ErrDuplicateKey.New(toIndexName)
		}
	}
	found := false
	for i, idx := range indexes {
		if strings.EqualFold(idx.Name, fromIndexName) {
			indexes[i].Name = toIndexName
			found = true
			break
		}
	}
	if !found {
		return sql.ErrIndexNotFound.New(fromIndexName)
	}
	raw, err := json.Marshal(indexes)
	if err != nil {
		return err
	}
	return t.store.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		if err := renameIndexBucket(bucket, fromIndexName, toIndexName); err != nil {
			return err
		}
		return bucket.Put(keyIndexes, raw)
	})
}

func (t *Table) readIndexes() ([]storedIndex, error) {
	var raw []byte
	err := t.store.view(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		raw = append([]byte(nil), bucket.Get(keyIndexes)...)
		return nil
	})
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var indexes []storedIndex
	if err := json.Unmarshal(raw, &indexes); err != nil {
		return nil, err
	}
	return indexes, nil
}

func (t *Table) writeIndexes(indexes []storedIndex) error {
	raw, err := json.Marshal(indexes)
	if err != nil {
		return err
	}
	return t.store.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		if err := bucket.Put(keyIndexes, raw); err != nil {
			return err
		}
		return syncIndexBuckets(bucket, indexes)
	})
}

type indexedTable struct {
	*Table
	lookup sql.IndexLookup
	// editor is set when the lookup comes from an open editor. Its buffered edits
	// have to be visible here, because a self-referential foreign key checks its
	// parent row through this path while the statement that wrote that row is
	// still running. Sibling editors are included so a second writer on the same
	// table, such as INSERT ... ON DUPLICATE KEY UPDATE, can see those rows too.
	// A lookup that did not come from an editor stays on the committed view, so
	// an UPDATE join does not match a row it already changed.
	editor *editor
}

func (t *indexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	t.lookup = lookup
	return t.Partitions(ctx)
}

func (t *indexedTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	if t.lookup.IsEmptyRange {
		return sql.RowsToRowIter(), nil
	}
	idx, ok := t.lookup.Index.(*Index)
	ranges, rangesOK := t.lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok || !rangesOK || idx.full || idx.spatial || idx.vector {
		return t.filteredRows(ctx)
	}
	fields, err := t.lookupFields(idx)
	if err != nil {
		return nil, err
	}
	var opens []func() (sql.RowIter, error)
	for _, rang := range ranges {
		span, exact, empty, encoded, err := spanForRange(ctx, fields, rang)
		if err != nil {
			return nil, err
		}
		if !encoded {
			return t.filteredRows(ctx)
		}
		if empty {
			continue
		}
		sp, ex, one := span, exact, rang
		opens = append(opens, func() (sql.RowIter, error) {
			iter, err := t.openLookup(ctx, idx, fields, sp, ex)
			if err != nil || ex || spanCovers(one) {
				return iter, err
			}
			// The seek stops at the first ranged column. A constraint on a
			// later column is applied to the rows that seek returns.
			return t.restrict(ctx, iter, sql.MySQLRangeCollection{one})
		})
	}
	if len(opens) == 0 {
		return sql.RowsToRowIter(), nil
	}
	return &concatIter{open: opens}, nil
}

func (t *indexedTable) lookupFields(idx *Index) ([]indexField, error) {
	if idx.name == "PRIMARY" {
		return pkFields(t.meta.schema, t.meta.pk), nil
	}
	return indexFields(t.meta.schema, storedIndex{
		Name:       idx.name,
		Columns:    idx.columns,
		Lengths:    idx.lengths,
		Descending: idx.descending,
	})
}

func (t *indexedTable) openLookup(ctx *sql.Context, idx *Index, fields []indexField, span keySpan, exact bool) (sql.RowIter, error) {
	if exact && idx.name == "PRIMARY" {
		row, raw, ok, err := t.lookupImage(ctx, span.start, t.editor)
		if err != nil {
			return nil, err
		}
		if !ok {
			return sql.RowsToRowIter(), nil
		}
		if sess, ok := sessionFrom(ctx); ok {
			skip, err := sess.observeRow(ctx, t.ref(), span.start, raw)
			if err != nil {
				return nil, err
			}
			if skip {
				return sql.RowsToRowIter(), nil
			}
		}
		return &rowIter{rows: []sql.Row{row}}, nil
	}
	if exact && idx.unique {
		return t.secondaryPoint(ctx, idx, fields, span.start)
	}
	edits := t.editsFor(ctx, t.editor, nil)
	if idx.name == "PRIMARY" {
		var base context.Context
		if ctx != nil {
			base = ctx
		} else {
			base = context.Background()
		}
		return t.store.mergeRows(base, t.Table, t.editsFor(ctx, t.editor, nil), span, t.lookup.IsReverse)
	}
	return t.store.openIndexIter(ctx, t.Table, idx.name, fields, idx.unique, edits, span, t.lookup.IsReverse)
}

func (t *indexedTable) secondaryPoint(ctx *sql.Context, idx *Index, fields []indexField, colKey []byte) (sql.RowIter, error) {
	var rows []sql.Row
	seen := make(map[string]struct{})
	for _, entry := range buildOverlay(t.editsFor(ctx, t.editor, nil)) {
		seen[string(entry.key)] = struct{}{}
		if entry.tomb {
			continue
		}
		key, hasNull, err := encodeIndexColumns(ctx, fields, entry.row)
		if err != nil {
			return nil, err
		}
		if hasNull || !bytesEqual(key, colKey) {
			continue
		}
		rows = append(rows, entry.row)
	}
	pk, err := t.store.indexGet(ctx, t.Table, idx.name, colKey)
	if err != nil {
		return nil, err
	}
	if len(pk) > 0 {
		if _, ok := seen[string(pk)]; !ok {
			row, raw, ok, err := t.lookupImage(ctx, pk, t.editor)
			if err != nil {
				return nil, err
			}
			if ok {
				if sess, ok := sessionFrom(ctx); ok {
					skip, err := sess.observeRow(ctx, t.ref(), pk, raw)
					if err != nil {
						return nil, err
					}
					if skip {
						return &rowIter{rows: rows}, nil
					}
				}
				rows = append(rows, row)
			}
		}
	}
	return &rowIter{rows: rows}, nil
}

func (t *indexedTable) restrict(ctx *sql.Context, iter sql.RowIter, ranges sql.MySQLRangeCollection) (sql.RowIter, error) {
	idx, ok := t.lookup.Index.(*Index)
	if !ok {
		return iter, nil
	}
	filter, err := expression.NewRangeFilterExpr(ctx, idx.exprs(), ranges)
	if err != nil {
		_ = iter.Close(ctx)
		return nil, err
	}
	if filter == nil {
		return iter, nil
	}
	return &filterIter{iter: iter, filter: filter}, nil
}

func (t *indexedTable) filteredRows(ctx *sql.Context) (sql.RowIter, error) {
	var extra []edit
	if t.editor != nil {
		extra = append(extra, t.editor.edits...)
		if sess, ok := sessionFrom(ctx); ok {
			extra = append(extra, sess.openEditsExcept(t.ref(), t.editor)...)
		}
	}
	iter, err := t.streamRows(ctx, extra, keySpan{}, false)
	if err != nil {
		return nil, err
	}
	if t.lookup.IsEmptyRange {
		_ = iter.Close(ctx)
		return sql.RowsToRowIter(), nil
	}
	ranges, ok := t.lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok {
		_ = iter.Close(ctx)
		return nil, fmt.Errorf("persist: index lookup ranges are %T", t.lookup.Ranges)
	}
	idx, ok := t.lookup.Index.(*Index)
	if !ok {
		return iter, nil
	}
	filter, err := expression.NewRangeFilterExpr(ctx, idx.exprs(), ranges)
	if err != nil {
		_ = iter.Close(ctx)
		return nil, err
	}
	if filter == nil {
		return iter, nil
	}
	return &filterIter{iter: iter, filter: filter}, nil
}

type concatIter struct {
	open []func() (sql.RowIter, error)
	cur  sql.RowIter
	n    int
}

func (it *concatIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if it.cur == nil {
			if it.n >= len(it.open) {
				return nil, io.EOF
			}
			next, err := it.open[it.n]()
			it.n++
			if err != nil {
				return nil, err
			}
			it.cur = next
		}
		row, err := it.cur.Next(ctx)
		if err == io.EOF {
			if err := it.cur.Close(ctx); err != nil {
				return nil, err
			}
			it.cur = nil
			continue
		}
		return row, err
	}
}

func (it *concatIter) Close(ctx *sql.Context) error {
	if it.cur == nil {
		return nil
	}
	err := it.cur.Close(ctx)
	it.cur = nil
	return err
}

type filterIter struct {
	iter   sql.RowIter
	filter sql.Expression
}

func (it *filterIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		row, err := it.iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		ok, err := it.filter.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if sql.IsTrue(ok) {
			return row, nil
		}
	}
}

func (it *filterIter) Close(ctx *sql.Context) error {
	return it.iter.Close(ctx)
}

// GetNextAutoIncrementValue implements sql.AutoIncrementTable.
// A nil insertVal claims the current counter and advances it before returning,
// so the next caller cannot receive the same value. An explicit value greater
// than the counter raises the counter to that value and does not step past it.
// Values outside the column type are ignored so a rejected insert does not
// move the sequence. The engine reports that rejection itself.
func (t *Table) GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (uint64, error) {
	if insertVal == nil {
		return t.store.allocAutoIncrement(ctx, t)
	}
	col := autoIncrementColumn(t.meta.schema)
	if col == nil {
		return t.store.autoIncrement(t)
	}
	if _, inRange, convErr := col.Type.Convert(ctx, insertVal); convErr != nil || inRange != sql.InRange {
		return t.store.autoIncrement(t)
	}
	current, err := t.store.autoIncrement(t)
	if err != nil {
		return 0, err
	}
	cmp, err := col.Type.Compare(ctx, insertVal, current)
	if err != nil || cmp <= 0 {
		return current, err
	}
	converted, _, err := types.Uint64.Convert(ctx, insertVal)
	if err != nil {
		return current, nil
	}
	return t.store.raiseAutoIncrement(t, converted.(uint64))
}

// PeekNextAutoIncrementValue implements sql.AutoIncrementGetter.
func (t *Table) PeekNextAutoIncrementValue(ctx *sql.Context) (uint64, error) {
	current, err := t.store.autoIncrement(t)
	if err != nil {
		return 0, err
	}
	col := autoIncrementColumn(t.meta.schema)
	if col == nil {
		return current, nil
	}
	if _, inRange, convErr := col.Type.Convert(ctx, current); convErr == nil && inRange != sql.InRange && current > 0 {
		return current - 1, nil
	}
	return current, nil
}

func autoIncrementColumn(schema sql.Schema) *sql.Column {
	ord := autoIncrementOrdinal(schema)
	if ord < 0 {
		return nil
	}
	return schema[ord]
}

// AutoIncrementSetter implements sql.AutoIncrementTable.
func (t *Table) AutoIncrementSetter(ctx *sql.Context) sql.AutoIncrementSetter {
	return t.newEditor()
}

func (e *editor) SetAutoIncrementValue(ctx *sql.Context, val uint64) error {
	return e.table.store.setAutoIncrement(e.table, val)
}

func (e *editor) AcquireAutoIncrementLock(ctx *sql.Context) (func(), error) {
	return func() {}, nil
}

func (e *editor) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return &indexedTable{Table: e.table, lookup: lookup, editor: e}
}

func (e *editor) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return e.table.GetIndexes(ctx)
}

func (e *editor) PreciseMatch() bool { return true }

// autoIncrementStep is how many ids one durable counter update reserves.
// The rest are handed out from memory. A crash or a new leader continues
// from the stored high-water mark, so unused ids in the span are skipped.
const autoIncrementStep = 1000

// autoRange is a span of auto-increment ids reserved by one durable commit.
// next is the next id to hand out. end is the stored high-water mark.
type autoRange struct {
	next uint64
	end  uint64
}

func (s *Store) autoIncrement(t *Table) (uint64, error) {
	s.autoMu.Lock()
	defer s.autoMu.Unlock()
	if next, _, ok := s.openAutoRange(t.ref()); ok {
		return next, nil
	}
	return s.storedAutoIncrement(t)
}

func (s *Store) storedAutoIncrement(t *Table) (uint64, error) {
	current := uint64(1)
	err := s.view(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		raw := bucket.Get(keyAutoInc)
		if len(raw) == 8 {
			current = binary.BigEndian.Uint64(raw)
		}
		return nil
	})
	return current, err
}

func (s *Store) setAutoIncrement(t *Table, val uint64) error {
	s.autoMu.Lock()
	defer s.autoMu.Unlock()
	if err := s.writeAutoIncrement(t, val); err != nil {
		return err
	}
	s.deleteAutoRange(t.ref())
	return nil
}

// allocAutoIncrement claims the next id. The mutex is held across a refill
// commit so two sessions cannot reserve the same span. A span is installed
// only when leadership has not changed since the commit started.
func (s *Store) allocAutoIncrement(ctx *sql.Context, t *Table) (uint64, error) {
	s.autoMu.Lock()
	defer s.autoMu.Unlock()
	ref := t.ref()
	if r, ok := s.autoRanges[ref]; ok && r.next < r.end {
		issued := r.next
		r.next++
		if r.next < r.end {
			s.autoRanges[ref] = r
		} else {
			delete(s.autoRanges, ref)
		}
		return issued, nil
	}
	epoch := s.autoEpoch
	col := autoIncrementColumn(t.meta.schema)
	var issued, end uint64
	err := s.update(func(tx *kvTx) error {
		bucket, current, err := autoIncrementIn(tx, t)
		if err != nil {
			return err
		}
		issued = current
		end = advanceAutoIncrement(ctx, col, current, autoIncrementStep)
		if end == current {
			return nil
		}
		return putAutoIncrement(bucket, end)
	})
	if err != nil {
		return 0, err
	}
	if end > issued && s.autoEpoch == epoch {
		next := issued
		bumpAutoIncrement(ctx, col, &next)
		if next < end {
			s.setAutoRange(ref, autoRange{next: next, end: end})
		}
	}
	return issued, nil
}

// raiseAutoIncrement stores explicit when it is greater than the counter.
// The stored value is the explicit value itself. Insert then steps past it.
// An explicit id inside an open span only moves the in-memory cursor.
func (s *Store) raiseAutoIncrement(t *Table, explicit uint64) (uint64, error) {
	s.autoMu.Lock()
	defer s.autoMu.Unlock()
	ref := t.ref()
	if next, end, ok := s.openAutoRange(ref); ok {
		if explicit <= next {
			return next, nil
		}
		if explicit < end {
			s.setAutoRange(ref, autoRange{next: explicit, end: end})
			return explicit, nil
		}
	} else {
		current, err := s.storedAutoIncrement(t)
		if err != nil {
			return 0, err
		}
		if explicit <= current {
			return current, nil
		}
	}
	if err := s.writeAutoIncrement(t, explicit); err != nil {
		return 0, err
	}
	s.deleteAutoRange(ref)
	return explicit, nil
}

func (s *Store) writeAutoIncrement(t *Table, val uint64) error {
	return s.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		return putAutoIncrement(bucket, val)
	})
}

func autoIncrementIn(tx *kvTx, t *Table) (*kvBucket, uint64, error) {
	bucket := tableBucket(tx, t.dbName, t.name)
	if bucket == nil {
		return nil, 0, sql.ErrTableNotFound.New(t.name)
	}
	current := uint64(1)
	if raw := bucket.Get(keyAutoInc); len(raw) == 8 {
		current = binary.BigEndian.Uint64(raw)
	}
	return bucket, current, nil
}

func putAutoIncrement(bucket *kvBucket, val uint64) error {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], val)
	return bucket.Put(keyAutoInc, raw[:])
}

func (e *editor) noteAutoIncrement(ctx *sql.Context, row sql.Row) error {
	ord := autoIncrementOrdinal(e.meta.schema)
	if ord < 0 || ord >= len(row) || row[ord] == nil {
		return nil
	}
	col := e.meta.schema[ord]
	if _, inRange, convErr := col.Type.Convert(ctx, row[ord]); convErr != nil || inRange != sql.InRange {
		return nil
	}
	current, err := e.table.store.autoIncrement(e.table)
	if err != nil {
		return err
	}
	cmp, err := col.Type.Compare(ctx, row[ord], current)
	if err != nil || cmp < 0 {
		return err
	}
	converted, _, err := types.Uint64.Convert(ctx, row[ord])
	if err != nil {
		return err
	}
	return e.table.store.observeAutoIncrement(ctx, e.table, converted.(uint64))
}

// observeAutoIncrement steps the counter past value when value has reached it.
// A generated value already claimed by allocAutoIncrement is left alone.
// A step that stays inside an open span is memory only.
func (s *Store) observeAutoIncrement(ctx *sql.Context, t *Table, value uint64) error {
	s.autoMu.Lock()
	defer s.autoMu.Unlock()
	ref := t.ref()
	col := autoIncrementColumn(t.meta.schema)
	if next, end, ok := s.openAutoRange(ref); ok {
		if value < next {
			return nil
		}
		current := next
		if value > current {
			current = value
		}
		bumpAutoIncrement(ctx, col, &current)
		if current <= end {
			if current < end {
				s.setAutoRange(ref, autoRange{next: current, end: end})
			} else {
				s.deleteAutoRange(ref)
			}
			return nil
		}
		if err := s.writeAutoIncrement(t, current); err != nil {
			return err
		}
		s.deleteAutoRange(ref)
		return nil
	}
	return s.update(func(tx *kvTx) error {
		bucket, current, err := autoIncrementIn(tx, t)
		if err != nil {
			return err
		}
		if value < current {
			return nil
		}
		if value > current {
			current = value
		}
		bumpAutoIncrement(ctx, col, &current)
		return putAutoIncrement(bucket, current)
	})
}

// clearAutoRanges drops every span and bumps the epoch so a refill that
// committed across the change cannot install the old span.
func (s *Store) clearAutoRanges() {
	s.autoMu.Lock()
	s.autoEpoch++
	s.autoRanges = nil
	s.autoMu.Unlock()
}

func (s *Store) forgetAutoIncrement(ref tableRef) {
	s.autoMu.Lock()
	delete(s.autoRanges, ref)
	s.autoMu.Unlock()
}

func (s *Store) forgetAutoDatabase(db string) {
	db = strings.ToLower(db)
	s.autoMu.Lock()
	for ref := range s.autoRanges {
		if ref.db == db {
			delete(s.autoRanges, ref)
		}
	}
	s.autoMu.Unlock()
}

func (s *Store) openAutoRange(ref tableRef) (next, end uint64, ok bool) {
	r, found := s.autoRanges[ref]
	if !found || r.next >= r.end {
		return 0, 0, false
	}
	return r.next, r.end, true
}

func (s *Store) setAutoRange(ref tableRef, r autoRange) {
	if s.autoRanges == nil {
		s.autoRanges = make(map[tableRef]autoRange)
	}
	s.autoRanges[ref] = r
}

func (s *Store) deleteAutoRange(ref tableRef) {
	delete(s.autoRanges, ref)
}

func advanceAutoIncrement(ctx *sql.Context, col *sql.Column, current uint64, steps int) uint64 {
	end := current
	for i := 0; i < steps; i++ {
		next := end
		bumpAutoIncrement(ctx, col, &next)
		if next == end {
			break
		}
		end = next
	}
	return end
}

func bumpAutoIncrement(ctx *sql.Context, col *sql.Column, value *uint64) {
	if *value == math.MaxUint64 {
		return
	}
	next := *value + 1
	if col == nil {
		*value = next
		return
	}
	if _, inRange, err := col.Type.Convert(ctx, next); err == nil && inRange == sql.InRange {
		*value = next
	}
}

func autoIncrementOrdinal(schema sql.Schema) int {
	for i, col := range schema {
		if col.AutoIncrement {
			return i
		}
	}
	return -1
}

func (e *editor) checkUniqueIndexes(ctx *sql.Context, row sql.Row, skip sql.Row) error {
	indexes, err := e.table.readIndexes()
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		if !indexIsUnique(idx) || !indexMaintained(idx) {
			continue
		}
		hit, existing, err := e.uniqueHit(ctx, idx, row, skip)
		if err != nil {
			return err
		}
		if hit {
			return sql.NewUniqueKeyErr(idx.Name, false, existing)
		}
	}
	return nil
}

func indexColumnsMatch(idx storedIndex, columns []string) bool {
	if len(idx.Columns) != len(columns) {
		return false
	}
	for i, name := range idx.Columns {
		if !strings.EqualFold(name, columns[i]) {
			return false
		}
	}
	return true
}

func indexRowsConflict(ctx *sql.Context, schema sql.Schema, idx storedIndex, existing, row sql.Row) (bool, error) {
	for i, name := range idx.Columns {
		ord := columnOrdinal(schema, name)
		if ord < 0 || ord >= len(existing) || ord >= len(row) {
			return false, fmt.Errorf("persist: index %s column %s is not in the schema", idx.Name, name)
		}
		if existing[ord] == nil || row[ord] == nil {
			return false, nil
		}
		left := existing[ord]
		right := row[ord]
		if i < len(idx.Lengths) && idx.Lengths[i] > 0 {
			left = prefixValue(left, idx.Lengths[i])
			right = prefixValue(right, idx.Lengths[i])
		}
		cmp, err := schema[ord].Type.Compare(ctx, left, right)
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func prefixValue(value interface{}, length uint16) interface{} {
	switch v := value.(type) {
	case string:
		if len(v) > int(length) {
			return v[:length]
		}
	case []byte:
		if len(v) > int(length) {
			return v[:length]
		}
	}
	return value
}

func columnOrdinal(schema sql.Schema, name string) int {
	for i, col := range schema {
		if strings.EqualFold(col.Name, name) {
			return i
		}
	}
	return -1
}

// CreateIndexForForeignKey implements sql.ForeignKeyTable.
func (t *Table) CreateIndexForForeignKey(ctx *sql.Context, indexDef sql.IndexDef) error {
	return t.CreateIndex(ctx, indexDef)
}

// GetDeclaredForeignKeys implements sql.ForeignKeyTable.
func (t *Table) GetDeclaredForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	keys, err := t.database().readForeignKeys()
	if err != nil {
		return nil, err
	}
	var declared []sql.ForeignKeyConstraint
	for _, key := range keys {
		if strings.EqualFold(key.Table, t.name) {
			declared = append(declared, key)
		}
	}
	sort.Slice(declared, func(i, j int) bool { return declared[i].Name < declared[j].Name })
	return declared, nil
}

// GetReferencedForeignKeys implements sql.ForeignKeyTable.
func (t *Table) GetReferencedForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	keys, err := t.database().readForeignKeys()
	if err != nil {
		return nil, err
	}
	var referenced []sql.ForeignKeyConstraint
	for _, key := range keys {
		if strings.EqualFold(key.ParentTable, t.name) {
			referenced = append(referenced, key)
		}
	}
	sort.Slice(referenced, func(i, j int) bool { return referenced[i].Name < referenced[j].Name })
	return referenced, nil
}

// AddForeignKey implements sql.ForeignKeyTable.
func (t *Table) AddForeignKey(ctx *sql.Context, fk sql.ForeignKeyConstraint) error {
	db := t.database()
	keys, err := db.readForeignKeys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if strings.EqualFold(key.Name, fk.Name) {
			return sql.ErrForeignKeyDuplicateName.New(fk.Name)
		}
	}
	keys = append(keys, fk)
	return db.writeForeignKeys(keys)
}

// DropForeignKey implements sql.ForeignKeyTable.
func (t *Table) DropForeignKey(ctx *sql.Context, fkName string, tableName string, schemaName string) error {
	db := t.database()
	keys, err := db.readForeignKeys()
	if err != nil {
		return err
	}
	next := keys[:0]
	found := false
	for _, key := range keys {
		if strings.EqualFold(key.Name, fkName) && (tableName == "" || strings.EqualFold(key.Table, tableName)) {
			found = true
			continue
		}
		next = append(next, key)
	}
	if !found {
		return sql.ErrForeignKeyNotFound.New(fkName, t.name)
	}
	return db.writeForeignKeys(next)
}

// UpdateForeignKey implements sql.ForeignKeyTable.
func (t *Table) UpdateForeignKey(ctx *sql.Context, fkName string, fk sql.ForeignKeyConstraint) error {
	db := t.database()
	keys, err := db.readForeignKeys()
	if err != nil {
		return err
	}
	found := false
	for i, key := range keys {
		if strings.EqualFold(key.Name, fkName) {
			keys[i] = fk
			found = true
			break
		}
	}
	if !found {
		return sql.ErrForeignKeyNotFound.New(fkName, t.name)
	}
	return db.writeForeignKeys(keys)
}

// GetForeignKeyEditor implements sql.ForeignKeyTable.
func (t *Table) GetForeignKeyEditor(ctx *sql.Context) sql.ForeignKeyEditor {
	return t.mustWriteEditor(ctx).(sql.ForeignKeyEditor)
}

func (t *Table) database() *Database {
	return &Database{store: t.store, name: t.dbName}
}

// RowCount implements sql.StatisticsTable. The count includes uncommitted edits.
func (t *Table) RowCount(ctx *sql.Context) (uint64, bool, error) {
	base, err := t.stat(ctx, keyRowCount)
	if err != nil {
		return 0, false, err
	}
	delta, _, err := t.pendingDelta(ctx, t.editsFor(ctx, nil, nil))
	if err != nil {
		return 0, false, err
	}
	return addUint(base, delta), true, nil
}

// DataLength implements sql.StatisticsTable.
func (t *Table) DataLength(ctx *sql.Context) (uint64, error) {
	base, err := t.stat(ctx, keyDataBytes)
	if err != nil {
		return 0, err
	}
	_, delta, err := t.pendingDelta(ctx, t.editsFor(ctx, nil, nil))
	if err != nil {
		return 0, err
	}
	return addUint(base, delta), nil
}

func (t *Table) stat(ctx *sql.Context, key []byte) (uint64, error) {
	var value uint64
	var ok bool
	err := t.store.view(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		value, ok = getUint64(bucket, key)
		if ok {
			return nil
		}
		rows := bucket.Bucket(bucketRows)
		count, nbytes, err := countRows(rows)
		if err != nil {
			return err
		}
		if bytesEqual(key, keyRowCount) {
			value = count
		} else {
			value = nbytes
		}
		return nil
	})
	if err != nil || ok {
		return value, err
	}
	// Remember the count so the next read is a point get. Writers are serialized.
	err = t.store.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		if _, exists := getUint64(bucket, key); exists {
			return nil
		}
		rows := bucket.Bucket(bucketRows)
		count, nbytes, err := countRows(rows)
		if err != nil {
			return err
		}
		if err := putUint64(bucket, keyRowCount, count); err != nil {
			return err
		}
		return putUint64(bucket, keyDataBytes, nbytes)
	})
	return value, err
}

func (t *Table) pendingDelta(ctx *sql.Context, edits []edit) (int64, int64, error) {
	overlay := buildOverlay(edits)
	if len(overlay) == 0 {
		return 0, 0, nil
	}
	var count, nbytes int64
	err := t.store.view(func(tx *kvTx) error {
		rows := rowsBucket(tx, t.dbName, t.name)
		if rows == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		for _, entry := range overlay {
			raw := rows.GetRaw(entry.key)
			onDisk := len(raw) > 0
			if entry.tomb {
				if onDisk {
					count--
					nbytes -= int64(len(raw))
				}
				continue
			}
			encoded, err := encodeRow(ctx, t.meta.schema, entry.row)
			if err != nil {
				return err
			}
			if onDisk {
				nbytes += int64(len(encoded)) - int64(len(raw))
				continue
			}
			count++
			nbytes += int64(len(encoded))
		}
		return nil
	})
	return count, nbytes, err
}

func addUint(base uint64, delta int64) uint64 {
	if delta >= 0 {
		return base + uint64(delta)
	}
	sub := uint64(-delta)
	if sub > base {
		return 0
	}
	return base - sub
}
