package persist

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/dgraph-io/badger/v4"

	"github.com/dolthub/go-mysql-server/sql"
)

const (
	edgeUnb = iota
	edgeNull
	edgeAfterNull
	edgeValue
)

type edge struct {
	kind  int
	value interface{}
	incl  bool
}

func rangeExprEdges(expr sql.MySQLRangeColumnExpr) (lo, hi edge, empty, ok bool) {
	switch expr.Type() {
	case sql.RangeType_Invalid:
		return edge{}, edge{}, false, false
	case sql.RangeType_Empty:
		return edge{}, edge{}, true, true
	}
	// IS NULL is BelowNull..AboveNull. A null equality keeps DESC inversion on
	// the null tag. Type() also reports OpenOpen for AboveNull..Below, which is
	// "x < v" and excludes NULL, so the cuts are read directly.
	if _, loNull := expr.LowerBound.(sql.BelowNull); loNull {
		if _, hiNull := expr.UpperBound.(sql.AboveNull); hiNull {
			return edge{kind: edgeNull, incl: true}, edge{kind: edgeNull, incl: true}, false, true
		}
	}
	lo, ok1 := cutToEdge(expr.LowerBound, true)
	hi, ok2 := cutToEdge(expr.UpperBound, false)
	if !ok1 || !ok2 {
		return edge{}, edge{}, false, false
	}
	return lo, hi, false, true
}

func cutToEdge(c sql.MySQLRangeCut, lower bool) (edge, bool) {
	switch c := c.(type) {
	case sql.Below:
		// The cut sits just under the key, so a lower bound includes it and an
		// upper bound stops before it.
		return edge{kind: edgeValue, value: c.Key, incl: lower}, true
	case sql.Above:
		return edge{kind: edgeValue, value: c.Key, incl: !lower}, true
	case sql.AboveAll:
		return edge{kind: edgeUnb}, true
	case sql.AboveNull:
		return edge{kind: edgeAfterNull}, true
	case sql.BelowNull:
		// Below NULL is the start of the keyspace. NULL itself sorts first.
		return edge{kind: edgeUnb}, true
	default:
		return edge{}, false
	}
}

func edgesEqual(ctx context.Context, typ sql.Type, lo, hi edge) (bool, error) {
	if lo.kind == edgeNull && hi.kind == edgeNull {
		return true, nil
	}
	if lo.kind != edgeValue || hi.kind != edgeValue || !lo.incl || !hi.incl {
		return false, nil
	}
	if typ == nil {
		return fmt.Sprint(lo.value) == fmt.Sprint(hi.value), nil
	}
	cmp, err := typ.Compare(ctx, lo.value, hi.value)
	if err != nil {
		// A value that does not fit the column, such as a varbinary literal
		// longer than the column, still compares equal to itself. The lookup
		// encodes that value and misses, which is an empty result.
		return sameBound(lo.value, hi.value), nil
	}
	return cmp == 0, nil
}

func byteOrderType(typ sql.Type) bool {
	with, ok := typ.(sql.TypeWithCollation)
	if !ok || typ == nil {
		return true
	}
	col := with.Collation()
	if col == sql.Collation_Unspecified {
		return true
	}
	return col.IsBinary()
}

func sameBound(a, b interface{}) bool {
	switch a := a.(type) {
	case []byte:
		switch b := b.(type) {
		case []byte:
			return bytes.Equal(a, b)
		case string:
			return string(a) == b
		}
	case string:
		switch b := b.(type) {
		case string:
			return a == b
		case []byte:
			return a == string(b)
		}
	}
	return false
}

// spanCovers reports that the key span is the whole predicate. The span stops
// at the first column that is not an equality, so a later column still has to
// be filtered.
func spanCovers(rang sql.MySQLRange) bool {
	bounded := false
	for _, expr := range rang {
		switch expr.Type() {
		case sql.RangeType_Invalid:
			return false
		case sql.RangeType_All:
			if !bounded {
				return false
			}
		default:
			if bounded {
				return false
			}
			bounded = true
		}
	}
	return bounded
}

// spanForRange encodes one index range as a half-open key span.
// exact reports that every column is an equality, so start is the full key.
func spanForRange(ctx context.Context, fields []indexField, rang sql.MySQLRange) (span keySpan, exact, empty, ok bool, err error) {
	if len(rang) == 0 {
		return keySpan{}, false, false, true, nil
	}
	var prefix []byte
	matched := 0
	sawNull := false
	loose := false
	for i, expr := range rang {
		if i >= len(fields) {
			break
		}
		lo, hi, isEmpty, edgesOK := rangeExprEdges(expr)
		if !edgesOK {
			return keySpan{}, false, false, false, nil
		}
		if isEmpty {
			return keySpan{}, false, true, true, nil
		}
		eq, err := edgesEqual(ctx, fields[i].typ, lo, hi)
		if err != nil {
			return keySpan{}, false, false, false, err
		}
		if eq {
			var value interface{}
			if lo.kind == edgeNull || lo.value == nil {
				value = nil
				sawNull = true
			} else {
				value = lo.value
			}
			part, err := seekPart(ctx, fields[i], value)
			if err != nil {
				return keySpan{}, false, false, false, err
			}
			prefix = append(prefix, part...)
			matched++
			if !byteOrderType(fields[i].typ) {
				// The stored key has a tie-breaker after the weight. A point
				// get of one original string would miss an equal spelling.
				loose = true
			}
			continue
		}
		if fields[i].desc {
			lo, hi = hi, lo
		}
		start, err := edgeStart(ctx, fields[i], prefix, lo)
		if err != nil {
			return keySpan{}, false, false, false, err
		}
		end, err := edgeEnd(ctx, fields[i], prefix, hi)
		if err != nil {
			return keySpan{}, false, false, false, err
		}
		return keySpan{start: start, end: end}, false, false, true, nil
	}
	allEq := matched == len(fields) && matched == len(rang) && !sawNull && !loose
	return keySpan{start: append([]byte(nil), prefix...), end: prefixEnd(prefix)}, allEq, false, true, nil
}

// seekPart is the key prefix a comparison should use. Non-binary strings seek
// by collation weight and leave the tie-breaker out.
func seekPart(ctx context.Context, field indexField, value interface{}) ([]byte, error) {
	if !byteOrderType(field.typ) {
		return collationPrefix(field.typ, value, field.desc)
	}
	return encodeField(ctx, field, value)
}

func edgeStart(ctx context.Context, field indexField, prefix []byte, e edge) ([]byte, error) {
	switch e.kind {
	case edgeUnb:
		if len(prefix) == 0 {
			return nil, nil
		}
		return append([]byte(nil), prefix...), nil
	case edgeNull:
		return append(append([]byte(nil), prefix...), nullPart(field.desc)...), nil
	case edgeAfterNull:
		return append(append([]byte(nil), prefix...), nonNullFloor(field.desc)...), nil
	case edgeValue:
		part, err := seekPart(ctx, field, e.value)
		if err != nil {
			return nil, err
		}
		full := append(append([]byte(nil), prefix...), part...)
		if e.incl {
			return full, nil
		}
		return prefixEnd(full), nil
	default:
		return nil, fmt.Errorf("persist: bad range edge")
	}
}

func edgeEnd(ctx context.Context, field indexField, prefix []byte, e edge) ([]byte, error) {
	switch e.kind {
	case edgeUnb:
		if len(prefix) == 0 {
			return nil, nil
		}
		return prefixEnd(prefix), nil
	case edgeNull:
		return prefixEnd(append(append([]byte(nil), prefix...), nullPart(field.desc)...)), nil
	case edgeAfterNull:
		// Stop before NULL. ASC NULLs are 0x00, so this edge is not an ASC end.
		// DESC NULLs are 0xFF and sit after every non-null key.
		if field.desc {
			return append(append([]byte(nil), prefix...), nullPart(true)...), nil
		}
		return append(append([]byte(nil), prefix...), nonNullFloor(false)...), nil
	case edgeValue:
		part, err := seekPart(ctx, field, e.value)
		if err != nil {
			return nil, err
		}
		full := append(append([]byte(nil), prefix...), part...)
		if e.incl {
			return prefixEnd(full), nil
		}
		return full, nil
	default:
		return nil, fmt.Errorf("persist: bad range edge")
	}
}

func nullPart(desc bool) []byte {
	if desc {
		return []byte{0xFF}
	}
	return []byte{0x00}
}

func nonNullFloor(desc bool) []byte {
	if desc {
		return []byte{0xFE}
	}
	return []byte{0x01}
}

// indexIter seeks an index bucket and fetches the primary-key rows.
type indexIter struct {
	ctx      context.Context
	ref      tableRef
	schema   sql.Schema
	rows     *kvBucket
	iter     *rawIter
	discard  func()
	pending  []indexHit
	pi       int
	skip     map[string]struct{}
	span     keySpan
	reverse  bool
	diskKey  []byte
	diskPK   []byte
	diskOn   bool
	diskDone bool
	closed   bool
	err      error
}

type indexHit struct {
	indexKey []byte
	row      sql.Row
}

func (it *indexIter) Next(ctx *sql.Context) (sql.Row, error) {
	if it.err != nil {
		return nil, it.err
	}
	base := it.ctx
	if ctx != nil {
		base = ctx
	}
	for {
		if err := it.pullDisk(); err != nil {
			it.err = err
			return nil, err
		}
		it.skipPending()
		over := it.pi >= 0 && it.pi < len(it.pending)
		if !it.diskOn && !over {
			return nil, io.EOF
		}
		if over && (!it.diskOn || it.pendingFirst()) {
			row := it.pending[it.pi].row
			if it.diskOn && bytes.Equal(it.diskKey, it.pending[it.pi].indexKey) {
				it.diskOn = false
			}
			it.step()
			return row, nil
		}
		pk := it.diskPK
		it.diskOn = false
		raw := it.rows.GetRaw(pk)
		if len(raw) == 0 {
			continue
		}
		row, err := decodeRow(base, it.schema, raw)
		if err != nil {
			it.err = err
			return nil, err
		}
		if sess, ok := sessionFrom(ctx); ok {
			skip, err := sess.observeRow(ctx, it.ref, pk, raw)
			if err != nil {
				it.err = err
				return nil, err
			}
			if skip {
				continue
			}
		}
		return row, nil
	}
}

func (it *indexIter) pendingFirst() bool {
	cmp := bytes.Compare(it.pending[it.pi].indexKey, it.diskKey)
	if it.reverse {
		return cmp >= 0
	}
	return cmp <= 0
}

func (it *indexIter) step() {
	if it.reverse {
		it.pi--
		return
	}
	it.pi++
}

func (it *indexIter) skipPending() {
	for it.pi >= 0 && it.pi < len(it.pending) && !keyInSpan(it.pending[it.pi].indexKey, it.span) {
		if it.reverse {
			if it.span.start != nil && bytes.Compare(it.pending[it.pi].indexKey, it.span.start) < 0 {
				it.pi = -1
				return
			}
			it.pi--
			continue
		}
		if it.span.end != nil && bytes.Compare(it.pending[it.pi].indexKey, it.span.end) >= 0 {
			it.pi = len(it.pending)
			return
		}
		it.pi++
	}
}

func (it *indexIter) pullDisk() error {
	if it.diskOn || it.diskDone || it.iter == nil {
		it.diskDone = true
		return nil
	}
	for it.iter.Valid() {
		key := it.iter.Key()
		if it.reverse {
			if it.span.end != nil && bytes.Compare(key, it.span.end) >= 0 {
				it.iter.Next()
				continue
			}
			if it.span.start != nil && bytes.Compare(key, it.span.start) < 0 {
				it.diskDone = true
				return nil
			}
		} else {
			if it.span.end != nil && bytes.Compare(key, it.span.end) >= 0 {
				it.diskDone = true
				return nil
			}
			if it.span.start != nil && bytes.Compare(key, it.span.start) < 0 {
				it.iter.Next()
				continue
			}
		}
		val, err := it.iter.Value()
		if err != nil {
			return err
		}
		if _, skip := it.skip[string(val)]; skip {
			it.iter.Next()
			continue
		}
		it.diskKey = key
		it.diskPK = append([]byte(nil), val...)
		it.diskOn = true
		it.iter.Next()
		return nil
	}
	it.diskDone = true
	return nil
}

func (s *Store) openIndexIter(ctx context.Context, t *Table, indexName string, fields []indexField, unique bool, edits []edit, span keySpan, reverse bool) (*indexIter, error) {
	tx, discard := s.readFor(ctx, wantsCurrentRead(ctx))
	rows := rowsBucket(tx, t.dbName, t.name)
	data := indexData(tx, t.dbName, t.name, indexName)
	if rows == nil {
		discard()
		return nil, sql.ErrTableNotFound.New(t.name)
	}
	var iter *rawIter
	diskDone := data == nil
	if data != nil {
		iter = data.rawIter(reverse)
		if sess := sessionOf(ctx); sess != nil && sess.snapshotTxn() == tx.txn {
			if txn, ok := tx.txn.(*badger.Txn); ok {
				sess.trackIterator(iter.it, txn)
			}
		}
		if reverse {
			if span.end != nil {
				iter.Seek(span.end)
			} else {
				iter.seekEnd()
			}
		} else if span.start != nil {
			iter.Seek(span.start)
		} else {
			iter.Rewind()
		}
	}
	overlay := buildOverlay(edits)
	skip := make(map[string]struct{}, len(overlay))
	var pending []indexHit
	for _, entry := range overlay {
		skip[string(entry.key)] = struct{}{}
		if entry.tomb {
			continue
		}
		colKey, hasNull, err := encodeIndexColumns(ctx, fields, entry.row)
		if err != nil {
			iter.Close()
			discard()
			return nil, err
		}
		storage := indexEntryKey(colKey, entry.key, unique, hasNull)
		if !keyInSpan(storage, span) {
			continue
		}
		pending = append(pending, indexHit{indexKey: storage, row: entry.row})
	}
	sort.Slice(pending, func(i, j int) bool {
		return bytes.Compare(pending[i].indexKey, pending[j].indexKey) < 0
	})
	pi := 0
	if reverse {
		pi = len(pending) - 1
	}
	return &indexIter{
		ctx:      ctx,
		ref:      t.ref(),
		schema:   t.meta.schema,
		rows:     rows,
		iter:     iter,
		discard:  discard,
		pending:  pending,
		pi:       pi,
		skip:     skip,
		span:     span,
		reverse:  reverse,
		diskDone: diskDone,
	}, nil
}

func indexData(tx *kvTx, dbName, tableName, indexName string) *kvBucket {
	table := tableBucket(tx, dbName, tableName)
	if table == nil {
		return nil
	}
	parent := table.Bucket(bucketIndex)
	if parent == nil {
		return nil
	}
	return parent.Bucket([]byte(indexName))
}

func (it *indexIter) Close(*sql.Context) error {
	if it.closed {
		return nil
	}
	it.closed = true
	if it.iter != nil {
		it.iter.Close()
		if sess := sessionOf(it.ctx); sess != nil {
			sess.noteIteratorClosed(it.iter.it)
		}
	}
	if it.discard != nil {
		it.discard()
	}
	return nil
}
