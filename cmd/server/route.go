package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/bongani-m/persist"
)

// RouteKind is where one statement runs.
type RouteKind int

const (
	// RouteLocal runs on this node's shard, including statements that are not
	// table data (BEGIN, SET, SHOW).
	RouteLocal RouteKind = iota
	// RouteShard runs on one shard.
	RouteShard
	// RouteScatter reads every shard and merges the rows.
	RouteScatter
	// RouteDDL is schema. It is applied to meta and to every shard.
	RouteDDL
	// RoutePlace records a table placement in meta.
	RoutePlace
)

// Route is the decision for one statement.
type Route struct {
	Kind   RouteKind
	Place  persist.TablePlacement
	Checks []routedValue
	// Ranged is set when the table is stored as key ranges. Groups are the
	// Raft groups that own the overlapping spans, in range-list order.
	Ranged bool
	Groups []string
	// Span is set when SHARD TABLE names a group. Start and End are the
	// memcomparable bounds; an empty side is unbounded.
	Span    persist.KeyRange
	SpanSet bool
}

// routedValue is one unique-column value that must be absent on every shard.
type routedValue struct {
	Column string
	Text   string
	Quote  bool
}

// RouteQuery decides which range group owns query. ranges is the ordered
// span list from meta. A placed table is routed by those spans.
func RouteQuery(query, db string, binds []persist.ForwardBind, places []persist.TablePlacement, ranges []persist.KeyRange) (Route, error) {
	if p, kr, ranged, ok := parseShardDetail(query); ok {
		if !ranged {
			return Route{}, fmt.Errorf("persist: SHARD TABLE needs RANGE")
		}
		return Route{Kind: RoutePlace, Place: p, Ranged: true, Span: kr, SpanSet: kr.Group != ""}, nil
	}
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return Route{Kind: RouteLocal}, nil
	}
	switch stmt.(type) {
	case *sqlparser.DDL, *sqlparser.DBDDL:
		return Route{Kind: RouteDDL}, nil
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback, *sqlparser.Set, *sqlparser.Use, *sqlparser.Show, *sqlparser.Explain, *sqlparser.OtherAdmin:
		return Route{Kind: RouteLocal}, nil
	}
	route, ok, err := tryRange(stmt, db, binds, places, ranges)
	if err != nil || ok {
		return route, err
	}
	return Route{Kind: RouteLocal}, nil
}

func isReadStmt(stmt sqlparser.Statement) bool {
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return false
	}
	return !hasLock(sel.Lock) && sel.Into == nil
}

func findPlace(places []persist.TablePlacement, db string, name sqlparser.TableName) (persist.TablePlacement, bool) {
	qual := db
	if !name.DbQualifier.IsEmpty() {
		qual = name.DbQualifier.String()
	}
	key := strings.ToLower(qual) + "." + strings.ToLower(name.Name.String())
	for _, p := range places {
		if p.Key() == key {
			return p, true
		}
	}
	return persist.TablePlacement{}, false
}

func singleTable(exprs sqlparser.TableExprs) (sqlparser.TableName, bool) {
	if len(exprs) != 1 {
		return sqlparser.TableName{}, false
	}
	aliased, ok := exprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return sqlparser.TableName{}, false
	}
	name, ok := aliased.Expr.(sqlparser.TableName)
	return name, ok
}

func insertIDs(ins *sqlparser.Insert, column string, binds []persist.ForwardBind) ([]int64, error) {
	idx := columnIndex(ins.Columns, column)
	if idx < 0 {
		return nil, fmt.Errorf("persist: sharded insert must include %s", column)
	}
	rows, err := insertRows(ins.Rows)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		if idx >= len(row) {
			return nil, fmt.Errorf("persist: sharded insert must include %s", column)
		}
		id, ok, err := intValue(row[idx], binds)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("persist: sharded insert must include %s", column)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func insertChecks(ins *sqlparser.Insert, place persist.TablePlacement, binds []persist.ForwardBind) ([]routedValue, error) {
	if len(place.Check) == 0 {
		return nil, nil
	}
	rows, err := insertRows(ins.Rows)
	if err != nil {
		return nil, err
	}
	var out []routedValue
	for _, column := range place.Check {
		idx := columnIndex(ins.Columns, column)
		if idx < 0 {
			continue
		}
		for _, row := range rows {
			if idx >= len(row) {
				continue
			}
			text, quote, ok, err := scalarValue(row[idx], binds)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			out = append(out, routedValue{Column: column, Text: text, Quote: quote})
		}
	}
	return out, nil
}

func insertRows(rows sqlparser.InsertRows) ([]sqlparser.ValTuple, error) {
	switch n := rows.(type) {
	case sqlparser.Values:
		return []sqlparser.ValTuple(n), nil
	case *sqlparser.Values:
		return []sqlparser.ValTuple(*n), nil
	case sqlparser.AliasedValues:
		return []sqlparser.ValTuple(n.Values), nil
	case *sqlparser.AliasedValues:
		return []sqlparser.ValTuple(n.Values), nil
	default:
		return nil, fmt.Errorf("persist: sharded insert must list its values")
	}
}

func columnIndex(cols sqlparser.Columns, name string) int {
	for i, col := range cols {
		if strings.EqualFold(col.String(), name) {
			return i
		}
	}
	return -1
}

func intValue(expr sqlparser.Expr, binds []persist.ForwardBind) (int64, bool, error) {
	text, _, ok, err := scalarValue(expr, binds)
	if err != nil || !ok {
		return 0, false, err
	}
	id, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, false, nil
	}
	return id, true, nil
}

func scalarValue(expr sqlparser.Expr, binds []persist.ForwardBind) (string, bool, bool, error) {
	val, ok := expr.(*sqlparser.SQLVal)
	if !ok {
		return "", false, false, nil
	}
	switch val.Type {
	case sqlparser.IntVal:
		return string(val.Val), false, true, nil
	case sqlparser.StrVal:
		return string(val.Val), true, true, nil
	case sqlparser.ValArg:
		name := strings.TrimPrefix(string(val.Val), ":")
		for _, bind := range binds {
			if bind.Name != name {
				continue
			}
			if len(bind.Value) == 0 {
				return "", false, false, nil
			}
			text := string(bind.Value)
			quote := bind.Type != 0 && !intBind(bind.Type)
			return text, quote, true, nil
		}
		return "", false, false, fmt.Errorf("persist: missing bind %s", name)
	default:
		return "", false, false, nil
	}
}

func intBind(typ int32) bool {
	switch query.Type(typ) {
	case query.Type_INT8, query.Type_UINT8, query.Type_INT16, query.Type_UINT16,
		query.Type_INT24, query.Type_UINT24, query.Type_INT32, query.Type_UINT32,
		query.Type_INT64, query.Type_UINT64:
		return true
	default:
		return false
	}
}

// parseShardDetail recognizes SHARD TABLE ... RANGE. A statement with no
// RANGE is still recognized so the caller can reject it. A range may name
// START and END as exclusive integer bounds, then GROUP and PEER lines for a
// group that already exists. Without GROUP the range covers this process's
// data group.
func parseShardDetail(query string) (persist.TablePlacement, persist.KeyRange, bool, bool) {
	q := strings.TrimSpace(query)
	if strings.HasSuffix(q, ";") {
		q = strings.TrimSpace(strings.TrimSuffix(q, ";"))
	}
	fields, err := splitAdmin(q)
	if err != nil || len(fields) < 5 {
		return persist.TablePlacement{}, persist.KeyRange{}, false, false
	}
	if !strings.EqualFold(fields[0], "SHARD") || !strings.EqualFold(fields[1], "TABLE") || !strings.EqualFold(fields[3], "BY") {
		return persist.TablePlacement{}, persist.KeyRange{}, false, false
	}
	db, table, ok := splitQual(fields[2])
	if !ok || fields[4] == "" {
		return persist.TablePlacement{}, persist.KeyRange{}, false, false
	}
	p := persist.TablePlacement{DB: db, Table: table, Column: fields[4]}
	i := 5
	if i+1 < len(fields) && strings.EqualFold(fields[i], "CHECK") {
		if fields[i+1] == "" {
			return persist.TablePlacement{}, persist.KeyRange{}, false, false
		}
		p.Check = []string{fields[i+1]}
		i += 2
	}
	if i == len(fields) {
		return p, persist.KeyRange{}, false, true
	}
	if !strings.EqualFold(fields[i], "RANGE") {
		return persist.TablePlacement{}, persist.KeyRange{}, false, false
	}
	i++
	kr := persist.KeyRange{DB: db, Table: table, State: persist.RangeActive}
	if i+1 < len(fields) && strings.EqualFold(fields[i], "START") {
		n, err := strconv.ParseInt(fields[i+1], 10, 64)
		if err != nil {
			return persist.TablePlacement{}, persist.KeyRange{}, false, false
		}
		kr.Start = persist.EncodeIntKey(n)
		i += 2
	}
	if i+1 < len(fields) && strings.EqualFold(fields[i], "END") {
		n, err := strconv.ParseInt(fields[i+1], 10, 64)
		if err != nil {
			return persist.TablePlacement{}, persist.KeyRange{}, false, false
		}
		kr.End = persist.EncodeIntKey(n)
		i += 2
	}
	if i == len(fields) {
		return p, kr, true, true
	}
	if i+1 >= len(fields) || !strings.EqualFold(fields[i], "GROUP") || fields[i+1] == "" {
		return persist.TablePlacement{}, persist.KeyRange{}, false, false
	}
	kr.Group = fields[i+1]
	kr.ID = kr.Group + "/" + strings.ToLower(db) + "." + strings.ToLower(table)
	i += 2
	for i < len(fields) {
		if i+3 >= len(fields) || !strings.EqualFold(fields[i], "PEER") || fields[i+1] == "" || fields[i+2] == "" || fields[i+3] == "" {
			return persist.TablePlacement{}, persist.KeyRange{}, false, false
		}
		kr.Peers = append(kr.Peers, persist.RangePeer{ID: fields[i+1], Raft: fields[i+2], Forward: fields[i+3]})
		i += 4
	}
	if len(kr.Peers) == 0 {
		return persist.TablePlacement{}, persist.KeyRange{}, false, false
	}
	return p, kr, true, true
}

func splitQual(name string) (string, string, bool) {
	db, table, ok := strings.Cut(name, ".")
	if !ok || db == "" || table == "" || strings.Contains(table, ".") {
		return "", "", false
	}
	return db, table, true
}

func tryRange(stmt sqlparser.Statement, db string, binds []persist.ForwardBind, places []persist.TablePlacement, ranges []persist.KeyRange) (Route, bool, error) {
	place, where, ins, ok := rangeSubject(stmt, db, places)
	if !ok {
		return Route{}, false, nil
	}
	owned := persist.RangesFor(ranges, place.DB, place.Table)
	if len(owned) == 0 {
		return Route{}, false, fmt.Errorf("persist: table %s has no ranges", place.Key())
	}
	route, err := routeRanges(stmt, binds, place, owned, where, ins)
	return route, true, err
}

func rangeSubject(stmt sqlparser.Statement, db string, places []persist.TablePlacement) (persist.TablePlacement, *sqlparser.Where, *sqlparser.Insert, bool) {
	switch n := stmt.(type) {
	case *sqlparser.Select:
		name, ok := singleTable(n.From)
		if !ok {
			return persist.TablePlacement{}, nil, nil, false
		}
		place, ok := findPlace(places, db, name)
		return place, n.Where, nil, ok
	case *sqlparser.Insert:
		place, ok := findPlace(places, db, n.Table)
		return place, nil, n, ok
	case *sqlparser.Update:
		name, ok := singleTable(n.TableExprs)
		if !ok {
			return persist.TablePlacement{}, nil, nil, false
		}
		place, ok := findPlace(places, db, name)
		return place, n.Where, nil, ok
	case *sqlparser.Delete:
		name, ok := singleTable(n.TableExprs)
		if !ok {
			return persist.TablePlacement{}, nil, nil, false
		}
		place, ok := findPlace(places, db, name)
		return place, n.Where, nil, ok
	default:
		return persist.TablePlacement{}, nil, nil, false
	}
}

func routeRanges(stmt sqlparser.Statement, binds []persist.ForwardBind, place persist.TablePlacement, owned []persist.KeyRange, where *sqlparser.Where, ins *sqlparser.Insert) (Route, error) {
	if ins != nil {
		ids, err := insertIDs(ins, place.Column, binds)
		if err != nil {
			return Route{}, err
		}
		checks, err := insertChecks(ins, place, binds)
		if err != nil {
			return Route{}, err
		}
		return routePoints(ids, owned, place, checks)
	}
	read := isReadStmt(stmt)
	var expr sqlparser.Expr
	if where != nil {
		expr = where.Expr
	}
	return routeSpan(expr, place, owned, binds, read)
}

func routePoints(ids []int64, owned []persist.KeyRange, place persist.TablePlacement, checks []routedValue) (Route, error) {
	if len(ids) == 0 {
		return Route{}, fmt.Errorf("persist: statement has no shard key")
	}
	group := ""
	for _, id := range ids {
		hit, ok := persist.RangeHolding(owned, place.DB, place.Table, persist.EncodeIntKey(id))
		if !ok {
			return Route{}, fmt.Errorf("persist: statement has no shard key")
		}
		if hit.State == persist.RangeCopying {
			return Route{}, fmt.Errorf("persist: range is splitting")
		}
		if group == "" {
			group = hit.Group
			continue
		}
		if group != hit.Group {
			return Route{}, fmt.Errorf("persist: statement spans shards")
		}
	}
	return Route{Kind: RouteShard, Ranged: true, Groups: []string{group}, Place: place, Checks: checks}, nil
}

type spanKind int

const (
	spanAll spanKind = iota
	spanEmpty
	spanKeys
	spanBad
)

type intSpan struct {
	lo, hi int64
	hasLo  bool
	hasHi  bool
	kind   spanKind
}

func routeSpan(expr sqlparser.Expr, place persist.TablePlacement, owned []persist.KeyRange, binds []persist.ForwardBind, read bool) (Route, error) {
	span := constrain(expr, place.Column, binds)
	if span.kind == spanAll || span.kind == spanBad {
		if read {
			return scatterRanges(owned, place), nil
		}
		if span.kind == spanAll {
			return Route{}, fmt.Errorf("persist: statement has no shard key")
		}
		return Route{}, fmt.Errorf("persist: statement spans shards")
	}
	if span.kind == spanEmpty {
		if read {
			return Route{Kind: RouteScatter, Ranged: true, Place: place}, nil
		}
		return Route{}, fmt.Errorf("persist: statement spans shards")
	}
	var lo, hi []byte
	if span.hasLo {
		lo = persist.EncodeIntKey(span.lo)
	}
	if span.hasHi {
		hi = persist.EncodeIntKey(span.hi)
	}
	var groups []string
	for _, r := range owned {
		if !r.OverlapsInclusive(lo, hi, span.hasLo, span.hasHi) {
			continue
		}
		if !read && r.State == persist.RangeCopying {
			return Route{}, fmt.Errorf("persist: range is splitting")
		}
		if len(groups) == 0 || groups[len(groups)-1] != r.Group {
			groups = append(groups, r.Group)
		}
	}
	if len(groups) == 0 {
		if read {
			return Route{Kind: RouteScatter, Ranged: true, Place: place}, nil
		}
		return Route{}, fmt.Errorf("persist: statement has no shard key")
	}
	if len(groups) == 1 {
		return Route{Kind: RouteShard, Ranged: true, Groups: groups, Place: place}, nil
	}
	if read {
		return Route{Kind: RouteScatter, Ranged: true, Groups: groups, Place: place}, nil
	}
	return Route{}, fmt.Errorf("persist: statement spans shards")
}

func scatterRanges(owned []persist.KeyRange, place persist.TablePlacement) Route {
	var groups []string
	for _, r := range owned {
		if len(groups) == 0 || groups[len(groups)-1] != r.Group {
			groups = append(groups, r.Group)
		}
	}
	return Route{Kind: RouteScatter, Ranged: true, Groups: groups, Place: place}
}

func constrain(expr sqlparser.Expr, column string, binds []persist.ForwardBind) intSpan {
	if expr == nil {
		return intSpan{kind: spanAll}
	}
	switch n := expr.(type) {
	case *sqlparser.ParenExpr:
		return constrain(n.Expr, column, binds)
	case *sqlparser.AndExpr:
		return intersectSpan(constrain(n.Left, column, binds), constrain(n.Right, column, binds))
	case *sqlparser.OrExpr:
		return intSpan{kind: spanBad}
	case *sqlparser.RangeCond:
		return betweenSpan(n, column, binds)
	case *sqlparser.ComparisonExpr:
		return compareSpan(n, column, binds)
	default:
		return intSpan{kind: spanAll}
	}
}

func betweenSpan(n *sqlparser.RangeCond, column string, binds []persist.ForwardBind) intSpan {
	if !colIs(n.Left, column) || !strings.EqualFold(n.Operator, sqlparser.BetweenStr) {
		return intSpan{kind: spanAll}
	}
	lo, lok, err := intValue(n.From, binds)
	if err != nil || !lok {
		return intSpan{kind: spanBad}
	}
	hi, hok, err := intValue(n.To, binds)
	if err != nil || !hok {
		return intSpan{kind: spanBad}
	}
	if lo > hi {
		return intSpan{kind: spanEmpty}
	}
	return intSpan{kind: spanKeys, lo: lo, hi: hi, hasLo: true, hasHi: true}
}

func compareSpan(n *sqlparser.ComparisonExpr, column string, binds []persist.ForwardBind) intSpan {
	op := n.Operator
	var valExpr sqlparser.Expr
	switch {
	case colIs(n.Left, column):
		valExpr = n.Right
	case colIs(n.Right, column):
		valExpr = n.Left
		op = flipOp(op)
	default:
		return intSpan{kind: spanAll}
	}
	val, ok, err := intValue(valExpr, binds)
	if err != nil || !ok {
		return intSpan{kind: spanBad}
	}
	switch op {
	case sqlparser.EqualStr:
		return intSpan{kind: spanKeys, lo: val, hi: val, hasLo: true, hasHi: true}
	case sqlparser.GreaterThanStr:
		if val == math.MaxInt64 {
			return intSpan{kind: spanEmpty}
		}
		return intSpan{kind: spanKeys, lo: val + 1, hasLo: true}
	case sqlparser.GreaterEqualStr:
		return intSpan{kind: spanKeys, lo: val, hasLo: true}
	case sqlparser.LessThanStr:
		if val == math.MinInt64 {
			return intSpan{kind: spanEmpty}
		}
		return intSpan{kind: spanKeys, hi: val - 1, hasHi: true}
	case sqlparser.LessEqualStr:
		return intSpan{kind: spanKeys, hi: val, hasHi: true}
	default:
		return intSpan{kind: spanBad}
	}
}

func flipOp(op string) string {
	switch op {
	case sqlparser.GreaterThanStr:
		return sqlparser.LessThanStr
	case sqlparser.GreaterEqualStr:
		return sqlparser.LessEqualStr
	case sqlparser.LessThanStr:
		return sqlparser.GreaterThanStr
	case sqlparser.LessEqualStr:
		return sqlparser.GreaterEqualStr
	default:
		return op
	}
}

func intersectSpan(a, b intSpan) intSpan {
	if a.kind == spanBad || b.kind == spanBad {
		return intSpan{kind: spanBad}
	}
	if a.kind == spanEmpty || b.kind == spanEmpty {
		return intSpan{kind: spanEmpty}
	}
	if a.kind == spanAll {
		return b
	}
	if b.kind == spanAll {
		return a
	}
	out := intSpan{kind: spanKeys}
	if a.hasLo && b.hasLo {
		out.hasLo = true
		if a.lo > b.lo {
			out.lo = a.lo
		} else {
			out.lo = b.lo
		}
	} else if a.hasLo {
		out.hasLo = true
		out.lo = a.lo
	} else if b.hasLo {
		out.hasLo = true
		out.lo = b.lo
	}
	if a.hasHi && b.hasHi {
		out.hasHi = true
		if a.hi < b.hi {
			out.hi = a.hi
		} else {
			out.hi = b.hi
		}
	} else if a.hasHi {
		out.hasHi = true
		out.hi = a.hi
	} else if b.hasHi {
		out.hasHi = true
		out.hi = b.hi
	}
	if out.hasLo && out.hasHi && out.lo > out.hi {
		return intSpan{kind: spanEmpty}
	}
	return out
}

func colIs(expr sqlparser.Expr, column string) bool {
	col, ok := expr.(*sqlparser.ColName)
	return ok && strings.EqualFold(col.Name.String(), column)
}
