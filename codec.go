package persist

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// storedTable is the on-disk description of one table.
type storedTable struct {
	Columns    []storedColumn `json:"columns"`
	PkOrdinals []int          `json:"pkOrdinals"`
	Collation  uint16         `json:"collation"`
	Comment    string         `json:"comment,omitempty"`
}

type storedColumn struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Nullable      bool   `json:"nullable"`
	PrimaryKey    bool   `json:"primaryKey"`
	AutoIncrement bool   `json:"autoIncrement,omitempty"`
	Virtual       bool   `json:"virtual,omitempty"`
	Hidden        bool   `json:"hidden,omitempty"`
	HiddenSystem  bool   `json:"hiddenSystem,omitempty"`
	Comment       string `json:"comment,omitempty"`
	Default       string `json:"default,omitempty"`
	Generated     string `json:"generated,omitempty"`
	OnUpdate      string `json:"onUpdate,omitempty"`
	// SRID and DefinedSRID carry the spatial reference that a geometry type's
	// string form leaves out.
	SRID        uint32 `json:"srid,omitempty"`
	DefinedSRID bool   `json:"definedSrid,omitempty"`
}

type tableMeta struct {
	schema        sql.Schema
	pk            []int
	collation     sql.CollationID
	comment       string
	targetRowSize uint64
}

// cell is one encoded SQL value. Null is a JSON null. Otherwise Kind selects
// how Str is turned back into a Go value before sql.Type.Convert.
type cell struct {
	Null bool   `json:"null,omitempty"`
	Kind string `json:"k,omitempty"`
	Str  string `json:"s,omitempty"`
}

func encodeSchema(ctx *sql.Context, sch sql.PrimaryKeySchema, collation sql.CollationID, comment string) ([]byte, error) {
	ordinals := sch.PkOrdinals
	if len(ordinals) == 0 {
		for i, col := range sch.Schema {
			if col.PrimaryKey {
				ordinals = append(ordinals, i)
			}
		}
	}
	pk := make(map[int]bool, len(ordinals))
	for _, ord := range ordinals {
		if ord < 0 || ord >= len(sch.Schema) {
			return nil, fmt.Errorf("persist: primary key ordinal %d is outside the schema", ord)
		}
		pk[ord] = true
	}

	stored := storedTable{
		Columns:    make([]storedColumn, len(sch.Schema)),
		PkOrdinals: append([]int(nil), ordinals...),
		Collation:  uint16(collation),
		Comment:    comment,
	}
	for i, col := range sch.Schema {
		typeName := col.Type.String()
		if _, err := parseSQLType(typeName); err != nil {
			return nil, fmt.Errorf("persist: column %s type %q: %w", col.Name, typeName, err)
		}
		stored.Columns[i] = storedColumn{
			Name:          col.Name,
			Type:          typeName,
			Nullable:      col.Nullable,
			PrimaryKey:    pk[i],
			AutoIncrement: col.AutoIncrement,
			Virtual:       col.Virtual,
			Hidden:        col.Hidden,
			HiddenSystem:  col.HiddenSystem,
			Comment:       col.Comment,
			Default:       defaultExprString(ctx, col.Default),
			Generated:     defaultExprString(ctx, col.Generated),
			OnUpdate:      defaultExprString(ctx, col.OnUpdate),
		}
		if spatial, ok := col.Type.(sql.SpatialColumnType); ok {
			stored.Columns[i].SRID, stored.Columns[i].DefinedSRID = spatial.GetSpatialTypeSRID()
		}
	}
	return json.Marshal(stored)
}

func decodeSchema(raw []byte, dbName, tableName string) (tableMeta, error) {
	var stored storedTable
	if err := json.Unmarshal(raw, &stored); err != nil {
		return tableMeta{}, fmt.Errorf("persist: schema for %s.%s: %w", dbName, tableName, err)
	}
	pk := make(map[int]bool, len(stored.PkOrdinals))
	for _, ord := range stored.PkOrdinals {
		if ord < 0 || ord >= len(stored.Columns) {
			return tableMeta{}, fmt.Errorf("persist: primary key ordinal %d is outside %s.%s", ord, dbName, tableName)
		}
		pk[ord] = true
	}

	schema := make(sql.Schema, len(stored.Columns))
	for i, col := range stored.Columns {
		typ, err := parseSQLType(col.Type)
		if err != nil {
			return tableMeta{}, fmt.Errorf("persist: column %s.%s.%s type %q: %w", dbName, tableName, col.Name, col.Type, err)
		}
		typ = restoreTypeCollation(typ)
		if col.DefinedSRID {
			if spatial, ok := typ.(sql.SpatialColumnType); ok {
				typ = spatial.SetSRID(col.SRID)
			}
		}
		schema[i] = &sql.Column{
			Name:           col.Name,
			Type:           typ,
			Nullable:       col.Nullable,
			PrimaryKey:     pk[i] || col.PrimaryKey,
			AutoIncrement:  col.AutoIncrement,
			Virtual:        col.Virtual,
			Hidden:         col.Hidden,
			HiddenSystem:   col.HiddenSystem,
			Comment:        col.Comment,
			Default:        unresolvedDefault(col.Default),
			Generated:      unresolvedDefault(col.Generated),
			OnUpdate:       unresolvedDefault(col.OnUpdate),
			Source:         tableName,
			DatabaseSource: dbName,
		}
	}
	collation := sql.CollationID(stored.Collation)
	if collation == sql.Collation_Unspecified {
		collation = sql.Collation_Default
	}
	return tableMeta{
		schema:    schema,
		pk:        append([]int(nil), stored.PkOrdinals...),
		collation: collation,
		comment:   stored.Comment,
	}, nil
}

// defaultExprString renders a column default back to the SQL text that
// unresolvedDefault will re-parse. It must be the string form of the whole
// ColumnDefaultValue, not of def.Expr: only the outer form writes the
// parentheses that tell an expression default apart from a literal one, and
// reparsing "2" instead of "(2)" would turn DEFAULT (2) into DEFAULT '2' and
// make a column reference resolve against the wrong row.
func defaultExprString(ctx *sql.Context, def *sql.ColumnDefaultValue) string {
	if def == nil || def.Expr == nil {
		return ""
	}
	switch expr := def.Expr.(type) {
	case sql.UnresolvedColumnDefault:
		return expr.ExprString
	case *sql.UnresolvedColumnDefault:
		return expr.ExprString
	}
	// Table-qualified column references do not resolve when the expression is
	// re-parsed on its own, so reduce them to bare column names.
	stripped, _, err := transform.Expr(ctx, def, stripTableNames)
	if err != nil {
		return def.String()
	}
	return stripped.String()
}

func stripTableNames(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
	field, ok := e.(*expression.GetField)
	if !ok {
		return e, transform.SameTree, nil
	}
	bare := expression.NewGetField(field.Index(), field.Type(ctx), field.Name(), field.IsNullable(ctx))
	bare = bare.WithQuotedNames(sql.DefaultMySQLSchemaFormatter, field.IsQuotedIdentifier())
	return bare, transform.NewTree, nil
}

func unresolvedDefault(expr string) *sql.ColumnDefaultValue {
	if expr == "" {
		return nil
	}
	return sql.NewUnresolvedColumnDefaultValue(expr)
}

func encodeTime(v time.Time) string {
	u := v.UTC()
	year, month, day := u.Date()
	hour, minute, second := u.Clock()
	return fmt.Sprintf("%d %d %d %d %d %d %d", year, int(month), day, hour, minute, second, u.Nanosecond())
}

func decodeTime(raw string) (time.Time, error) {
	if strings.Contains(raw, "T") {
		return time.Parse(time.RFC3339Nano, raw)
	}
	var year, month, day, hour, minute, second, nsec int
	n, err := fmt.Sscanf(raw, "%d %d %d %d %d %d %d", &year, &month, &day, &hour, &minute, &second, &nsec)
	if err != nil || n != 7 {
		if err == nil {
			err = fmt.Errorf("persist: time %q", raw)
		}
		return time.Time{}, err
	}
	return time.Date(year, time.Month(month), day, hour, minute, second, nsec, time.UTC), nil
}

// restoreTypeCollation puts back the collation that parseSQLType drops. A type's
// string form omits COLLATE when the collation is the default one, and the
// parser then builds the type with Collation_Unspecified rather than resolving
// the default. Its name still prints as "utf8mb4_0900_bin", so the difference is
// invisible until something compares the ids: charset validation stops
// recognising the column as utf8mb4, and SHOW CREATE TABLE starts spelling out a
// redundant CHARACTER SET / COLLATE.
func restoreTypeCollation(typ sql.Type) sql.Type {
	switch t := typ.(type) {
	case sql.EnumType:
		if t.Collation() != sql.Collation_Unspecified {
			return typ
		}
		restored, err := types.CreateEnumType(t.Values(), sql.Collation_Default)
		if err != nil {
			return typ
		}
		return restored
	case sql.SetType:
		if t.Collation() != sql.Collation_Unspecified {
			return typ
		}
		restored, err := types.CreateSetType(t.Values(), sql.Collation_Default)
		if err != nil {
			return typ
		}
		return restored
	case sql.StringType:
		if t.Collation() != sql.Collation_Unspecified {
			return typ
		}
		restored, err := types.CreateString(t.Type(), t.Length(), sql.Collation_Default)
		if err != nil {
			return typ
		}
		return restored
	default:
		return typ
	}
}

func parseSQLType(typeName string) (sql.Type, error) {
	parsed, err := sqlparser.Parse(fmt.Sprintf("create table t (a %s)", typeName))
	if err != nil {
		return nil, err
	}
	ddl, ok := parsed.(*sqlparser.DDL)
	if !ok || ddl.TableSpec == nil || len(ddl.TableSpec.Columns) != 1 {
		return nil, fmt.Errorf("not a column type")
	}
	parsedType := ddl.TableSpec.Columns[0].Type
	return types.ColumnTypeToType(&parsedType)
}

// rowFormatBinary is the first byte of a compact row. A JSON row from before
// that format starts with '['.
const rowFormatBinary byte = 1

func encodeRow(ctx context.Context, schema sql.Schema, row sql.Row) ([]byte, error) {
	if schema != nil && len(schema) != len(row) {
		return nil, fmt.Errorf("persist: row has %d values for %d columns", len(row), len(schema))
	}
	n := len(row)
	bitmap := make([]byte, (n+7)/8)
	var payloads []byte
	for i, value := range row {
		if value == nil {
			bitmap[i/8] |= 1 << (uint(i) % 8)
			continue
		}
		payload, err := encodePayload(ctx, value)
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, payload...)
	}
	buf := make([]byte, 0, 1+len(bitmap)+len(payloads))
	buf = append(buf, rowFormatBinary)
	buf = append(buf, bitmap...)
	buf = append(buf, payloads...)
	return buf, nil
}

func encodeRowJSON(ctx context.Context, row sql.Row) ([]byte, error) {
	cells := make([]cell, len(row))
	for i, value := range row {
		encoded, err := encodeValue(ctx, value)
		if err != nil {
			return nil, err
		}
		cells[i] = encoded
	}
	return json.Marshal(cells)
}

func decodeRow(ctx context.Context, schema sql.Schema, raw []byte) (sql.Row, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("persist: row is empty")
	}
	if raw[0] == '[' {
		return decodeRowJSON(ctx, schema, raw)
	}
	if raw[0] != rowFormatBinary {
		return nil, fmt.Errorf("persist: row format %d", raw[0])
	}
	n := len(schema)
	bitmapLen := (n + 7) / 8
	if len(raw) < 1+bitmapLen {
		return nil, fmt.Errorf("persist: row is shorter than its null bitmap")
	}
	bitmap := raw[1 : 1+bitmapLen]
	rest := raw[1+bitmapLen:]
	row := make(sql.Row, n)
	for i := 0; i < n; i++ {
		if bitmap[i/8]&(1<<(uint(i)%8)) != 0 {
			continue
		}
		value, read, err := decodePayload(ctx, schema[i].Type, rest)
		if err != nil {
			return nil, fmt.Errorf("persist: column %s: %w", schema[i].Name, err)
		}
		row[i] = value
		rest = rest[read:]
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("persist: row has %d trailing bytes", len(rest))
	}
	return row, nil
}

func decodeRowJSON(ctx context.Context, schema sql.Schema, raw []byte) (sql.Row, error) {
	var cells []cell
	if err := json.Unmarshal(raw, &cells); err != nil {
		return nil, fmt.Errorf("persist: row: %w", err)
	}
	if len(cells) != len(schema) {
		return nil, fmt.Errorf("persist: row has %d values for %d columns", len(cells), len(schema))
	}
	row := make(sql.Row, len(cells))
	for i, encoded := range cells {
		value, err := decodeValue(ctx, schema[i].Type, encoded)
		if err != nil {
			return nil, fmt.Errorf("persist: column %s: %w", schema[i].Name, err)
		}
		row[i] = value
	}
	return row, nil
}

func encodePayload(ctx context.Context, value interface{}) ([]byte, error) {
	encoded, err := encodeValue(ctx, value)
	if err != nil {
		return nil, err
	}
	if encoded.Null {
		return nil, fmt.Errorf("persist: null payload")
	}
	body, err := payloadBody(encoded)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 1+len(body))
	buf = append(buf, encoded.Kind[0])
	buf = append(buf, body...)
	return buf, nil
}

func payloadBody(encoded cell) ([]byte, error) {
	switch encoded.Kind {
	case "i":
		n, err := strconv.ParseInt(encoded.Str, 10, 64)
		if err != nil {
			return nil, err
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(n))
		return buf[:], nil
	case "u":
		n, err := strconv.ParseUint(encoded.Str, 10, 64)
		if err != nil {
			return nil, err
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], n)
		return buf[:], nil
	case "f":
		n, err := strconv.ParseFloat(encoded.Str, 64)
		if err != nil {
			return nil, err
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(n))
		return buf[:], nil
	case "t":
		parsed, err := decodeTime(encoded.Str)
		if err != nil {
			return nil, err
		}
		return timePayload(parsed), nil
	case "s", "d", "j", "J", "z":
		return lenPrefixed([]byte(encoded.Str)), nil
	case "y", "g":
		raw, err := base64.StdEncoding.DecodeString(encoded.Str)
		if err != nil {
			return nil, err
		}
		return lenPrefixed(raw), nil
	default:
		return nil, fmt.Errorf("persist: cannot encode kind %q", encoded.Kind)
	}
}

func timePayload(v time.Time) []byte {
	u := v.UTC()
	var buf [13]byte
	binary.BigEndian.PutUint32(buf[0:4], uint32(int32(u.Year())))
	buf[4] = byte(u.Month())
	buf[5] = byte(u.Day())
	buf[6] = byte(u.Hour())
	buf[7] = byte(u.Minute())
	buf[8] = byte(u.Second())
	binary.BigEndian.PutUint32(buf[9:], uint32(u.Nanosecond()))
	return buf[:]
}

func lenPrefixed(b []byte) []byte {
	var n [binary.MaxVarintLen64]byte
	nn := binary.PutUvarint(n[:], uint64(len(b)))
	buf := make([]byte, 0, nn+len(b))
	buf = append(buf, n[:nn]...)
	buf = append(buf, b...)
	return buf
}

func decodePayload(ctx context.Context, typ sql.Type, raw []byte) (interface{}, int, error) {
	if len(raw) == 0 {
		return nil, 0, fmt.Errorf("missing payload")
	}
	kind := string(raw[0:1])
	body := raw[1:]
	var encoded cell
	var read int
	switch kind {
	case "i":
		if len(body) < 8 {
			return nil, 0, fmt.Errorf("short integer")
		}
		encoded = cell{Kind: "i", Str: strconv.FormatInt(int64(binary.BigEndian.Uint64(body[:8])), 10)}
		read = 9
	case "u":
		if len(body) < 8 {
			return nil, 0, fmt.Errorf("short integer")
		}
		encoded = cell{Kind: "u", Str: strconv.FormatUint(binary.BigEndian.Uint64(body[:8]), 10)}
		read = 9
	case "f":
		if len(body) < 8 {
			return nil, 0, fmt.Errorf("short float")
		}
		encoded = cell{Kind: "f", Str: strconv.FormatFloat(math.Float64frombits(binary.BigEndian.Uint64(body[:8])), 'g', -1, 64)}
		read = 9
	case "t":
		if len(body) < 13 {
			return nil, 0, fmt.Errorf("short time")
		}
		encoded = cell{Kind: "t", Str: encodeTime(timeFromPayload(body[:13]))}
		read = 14
	case "s", "d", "j", "J", "z", "y", "g":
		n, k := binary.Uvarint(body)
		if k <= 0 || uint64(k)+n > uint64(len(body)) {
			return nil, 0, fmt.Errorf("short bytes")
		}
		payload := body[k : k+int(n)]
		if kind == "y" || kind == "g" {
			encoded = cell{Kind: kind, Str: base64.StdEncoding.EncodeToString(payload)}
		} else {
			encoded = cell{Kind: kind, Str: string(payload)}
		}
		read = 1 + k + int(n)
	default:
		return nil, 0, fmt.Errorf("unknown value kind %q", kind)
	}
	value, err := decodeValue(ctx, typ, encoded)
	if err != nil {
		return nil, 0, err
	}
	return value, read, nil
}

func timeFromPayload(b []byte) time.Time {
	year := int32(binary.BigEndian.Uint32(b[0:4]))
	return time.Date(int(year), time.Month(b[4]), int(b[5]), int(b[6]), int(b[7]), int(b[8]), int(binary.BigEndian.Uint32(b[9:13])), time.UTC)
}

func encodeValue(ctx context.Context, value interface{}) (cell, error) {
	if value == nil {
		return cell{Null: true}, nil
	}
	if geo, ok := value.(types.GeometryValue); ok {
		return cell{Kind: "g", Str: base64.StdEncoding.EncodeToString(geo.Serialize())}, nil
	}
	if wrapper, ok := value.(sql.JSONWrapper); ok {
		iface, err := wrapper.ToInterface(ctx)
		if err != nil {
			return cell{}, err
		}
		raw, err := json.Marshal(tagJSON(iface))
		if err != nil {
			return cell{}, err
		}
		return cell{Kind: "J", Str: string(raw)}, nil
	}
	switch v := value.(type) {
	case string:
		// Base64, not a JSON string: binary charsets are not valid UTF-8, and
		// encoding/json replaces those bytes with U+FFFD.
		return cell{Kind: "z", Str: base64.StdEncoding.EncodeToString([]byte(v))}, nil
	case []byte:
		return cell{Kind: "y", Str: base64.StdEncoding.EncodeToString(v)}, nil
	case bool:
		if v {
			return cell{Kind: "i", Str: "1"}, nil
		}
		return cell{Kind: "i", Str: "0"}, nil
	case int:
		return cell{Kind: "i", Str: strconv.FormatInt(int64(v), 10)}, nil
	case int8:
		return cell{Kind: "i", Str: strconv.FormatInt(int64(v), 10)}, nil
	case int16:
		return cell{Kind: "i", Str: strconv.FormatInt(int64(v), 10)}, nil
	case int32:
		return cell{Kind: "i", Str: strconv.FormatInt(int64(v), 10)}, nil
	case int64:
		return cell{Kind: "i", Str: strconv.FormatInt(v, 10)}, nil
	case uint:
		return cell{Kind: "u", Str: strconv.FormatUint(uint64(v), 10)}, nil
	case uint8:
		return cell{Kind: "u", Str: strconv.FormatUint(uint64(v), 10)}, nil
	case uint16:
		return cell{Kind: "u", Str: strconv.FormatUint(uint64(v), 10)}, nil
	case uint32:
		return cell{Kind: "u", Str: strconv.FormatUint(uint64(v), 10)}, nil
	case uint64:
		return cell{Kind: "u", Str: strconv.FormatUint(v, 10)}, nil
	case float32:
		return cell{Kind: "f", Str: strconv.FormatFloat(float64(v), 'g', -1, 32)}, nil
	case float64:
		return cell{Kind: "f", Str: strconv.FormatFloat(v, 'g', -1, 64)}, nil
	case time.Time:
		// Calendar fields, not Unix nanos: MySQL's zero date is year 0, which
		// overflows time.Time.UnixNano.
		return cell{Kind: "t", Str: encodeTime(v)}, nil
	case *apd.Decimal:
		return cell{Kind: "d", Str: v.String()}, nil
	case types.Timespan:
		// TIME values are microsecond counts. Converting that integer back through
		// TIME treats it as a clock literal, so store the printable form instead.
		return cell{Kind: "s", Str: v.String()}, nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return cell{}, fmt.Errorf("persist: cannot encode %T", value)
		}
		return cell{Kind: "j", Str: string(raw)}, nil
	}
}

func decodeValue(ctx context.Context, typ sql.Type, encoded cell) (interface{}, error) {
	if encoded.Null {
		return nil, nil
	}
	var value interface{}
	var err error
	switch encoded.Kind {
	case "s", "j", "d":
		value = encoded.Str
	case "i":
		value, err = strconv.ParseInt(encoded.Str, 10, 64)
	case "u":
		value, err = strconv.ParseUint(encoded.Str, 10, 64)
	case "f":
		value, err = strconv.ParseFloat(encoded.Str, 64)
	case "t":
		value, err = decodeTime(encoded.Str)
	case "g":
		value, err = base64.StdEncoding.DecodeString(encoded.Str)
	case "J":
		var tagged interface{}
		err = json.Unmarshal([]byte(encoded.Str), &tagged)
		value = untagJSON(tagged)
	case "y":
		value, err = base64.StdEncoding.DecodeString(encoded.Str)
	case "z":
		var raw []byte
		raw, err = base64.StdEncoding.DecodeString(encoded.Str)
		value = string(raw)
	default:
		return nil, fmt.Errorf("unknown value kind %q", encoded.Kind)
	}
	if err != nil {
		return nil, err
	}
	// Tagged JSON is already the document value. JsonType.Convert parses Go
	// strings as JSON text, which turns a stored JSON string into EOF or a
	// different type.
	if encoded.Kind == "J" {
		if _, ok := typ.(types.JsonType); ok {
			return types.JSONDocument{Val: value}, nil
		}
	}
	converted, _, err := typ.Convert(ctx, narrowForType(typ, value))
	return converted, err
}

// narrowForType keeps stored enum indexes as uint16. Decoding them as uint64
// sends 0 through the strict-mode integer path, which rejects the empty enum.
func narrowForType(typ sql.Type, value interface{}) interface{} {
	if _, ok := typ.(types.EnumType); !ok {
		return value
	}
	switch n := value.(type) {
	case int64:
		return uint16(n)
	case uint64:
		return uint16(n)
	default:
		return value
	}
}

func sequenceKey(seq uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, seq)
	return key
}

func pkString(ordinals []int, row sql.Row) string {
	parts := make([]interface{}, len(ordinals))
	for i, ord := range ordinals {
		if ord >= 0 && ord < len(row) {
			parts[i] = row[ord]
		}
	}
	return fmt.Sprint(parts)
}

func tagJSON(v interface{}) interface{} {
	if v == nil {
		return map[string]string{"k": "n"}
	}
	switch n := v.(type) {
	case bool:
		bit := "0"
		if n {
			bit = "1"
		}
		return map[string]string{"k": "b", "s": bit}
	case string:
		return map[string]string{"k": "s", "s": n}
	case int:
		return map[string]string{"k": "i", "s": strconv.FormatInt(int64(n), 10)}
	case int8:
		return map[string]string{"k": "i", "s": strconv.FormatInt(int64(n), 10)}
	case int16:
		return map[string]string{"k": "i", "s": strconv.FormatInt(int64(n), 10)}
	case int32:
		return map[string]string{"k": "i", "s": strconv.FormatInt(int64(n), 10)}
	case int64:
		return map[string]string{"k": "i", "s": strconv.FormatInt(n, 10)}
	case uint:
		return map[string]string{"k": "u", "s": strconv.FormatUint(uint64(n), 10)}
	case uint8:
		return map[string]string{"k": "u", "s": strconv.FormatUint(uint64(n), 10)}
	case uint16:
		return map[string]string{"k": "u", "s": strconv.FormatUint(uint64(n), 10)}
	case uint32:
		return map[string]string{"k": "u", "s": strconv.FormatUint(uint64(n), 10)}
	case uint64:
		return map[string]string{"k": "u", "s": strconv.FormatUint(n, 10)}
	case float32:
		return map[string]string{"k": "f", "s": strconv.FormatFloat(float64(n), 'g', -1, 32)}
	case float64:
		return map[string]string{"k": "f", "s": strconv.FormatFloat(n, 'g', -1, 64)}
	case *apd.Decimal:
		return map[string]string{"k": "d", "s": n.String()}
	case apd.Decimal:
		return map[string]string{"k": "d", "s": n.String()}
	case time.Time:
		return map[string]string{"k": "t", "s": encodeTime(n)}
	case []interface{}:
		arr := make([]interface{}, len(n))
		for i, item := range n {
			arr[i] = tagJSON(item)
		}
		return map[string]interface{}{"k": "a", "a": arr}
	case map[string]interface{}:
		keys := make([]string, 0, len(n))
		for key := range n {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		obj := make([]interface{}, 0, len(keys))
		for _, key := range keys {
			obj = append(obj, map[string]interface{}{"key": key, "val": tagJSON(n[key])})
		}
		return map[string]interface{}{"k": "o", "o": obj}
	default:
		return map[string]string{"k": "s", "s": fmt.Sprint(n)}
	}
}

func untagJSON(v interface{}) interface{} {
	m, ok := v.(map[string]interface{})
	if !ok {
		return v
	}
	kind, _ := m["k"].(string)
	raw, _ := m["s"].(string)
	switch kind {
	case "n":
		return nil
	case "b":
		return raw == "1"
	case "s":
		return raw
	case "i":
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return raw
		}
		return n
	case "u":
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return raw
		}
		return n
	case "f":
		n, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return raw
		}
		return n
	case "d":
		dec, _, err := apd.NewFromString(raw)
		if err != nil {
			return raw
		}
		return dec
	case "t":
		parsed, err := decodeTime(raw)
		if err != nil {
			return raw
		}
		return parsed
	case "a":
		items, _ := m["a"].([]interface{})
		out := make([]interface{}, len(items))
		for i, item := range items {
			out[i] = untagJSON(item)
		}
		return out
	case "o":
		items, _ := m["o"].([]interface{})
		out := make(map[string]interface{}, len(items))
		for _, item := range items {
			kv, _ := item.(map[string]interface{})
			key, _ := kv["key"].(string)
			out[key] = untagJSON(kv["val"])
		}
		return out
	default:
		return v
	}
}
