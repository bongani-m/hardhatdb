package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"unicode/utf8"

	"github.com/cockroachdb/apd/v3"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// primaryKey is the memcomparable row key for a primary key. Parts are
// concatenated. Each part is self-delimiting, so a shorter string sorts before
// a longer one instead of by its length.
func PrimaryKey(ctx context.Context, schema sql.Schema, ordinals []int, row sql.Row) ([]byte, error) {
	var buf []byte
	var tie []byte
	for _, ord := range ordinals {
		if ord < 0 || ord >= len(row) {
			return nil, fmt.Errorf("hardhatdb: primary key ordinal %d is outside the row", ord)
		}
		var typ sql.Type
		if ord < len(schema) {
			typ = schema[ord].Type
		}
		part, err := EncodeField(ctx, IndexField{Type: typ}, row[ord])
		if err != nil {
			return nil, err
		}
		buf = append(buf, part...)
		// The original bytes follow every column weight. A tie between columns
		// would stop a multi-column seek from being a prefix of the stored key.
		if row[ord] != nil && !byteOrderType(typ) {
			tie = append(tie, EncodeTextKey([]byte(ValueText(row[ord])))...)
		}
	}
	return append(buf, tie...), nil
}

type IndexField struct {
	Ordinal int
	Type    sql.Type
	Desc    bool
	Prefix  uint16
}

func PkFields(schema sql.Schema, ordinals []int) []IndexField {
	fields := make([]IndexField, len(ordinals))
	for i, ord := range ordinals {
		fields[i] = IndexField{Ordinal: ord, Type: schema[ord].Type}
	}
	return fields
}

// encodeField encodes one index or primary-key part. prefix truncates strings
// the way a prefix index does. desc inverts the bytes so the part sorts descending.
func EncodeField(ctx context.Context, field IndexField, value interface{}) ([]byte, error) {
	if value != nil && field.Type != nil {
		if converted, _, err := field.Type.Convert(ctx, value); err == nil {
			value = converted
		}
	}
	if field.Prefix > 0 {
		value = prefixValue(value, field.Prefix)
	}
	if !byteOrderType(field.Type) {
		encoded, err := EncodeCollationKey(field.Type, value)
		if err != nil {
			return nil, err
		}
		if field.Desc {
			for i := range encoded {
				encoded[i] = ^encoded[i]
			}
		}
		return encoded, nil
	}
	return EncodeKeyPart(value, field.Desc), nil
}

// encodeCollationKey orders a string by its collation weight. Strings the
// collation treats as equal share a key, so a unique index rejects the second.
// The original bytes follow the weight so two strings that are not equal still
// have distinct keys when their weights collide. Equality seeks use the weight
// prefix from collationPrefix.
func EncodeCollationKey(typ sql.Type, value interface{}) ([]byte, error) {
	return CollationPrefix(typ, value, false)
}

func ValueText(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

// collationWeight is the big-endian rune weights. WriteWeightString writes
// little-endian bytes, which do not sort in weight order.
func CollationWeight(typ sql.Type, text string) ([]byte, error) {
	with, ok := typ.(sql.TypeWithCollation)
	if !ok {
		return []byte(text), nil
	}
	sorter := with.Collation().Sorter()
	if sorter == nil {
		return []byte(text), nil
	}
	buf := make([]byte, 0, len(text)*4)
	var part [4]byte
	for len(text) > 0 {
		r, size := utf8.DecodeRuneInString(text)
		if r == utf8.RuneError && size == 1 {
			return nil, fmt.Errorf("hardhatdb: malformed string for collation key")
		}
		binary.BigEndian.PutUint32(part[:], uint32(sorter(r)))
		buf = append(buf, part[:]...)
		text = text[size:]
	}
	return buf, nil
}

// collationPrefix is the ordered weight of one string, without the tie-breaker.
func CollationPrefix(typ sql.Type, value interface{}, desc bool) ([]byte, error) {
	if value == nil {
		part := []byte{0x00}
		if desc {
			part[0] = ^part[0]
		}
		return part, nil
	}
	weight, err := CollationWeight(typ, ValueText(value))
	if err != nil {
		return nil, err
	}
	part := EncodeTextKey(weight)
	if desc {
		for i := range part {
			part[i] = ^part[i]
		}
	}
	return part, nil
}

func EncodeKeyPart(value interface{}, descending bool) []byte {
	part := EncodeAscending(value)
	if descending {
		for i, b := range part {
			part[i] = ^b
		}
	}
	return part
}

func EncodeAscending(value interface{}) []byte {
	if value == nil {
		return []byte{0x00}
	}
	switch v := value.(type) {
	case string:
		return EncodeTextKey([]byte(v))
	case []byte:
		return EncodeTextKey(v)
	case bool:
		if v {
			return EncodeSigned(1)
		}
		return EncodeSigned(0)
	case int:
		return EncodeSigned(int64(v))
	case int8:
		return EncodeSigned(int64(v))
	case int16:
		return EncodeSigned(int64(v))
	case int32:
		return EncodeSigned(int64(v))
	case int64:
		return EncodeSigned(v)
	case uint:
		return EncodeUnsigned(uint64(v))
	case uint8:
		return EncodeUnsigned(uint64(v))
	case uint16:
		return EncodeUnsigned(uint64(v))
	case uint32:
		return EncodeUnsigned(uint64(v))
	case uint64:
		return EncodeUnsigned(v)
	case float32:
		return EncodeFloat(float64(v))
	case float64:
		return EncodeFloat(v)
	case time.Time:
		return EncodeTimeKey(v)
	case *apd.Decimal:
		return EncodeDecimalKey(v)
	case apd.Decimal:
		return EncodeDecimalKey(&v)
	case types.Timespan:
		return EncodeSigned(int64(v))
	case types.GeometryValue:
		return EncodeTextKey(v.Serialize())
	default:
		return EncodeTextKey([]byte(fmt.Sprint(v)))
	}
}

func EncodeSigned(v int64) []byte {
	var buf [9]byte
	buf[0] = 0x01
	binary.BigEndian.PutUint64(buf[1:], uint64(v)^(1<<63))
	return buf[:]
}

func EncodeUnsigned(v uint64) []byte {
	var buf [9]byte
	buf[0] = 0x01
	binary.BigEndian.PutUint64(buf[1:], v)
	return buf[:]
}

func EncodeFloat(v float64) []byte {
	if v == 0 {
		v = 0
	}
	bits := math.Float64bits(v)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	var buf [9]byte
	buf[0] = 0x01
	binary.BigEndian.PutUint64(buf[1:], bits)
	return buf[:]
}

// encodeTextKey terminates the bytes so "aa" sorts before "b". A leading
// length would sort by size. A zero byte is escaped so the terminator is unique.
func EncodeTextKey(b []byte) []byte {
	buf := make([]byte, 0, len(b)+4)
	buf = append(buf, 0x01)
	for _, c := range b {
		if c == 0x00 {
			buf = append(buf, 0x00, 0xFF)
			continue
		}
		buf = append(buf, c)
	}
	return append(buf, 0x00, 0x00)
}

func EncodeTimeKey(v time.Time) []byte {
	u := v.UTC()
	var buf [1 + 4 + 5 + 4]byte
	buf[0] = 0x01
	binary.BigEndian.PutUint32(buf[1:5], uint32(int32(u.Year()))^(1<<31))
	buf[5] = byte(u.Month())
	buf[6] = byte(u.Day())
	buf[7] = byte(u.Hour())
	buf[8] = byte(u.Minute())
	buf[9] = byte(u.Second())
	binary.BigEndian.PutUint32(buf[10:], uint32(u.Nanosecond()))
	return buf[:]
}

// encodeDecimalKey orders -10 < -2 < 0 < 2 < 10, and encodes 1.0 the same as 1.
func EncodeDecimalKey(d *apd.Decimal) []byte {
	if d == nil || d.Form != apd.Finite {
		if d != nil && d.Form == apd.Infinite {
			if d.Negative {
				return []byte{0x01, 0x00}
			}
			return []byte{0x01, 0xFE}
		}
		return []byte{0x01, 0xFF}
	}
	n := new(apd.Decimal)
	n.Reduce(d)
	if n.IsZero() {
		return []byte{0x01, 0x80}
	}
	neg := n.Negative
	n.Negative = false
	text := n.Coeff.Text(10)
	msdExp := int64(n.Exponent) + int64(len(text)) - 1
	body := EncodePositiveDecimal(msdExp, text)
	if neg {
		for i := range body {
			body[i] = ^body[i]
		}
	}
	out := make([]byte, 1+len(body))
	out[0] = 0x01
	copy(out[1:], body)
	return out
}

func EncodePositiveDecimal(msdExp int64, text string) []byte {
	var expb [4]byte
	binary.BigEndian.PutUint32(expb[:], uint32(int32(msdExp))^(1<<31))
	buf := make([]byte, 0, 1+4+len(text)+1)
	buf = append(buf, 0x81)
	buf = append(buf, expb[:]...)
	buf = append(buf, text...)
	return append(buf, 0x00)
}

// prefixEnd is the smallest key strictly after every key with this prefix.
// A nil result means the prefix is the maximum possible key.
func PrefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func IndexEntryKey(colKey, rowKey []byte, unique, hasNull bool) []byte {
	if unique && !hasNull {
		return append([]byte(nil), colKey...)
	}
	out := make([]byte, 0, len(colKey)+len(rowKey))
	out = append(out, colKey...)
	out = append(out, rowKey...)
	return out
}

func EncodeIndexColumns(ctx context.Context, fields []IndexField, row sql.Row) (colKey []byte, hasNull bool, err error) {
	var buf []byte
	for _, field := range fields {
		if field.Ordinal < 0 || field.Ordinal >= len(row) {
			return nil, false, fmt.Errorf("hardhatdb: index ordinal %d is outside the row", field.Ordinal)
		}
		if row[field.Ordinal] == nil {
			hasNull = true
		}
		part, err := seekPart(ctx, field, row[field.Ordinal])
		if err != nil {
			return nil, false, err
		}
		buf = append(buf, part...)
	}
	return buf, hasNull, nil
}

func seekPart(ctx context.Context, field IndexField, value interface{}) ([]byte, error) {
	if !byteOrderType(field.Type) {
		return CollationPrefix(field.Type, value, field.Desc)
	}
	return EncodeField(ctx, field, value)
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
