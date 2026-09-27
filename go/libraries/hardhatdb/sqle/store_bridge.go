package sqle

import (
	"context"

	"github.com/bongani-m/hardhatdb/go/store"
	"github.com/dolthub/go-mysql-server/sql"
)

type tableMeta = store.TableMeta
type indexField = store.IndexField

func collationPrefix(typ sql.Type, value interface{}, desc bool) ([]byte, error) {
	return store.CollationPrefix(typ, value, desc)
}

func decodeRow(ctx context.Context, schema sql.Schema, raw []byte) (sql.Row, error) {
	return store.DecodeRow(ctx, schema, raw)
}

func decodeSchema(raw []byte, dbName, tableName string) (tableMeta, error) {
	return store.DecodeSchema(raw, dbName, tableName)
}

func encodeField(ctx context.Context, field indexField, value interface{}) ([]byte, error) {
	return store.EncodeField(ctx, field, value)
}

func encodeIndexColumns(ctx context.Context, fields []indexField, row sql.Row) (colKey []byte, hasNull bool, err error) {
	return store.EncodeIndexColumns(ctx, fields, row)
}

func encodeKeyPart(value interface{}, descending bool) []byte {
	return store.EncodeKeyPart(value, descending)
}

func encodeRow(ctx context.Context, schema sql.Schema, row sql.Row) ([]byte, error) {
	return store.EncodeRow(ctx, schema, row)
}

func encodeRowJSON(ctx context.Context, row sql.Row) ([]byte, error) {
	return store.EncodeRowJSON(ctx, row)
}

func encodeSchema(ctx *sql.Context, sch sql.PrimaryKeySchema, collation sql.CollationID, comment string) ([]byte, error) {
	return store.EncodeSchema(ctx, sch, collation, comment)
}

func encodeSigned(v int64) []byte {
	return store.EncodeSigned(v)
}

func indexEntryKey(colKey, rowKey []byte, unique, hasNull bool) []byte {
	return store.IndexEntryKey(colKey, rowKey, unique, hasNull)
}

func pkFields(schema sql.Schema, ordinals []int) []indexField {
	return store.PkFields(schema, ordinals)
}

func pkString(ordinals []int, row sql.Row) string {
	return store.PkString(ordinals, row)
}

func prefixEnd(prefix []byte) []byte {
	return store.PrefixEnd(prefix)
}

func primaryKey(ctx context.Context, schema sql.Schema, ordinals []int, row sql.Row) ([]byte, error) {
	return store.PrimaryKey(ctx, schema, ordinals, row)
}

func sequenceKey(seq uint64) []byte {
	return store.SequenceKey(seq)
}
