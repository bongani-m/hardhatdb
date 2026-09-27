package sqle

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

func indexFields(schema sql.Schema, idx storedIndex) ([]indexField, error) {
	fields := make([]indexField, len(idx.Columns))
	for i, name := range idx.Columns {
		ord := columnOrdinal(schema, name)
		if ord < 0 {
			return nil, fmt.Errorf("persist: index %s column %s is not in the schema", idx.Name, name)
		}
		field := indexField{Ordinal: ord, Type: schema[ord].Type}
		if i < len(idx.Lengths) {
			field.Prefix = idx.Lengths[i]
		}
		if i < len(idx.Descending) {
			field.Desc = idx.Descending[i]
		}
		fields[i] = field
	}
	return fields, nil
}
