package persist

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/fulltext"
)

var _ fulltext.Database = (*Database)(nil)
var _ fulltext.IndexAlterableTable = (*Table)(nil)
var _ fulltext.Index = (*Index)(nil)
var _ fulltext.EditableTable = (*Table)(nil)

// CreateFulltextTableNames implements fulltext.Database. The config table is
// shared by every FULLTEXT index on the parent. The other four tables are
// unique to this index.
func (d *Database) CreateFulltextTableNames(ctx *sql.Context, parentTableName string, parentIndexName string) (fulltext.IndexTableNames, error) {
	names, err := d.GetTableNames(ctx)
	if err != nil {
		return fulltext.IndexTableNames{}, err
	}
	var tablePrefix string
OuterLoop:
	for i := uint64(0); true; i++ {
		tablePrefix = strings.ToLower(fmt.Sprintf("%s_%s_%d", parentTableName, parentIndexName, i))
		for _, tableName := range names {
			if strings.HasPrefix(strings.ToLower(tableName), tablePrefix) {
				continue OuterLoop
			}
		}
		break
	}
	return fulltext.IndexTableNames{
		Config:      fmt.Sprintf("%s_FTS_CONFIG", parentTableName),
		Position:    fmt.Sprintf("%s_FTS_POSITION", tablePrefix),
		DocCount:    fmt.Sprintf("%s_FTS_DOC_COUNT", tablePrefix),
		GlobalCount: fmt.Sprintf("%s_FTS_GLOBAL_COUNT", tablePrefix),
		RowCount:    fmt.Sprintf("%s_FTS_ROW_COUNT", tablePrefix),
	}, nil
}

// CreateFulltextIndex implements fulltext.IndexAlterableTable. The engine has
// already created the pseudo-index tables named in tableNames.
func (t *Table) CreateFulltextIndex(ctx *sql.Context, indexDef sql.IndexDef, keyCols fulltext.KeyColumns, tableNames fulltext.IndexTableNames) error {
	indexDef.Constraint = sql.IndexConstraint_Fulltext
	return t.appendStoredIndex(ctx, indexDef, &storedFulltext{
		Config:       tableNames.Config,
		Position:     tableNames.Position,
		DocCount:     tableNames.DocCount,
		GlobalCount:  tableNames.GlobalCount,
		RowCount:     tableNames.RowCount,
		KeyName:      keyCols.Name,
		KeyType:      byte(keyCols.Type),
		KeyPositions: append([]int(nil), keyCols.Positions...),
	})
}

// FullTextTableNames implements fulltext.Index.
func (idx *Index) FullTextTableNames(*sql.Context) (fulltext.IndexTableNames, error) {
	if idx.ft == nil {
		return fulltext.IndexTableNames{}, fmt.Errorf("index `%s` has no FULLTEXT tables", idx.name)
	}
	return fulltext.IndexTableNames{
		Config:      idx.ft.Config,
		Position:    idx.ft.Position,
		DocCount:    idx.ft.DocCount,
		GlobalCount: idx.ft.GlobalCount,
		RowCount:    idx.ft.RowCount,
	}, nil
}

// FullTextKeyColumns implements fulltext.Index.
func (idx *Index) FullTextKeyColumns(*sql.Context) (fulltext.KeyColumns, error) {
	if idx.ft == nil {
		return fulltext.KeyColumns{}, fmt.Errorf("index `%s` has no FULLTEXT key columns", idx.name)
	}
	return fulltext.KeyColumns{
		Name:      idx.ft.KeyName,
		Type:      fulltext.KeyType(idx.ft.KeyType),
		Positions: append([]int(nil), idx.ft.KeyPositions...),
	}, nil
}

// mustWriteEditor returns the editor for a write. A table with no FULLTEXT
// index gets the plain editor. A table with one wraps that editor so the
// pseudo-index tables stay in step with the parent rows.
func (t *Table) mustWriteEditor(ctx *sql.Context) sql.TableEditor {
	editor, err := t.writeEditor(ctx)
	if err != nil {
		return &errEditor{err: err}
	}
	return editor
}

func (t *Table) writeEditor(ctx *sql.Context) (sql.TableEditor, error) {
	parent := t.newEditor().bind(ctx)
	config, sets, err := t.fulltextSets(ctx)
	if err != nil {
		return nil, err
	}
	if len(sets) == 0 {
		return parent, nil
	}
	ftEditor, err := fulltext.CreateEditor(ctx, t, config, sets...)
	if err != nil {
		return nil, err
	}
	return fulltext.CreateMultiTableEditor(ctx, parent, ftEditor)
}

func (t *Table) fulltextSets(ctx *sql.Context) (fulltext.EditableTable, []fulltext.TableSet, error) {
	indexes, err := t.GetIndexes(ctx)
	if err != nil {
		return nil, nil, err
	}
	var config fulltext.EditableTable
	var sets []fulltext.TableSet
	db := t.database()
	for _, idx := range indexes {
		if !idx.IsFullText() {
			continue
		}
		ftIdx, ok := idx.(fulltext.Index)
		if !ok {
			return nil, nil, fmt.Errorf("index `%s` is FULLTEXT but does not implement fulltext.Index", idx.ID())
		}
		names, err := ftIdx.FullTextTableNames(ctx)
		if err != nil {
			return nil, nil, err
		}
		if config == nil {
			config, err = editableTable(ctx, db, names.Config)
			if err != nil {
				return nil, nil, err
			}
		}
		position, err := editableTable(ctx, db, names.Position)
		if err != nil {
			return nil, nil, err
		}
		docCount, err := editableTable(ctx, db, names.DocCount)
		if err != nil {
			return nil, nil, err
		}
		globalCount, err := editableTable(ctx, db, names.GlobalCount)
		if err != nil {
			return nil, nil, err
		}
		rowCount, err := editableTable(ctx, db, names.RowCount)
		if err != nil {
			return nil, nil, err
		}
		sets = append(sets, fulltext.TableSet{
			Index:       ftIdx,
			Position:    position,
			DocCount:    docCount,
			GlobalCount: globalCount,
			RowCount:    rowCount,
		})
	}
	return config, sets, nil
}

func editableTable(ctx *sql.Context, db *Database, name string) (fulltext.EditableTable, error) {
	tbl, ok, err := db.GetTableInsensitive(ctx, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("FULLTEXT table `%s` could not be found", name)
	}
	editable, ok := tbl.(fulltext.EditableTable)
	if !ok {
		return nil, fmt.Errorf("FULLTEXT table `%s` cannot be edited", name)
	}
	return editable, nil
}

// rebuildFulltext recreates every FULLTEXT index still declared on the table
// so its pseudo tables match the schema the rewrite just installed.
func (t *Table) rebuildFulltext(ctx *sql.Context) error {
	indexes, err := t.readIndexes()
	if err != nil || len(indexes) == 0 {
		return err
	}
	for _, idx := range indexes {
		if sql.IndexConstraint(idx.Constraint) == sql.IndexConstraint_Fulltext {
			return fulltext.RebuildTables(ctx, t, t.database())
		}
	}
	return nil
}

// dropFulltextTables removes the pseudo-index tables of indexes the rewrite
// dropped. The shared config table goes away only when no FULLTEXT index remains.
func (t *Table) dropFulltextTables(ctx *sql.Context, dropped, kept []storedIndex) error {
	if len(dropped) == 0 {
		return nil
	}
	remaining := false
	for _, idx := range kept {
		if sql.IndexConstraint(idx.Constraint) == sql.IndexConstraint_Fulltext {
			remaining = true
			break
		}
	}
	db := t.database()
	droppedConfig := make(map[string]struct{})
	for _, idx := range dropped {
		if idx.Fulltext == nil {
			continue
		}
		ft := idx.Fulltext
		if !remaining {
			if _, ok := droppedConfig[ft.Config]; !ok {
				droppedConfig[ft.Config] = struct{}{}
				if err := db.DropTable(ctx, ft.Config); err != nil {
					return err
				}
			}
		}
		for _, name := range []string{ft.Position, ft.DocCount, ft.GlobalCount, ft.RowCount} {
			if err := db.DropTable(ctx, name); err != nil {
				return err
			}
		}
	}
	return nil
}

// errEditor is returned when a write editor cannot be built. The inserter
// interfaces do not return that error themselves, so the first edit reports it.
type errEditor struct {
	err error
}

func (e *errEditor) StatementBegin(*sql.Context) {}

func (e *errEditor) DiscardChanges(*sql.Context, error) error { return e.err }

func (e *errEditor) StatementComplete(*sql.Context) error { return e.err }

func (e *errEditor) Insert(*sql.Context, sql.Row) error { return e.err }

func (e *errEditor) Update(*sql.Context, sql.Row, sql.Row) error { return e.err }

func (e *errEditor) Delete(*sql.Context, sql.Row) error { return e.err }

func (e *errEditor) Close(*sql.Context) error { return e.err }

func (e *errEditor) IndexedAccess(*sql.Context, sql.IndexLookup) sql.IndexedTable { return nil }

func (e *errEditor) GetIndexes(*sql.Context) ([]sql.Index, error) { return nil, e.err }

func (e *errEditor) PreciseMatch() bool { return true }

var _ sql.TableEditor = (*errEditor)(nil)
var _ sql.ForeignKeyEditor = (*errEditor)(nil)
