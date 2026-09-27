package persist

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestRowsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gms.db")
	ctx := sql.NewContext(context.Background())
	created := time.Unix(0, 1667304000000001000).UTC()

	store := openAt(t, path)
	db := createPeopleTable(t, ctx, store)
	table := mustTable(t, ctx, db)

	jane := sql.NewRow(int64(1), "Jane Deo", "janedeo@gmail.com", types.MustJSON(`["556-565-566","777-777-777"]`), created)
	john := sql.NewRow(int64(2), "John Doe", "john@doe.com", types.MustJSON(`["555-555-555"]`), created)
	require.NoError(t, insertRows(ctx, table, jane, john))

	updated := jane.Copy()
	updated[2] = "jane@example.com"
	require.NoError(t, updateRow(ctx, table, jane, updated))
	require.NoError(t, deleteRow(ctx, table, john))
	require.NoError(t, store.Close())

	reopened := openAt(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	db, err := reopened.Database(ctx, "mydb")
	require.NoError(t, err)
	table = mustTable(t, ctx, db)
	rows := readRows(t, ctx, table)
	require.Len(t, rows, 1)
	require.Equal(t, int64(1), rows[0][0])
	require.Equal(t, "Jane Deo", rows[0][1])
	require.Equal(t, "jane@example.com", rows[0][2])
	phones, err := rows[0][3].(sql.JSONWrapper).ToInterface(ctx)
	require.NoError(t, err)
	require.Equal(t, []any{"556-565-566", "777-777-777"}, phones)
	gotTime, ok := rows[0][4].(time.Time)
	require.True(t, ok)
	require.True(t, created.Equal(gotTime))
}

func TestDuplicatePrimaryKey(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	table := mustTable(t, ctx, createPeopleTable(t, ctx, store))

	created := time.Unix(0, 1667304000000001000).UTC()
	row := sql.NewRow(int64(1), "Jane Deo", "janedeo@gmail.com", types.MustJSON(`[]`), created)
	require.NoError(t, insertRows(ctx, table, row))

	err := insertRows(ctx, table, row)
	require.Error(t, err)
	require.True(t, sql.ErrPrimaryKeyViolation.Is(err))
	require.Len(t, readRows(t, ctx, table), 1)
}

func TestTransactionRollbackDropsEdits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gms.db")
	store := openAt(t, path)
	base := sql.NewContext(context.Background())
	db := createPeopleTable(t, base, store)
	table := mustTable(t, base, db)

	sess := NewSession(sql.NewBaseSession(), store)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	tx, err := sess.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	ctx.SetTransaction(tx)
	ctx.SetIgnoreAutoCommit(true)

	created := time.Unix(0, 1667304000000001000).UTC()
	row := sql.NewRow(int64(1), "Jane Deo", "janedeo@gmail.com", types.MustJSON(`[]`), created)
	require.NoError(t, insertRows(ctx, table, row))
	require.Len(t, readRows(t, ctx, table), 1)

	require.NoError(t, sess.Rollback(ctx, tx))
	require.Empty(t, readRows(t, ctx, table))
	require.NoError(t, store.Close())

	reopened := openAt(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	db, err = reopened.Database(base, "mydb")
	require.NoError(t, err)
	require.Empty(t, readRows(t, base, mustTable(t, base, db)))
}

func openAt(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(path)
	require.NoError(t, err)
	return store
}

func createPeopleTable(t *testing.T, ctx *sql.Context, store *Store) sql.Database {
	t.Helper()
	require.NoError(t, store.CreateDatabase(ctx, "mydb"))
	db, err := store.Database(ctx, "mydb")
	require.NoError(t, err)
	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: "mytable", PrimaryKey: true, AutoIncrement: true},
		{Name: "name", Type: types.Text, Nullable: false, Source: "mytable"},
		{Name: "email", Type: types.Text, Nullable: false, Source: "mytable"},
		{Name: "phone_numbers", Type: types.JSON, Nullable: false, Source: "mytable"},
		{Name: "created_at", Type: types.MustCreateDatetimeType(query.Type_DATETIME, 6), Nullable: false, Source: "mytable"},
	})
	require.NoError(t, db.(sql.TableCreator).CreateTable(ctx, "mytable", schema, sql.Collation_Default, ""))
	return db
}

func mustTable(t *testing.T, ctx *sql.Context, db sql.Database) *Table {
	t.Helper()
	table, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(t, err)
	require.True(t, ok)
	return table.(*Table)
}

func insertRows(ctx *sql.Context, table sql.Table, rows ...sql.Row) error {
	inserter := table.(sql.InsertableTable).Inserter(ctx)
	inserter.StatementBegin(ctx)
	for _, row := range rows {
		if err := inserter.Insert(ctx, row); err != nil {
			_ = inserter.DiscardChanges(ctx, err)
			_ = inserter.Close(ctx)
			return err
		}
	}
	if err := inserter.StatementComplete(ctx); err != nil {
		return err
	}
	return inserter.Close(ctx)
}

func updateRow(ctx *sql.Context, table sql.Table, oldRow, newRow sql.Row) error {
	updater := table.(sql.UpdatableTable).Updater(ctx)
	updater.StatementBegin(ctx)
	if err := updater.Update(ctx, oldRow, newRow); err != nil {
		_ = updater.DiscardChanges(ctx, err)
		_ = updater.Close(ctx)
		return err
	}
	if err := updater.StatementComplete(ctx); err != nil {
		return err
	}
	return updater.Close(ctx)
}

func deleteRow(ctx *sql.Context, table sql.Table, row sql.Row) error {
	deleter := table.(sql.DeletableTable).Deleter(ctx)
	deleter.StatementBegin(ctx)
	if err := deleter.Delete(ctx, row); err != nil {
		_ = deleter.DiscardChanges(ctx, err)
		_ = deleter.Close(ctx)
		return err
	}
	if err := deleter.StatementComplete(ctx); err != nil {
		return err
	}
	return deleter.Close(ctx)
}

func readRows(t *testing.T, ctx *sql.Context, table sql.Table) []sql.Row {
	t.Helper()
	partitions, err := table.Partitions(ctx)
	require.NoError(t, err)
	defer partitions.Close(ctx)
	var rows []sql.Row
	for {
		partition, err := partitions.Next(ctx)
		if err != nil {
			break
		}
		iter, err := table.PartitionRows(ctx, partition)
		require.NoError(t, err)
		for {
			row, err := iter.Next(ctx)
			if err != nil {
				break
			}
			rows = append(rows, row)
		}
		require.NoError(t, iter.Close(ctx))
	}
	return rows
}
