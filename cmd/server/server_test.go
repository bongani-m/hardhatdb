package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bongani-m/persist"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestSQLSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gms.db")
	store, engine := openEngine(t, path)

	ctx := newCtx(store)
	runQuery(t, ctx, engine, "CREATE DATABASE mydb")
	ctx.SetCurrentDatabase("mydb")
	runQuery(t, ctx, engine, "CREATE TABLE mytable (id char(36) primary key, name text, email text, phone_numbers json, created_at datetime(6))")
	runQuery(t, ctx, engine, "INSERT INTO mytable VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'Jane Deo', 'janedeo@gmail.com', '[]', '2022-11-01 12:00:00.000001')")
	runQuery(t, ctx, engine, "UPDATE mytable SET email = 'jane@example.com' WHERE name = 'Jane Deo'")
	rows := runQuery(t, ctx, engine, "SELECT email, phone_numbers FROM mytable WHERE name = 'Jane Deo'")
	require.Equal(t, "jane@example.com", rows[0][0])
	require.NoError(t, store.Close())

	store, engine = openEngine(t, path)
	t.Cleanup(func() { _ = store.Close() })
	ctx = newCtx(store)
	ctx.SetCurrentDatabase("mydb")
	rows = runQuery(t, ctx, engine, "SELECT email FROM mytable")
	require.Equal(t, []sql.Row{{"jane@example.com"}}, rows)
	runQuery(t, ctx, engine, "DELETE FROM mytable")
	require.Empty(t, runQuery(t, ctx, engine, "SELECT id FROM mytable"))
}

func openEngine(t *testing.T, path string) (*persist.Store, *sqle.Engine) {
	t.Helper()
	store, err := persist.Open(path)
	require.NoError(t, err)
	return store, sqle.NewDefault(store)
}

func newCtx(store *persist.Store) *sql.Context {
	sess := persist.NewSession(sql.NewBaseSession(), store)
	return sql.NewContext(context.Background(), sql.WithSession(sess))
}

func runQuery(t *testing.T, ctx *sql.Context, engine *sqle.Engine, q string) []sql.Row {
	t.Helper()
	_, iter, _, err := engine.Query(ctx, q)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)
	return rows
}
