package persist

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestKeyOrder(t *testing.T) {
	require.Negative(t, bytes.Compare(encodeKeyPart(int64(2), false), encodeKeyPart(int64(10), false)))
	require.Negative(t, bytes.Compare(encodeKeyPart(int64(-10), false), encodeKeyPart(int64(-2), false)))
	require.Negative(t, bytes.Compare(encodeKeyPart(int64(-2), false), encodeKeyPart(int64(2), false)))
	require.Negative(t, bytes.Compare(encodeKeyPart("aa", false), encodeKeyPart("b", false)))
	require.Negative(t, bytes.Compare(encodeKeyPart("a", false), encodeKeyPart("aa", false)))

	one := apd.New(1, 0)
	onePointZero := apd.New(10, -1)
	require.Equal(t, encodeKeyPart(one, false), encodeKeyPart(onePointZero, false))
	require.Negative(t, bytes.Compare(encodeKeyPart(apd.New(-10, 0), false), encodeKeyPart(apd.New(-2, 0), false)))
	require.Negative(t, bytes.Compare(encodeKeyPart(apd.New(-2, 0), false), encodeKeyPart(apd.New(2, 0), false)))
	require.Negative(t, bytes.Compare(encodeKeyPart(apd.New(2, 0), false), encodeKeyPart(apd.New(10, 0), false)))
}

func TestPrimaryKeyPointGetSeesUncommittedInsert(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	table := mustNamedTable(t, ctx, createIntTable(t, ctx, store, "nums"), "nums")

	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(2)), sql.NewRow(int64(10))))

	sess := NewSession(sql.NewBaseSession(), store)
	txCtx := sql.NewContext(context.Background(), sql.WithSession(sess))
	tx, err := sess.StartTransaction(txCtx, sql.ReadWrite)
	require.NoError(t, err)
	txCtx.SetTransaction(tx)
	txCtx.SetIgnoreAutoCommit(true)
	require.NoError(t, insertRows(txCtx, table, sql.NewRow(int64(1))))

	got := indexLookup(t, txCtx, table, primaryOf(t, txCtx, table), sql.MySQLRangeCollection{
		{sql.ClosedRangeColumnExpr(int64(1), int64(1), types.Int64)},
	}, false)
	require.Equal(t, []sql.Row{{int64(1)}}, got)

	missing := indexLookup(t, txCtx, table, primaryOf(t, txCtx, table), sql.MySQLRangeCollection{
		{sql.ClosedRangeColumnExpr(int64(3), int64(3), types.Int64)},
	}, false)
	require.Empty(t, missing)

	both := indexLookup(t, txCtx, table, primaryOf(t, txCtx, table), sql.MySQLRangeCollection{
		{sql.ClosedRangeColumnExpr(int64(1), int64(1), types.Int64)},
		{sql.ClosedRangeColumnExpr(int64(10), int64(10), types.Int64)},
	}, false)
	require.Equal(t, []sql.Row{{int64(1)}, {int64(10)}}, both)
}

func TestIntegerAndStringKeysSort(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	table := mustNamedTable(t, ctx, createIntTable(t, ctx, store, "nums"), "nums")
	require.NoError(t, insertRows(ctx, table,
		sql.NewRow(int64(10)),
		sql.NewRow(int64(2)),
		sql.NewRow(int64(-2)),
		sql.NewRow(int64(-10)),
	))

	require.Equal(t, []sql.Row{
		{int64(-10)},
		{int64(-2)},
		{int64(2)},
		{int64(10)},
	}, readRows(t, ctx, table))

	primary := primaryOf(t, ctx, table)
	require.Equal(t, sql.IndexOrderAsc, primary.Order(ctx))
	require.True(t, primary.Reversible(ctx))

	greater := indexLookup(t, ctx, table, primary, sql.MySQLRangeCollection{
		{sql.GreaterThanRangeColumnExpr(int64(2), types.Int64)},
	}, false)
	require.Equal(t, []sql.Row{{int64(10)}}, greater)

	atLeast := indexLookup(t, ctx, table, primary, sql.MySQLRangeCollection{
		{sql.GreaterOrEqualRangeColumnExpr(int64(2), types.Int64)},
	}, false)
	require.Equal(t, []sql.Row{{int64(2)}, {int64(10)}}, atLeast)

	less := indexLookup(t, ctx, table, primary, sql.MySQLRangeCollection{
		{sql.LessThanRangeColumnExpr(int64(2), types.Int64)},
	}, false)
	require.Equal(t, []sql.Row{{int64(-10)}, {int64(-2)}}, less)

	desc := indexLookup(t, ctx, table, primary, sql.MySQLRangeCollection{
		{sql.AllRangeColumnExpr(types.Int64)},
	}, true)
	require.Equal(t, []sql.Row{
		{int64(10)},
		{int64(2)},
		{int64(-2)},
		{int64(-10)},
	}, desc)

	words := mustNamedTable(t, ctx, createStringTable(t, ctx, store, "words"), "words")
	require.NoError(t, insertRows(ctx, words, sql.NewRow("b"), sql.NewRow("aa"), sql.NewRow("a")))
	require.Equal(t, []sql.Row{{"a"}, {"aa"}, {"b"}}, readRows(t, ctx, words))
	beforeB := indexLookup(t, ctx, words, primaryOf(t, ctx, words), sql.MySQLRangeCollection{
		{sql.LessThanRangeColumnExpr("b", types.Text)},
	}, false)
	require.Equal(t, []sql.Row{{"a"}, {"aa"}}, beforeB)
}

func TestUniqueSecondaryConflictIsAPointGet(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	db := createEmailTable(t, ctx, store)
	table := mustNamedTable(t, ctx, db, "people")

	require.NoError(t, insertRows(ctx, table,
		sql.NewRow(int64(1), "other@example.com"),
		sql.NewRow(int64(2), "ada@example.com"),
		sql.NewRow(int64(3), nil),
	))
	require.NoError(t, table.CreateIndex(ctx, sql.IndexDef{
		Name:       "email_uidx",
		Columns:    []sql.IndexColumn{{Name: "email"}},
		Constraint: sql.IndexConstraint_Unique,
	}))
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(4), nil)))

	err := insertRows(ctx, table, sql.NewRow(int64(5), "ada@example.com"))
	require.Error(t, err)
	require.True(t, sql.ErrUniqueKeyViolation.Is(err))
	require.Len(t, readRows(t, ctx, table), 4)

	emailIdx := indexByName(t, ctx, table, "email_uidx")
	got := indexLookup(t, ctx, table, emailIdx, sql.MySQLRangeCollection{
		{sql.ClosedRangeColumnExpr("ada@example.com", "ada@example.com", types.Text)},
	}, false)
	require.Equal(t, []sql.Row{{int64(2), "ada@example.com"}}, got)

	require.NoError(t, table.CreateIndex(ctx, sql.IndexDef{
		Name:    "email_idx",
		Columns: []sql.IndexColumn{{Name: "email"}},
	}))
	require.NoError(t, table.DropIndex(ctx, "email_uidx"))
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(6), "ada@example.com")))
	shared := indexLookup(t, ctx, table, indexByName(t, ctx, table, "email_idx"), sql.MySQLRangeCollection{
		{sql.ClosedRangeColumnExpr("ada@example.com", "ada@example.com", types.Text)},
	}, false)
	require.Equal(t, []sql.Row{{int64(2), "ada@example.com"}, {int64(6), "ada@example.com"}}, shared)
}

func TestDescendingIndexRange(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	db := createTable(t, ctx, store, "nums", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: "nums", PrimaryKey: true},
		{Name: "n", Type: types.Int64, Nullable: false, Source: "nums"},
	}))
	table := mustNamedTable(t, ctx, db, "nums")
	require.NoError(t, insertRows(ctx, table,
		sql.NewRow(int64(1), int64(-10)),
		sql.NewRow(int64(2), int64(-2)),
		sql.NewRow(int64(3), int64(2)),
		sql.NewRow(int64(4), int64(10)),
	))
	require.NoError(t, table.CreateIndex(ctx, sql.IndexDef{
		Name: "n_desc",
		Columns: []sql.IndexColumn{{
			Name:  "n",
			Order: &sql.IndexColumnOrder{Descending: true},
		}},
	}))
	idx := indexByName(t, ctx, table, "n_desc")
	require.Equal(t, sql.IndexOrderDesc, idx.Order(ctx))
	require.True(t, idx.Reversible(ctx))

	less := indexLookup(t, ctx, table, idx, sql.MySQLRangeCollection{
		{sql.LessThanRangeColumnExpr(int64(2), types.Int64)},
	}, false)
	require.Equal(t, []sql.Row{{int64(2), int64(-2)}, {int64(1), int64(-10)}}, less)

	greater := indexLookup(t, ctx, table, idx, sql.MySQLRangeCollection{
		{sql.GreaterThanRangeColumnExpr(int64(2), types.Int64)},
	}, false)
	require.Equal(t, []sql.Row{{int64(4), int64(10)}}, greater)

	equal := indexLookup(t, ctx, table, idx, sql.MySQLRangeCollection{
		{sql.ClosedRangeColumnExpr(int64(2), int64(2), types.Int64)},
	}, false)
	require.Equal(t, []sql.Row{{int64(3), int64(2)}}, equal)
}

func TestDescendingCompositeRange(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	db := createTable(t, ctx, store, "p", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "pk", Type: types.Int64, Nullable: false, Source: "p", PrimaryKey: true},
		{Name: "s", Type: types.Text, Nullable: true, Source: "p"},
		{Name: "n2", Type: types.Int64, Nullable: true, Source: "p"},
	}))
	table := mustNamedTable(t, ctx, db, "p")
	require.NoError(t, insertRows(ctx, table,
		sql.NewRow(int64(1), "apple", int64(1)),
		sql.NewRow(int64(2), "apricot", int64(2)),
		sql.NewRow(int64(3), "banana", int64(3)),
		sql.NewRow(int64(5), "app", int64(5)),
	))
	require.NoError(t, table.CreateIndex(ctx, sql.IndexDef{
		Name: "idx4",
		Columns: []sql.IndexColumn{
			{Name: "n2", Order: &sql.IndexColumnOrder{Descending: true}},
			{Name: "s"},
		},
	}))
	idx := indexByName(t, ctx, table, "idx4")
	rang := sql.MySQLRangeCollection{
		{sql.GreaterThanRangeColumnExpr(int64(1), types.Int64), sql.AllRangeColumnExpr(types.Text)},
	}
	got := indexLookup(t, ctx, table, idx, rang, false)
	require.Equal(t, []int64{5, 3, 2}, pks(t, got))
	rev := indexLookup(t, ctx, table, idx, rang, true)
	require.Equal(t, []int64{2, 3, 5}, pks(t, rev))

	require.NoError(t, table.CreateIndex(ctx, sql.IndexDef{
		Name: "idx5",
		Columns: []sql.IndexColumn{
			{Name: "n2"},
			{Name: "pk", Order: &sql.IndexColumnOrder{Descending: true}},
		},
	}))
	asc := indexByName(t, ctx, table, "idx5")
	forward := indexLookup(t, ctx, table, asc, rang, false)
	require.Equal(t, []int64{2, 3, 5}, pks(t, forward))
	backward := indexLookup(t, ctx, table, asc, rang, true)
	require.Equal(t, []int64{5, 3, 2}, pks(t, backward))
	require.NoError(t, table.DropIndex(ctx, "idx5"))
	require.Equal(t, []int64{5, 3, 2}, pks(t, indexLookup(t, ctx, table, idx, rang, false)))
	require.NoError(t, table.AddColumn(ctx, &sql.Column{Name: "m", Type: types.Int64, Nullable: true, Source: "p"}, nil))
	require.Equal(t, []int64{5, 3, 2}, pks(t, indexLookup(t, ctx, table, indexByName(t, ctx, table, "idx4"), rang, false)))
}

func pks(t *testing.T, rows []sql.Row) []int64 {
	t.Helper()
	out := make([]int64, len(rows))
	for i, row := range rows {
		out[i] = row[0].(int64)
	}
	return out
}

func TestBinaryRowsRoundTrip(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	path := filepath.Join(t.TempDir(), "gms.db")
	store := openAt(t, path)
	db := createWideTable(t, ctx, store)
	table := mustNamedTable(t, ctx, db, "wide")

	created := time.Unix(0, 1667304000000001000).UTC()
	decType := types.MustCreateDecimalType(10, 1)
	dec, _, err := decType.Convert(ctx, "1.0")
	require.NoError(t, err)
	span, _, err := types.Time.Convert(ctx, "01:02:03")
	require.NoError(t, err)
	row := sql.NewRow(
		int64(1),
		uint64(42),
		float64(1.5),
		dec,
		[]byte{0, 1, 255},
		"ada",
		created,
		span,
		types.MustJSON(`{"n":1}`),
		nil,
		true,
	)
	require.NoError(t, insertRows(ctx, table, row))
	require.NoError(t, store.Close())

	reopened := openAt(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	db, err = reopened.Database(ctx, "mydb")
	require.NoError(t, err)
	got := readRows(t, ctx, mustNamedTable(t, ctx, db, "wide"))
	require.Len(t, got, 1)
	require.Equal(t, int64(1), got[0][0])
	require.Equal(t, uint64(42), got[0][1])
	require.Equal(t, float64(1.5), got[0][2])
	cmp, err := decType.Compare(ctx, dec, got[0][3])
	require.NoError(t, err)
	require.Zero(t, cmp)
	require.Equal(t, []byte{0, 1, 255}, got[0][4])
	require.Equal(t, "ada", got[0][5])
	gotTime, ok := got[0][6].(time.Time)
	require.True(t, ok)
	require.True(t, created.Equal(gotTime))
	require.Equal(t, span, got[0][7])
	doc, err := got[0][8].(sql.JSONWrapper).ToInterface(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"n": float64(1)}, doc)
	require.Nil(t, got[0][9])
	require.Equal(t, int8(1), got[0][10])

	legacy, err := encodeRowJSON(ctx, row)
	require.NoError(t, err)
	decoded, err := decodeRow(ctx, mustNamedTable(t, ctx, db, "wide").meta.schema, legacy)
	require.NoError(t, err)
	require.Equal(t, "ada", decoded[5])
}

func TestRowCountIncludesPendingInsert(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	table := mustNamedTable(t, ctx, createIntTable(t, ctx, store, "nums"), "nums")
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(2)), sql.NewRow(int64(10))))

	count, exact, err := table.RowCount(ctx)
	require.NoError(t, err)
	require.True(t, exact)
	require.Equal(t, uint64(2), count)
	before, err := table.DataLength(ctx)
	require.NoError(t, err)
	require.NotZero(t, before)

	sess := NewSession(sql.NewBaseSession(), store)
	txCtx := sql.NewContext(context.Background(), sql.WithSession(sess))
	tx, err := sess.StartTransaction(txCtx, sql.ReadWrite)
	require.NoError(t, err)
	txCtx.SetTransaction(tx)
	txCtx.SetIgnoreAutoCommit(true)
	require.NoError(t, insertRows(txCtx, table, sql.NewRow(int64(1))))

	count, exact, err = table.RowCount(txCtx)
	require.NoError(t, err)
	require.True(t, exact)
	require.Equal(t, uint64(len(readRows(t, txCtx, table))), count)
	require.Equal(t, uint64(3), count)
	after, err := table.DataLength(txCtx)
	require.NoError(t, err)
	require.Greater(t, after, before)
}

func TestKeylessInsertKeepsSequence(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	path := filepath.Join(t.TempDir(), "gms.db")
	store := openAt(t, path)
	db := createKeylessTable(t, ctx, store)
	table := mustNamedTable(t, ctx, db, "notes")

	ed := table.newEditor().bind(ctx)
	require.NoError(t, ed.Insert(ctx, sql.NewRow("gone")))
	require.NoError(t, ed.Delete(ctx, sql.NewRow("gone")))
	require.NoError(t, ed.Close(ctx))

	require.NoError(t, insertRows(ctx, table, sql.NewRow("alpha"), sql.NewRow("beta")))
	keys := rawRowKeys(t, store, "mydb", "notes")
	require.Equal(t, [][]byte{sequenceKey(1), sequenceKey(2)}, keys)
	require.NoError(t, store.Close())

	reopened := openAt(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	require.Equal(t, keys, rawRowKeys(t, reopened, "mydb", "notes"))
	db, err := reopened.Database(ctx, "mydb")
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{"alpha"}, {"beta"}}, readRows(t, ctx, mustNamedTable(t, ctx, db, "notes")))
}

func TestLegacyJSONRowIsRewritten(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	path := filepath.Join(t.TempDir(), "gms.db")
	store := openAt(t, path)
	createPeopleTable(t, ctx, store)
	created := time.Unix(0, 1667304000000001000).UTC()
	row := sql.NewRow(int64(10), "Ada", "ada@example.com", types.MustJSON(`[]`), created)
	raw, err := encodeRowJSON(ctx, row)
	require.NoError(t, err)
	require.NoError(t, store.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, "mydb", "mytable")
		if err := bucket.Delete(keyFormat); err != nil {
			return err
		}
		return bucket.Bucket(bucketRows).Put([]byte("legacy"), raw)
	}))
	require.NoError(t, store.Close())

	reopened := openAt(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	db, err := reopened.Database(ctx, "mydb")
	require.NoError(t, err)
	table := mustTable(t, ctx, db)
	rows := readRows(t, ctx, table)
	require.Equal(t, []sql.Row{row}, []sql.Row{rows[0]})
	require.Equal(t, [][]byte{mustPrimaryKey(t, ctx, table, row)}, rawRowKeys(t, reopened, "mydb", "mytable"))

	two := sql.NewRow(int64(2), "Bea", "bea@example.com", types.MustJSON(`[]`), created)
	require.NoError(t, insertRows(ctx, table, two))
	got := readRows(t, ctx, table)
	require.Equal(t, int64(2), got[0][0])
	require.Equal(t, int64(10), got[1][0])
}

func TestBulkLoadOpen(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	path := filepath.Join(t.TempDir(), "gms.db")
	store, err := OpenWithOptions(path, OpenOptions{BulkLoad: true})
	require.NoError(t, err)
	table := mustNamedTable(t, ctx, createIntTable(t, ctx, store, "nums"), "nums")
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(2)), sql.NewRow(int64(10))))
	require.NoError(t, store.Close())

	reopened := openAt(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	db, err := reopened.Database(ctx, "mydb")
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(2)}, {int64(10)}}, readRows(t, ctx, mustNamedTable(t, ctx, db, "nums")))
}

func createIntTable(t *testing.T, ctx *sql.Context, store *Store, name string) sql.Database {
	t.Helper()
	return createTable(t, ctx, store, name, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: name, PrimaryKey: true},
	}))
}

func createStringTable(t *testing.T, ctx *sql.Context, store *Store, name string) sql.Database {
	t.Helper()
	return createTable(t, ctx, store, name, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "name", Type: types.Text, Nullable: false, Source: name, PrimaryKey: true},
	}))
}

func createEmailTable(t *testing.T, ctx *sql.Context, store *Store) sql.Database {
	t.Helper()
	return createTable(t, ctx, store, "people", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: "people", PrimaryKey: true},
		{Name: "email", Type: types.Text, Nullable: true, Source: "people"},
	}))
}

func createKeylessTable(t *testing.T, ctx *sql.Context, store *Store) sql.Database {
	t.Helper()
	return createTable(t, ctx, store, "notes", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "name", Type: types.Text, Nullable: false, Source: "notes"},
	}))
}

func createWideTable(t *testing.T, ctx *sql.Context, store *Store) sql.Database {
	t.Helper()
	return createTable(t, ctx, store, "wide", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: "wide", PrimaryKey: true},
		{Name: "n", Type: types.Uint64, Nullable: false, Source: "wide"},
		{Name: "f", Type: types.Float64, Nullable: false, Source: "wide"},
		{Name: "d", Type: types.MustCreateDecimalType(10, 1), Nullable: false, Source: "wide"},
		{Name: "b", Type: types.Blob, Nullable: false, Source: "wide"},
		{Name: "s", Type: types.Text, Nullable: false, Source: "wide"},
		{Name: "created_at", Type: types.MustCreateDatetimeType(query.Type_DATETIME, 6), Nullable: false, Source: "wide"},
		{Name: "clock", Type: types.Time, Nullable: false, Source: "wide"},
		{Name: "doc", Type: types.JSON, Nullable: false, Source: "wide"},
		{Name: "note", Type: types.Text, Nullable: true, Source: "wide"},
		{Name: "flag", Type: types.Boolean, Nullable: false, Source: "wide"},
	}))
}

func createTable(t *testing.T, ctx *sql.Context, store *Store, name string, schema sql.PrimaryKeySchema) sql.Database {
	t.Helper()
	if err := store.CreateDatabase(ctx, "mydb"); err != nil && !sql.ErrDatabaseExists.Is(err) {
		require.NoError(t, err)
	}
	db, err := store.Database(ctx, "mydb")
	require.NoError(t, err)
	require.NoError(t, db.(sql.TableCreator).CreateTable(ctx, name, schema, sql.Collation_Default, ""))
	return db
}

func mustNamedTable(t *testing.T, ctx *sql.Context, db sql.Database, name string) *Table {
	t.Helper()
	table, ok, err := db.GetTableInsensitive(ctx, name)
	require.NoError(t, err)
	require.True(t, ok)
	return table.(*Table)
}

func primaryOf(t *testing.T, ctx *sql.Context, table *Table) *Index {
	t.Helper()
	indexes, err := table.GetIndexes(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, indexes)
	idx, ok := indexes[0].(*Index)
	require.True(t, ok)
	require.Equal(t, "PRIMARY", idx.ID())
	return idx
}

func indexByName(t *testing.T, ctx *sql.Context, table *Table, name string) *Index {
	t.Helper()
	indexes, err := table.GetIndexes(ctx)
	require.NoError(t, err)
	for _, idx := range indexes {
		if idx.ID() == name {
			return idx.(*Index)
		}
	}
	t.Fatalf("missing index %s", name)
	return nil
}

func indexLookup(t *testing.T, ctx *sql.Context, table *Table, idx sql.Index, ranges sql.MySQLRangeCollection, reverse bool) []sql.Row {
	t.Helper()
	lookup := sql.NewIndexLookup(idx, ranges, false, false, false, reverse)
	access := table.IndexedAccess(ctx, lookup)
	iter, err := access.(sql.IndexedTable).PartitionRows(ctx, nil)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)
	return rows
}

func rawRowKeys(t *testing.T, store *Store, dbName, tableName string) [][]byte {
	t.Helper()
	var keys [][]byte
	require.NoError(t, store.view(func(tx *kvTx) error {
		rows := rowsBucket(tx, dbName, tableName)
		return rows.forEachRaw(func(k, _ []byte) error {
			keys = append(keys, append([]byte(nil), k...))
			return nil
		})
	}))
	return keys
}

func TestCollationPrimaryKeyLookup(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	text, err := types.CreateString(query.Type_VARCHAR, 84, sql.Collation_utf8mb4_general_ci)
	require.NoError(t, err)
	db := createTable(t, ctx, store, "words", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "word", Type: text, Nullable: false, Source: "words", PrimaryKey: true},
		{Name: "n", Type: types.Int64, Nullable: false, Source: "words"},
	}))
	table := mustNamedTable(t, ctx, db, "words")
	require.NoError(t, insertRows(ctx, table, sql.NewRow("aaaa", int64(1)), sql.NewRow("ghi", int64(2))))
	idx := indexByName(t, ctx, table, "PRIMARY")
	got := indexLookup(t, ctx, table, idx, sql.MySQLRangeCollection{
		{sql.ClosedRangeColumnExpr("aaaa", "aaaa", text)},
	}, false)
	require.Equal(t, []sql.Row{{"aaaa", int64(1)}}, got)
	folded := indexLookup(t, ctx, table, idx, sql.MySQLRangeCollection{
		{sql.ClosedRangeColumnExpr("AAAA", "AAAA", text)},
	}, false)
	require.Equal(t, []sql.Row{{"aaaa", int64(1)}}, folded)
}

func mustPrimaryKey(t *testing.T, ctx *sql.Context, table *Table, row sql.Row) []byte {
	t.Helper()
	key, err := primaryKey(ctx, table.meta.schema, table.meta.pk, row)
	require.NoError(t, err)
	return key
}
