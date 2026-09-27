package persist

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestLostUpdate(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	orig := personRow(1, "Jane", "jane@b.c")
	require.NoError(t, insertRows(base, table, orig))

	sessA, ctxA := beginSession(t, store)
	sessB, ctxB := beginSession(t, store)
	winner := personRow(1, "Jane", "winner@b.c")
	loser := personRow(1, "Jane", "loser@b.c")
	require.NoError(t, updateRow(ctxA, table, orig, winner))

	done := make(chan error, 1)
	go func() {
		done <- updateRow(ctxB, table, orig, loser)
	}()
	store.locksFor().awaitWaiting(sessB)
	require.NoError(t, sessA.CommitTransaction(ctxA, ctxA.GetTransaction()))
	require.NoError(t, <-done)
	require.NoError(t, sessB.CommitTransaction(ctxB, ctxB.GetTransaction()))
	require.Equal(t, "loser@b.c", readRows(t, base, table)[0][2])
}

func TestSameTransactionRewritesRow(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	orig := personRow(1, "Jane", "jane@b.c")
	require.NoError(t, insertRows(base, table, orig))

	sess, ctx := beginSession(t, store)
	mid := personRow(1, "Jane", "mid@b.c")
	final := personRow(1, "Jane", "final@b.c")
	require.NoError(t, updateRow(ctx, table, orig, mid))
	require.NoError(t, updateRow(ctx, table, mid, final))
	require.NoError(t, sess.CommitTransaction(ctx, ctx.GetTransaction()))
	require.Equal(t, "final@b.c", readRows(t, base, table)[0][2])
}

func TestConcurrentAutoIncrement(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))

	const n = 8
	ids := make([]uint64, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ids[i], errs[i] = table.GetNextAutoIncrementValue(sql.NewContext(context.Background()), nil)
		}(i)
	}
	close(start)
	wg.Wait()

	seen := make(map[uint64]struct{}, n)
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		seen[ids[i]] = struct{}{}
	}
	require.Len(t, seen, n)
}

func TestAutoIncrementRange(t *testing.T) {
	store, base, table := openPeopleTable(t)
	ctx := sql.NewContext(context.Background())

	id, err := table.GetNextAutoIncrementValue(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)
	require.Equal(t, uint64(1+autoIncrementStep), storedAutoInc(t, store, table))

	for want := uint64(2); want <= 5; want++ {
		id, err = table.GetNextAutoIncrementValue(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, want, id)
	}
	require.Equal(t, uint64(1+autoIncrementStep), storedAutoInc(t, store, table))

	for want := uint64(6); want <= autoIncrementStep; want++ {
		id, err = table.GetNextAutoIncrementValue(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, want, id)
	}
	require.Equal(t, uint64(1+autoIncrementStep), storedAutoInc(t, store, table))

	id, err = table.GetNextAutoIncrementValue(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(autoIncrementStep+1), id)
	require.Equal(t, uint64(1+2*autoIncrementStep), storedAutoInc(t, store, table))

	peek, err := table.PeekNextAutoIncrementValue(base)
	require.NoError(t, err)
	require.Equal(t, uint64(autoIncrementStep+2), peek)
}

func TestAutoIncrementExplicitInsideRange(t *testing.T) {
	store, _, table := openPeopleTable(t)
	ctx := sql.NewContext(context.Background())

	_, err := table.GetNextAutoIncrementValue(ctx, nil)
	require.NoError(t, err)
	_, err = table.GetNextAutoIncrementValue(ctx, int64(50))
	require.NoError(t, err)
	require.NoError(t, store.observeAutoIncrement(ctx, table, 50))

	id, err := table.GetNextAutoIncrementValue(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(51), id)
	require.Equal(t, uint64(1+autoIncrementStep), storedAutoInc(t, store, table))
	id, err = table.GetNextAutoIncrementValue(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(52), id)
}

func TestAutoIncrementExplicitPastRange(t *testing.T) {
	store, _, table := openPeopleTable(t)
	ctx := sql.NewContext(context.Background())

	_, err := table.GetNextAutoIncrementValue(ctx, nil)
	require.NoError(t, err)
	_, err = table.GetNextAutoIncrementValue(ctx, int64(5000))
	require.NoError(t, err)
	require.NoError(t, store.observeAutoIncrement(ctx, table, 5000))

	id, err := table.GetNextAutoIncrementValue(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(5001), id)
}

func TestAutoIncrementLeadershipDropsRange(t *testing.T) {
	store, _, table := openPeopleTable(t)
	ctx := sql.NewContext(context.Background())

	_, err := table.GetNextAutoIncrementValue(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1+autoIncrementStep), storedAutoInc(t, store, table))

	store.onLeadership(false)
	store.onLeadership(true)

	id, err := table.GetNextAutoIncrementValue(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1+autoIncrementStep), id)
}

func TestAutoIncrementRecreateStartsOver(t *testing.T) {
	store, base, table := openPeopleTable(t)
	ctx := sql.NewContext(context.Background())
	db, err := store.Database(base, "mydb")
	require.NoError(t, err)

	_, err = table.GetNextAutoIncrementValue(ctx, nil)
	require.NoError(t, err)

	pk := table.PrimaryKeySchema(base)
	collation := table.Collation()
	require.NoError(t, db.(sql.TableDropper).DropTable(base, "mytable"))
	require.NoError(t, db.(sql.TableCreator).CreateTable(base, "mytable", pk, collation, ""))
	table = mustTable(t, base, db)

	id, err := table.GetNextAutoIncrementValue(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)
}

func openPeopleTable(t *testing.T) (*Store, *sql.Context, *Table) {
	t.Helper()
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	db := createPeopleTable(t, base, store)
	return store, base, mustTable(t, base, db)
}

func storedAutoInc(t *testing.T, store *Store, table *Table) uint64 {
	t.Helper()
	stored, err := store.storedAutoIncrement(table)
	require.NoError(t, err)
	return stored
}

func TestSavepoints(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	sess, ctx := beginSession(t, store)
	tx := ctx.GetTransaction()

	require.NoError(t, insertRows(ctx, table, personRow(1, "one", "one@b.c")))
	require.NoError(t, sess.CreateSavepoint(ctx, tx, "a"))
	require.NoError(t, insertRows(ctx, table, personRow(2, "two", "two@b.c")))
	require.NoError(t, sess.CreateSavepoint(ctx, tx, "b"))
	require.NoError(t, insertRows(ctx, table, personRow(3, "three", "three@b.c")))
	require.NoError(t, sess.RollbackToSavepoint(ctx, tx, "a"))
	require.Error(t, sess.RollbackToSavepoint(ctx, tx, "b"))
	require.True(t, sql.ErrSavepointDoesNotExist.Is(sess.RollbackToSavepoint(ctx, tx, "b")))

	require.NoError(t, sess.CreateSavepoint(ctx, tx, "a"))
	require.NoError(t, insertRows(ctx, table, personRow(4, "four", "four@b.c")))
	require.NoError(t, sess.CreateSavepoint(ctx, tx, "c"))
	require.NoError(t, sess.ReleaseSavepoint(ctx, tx, "a"))
	require.True(t, sql.ErrSavepointDoesNotExist.Is(sess.RollbackToSavepoint(ctx, tx, "c")))
	require.NoError(t, sess.CommitTransaction(ctx, tx))

	rows := readRows(t, base, table)
	require.Len(t, rows, 2)
	require.Equal(t, int64(1), rows[0][0])
	require.Equal(t, int64(4), rows[1][0])
}

func TestSavepointRollsBackOpenEditor(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	sess, ctx := beginSession(t, store)
	tx := ctx.GetTransaction()

	require.NoError(t, insertRows(ctx, table, personRow(1, "one", "one@b.c")))
	require.NoError(t, sess.CreateSavepoint(ctx, tx, "s"))

	inserter := table.Inserter(ctx)
	inserter.StatementBegin(ctx)
	require.NoError(t, inserter.Insert(ctx, personRow(2, "two", "two@b.c")))
	require.NoError(t, sess.RollbackToSavepoint(ctx, tx, "s"))
	require.NoError(t, inserter.Close(ctx))
	require.NoError(t, sess.CommitTransaction(ctx, tx))

	rows := readRows(t, base, table)
	require.Len(t, rows, 1)
	require.Equal(t, int64(1), rows[0][0])
}

func TestLockingReadConflict(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	orig := personRow(1, "Jane", "jane@b.c")
	require.NoError(t, insertRows(base, table, orig))

	sess, ctx := beginSession(t, store)
	sess.SetLockingRead(true)
	require.Len(t, readRows(t, ctx, table), 1)
	sess.SetLockingRead(false)

	sessB, ctxB := beginSession(t, store)
	done := make(chan error, 1)
	go func() {
		done <- updateRow(ctxB, table, orig, personRow(1, "Jane", "other@b.c"))
	}()
	store.locksFor().awaitWaiting(sessB)
	require.NoError(t, sess.CommitTransaction(ctx, ctx.GetTransaction()))
	require.NoError(t, <-done)
	require.NoError(t, sessB.CommitTransaction(ctxB, ctxB.GetTransaction()))
	require.Equal(t, "other@b.c", readRows(t, base, table)[0][2])
}

func TestRepeatableReadSnapshot(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	orig := personRow(1, "Jane", "jane@b.c")
	require.NoError(t, insertRows(base, table, orig))

	sess, ctx := beginSession(t, store)
	require.Equal(t, "jane@b.c", readRows(t, ctx, table)[0][2])
	require.NoError(t, updateRow(base, table, orig, personRow(1, "Jane", "other@b.c")))
	require.Equal(t, "jane@b.c", readRows(t, ctx, table)[0][2])
	require.NoError(t, sess.CommitTransaction(ctx, ctx.GetTransaction()))
	require.Equal(t, "other@b.c", readRows(t, base, table)[0][2])
}

func TestReadCommittedSeesLatest(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	orig := personRow(1, "Jane", "jane@b.c")
	require.NoError(t, insertRows(base, table, orig))

	sess := NewSession(sql.NewBaseSession(), store)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	require.NoError(t, sess.SetSessionVariable(ctx, "transaction_isolation", "READ-COMMITTED"))
	tx, err := sess.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	ctx.SetTransaction(tx)
	ctx.SetIgnoreAutoCommit(true)

	require.Equal(t, "jane@b.c", readRows(t, ctx, table)[0][2])
	require.NoError(t, updateRow(base, table, orig, personRow(1, "Jane", "other@b.c")))
	require.Equal(t, "other@b.c", readRows(t, ctx, table)[0][2])
	require.NoError(t, sess.CommitTransaction(ctx, tx))
}

func TestLockNowait(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	orig := personRow(1, "Jane", "jane@b.c")
	require.NoError(t, insertRows(base, table, orig))

	sessA, ctxA := beginSession(t, store)
	require.NoError(t, updateRow(ctxA, table, orig, personRow(1, "Jane", "held@b.c")))

	sessB, ctxB := beginSession(t, store)
	sessB.SetLockingRead(true)
	sessB.SetLockingReadMode(true, false)
	_, err := readRowsErr(ctxB, table)
	require.Error(t, err)
	require.True(t, sql.ErrLockNowait.Is(err))
	require.NoError(t, sessA.Rollback(ctxA, ctxA.GetTransaction()))
	require.NoError(t, sessB.Rollback(ctxB, ctxB.GetTransaction()))
}

func TestSkipLocked(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	first := personRow(1, "Jane", "jane@b.c")
	second := personRow(2, "John", "john@b.c")
	require.NoError(t, insertRows(base, table, first, second))

	sessA, ctxA := beginSession(t, store)
	require.NoError(t, updateRow(ctxA, table, first, personRow(1, "Jane", "held@b.c")))

	sessB, ctxB := beginSession(t, store)
	sessB.SetLockingRead(true)
	sessB.SetLockingReadMode(false, true)
	rows := readRows(t, ctxB, table)
	require.Len(t, rows, 1)
	require.Equal(t, int64(2), rows[0][0])
	require.NoError(t, sessA.Rollback(ctxA, ctxA.GetTransaction()))
	require.NoError(t, sessB.Rollback(ctxB, ctxB.GetTransaction()))
}

func TestRowLockDeadlock(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	row1 := personRow(1, "Jane", "jane@b.c")
	row2 := personRow(2, "John", "john@b.c")
	require.NoError(t, insertRows(base, table, row1, row2))

	sessA, ctxA := beginSession(t, store)
	sessB, ctxB := beginSession(t, store)
	require.NoError(t, updateRow(ctxA, table, row1, personRow(1, "Jane", "a@b.c")))
	require.NoError(t, updateRow(ctxB, table, row2, personRow(2, "John", "b@b.c")))

	done := make(chan error, 1)
	go func() {
		done <- updateRow(ctxA, table, row2, personRow(2, "John", "a2@b.c"))
	}()
	store.locksFor().awaitWaiting(sessA)
	err := updateRow(ctxB, table, row1, personRow(1, "Jane", "b2@b.c"))
	require.Error(t, err)
	require.True(t, sql.ErrLockDeadlock.Is(err))
	require.NoError(t, sessB.Rollback(ctxB, ctxB.GetTransaction()))
	require.NoError(t, <-done)
	require.NoError(t, sessA.CommitTransaction(ctxA, ctxA.GetTransaction()))
}

func readRowsErr(ctx *sql.Context, table sql.Table) ([]sql.Row, error) {
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	defer partitions.Close(ctx)
	var rows []sql.Row
	for {
		partition, err := partitions.Next(ctx)
		if err != nil {
			break
		}
		iter, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return nil, err
		}
		for {
			row, err := iter.Next(ctx)
			if err != nil {
				_ = iter.Close(ctx)
				if err == io.EOF {
					break
				}
				return rows, err
			}
			rows = append(rows, row)
		}
		if err := iter.Close(ctx); err != nil {
			return rows, err
		}
	}
	return rows, nil
}

func beginSession(t *testing.T, store *Store) (*Session, *sql.Context) {
	t.Helper()
	sess := NewSession(sql.NewBaseSession(), store)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	tx, err := sess.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	ctx.SetTransaction(tx)
	ctx.SetIgnoreAutoCommit(true)
	return sess, ctx
}

func personRow(id int64, name, email string) sql.Row {
	created := time.Unix(0, 1667304000000001000).UTC()
	return sql.NewRow(id, name, email, types.MustJSON(`[]`), created)
}
