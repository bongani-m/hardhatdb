package persist

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestRangeRoundTripAndOverlap(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms"))
	t.Cleanup(func() { _ = store.Close() })
	err := store.PutRange(KeyRange{
		ID: "r1", DB: "Stress", Table: "Accounts", Group: "g",
		Peers: []RangePeer{{ID: "n1", Raft: "10.0.0.1:7101", Forward: "10.0.0.1:7102"}},
	})
	require.NoError(t, err)
	list, err := store.Ranges()
	require.NoError(t, err)
	require.Equal(t, "stress", list[0].DB)
	require.Equal(t, "accounts", list[0].Table)
	require.Equal(t, RangeActive, list[0].State)
	require.True(t, list[0].Holds(EncodeIntKey(5)))

	mid := KeySuccessor(EncodeIntKey(100))
	left := KeyRange{ID: "lo", DB: "stress", Table: "accounts", End: mid, Group: "g0", State: RangeActive}
	right := KeyRange{ID: "hi", DB: "stress", Table: "accounts", Start: mid, Group: "g1", State: RangeActive}
	require.True(t, left.Holds(EncodeIntKey(100)))
	require.False(t, left.Holds(EncodeIntKey(101)))
	require.True(t, right.Holds(EncodeIntKey(101)))
	require.False(t, right.OverlapsInclusive(EncodeIntKey(1), EncodeIntKey(50), true, true))
	require.True(t, right.OverlapsInclusive(EncodeIntKey(100), EncodeIntKey(120), true, true))
}

func TestSplitKeepsBoundaryOnTheLeft(t *testing.T) {
	src, _ := openAlone(t, "split-src")
	dst, _ := openAlone(t, "split-dst")
	meta := openAt(t, filepath.Join(t.TempDir(), "meta"))
	t.Cleanup(func() { _ = meta.Close() })
	ctx := sql.NewContext(context.Background())
	srcTable := kvTable(t, ctx, src)
	_ = kvTable(t, ctx, dst)
	for _, id := range []int64{1, 2, 3, 4} {
		require.NoError(t, insertRows(ctx, srcTable, sql.NewRow(id, "n")))
	}
	require.NoError(t, meta.PutRange(KeyRange{
		ID: "all", DB: "mydb", Table: "kv", Group: "left", State: RangeActive,
	}))
	right := KeyRange{ID: "right", Group: "right", State: RangeActive}
	require.NoError(t, SplitRange(ctx, meta, src, dst, "mydb", "kv", EncodeIntKey(2), right))

	require.True(t, hasID(t, ctx, srcTable, 1))
	require.True(t, hasID(t, ctx, srcTable, 2))
	require.False(t, hasID(t, ctx, srcTable, 3))
	require.False(t, hasID(t, ctx, srcTable, 4))
	dstTable := tableNamed(t, ctx, dst)
	require.False(t, hasID(t, ctx, dstTable, 2))
	require.True(t, hasID(t, ctx, dstTable, 3))
	require.True(t, hasID(t, ctx, dstTable, 4))

	list, err := meta.Ranges()
	require.NoError(t, err)
	require.Len(t, list, 2)
	var left, moved KeyRange
	for _, r := range list {
		if r.ID == "all" {
			left = r
		}
		if r.ID == "right" {
			moved = r
		}
	}
	require.Equal(t, RangeActive, left.State)
	require.True(t, left.Holds(EncodeIntKey(2)))
	require.False(t, left.Holds(EncodeIntKey(3)))
	require.True(t, moved.Holds(EncodeIntKey(3)))
	require.Equal(t, "right", moved.Group)
}

func TestPreparedRowStaysHiddenUntilCommit(t *testing.T) {
	store, _ := openAlone(t, "phase-hide")
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, store)
	prepareRow(t, store, table, "tx-hide", 1, "hidden")
	require.False(t, hasID(t, ctx, table, 1))
	require.NoError(t, store.CommitPrepared("tx-hide", 7))
	require.True(t, hasID(t, ctx, table, 1))
}

func TestTwoPhaseCrashMatchesBothSides(t *testing.T) {
	left, _ := openAlone(t, "phase-left")
	right, _ := openAlone(t, "phase-right")
	meta := openAt(t, filepath.Join(t.TempDir(), "phase-meta"))
	t.Cleanup(func() { _ = meta.Close() })
	ctx := sql.NewContext(context.Background())
	leftTable := kvTable(t, ctx, left)
	rightTable := kvTable(t, ctx, right)
	stores := []*Store{left, right}

	prepareRow(t, left, leftTable, "tx-abort", 1, "a")
	prepareRow(t, right, rightTable, "tx-abort", 2, "b")
	require.False(t, hasID(t, ctx, leftTable, 1))
	require.False(t, hasID(t, ctx, rightTable, 2))
	err := FinishTwoPhase(meta, "tx-abort", stores, "prepare")
	require.ErrorIs(t, err, ErrTwoPhaseCrash)
	require.NoError(t, RecoverTwoPhase(meta, stores))
	require.False(t, hasID(t, ctx, leftTable, 1))
	require.False(t, hasID(t, ctx, rightTable, 2))

	prepareRow(t, left, leftTable, "tx-commit", 1, "a")
	prepareRow(t, right, rightTable, "tx-commit", 2, "b")
	err = FinishTwoPhase(meta, "tx-commit", stores, "decision")
	require.ErrorIs(t, err, ErrTwoPhaseCrash)
	require.False(t, hasID(t, ctx, leftTable, 1))
	require.False(t, hasID(t, ctx, rightTable, 2))
	require.NoError(t, RecoverTwoPhase(meta, stores))
	require.True(t, hasID(t, ctx, leftTable, 1))
	require.True(t, hasID(t, ctx, rightTable, 2))
}

func TestRangeReplicaJoins(t *testing.T) {
	mem := newMemCluster(t, 2)
	leader := mem.open(t, 0, true, nil)
	require.NoError(t, leader.WaitReady(10*time.Second))
	joiner := mem.open(t, 1, false, nil)
	require.NoError(t, leader.AddVoter("node-1", string(mem.addrs[1])))
	waitCaughtUp(t, leader, joiner)

	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(9), "nine")))
	require.NoError(t, leader.Snapshot())
	waitCaughtUp(t, leader, joiner)
	rows := waitRows(t, joiner, 1)
	require.Equal(t, int64(9), rows[0][0])

	meta := openAt(t, filepath.Join(t.TempDir(), "move-meta"))
	t.Cleanup(func() { _ = meta.Close() })
	err := meta.PutRange(KeyRange{
		ID: "r", DB: "mydb", Table: "kv", Group: "g", State: RangeActive,
		Peers: []RangePeer{{ID: "node-0", Raft: string(mem.addrs[0]), Forward: "127.0.0.1:1"}},
	})
	require.NoError(t, err)
	err = meta.UpdateRanges(func(list []KeyRange) ([]KeyRange, error) {
		list[0].Peers = append(list[0].Peers, RangePeer{ID: "node-1", Raft: string(mem.addrs[1]), Forward: "127.0.0.1:2"})
		return list, nil
	})
	require.NoError(t, err)
	require.NoError(t, leader.RemoveServer("node-0"))
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !joiner.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	require.True(t, joiner.IsLeader())
	got := tableNamed(t, ctx, joiner)
	require.True(t, hasID(t, ctx, got, 9))
	list, err := meta.Ranges()
	require.NoError(t, err)
	require.Len(t, list[0].Peers, 2)
}

func prepareRow(t *testing.T, store *Store, table *Table, id string, key int64, name string) {
	t.Helper()
	sess := NewSession(sql.NewBaseSession(), store)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	_, err := sess.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	ctx.SetIgnoreAutoCommit(true)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(key, name)))
	require.NoError(t, sess.PrepareTransaction(ctx, id))
}
