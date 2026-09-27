package sqle

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestPlacementRoundTrip(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "hardhatdb"))
	t.Cleanup(func() { _ = store.Close() })
	err := store.SavePlacement(TablePlacement{DB: "Stress", Table: "Accounts", Column: "ID", Check: []string{"Email"}})
	require.NoError(t, err)
	list, err := store.Placements()
	require.NoError(t, err)
	require.Equal(t, []TablePlacement{{
		DB: "stress", Table: "accounts", Column: "id", Check: []string{"email"},
	}}, list)
}

func TestNonvoterReplicatesAndDoesNotLead(t *testing.T) {
	mem := newMemCluster(t, 2)
	voter := mem.open(t, 0, true, nil)
	require.NoError(t, voter.WaitReady(10*time.Second))
	non := mem.open(t, 1, false, nil)
	require.NoError(t, voter.AddNonvoter("node-1", string(mem.addrs[1])))
	waitCaughtUp(t, voter, non)

	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, voter)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(7), "ada")))
	rows := waitRows(t, non, 1)
	require.Equal(t, int64(7), rows[0][0])

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		require.False(t, non.IsLeader())
		require.True(t, voter.IsLeader())
		time.Sleep(20 * time.Millisecond)
	}
}

func TestShardGroupsStayIndependent(t *testing.T) {
	// Separate in-memory addresses. Raft's in-memory transport registry is
	// global, so the two groups cannot both be named node-0.
	left, stopLeft := openAlone(t, "shard-a")
	right, _ := openAlone(t, "shard-b")
	ctx := sql.NewContext(context.Background())
	tableA := kvTable(t, ctx, left)
	tableB := kvTable(t, ctx, right)
	require.NoError(t, insertRows(ctx, tableB, sql.NewRow(int64(1), "one")))
	require.False(t, hasID(t, ctx, tableA, 1))
	stopLeft()
	require.NoError(t, insertRows(ctx, tableB, sql.NewRow(int64(2), "two")))
	require.True(t, hasID(t, ctx, tableB, 2))
}

func openAlone(t *testing.T, id string) (*Store, func()) {
	t.Helper()
	_, trans := raft.NewInmemTransportWithTimeout(raft.ServerAddress(id), 2*time.Second)
	dir := t.TempDir()
	store, err := OpenCluster(filepath.Join(dir, "hardhatdb"), ClusterOptions{
		ID:           id,
		Advertise:    id,
		RaftDir:      filepath.Join(dir, "raft"),
		Bootstrap:    true,
		ServerUUID:   testServerUUID,
		Transport:    trans,
		Config:       testRaftConfig(),
		ApplyTimeout: 10 * time.Second,
	})
	require.NoError(t, err)
	require.NoError(t, store.WaitReady(10*time.Second))
	var once sync.Once
	stop := func() { once.Do(func() { _ = store.Close() }) }
	t.Cleanup(stop)
	return store, stop
}

func hasID(t *testing.T, ctx *sql.Context, table *Table, id int64) bool {
	t.Helper()
	for _, row := range readRows(t, ctx, table) {
		if row[0] == id {
			return true
		}
	}
	return false
}
