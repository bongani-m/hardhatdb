package persist

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/types"
)

const testServerUUID = "11111111-1111-1111-1111-111111111111"

func testRaftConfig() *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LogLevel = "ERROR"
	cfg.HeartbeatTimeout = 100 * time.Millisecond
	cfg.ElectionTimeout = 100 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.SnapshotInterval = 120 * time.Second
	cfg.SnapshotThreshold = 8192
	return cfg
}

type memCluster struct {
	addrs        []raft.ServerAddress
	trans        []*raft.InmemTransport
	onLeadership func(id string, isLeader bool)
	binlogMax    uint64
}

func newMemCluster(t *testing.T, n int) *memCluster {
	t.Helper()
	c := &memCluster{
		addrs: make([]raft.ServerAddress, n),
		trans: make([]*raft.InmemTransport, n),
	}
	for i := 0; i < n; i++ {
		addr, trans := raft.NewInmemTransportWithTimeout(raft.ServerAddress(fmt.Sprintf("node-%d", i)), 2*time.Second)
		c.addrs[i] = addr
		c.trans[i] = trans
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				c.trans[i].Connect(c.addrs[j], c.trans[j])
			}
		}
	}
	return c
}

func (c *memCluster) open(t *testing.T, i int, bootstrap bool, cfg *raft.Config) *Store {
	t.Helper()
	if cfg == nil {
		cfg = testRaftConfig()
	}
	dir := t.TempDir()
	id := fmt.Sprintf("node-%d", i)
	hook := c.onLeadership
	store, err := OpenCluster(filepath.Join(dir, "gms"), ClusterOptions{
		ID:             id,
		Advertise:      string(c.addrs[i]),
		RaftDir:        filepath.Join(dir, "raft"),
		Bootstrap:      bootstrap,
		ServerUUID:     testServerUUID,
		Transport:      c.trans[i],
		Config:         cfg,
		ApplyTimeout:   10 * time.Second,
		BinlogMaxBytes: c.binlogMax,
		OnLeadership: func(isLeader bool) {
			if hook != nil {
				hook(id, isLeader)
			}
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func waitLeader(t *testing.T, stores []*Store) *Store {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, store := range stores {
			if store.IsLeader() {
				return store
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}

func waitCaughtUp(t *testing.T, leader, follower *Store) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		last, err := leader.LastIndex()
		require.NoError(t, err)
		if last > 0 && follower.caughtUpTo(last) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	last, _ := leader.LastIndex()
	t.Fatalf("follower applied %d, leader last %d", follower.AppliedIndex(), last)
}

func kvTable(t *testing.T, ctx *sql.Context, store *Store) *Table {
	t.Helper()
	require.NoError(t, store.CreateDatabase(ctx, "mydb"))
	db, err := store.Database(ctx, "mydb")
	require.NoError(t, err)
	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: "kv", PrimaryKey: true},
		{Name: "name", Type: types.Text, Nullable: false, Source: "kv"},
	})
	require.NoError(t, db.(sql.TableCreator).CreateTable(ctx, "kv", schema, sql.Collation_Default, ""))
	table, ok, err := db.GetTableInsensitive(ctx, "kv")
	require.NoError(t, err)
	require.True(t, ok)
	return table.(*Table)
}

func tableNamed(t *testing.T, ctx *sql.Context, store *Store) *Table {
	t.Helper()
	db, err := store.Database(ctx, "mydb")
	require.NoError(t, err)
	table, ok, err := db.GetTableInsensitive(ctx, "kv")
	require.NoError(t, err)
	require.True(t, ok)
	return table.(*Table)
}

func waitRows(t *testing.T, store *Store, n int) []sql.Row {
	t.Helper()
	ctx := sql.NewContext(context.Background())
	deadline := time.Now().Add(10 * time.Second)
	var rows []sql.Row
	for time.Now().Before(deadline) {
		db, err := store.Database(ctx, "mydb")
		if err == nil {
			table, ok, err := db.GetTableInsensitive(ctx, "kv")
			if err == nil && ok {
				rows = readRows(t, ctx, table)
				if len(rows) == n {
					return rows
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("got %d rows, want %d", len(rows), n)
	return nil
}

func TestClusterRestartServesWrites(t *testing.T) {
	dir := t.TempDir()
	open := func() *Store {
		t.Helper()
		_, trans := raft.NewInmemTransportWithTimeout(raft.ServerAddress("node-0"), 2*time.Second)
		store, err := OpenCluster(filepath.Join(dir, "gms"), ClusterOptions{
			ID:           "node-0",
			Advertise:    "node-0",
			RaftDir:      filepath.Join(dir, "raft"),
			Bootstrap:    true,
			ServerUUID:   testServerUUID,
			Transport:    trans,
			Config:       testRaftConfig(),
			ApplyTimeout: 10 * time.Second,
		})
		require.NoError(t, err)
		require.False(t, store.syncWrites)
		return store
	}

	store := open()
	require.NoError(t, store.WaitReady(10*time.Second))
	require.NoError(t, store.WaitCaughtUp(10*time.Second))
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, store)

	const n = 4
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := sql.NewContext(context.Background())
			errs[i] = insertRows(c, table, sql.NewRow(int64(i+1), fmt.Sprintf("n%d", i)))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoErrorf(t, err, "insert %d", i)
	}
	require.NoError(t, store.Close())

	reopened := open()
	t.Cleanup(func() { _ = reopened.Close() })
	require.NoError(t, reopened.WaitReady(10*time.Second))
	require.NoError(t, reopened.WaitCaughtUp(10*time.Second))
	rows := waitRows(t, reopened, n)
	got := map[int64]string{}
	for _, row := range rows {
		got[row[0].(int64)] = row[1].(string)
	}
	for i := 0; i < n; i++ {
		require.Equal(t, fmt.Sprintf("n%d", i), got[int64(i+1)])
	}
}

func TestClusterReplaysLogAfterBadgerLoss(t *testing.T) {
	dir := t.TempDir()
	gms := filepath.Join(dir, "gms")
	open := func() *Store {
		t.Helper()
		_, trans := raft.NewInmemTransportWithTimeout(raft.ServerAddress("node-0"), 2*time.Second)
		store, err := OpenCluster(gms, ClusterOptions{
			ID:           "node-0",
			Advertise:    "node-0",
			RaftDir:      filepath.Join(dir, "raft"),
			Bootstrap:    true,
			ServerUUID:   testServerUUID,
			Transport:    trans,
			Config:       testRaftConfig(),
			ApplyTimeout: 10 * time.Second,
		})
		require.NoError(t, err)
		require.False(t, store.syncWrites)
		return store
	}

	store := open()
	require.NoError(t, store.WaitReady(10*time.Second))
	require.NoError(t, store.WaitCaughtUp(10*time.Second))
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, store)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))
	require.NoError(t, store.WaitCaughtUp(10*time.Second))
	require.NoError(t, store.Close())

	// The Raft log is the commit record. Dropping Badger simulates a crash
	// before that directory was fsynced.
	require.NoError(t, os.RemoveAll(gms))

	reopened := open()
	t.Cleanup(func() { _ = reopened.Close() })
	require.NoError(t, reopened.WaitReady(10*time.Second))
	require.NoError(t, reopened.WaitCaughtUp(10*time.Second))
	rows := waitRows(t, reopened, 1)
	require.Equal(t, int64(1), rows[0][0])
	require.Equal(t, "ada", rows[0][1])
}

func TestClusterReplicatesInsert(t *testing.T) {
	mem := newMemCluster(t, 3)
	stores := make([]*Store, 3)
	stores[0] = mem.open(t, 0, true, nil)
	require.NoError(t, stores[0].WaitReady(10*time.Second))
	for i := 1; i < 3; i++ {
		stores[i] = mem.open(t, i, false, nil)
		require.NoError(t, stores[0].AddVoter(fmt.Sprintf("node-%d", i), string(mem.addrs[i])))
		waitCaughtUp(t, stores[0], stores[i])
	}

	leader := waitLeader(t, stores)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))

	for _, store := range stores {
		rows := waitRows(t, store, 1)
		require.Equal(t, int64(1), rows[0][0])
		require.Equal(t, "ada", rows[0][1])
	}

	var follower *Store
	for _, store := range stores {
		if store != leader {
			follower = store
			break
		}
	}
	err := insertRows(ctx, tableNamed(t, ctx, follower), sql.NewRow(int64(2), "bea"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not the leader")

	events, format, err := leader.ReadBinlog()
	require.NoError(t, err)
	var gtidSeq int64
	var writeSeq int64
	sawWrite := false
	for _, ev := range events {
		if ev.IsGTID() {
			gtid, _, gerr := ev.GTID(format)
			require.NoError(t, gerr)
			gtidSeq = gtid.(mysql.Mysql56GTID).Sequence
		}
		if ev.IsWriteRows() {
			sawWrite = true
			writeSeq = gtidSeq
		}
	}
	require.True(t, sawWrite)
	last, err := leader.LastIndex()
	require.NoError(t, err)
	require.Equal(t, int64(last), writeSeq)
}

func TestClusterLeadershipTransfer(t *testing.T) {
	mem := newMemCluster(t, 3)
	stores := make([]*Store, 3)
	stores[0] = mem.open(t, 0, true, nil)
	require.NoError(t, stores[0].WaitReady(10*time.Second))
	for i := 1; i < 3; i++ {
		stores[i] = mem.open(t, i, false, nil)
		require.NoError(t, stores[0].AddVoter(fmt.Sprintf("node-%d", i), string(mem.addrs[i])))
		waitCaughtUp(t, stores[0], stores[i])
	}

	old := waitLeader(t, stores)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, old)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))
	for _, store := range stores {
		waitRows(t, store, 1)
	}

	require.NoError(t, old.TransferLeadership())
	var next *Store
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && next == nil {
		for _, store := range stores {
			if store != old && store.IsLeader() {
				next = store
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NotNil(t, next)

	err := insertRows(ctx, tableNamed(t, ctx, old), sql.NewRow(int64(2), "bea"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not the leader")

	require.NoError(t, insertRows(ctx, tableNamed(t, ctx, next), sql.NewRow(int64(2), "bea")))
	for _, store := range stores {
		rows := waitRows(t, store, 2)
		require.Equal(t, "bea", rows[1][1])
	}
}

func TestClusterRemoveServer(t *testing.T) {
	stores, leader := startTrio(t)
	ctx := sql.NewContext(context.Background())
	_ = kvTable(t, ctx, leader)

	var removed string
	var keep []*Store
	for i, store := range stores {
		if store == leader {
			keep = append(keep, store)
			continue
		}
		if removed == "" {
			removed = fmt.Sprintf("node-%d", i)
			continue
		}
		keep = append(keep, store)
	}
	require.NotEmpty(t, removed)
	require.NoError(t, leader.RemoveServer(removed))
	require.NoError(t, insertRows(ctx, tableNamed(t, ctx, leader), sql.NewRow(int64(1), "ada")))
	for _, store := range keep {
		rows := waitRows(t, store, 1)
		require.Equal(t, "ada", rows[0][1])
	}
}

func TestBootstrapIgnoredWhenStateExists(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "gms")
	raftDir := filepath.Join(dir, "raft")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	opts := ClusterOptions{
		ID:           "n1",
		Bind:         addr,
		Advertise:    addr,
		RaftDir:      raftDir,
		Bootstrap:    true,
		ServerUUID:   testServerUUID,
		Config:       testRaftConfig(),
		ApplyTimeout: 10 * time.Second,
	}
	first, err := OpenCluster(data, opts)
	require.NoError(t, err)
	require.True(t, first.Bootstrapped())
	require.NoError(t, first.WaitReady(10*time.Second))
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, first)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))
	last, err := first.LastIndex()
	require.NoError(t, err)
	require.Greater(t, last, uint64(1))
	require.NoError(t, first.Close())

	second, err := OpenCluster(data, opts)
	require.NoError(t, err)
	require.False(t, second.Bootstrapped())
	t.Cleanup(func() { _ = second.Close() })
	require.NoError(t, second.WaitReady(10*time.Second))
	again, err := second.LastIndex()
	require.NoError(t, err)
	require.GreaterOrEqual(t, again, last)
	rows := waitRows(t, second, 1)
	require.Equal(t, "ada", rows[0][1])
	require.Equal(t, "leader", second.Status().Role)
}

func TestSnapshotDoesNotRetainBytes(t *testing.T) {
	mem := newMemCluster(t, 1)
	leader := mem.open(t, 0, true, nil)
	require.NoError(t, leader.WaitReady(10*time.Second))
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))

	snap, err := (&storeFSM{store: leader}).Snapshot()
	require.NoError(t, err)
	ss := snap.(*storeSnapshot)
	require.NotEmpty(t, ss.path)
	info, err := os.Stat(ss.path)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(8))
	rt := reflect.TypeOf(*ss)
	for i := 0; i < rt.NumField(); i++ {
		require.NotEqualf(t, reflect.Slice, rt.Field(i).Type.Kind(), "field %s holds the backup", rt.Field(i).Name)
	}
	snap.Release()
	_, err = os.Stat(ss.path)
	require.True(t, os.IsNotExist(err))
}

func TestClusterSnapshotJoin(t *testing.T) {
	mem := newMemCluster(t, 2)
	cfg := testRaftConfig()
	cfg.SnapshotThreshold = 1
	cfg.TrailingLogs = 1
	leader := mem.open(t, 0, true, cfg)
	require.NoError(t, leader.WaitReady(10*time.Second))

	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada"), sql.NewRow(int64(2), "bea")))
	require.NoError(t, deleteRow(ctx, table, sql.NewRow(int64(2), "bea")))
	require.NoError(t, leader.Snapshot())

	followerCfg := testRaftConfig()
	followerCfg.SnapshotThreshold = 1
	followerCfg.TrailingLogs = 1
	follower := mem.open(t, 1, false, followerCfg)
	require.NoError(t, leader.AddVoter("node-1", string(mem.addrs[1])))

	rows := waitRows(t, follower, 1)
	require.Equal(t, int64(1), rows[0][0])
	require.Equal(t, "ada", rows[0][1])
	leaderRows := waitRows(t, leader, 1)
	require.Equal(t, leaderRows[0][0], rows[0][0])
	require.Equal(t, leaderRows[0][1], rows[0][1])
}

func startTrio(t *testing.T) (stores []*Store, leader *Store) {
	t.Helper()
	mem := newMemCluster(t, 3)
	stores = make([]*Store, 3)
	stores[0] = mem.open(t, 0, true, nil)
	require.NoError(t, stores[0].WaitReady(10*time.Second))
	for i := 1; i < 3; i++ {
		stores[i] = mem.open(t, i, false, nil)
		require.NoError(t, stores[0].AddVoter(fmt.Sprintf("node-%d", i), string(mem.addrs[i])))
		waitCaughtUp(t, stores[0], stores[i])
	}
	return stores, waitLeader(t, stores)
}

func TestClusterConcurrentInserts(t *testing.T) {
	stores, leader := startTrio(t)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := sql.NewContext(context.Background())
			errs[i] = insertRows(c, table, sql.NewRow(int64(i+1), fmt.Sprintf("n%d", i)))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoErrorf(t, err, "insert %d", i)
	}

	for _, store := range stores {
		rows := waitRows(t, store, n)
		got := map[int64]string{}
		for _, row := range rows {
			got[row[0].(int64)] = row[1].(string)
		}
		for i := 0; i < n; i++ {
			require.Equal(t, fmt.Sprintf("n%d", i), got[int64(i+1)])
		}
	}

	seqs := writeGTIDSequences(t, leader)
	require.Len(t, seqs, n)
	seen := map[int64]bool{}
	var maxSeq int64
	for _, seq := range seqs {
		require.False(t, seen[seq], "duplicate GTID sequence %d", seq)
		seen[seq] = true
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	last, err := leader.LastIndex()
	require.NoError(t, err)
	require.Equal(t, int64(last), maxSeq)
}

func TestClusterConcurrentDuplicateKey(t *testing.T) {
	stores, leader := startTrio(t)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			c := sql.NewContext(context.Background())
			errs[i] = insertRows(c, table, sql.NewRow(int64(1), "ada"))
		}(i)
	}
	close(start)
	wg.Wait()

	var ok, bad int
	for _, err := range errs {
		if err == nil {
			ok++
			continue
		}
		require.Truef(t, sql.ErrPrimaryKeyViolation.Is(err), "got %v", err)
		bad++
	}
	require.Equal(t, 1, ok)
	require.Equal(t, 1, bad)

	for _, store := range stores {
		rows := waitRows(t, store, 1)
		require.Equal(t, int64(1), rows[0][0])
		require.Equal(t, "ada", rows[0][1])
	}
}

func TestClusterCommitSeesInflight(t *testing.T) {
	stores, leader := startTrio(t)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)

	gateEntered := make(chan struct{})
	release := make(chan struct{})
	var entered sync.Once
	var released sync.Once
	releaseGate := func() { released.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	leader.cluster.gate = func() {
		entered.Do(func() { close(gateEntered) })
		<-release
	}

	first := make(chan error, 1)
	go func() {
		c := sql.NewContext(context.Background())
		first <- insertRows(c, table, sql.NewRow(int64(1), "ada"))
	}()
	select {
	case <-gateEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("proposer did not reach the gate")
	}

	dupErr := make(chan error, 1)
	go func() {
		c := sql.NewContext(context.Background())
		dupErr <- insertRows(c, table, sql.NewRow(int64(1), "ada"))
	}()
	select {
	case err := <-dupErr:
		require.Truef(t, sql.ErrPrimaryKeyViolation.Is(err), "got %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("duplicate insert blocked on raft")
	}

	second := make(chan error, 1)
	go func() {
		c := sql.NewContext(context.Background())
		second <- insertRows(c, table, sql.NewRow(int64(2), "bea"))
	}()
	require.Eventually(t, func() bool {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return len(leader.cluster.queue) == 1
	}, 10*time.Second, 10*time.Millisecond)

	releaseGate()
	require.NoError(t, <-first)
	require.NoError(t, <-second)

	for _, store := range stores {
		rows := waitRows(t, store, 2)
		got := map[int64]string{}
		for _, row := range rows {
			got[row[0].(int64)] = row[1].(string)
		}
		require.Equal(t, "ada", got[int64(1)])
		require.Equal(t, "bea", got[int64(2)])
	}
}

func writeGTIDSequences(t *testing.T, store *Store) []int64 {
	t.Helper()
	events, format, err := store.ReadBinlog()
	require.NoError(t, err)
	var seq int64
	var out []int64
	for _, ev := range events {
		if ev.IsGTID() {
			gtid, _, gerr := ev.GTID(format)
			require.NoError(t, gerr)
			seq = gtid.(mysql.Mysql56GTID).Sequence
		}
		if ev.IsWriteRows() {
			out = append(out, seq)
		}
	}
	return out
}

func TestClusterLeadershipHook(t *testing.T) {
	mem := newMemCluster(t, 3)
	var mu sync.Mutex
	leaders := map[string]bool{}
	mem.onLeadership = func(id string, isLeader bool) {
		mu.Lock()
		leaders[id] = isLeader
		mu.Unlock()
	}
	stores := make([]*Store, 3)
	stores[0] = mem.open(t, 0, true, nil)
	require.NoError(t, stores[0].WaitReady(10*time.Second))
	for i := 1; i < 3; i++ {
		stores[i] = mem.open(t, i, false, nil)
		require.NoError(t, stores[0].AddVoter(fmt.Sprintf("node-%d", i), string(mem.addrs[i])))
		waitCaughtUp(t, stores[0], stores[i])
	}

	old := waitLeader(t, stores)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, old)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))
	require.NotEmpty(t, old.Leader())

	require.NoError(t, old.TransferLeadership())
	var nextID string
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		var on string
		for id, leader := range leaders {
			if !leader {
				continue
			}
			if on != "" {
				return false
			}
			on = id
		}
		if on == "" || on == raftID(old) {
			return false
		}
		nextID = on
		return true
	}, 10*time.Second, 20*time.Millisecond)
	next := storeByID(stores, nextID)
	require.NotNil(t, next)
	require.NotEqual(t, old, next)

	err := insertRows(ctx, tableNamed(t, ctx, old), sql.NewRow(int64(2), "bea"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not the leader")
	require.NoError(t, insertRows(ctx, tableNamed(t, ctx, next), sql.NewRow(int64(2), "bea")))
	for _, store := range stores {
		rows := waitRows(t, store, 2)
		require.Equal(t, "bea", rows[1][1])
	}
}

func raftID(store *Store) string {
	return string(store.cluster.id)
}

func storeByID(stores []*Store, id string) *Store {
	for _, store := range stores {
		if raftID(store) == id {
			return store
		}
	}
	return nil
}

func TestClusterRotateBinlog(t *testing.T) {
	stores, leader := startTrio(t)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))
	for _, store := range stores {
		waitCaughtUp(t, leader, store)
	}
	require.NoError(t, leader.RotateBinaryLog(ctx))
	for _, store := range stores {
		waitCaughtUp(t, leader, store)
		logs, err := store.ListBinaryLogs(ctx)
		require.NoError(t, err)
		require.Len(t, logs, 2)
		require.Equal(t, "binlog.000001", logs[0].Name)
		require.Equal(t, "binlog.000002", logs[1].Name)
		status, err := store.GetBinaryLogStatus(ctx)
		require.NoError(t, err)
		require.Equal(t, "binlog.000002", status[0].File)
	}
}

func TestReplicaResumesAfterLeadershipMove(t *testing.T) {
	srcMem := newMemCluster(t, 1)
	src := srcMem.open(t, 0, true, nil)
	require.NoError(t, src.WaitReady(10*time.Second))
	ctx := sql.NewContext(context.Background())
	srcTable := kvTable(t, ctx, src)
	require.NoError(t, insertRows(ctx, srcTable, sql.NewRow(int64(1), "ada")))
	require.NoError(t, insertRows(ctx, srcTable, sql.NewRow(int64(2), "bea")))
	events, format, err := src.ReadBinlog()
	require.NoError(t, err)
	txns := writeRowTxns(events, format)
	require.Len(t, txns, 2)
	var first mysql.GTID
	for _, ev := range txns[0] {
		if ev.IsGTID() {
			first, _, err = ev.GTID(format)
			require.NoError(t, err)
		}
	}
	require.NotNil(t, first)

	mem := newMemCluster(t, 3)
	stores := make([]*Store, 3)
	stores[0] = mem.open(t, 0, true, nil)
	require.NoError(t, stores[0].WaitReady(10*time.Second))
	for i := 1; i < 3; i++ {
		stores[i] = mem.open(t, i, false, nil)
		require.NoError(t, stores[0].AddVoter(fmt.Sprintf("node-%d", i), string(mem.addrs[i])))
		waitCaughtUp(t, stores[0], stores[i])
	}
	leader := waitLeader(t, stores)
	_ = kvTable(t, ctx, leader)

	var mu sync.Mutex
	var calls []string
	dial := func(_ context.Context, _ string, _ uint16, _, _ string, executed mysql.GTIDSet) (binlogStream, error) {
		mu.Lock()
		calls = append(calls, executed.String())
		mu.Unlock()
		evs := txns[0]
		if executed != nil && executed.ContainsGTID(first) {
			evs = txns[1]
		}
		return newSliceStream(evs), nil
	}
	for _, store := range stores {
		store.SetBinlogDial(dial)
	}
	require.NoError(t, leader.SetReplicationSourceOptions(ctx, []binlogreplication.ReplicationOption{
		{Name: "SOURCE_HOST", Value: "example.invalid"},
		{Name: "SOURCE_USER", Value: "repl"},
		{Name: "SOURCE_PORT", Value: 3306},
		{Name: "SOURCE_PASSWORD", Value: "secret"},
	}))
	info, err := os.Stat(filepath.Join(leader.raftDir, sourcePasswordFile))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.NoError(t, leader.StartReplica(ctx))

	for _, store := range stores {
		waitRows(t, store, 1)
	}
	require.NoError(t, leader.TransferLeadership())
	require.Eventually(t, func() bool {
		for _, store := range stores {
			if store != leader && store.IsLeader() {
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
	for _, store := range stores {
		rows := waitRows(t, store, 2)
		got := map[int64]string{}
		for _, row := range rows {
			got[row[0].(int64)] = row[1].(string)
		}
		require.Equal(t, "ada", got[int64(1)])
		require.Equal(t, "bea", got[int64(2)])
	}
	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(calls), 2)
	var resumed bool
	for _, call := range calls[1:] {
		set, err := mysql.ParseMysql56GTIDSet(call)
		if err == nil && set.ContainsGTID(first) {
			resumed = true
		}
	}
	require.True(t, resumed)
}

type sliceStream struct {
	mu     sync.Mutex
	events []mysql.BinlogEvent
	i      int
	done   chan struct{}
	once   sync.Once
}

func newSliceStream(events []mysql.BinlogEvent) *sliceStream {
	return &sliceStream{events: events, done: make(chan struct{})}
}

func (s *sliceStream) ReadEvent() (mysql.BinlogEvent, error) {
	s.mu.Lock()
	if s.i < len(s.events) {
		ev := s.events[s.i]
		s.i++
		s.mu.Unlock()
		return ev, nil
	}
	s.mu.Unlock()
	<-s.done
	return nil, io.EOF
}

func (s *sliceStream) Close() error {
	s.once.Do(func() { close(s.done) })
	return nil
}

func writeRowTxns(events []mysql.BinlogEvent, format mysql.BinlogFormat) [][]mysql.BinlogEvent {
	var formatEv mysql.BinlogEvent
	for _, ev := range events {
		if ev.IsFormatDescription() {
			formatEv = ev
			break
		}
	}
	var cur []mysql.BinlogEvent
	var out [][]mysql.BinlogEvent
	in := false
	hasWrite := false
	flush := func() {
		if in && hasWrite && formatEv != nil {
			txn := make([]mysql.BinlogEvent, 0, len(cur)+1)
			txn = append(txn, formatEv)
			txn = append(txn, cur...)
			out = append(out, txn)
		}
		cur = nil
		in = false
		hasWrite = false
	}
	for _, ev := range events {
		if ev.IsGTID() {
			flush()
			cur = []mysql.BinlogEvent{ev}
			in = true
			continue
		}
		if !in {
			continue
		}
		cur = append(cur, ev)
		if ev.IsWriteRows() {
			hasWrite = true
		}
		if ev.IsXID() {
			flush()
		}
	}
	flush()
	_ = format
	return out
}
