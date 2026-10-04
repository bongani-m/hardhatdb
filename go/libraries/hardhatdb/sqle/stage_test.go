package sqle

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/bongani-m/hardhatdb/go/store"
	"github.com/dolthub/go-mysql-server/sql"
)

func withEntryLimit(t *testing.T, n int) {
	t.Helper()
	old := store.EntryLimit
	store.EntryLimit = n
	t.Cleanup(func() { store.EntryLimit = old })
}

func TestPublishResumesCursor(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	const id = "txn-partial"
	marker := replBatch{
		Phase:      phaseCommit,
		PrepareID:  id,
		ChunkCount: 3,
		Unix:       1,
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, s.saveStage(0, replBatch{
			Phase:     phaseStage,
			PrepareID: id,
			Chunk:     uint64(i),
			Ops:       []kvOp{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}},
		}))
	}
	raw, err := encodeCursor(stageCursor{Next: 1, Index: 9, Marker: marker})
	require.NoError(t, err)
	require.NoError(t, s.badgerDB().Update(func(txn *badger.Txn) error {
		if err := applyOpsTxn(txn, []kvOp{{Key: []byte("k0"), Value: []byte("v")}}); err != nil {
			return err
		}
		return txn.Set(stageCursorKey(id), raw)
	}))

	out, err := s.publishStaged(9, marker)
	require.NoError(t, err)
	require.Len(t, out.Ops, 3)
	again, err := s.publishStaged(9, marker)
	require.NoError(t, err)
	require.Empty(t, again.Ops)
	for i := 0; i < 3; i++ {
		require.Equal(t, "v", string(mustGet(t, s, []byte(fmt.Sprintf("k%d", i)))))
	}
	ok, err := s.hasStages(id)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestStandaloneChunkedInsert(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	require.NoError(t, err)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, s)
	withEntryLimit(t, 128)

	var rows []sql.Row
	for i := 0; i < 12; i++ {
		rows = append(rows, sql.NewRow(int64(i+1), fmt.Sprintf("name-%d", i)))
	}
	require.NoError(t, insertRows(ctx, table, rows...))
	got := readRows(t, ctx, table)
	require.Len(t, got, 12)
	require.NoError(t, s.Close())

	s, err = Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	ctx = sql.NewContext(context.Background())
	got = readRows(t, ctx, tableNamed(t, ctx, s))
	require.Len(t, got, 12)
}

func TestOpenDropsUnmarkedStages(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	require.NoError(t, err)
	require.NoError(t, s.saveStage(0, replBatch{
		Phase:     phaseStage,
		PrepareID: "orphan",
		Chunk:     0,
		Ops:       []kvOp{{Key: []byte("hidden"), Value: []byte("no")}},
	}))
	require.NoError(t, s.Close())

	s, err = Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	ok, err := s.hasStages("orphan")
	require.NoError(t, err)
	require.False(t, ok)
	err = s.badgerDB().View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("hidden"))
		return err
	})
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestClusterChunkedInsert(t *testing.T) {
	stores, leader := startTrio(t)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)
	withEntryLimit(t, 128)

	var rows []sql.Row
	for i := 0; i < 12; i++ {
		rows = append(rows, sql.NewRow(int64(i+1), fmt.Sprintf("name-%d", i)))
	}
	require.NoError(t, insertRows(ctx, table, rows...))

	var sawStage, sawMark bool
	last, err := leader.LastIndex()
	require.NoError(t, err)
	for i := uint64(1); i <= last; i++ {
		batch, err := clusterOf(leader).ReadBatch(i)
		if err != nil {
			continue
		}
		if batch.Phase == store.PhaseStage {
			sawStage = true
			raw, err := store.EncodeBatch(batch)
			require.NoError(t, err)
			if len(batch.Ops) > 1 {
				require.LessOrEqual(t, len(raw), store.EntryLimit)
			}
		}
		if batch.Phase == store.PhaseCommit && batch.ChunkCount > 0 {
			sawMark = true
			require.Empty(t, batch.Ops)
		}
	}
	require.True(t, sawStage)
	require.True(t, sawMark)

	for _, store := range stores {
		got := waitRows(t, store, 12)
		require.Len(t, got, 12)
	}
}

func TestClusterChunkSeesInflight(t *testing.T) {
	_, leader := startTrio(t)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)
	withEntryLimit(t, 128)

	gateEntered := make(chan struct{})
	release := make(chan struct{})
	var entered sync.Once
	var released sync.Once
	releaseGate := func() { released.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	clusterOf(leader).SetGate(func() {
		entered.Do(func() { close(gateEntered) })
		<-release
	})

	first := make(chan error, 1)
	go func() {
		c := sql.NewContext(context.Background())
		var rows []sql.Row
		for i := 0; i < 8; i++ {
			rows = append(rows, sql.NewRow(int64(i+1), fmt.Sprintf("n%d", i)))
		}
		first <- insertRows(c, table, rows...)
	}()
	select {
	case <-gateEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("proposer did not reach the gate")
	}

	dup := make(chan error, 1)
	go func() {
		c := sql.NewContext(context.Background())
		dup <- insertRows(c, table, sql.NewRow(int64(1), "other"))
	}()
	select {
	case err := <-dup:
		require.Truef(t, sql.ErrPrimaryKeyViolation.Is(err), "got %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("duplicate insert did not observe the staged row")
	}
	releaseGate()
	require.NoError(t, <-first)
}

func TestClusterStopBeforeMarker(t *testing.T) {
	dir := t.TempDir()
	open := func() *Store {
		t.Helper()
		_, trans := raft.NewInmemTransportWithTimeout(raft.ServerAddress("node-0"), 2*time.Second)
		st, err := OpenCluster(filepath.Join(dir, "hardhatdb"), ClusterOptions{
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
		return st
	}

	st := open()
	require.NoError(t, st.WaitReady(10*time.Second))
	require.NoError(t, st.WaitCaughtUp(10*time.Second))
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, st)
	withEntryLimit(t, 128)
	clusterOf(st).SetProposeHook(func(batch store.ReplBatch) error {
		if batch.Phase == store.PhaseCommit && batch.ChunkCount > 0 {
			return errors.New("stop before marker")
		}
		return nil
	})

	var rows []sql.Row
	for i := 0; i < 8; i++ {
		rows = append(rows, sql.NewRow(int64(i+1), fmt.Sprintf("n%d", i)))
	}
	err := insertRows(ctx, table, rows...)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stop before marker")
	require.NoError(t, st.Close())

	st = open()
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, st.WaitReady(10*time.Second))
	require.NoError(t, st.WaitCaughtUp(10*time.Second))
	require.NoError(t, st.abortOrphanStages())
	got := readRows(t, sql.NewContext(context.Background()), tableNamed(t, sql.NewContext(context.Background()), st))
	require.Empty(t, got)
}

func TestClusterSmallCommitIsOneEntry(t *testing.T) {
	_, leader := startTrio(t)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)
	before, err := leader.LastIndex()
	require.NoError(t, err)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))
	require.NoError(t, leader.WaitCaughtUp(10*time.Second))
	after, err := leader.LastIndex()
	require.NoError(t, err)

	var phases []byte
	for i := before + 1; i <= after; i++ {
		batch, err := clusterOf(leader).ReadBatch(i)
		if err != nil {
			continue
		}
		phases = append(phases, batch.Phase)
	}
	require.Equal(t, []byte{store.PhaseApply}, phases)
}

func mustGet(t *testing.T, s *Store, key []byte) []byte {
	t.Helper()
	var val []byte
	err := s.badgerDB().View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	require.NoError(t, err)
	return val
}
