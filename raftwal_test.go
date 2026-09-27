package persist

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/stretchr/testify/require"
)

func TestRaftWALReopenAndDelete(t *testing.T) {
	dir := t.TempDir()
	w, err := openRaftWAL(dir, nil)
	require.NoError(t, err)
	now := time.Now().Truncate(time.Microsecond)
	logs := []*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("one"), AppendedAt: now},
		{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("two"), Extensions: []byte("x"), AppendedAt: now},
		{Index: 3, Term: 2, Type: raft.LogNoop, Data: []byte("three"), AppendedAt: now},
	}
	require.NoError(t, w.StoreLogs(logs))
	require.NoError(t, w.Close())

	w, err = openRaftWAL(dir, nil)
	require.NoError(t, err)
	first, err := w.FirstIndex()
	require.NoError(t, err)
	last, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	require.Equal(t, uint64(3), last)
	var got raft.Log
	require.NoError(t, w.GetLog(2, &got))
	require.Equal(t, []byte("two"), got.Data)
	require.Equal(t, []byte("x"), got.Extensions)
	require.Equal(t, raft.LogCommand, got.Type)
	require.True(t, got.AppendedAt.Equal(now))

	require.NoError(t, w.DeleteRange(1, 1))
	require.NoError(t, w.Close())
	w, err = openRaftWAL(dir, nil)
	require.NoError(t, err)
	defer w.Close()
	first, err = w.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), first)
	require.ErrorIs(t, w.GetLog(1, &got), raft.ErrLogNotFound)
	require.NoError(t, w.GetLog(3, &got))
	require.Equal(t, []byte("three"), got.Data)

	require.NoError(t, w.DeleteRange(3, 3))
	require.NoError(t, w.GetLog(2, &got))
	require.ErrorIs(t, w.GetLog(3, &got), raft.ErrLogNotFound)
	require.NoError(t, w.Close())

	w, err = openRaftWAL(dir, nil)
	require.NoError(t, err)
	defer w.Close()
	last, err = w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), last)
	require.ErrorIs(t, w.GetLog(3, &got), raft.ErrLogNotFound)
}

func TestRaftWALImportsBolt(t *testing.T) {
	dir := t.TempDir()
	bolt, err := raftboltdb.NewBoltStore(dir + "/raft.db")
	require.NoError(t, err)
	require.NoError(t, bolt.StoreLog(&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("from-bolt")}))
	require.NoError(t, bolt.StoreLog(&raft.Log{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("more")}))

	w, err := openRaftWAL(dir, bolt)
	require.NoError(t, err)
	var got raft.Log
	require.NoError(t, w.GetLog(1, &got))
	require.Equal(t, []byte("from-bolt"), got.Data)
	require.NoError(t, w.Close())

	w, err = openRaftWAL(dir, bolt)
	require.NoError(t, err)
	defer w.Close()
	last, err := w.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), last)
	require.NoError(t, w.GetLog(2, &got))
	require.Equal(t, []byte("more"), got.Data)
	require.NoError(t, bolt.Close())
}
