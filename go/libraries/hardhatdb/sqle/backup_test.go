package sqle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/bongani-m/hardhatdb/go/store"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestStandaloneBackupRestore(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	st, err := Open(data)
	require.NoError(t, err)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, st)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))

	backup := filepath.Join(dir, "backup")
	index, err := st.BackupTo(backup)
	require.NoError(t, err)
	require.Equal(t, uint64(0), index)
	meta, err := readBackupMeta(backup)
	require.NoError(t, err)
	require.Equal(t, uint64(0), meta.Index)
	_, err = os.Stat(filepath.Join(backup, "state.bin"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(backup, "binlog"))
	require.True(t, os.IsNotExist(err))

	occupied := filepath.Join(dir, "occupied")
	require.NoError(t, os.Mkdir(occupied, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(occupied, "keep"), []byte("x"), 0o644))
	_, err = st.BackupTo(occupied)
	require.Error(t, err)
	_, statErr := os.Stat(filepath.Join(occupied, "keep"))
	require.NoError(t, statErr)

	require.NoError(t, st.Close())

	restored := filepath.Join(dir, "restored")
	require.NoError(t, store.RestoreBackup(backup, restored))
	require.Error(t, store.RestoreBackup(backup, restored))

	again, err := Open(restored)
	require.NoError(t, err)
	t.Cleanup(func() { _ = again.Close() })
	ctx = sql.NewContext(context.Background())
	rows := readRows(t, ctx, tableNamed(t, ctx, again))
	require.Equal(t, []sql.Row{{int64(1), "ada"}}, rows)
}

func TestClusterBackupRestoreReplaysTail(t *testing.T) {
	mem := newMemCluster(t, 1)
	leader := mem.open(t, 0, true, nil)
	require.NoError(t, leader.WaitReady(10*time.Second))
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))

	backup := filepath.Join(t.TempDir(), "backup")
	index, err := leader.BackupTo(backup)
	require.NoError(t, err)
	require.Greater(t, index, uint64(0))
	meta, err := readBackupMeta(backup)
	require.NoError(t, err)
	require.Equal(t, index, meta.Index)
	require.Equal(t, testServerUUID, meta.UUID)

	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(2), "bea")))
	require.NoError(t, os.RemoveAll(filepath.Join(backup, "binlog")))
	require.NoError(t, copyDirFiles(filepath.Join(leader.RaftDir(), "binlog"), filepath.Join(backup, "binlog")))

	restored := filepath.Join(t.TempDir(), "data")
	require.NoError(t, store.RestoreBackup(backup, restored))
	require.Error(t, store.RestoreBackup(backup, restored))

	_, trans := raft.NewInmemTransportWithTimeout(raft.ServerAddress("restored"), 2*time.Second)
	again, err := OpenCluster(restored, ClusterOptions{
		ID:           "restored",
		Advertise:    "restored",
		RaftDir:      filepath.Join(t.TempDir(), "raft"),
		Bootstrap:    true,
		ServerUUID:   testServerUUID,
		Transport:    trans,
		Config:       testRaftConfig(),
		ApplyTimeout: 10 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = again.Close() })
	require.True(t, again.Bootstrapped())
	require.NoError(t, again.WaitReady(10*time.Second))

	rows := waitRows(t, again, 1)
	require.Equal(t, "ada", rows[0][1])

	require.NoError(t, again.ReplayBinlogAfter(filepath.Join(backup, "binlog"), index))
	rows = waitRows(t, again, 2)
	require.Equal(t, "ada", rows[0][1])
	require.Equal(t, "bea", rows[1][1])
}
