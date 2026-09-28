package sqlserver

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/cluster"
	hardhatdb "github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/sqle"
	"github.com/bongani-m/hardhatdb/go/store"
	sqle "github.com/dolthub/go-mysql-server"
	gmsql "github.com/dolthub/go-mysql-server/sql"
)

func TestMetaBackupRestoreAddsVoter(t *testing.T) {
	nodes := startForwardCluster(t, 1)
	leader := nodes[0]
	conn := openMySQL(t, leader.addr)
	execSQL(t, conn, "CREATE DATABASE mydb")
	execSQL(t, conn, "CREATE TABLE mydb.t (id BIGINT PRIMARY KEY, name TEXT)")
	execSQL(t, conn, "INSERT INTO mydb.t VALUES (1, 'ada')")

	backup := t.TempDir()
	backupCmd, ok := parseAdmin("BACKUP META TO '" + backup + "'")
	require.True(t, ok)
	require.NoError(t, finishAdmin(leader.store, backupCmd, discardResult))

	execSQL(t, conn, "INSERT INTO mydb.t VALUES (2, 'bea')")
	copyFiles(t, filepath.Join(leader.store.RaftDir(), "binlog"), filepath.Join(backup, "binlog"))

	raw, err := os.ReadFile(filepath.Join(backup, "meta"))
	require.NoError(t, err)
	var meta struct {
		Index uint64 `json:"index"`
	}
	require.NoError(t, json.Unmarshal(raw, &meta))
	require.Greater(t, meta.Index, uint64(0))

	stopForwardNode(t, leader)

	restoredPath := filepath.Join(t.TempDir(), "data")
	require.NoError(t, store.RestoreBackup(backup, restoredPath))
	require.Error(t, store.RestoreBackup(backup, restoredPath))

	restoredAddr, restoredTrans := raft.NewInmemTransportWithTimeout(raft.ServerAddress("restored"), 2*time.Second)
	restored, err := openCluster(restoredPath, cluster.ClusterOptions{
		ID:           "restored",
		Advertise:    "restored",
		RaftDir:      filepath.Join(t.TempDir(), "raft"),
		Bootstrap:    true,
		ServerUUID:   "11111111-1111-1111-1111-111111111111",
		Transport:    restoredTrans,
		Config:       fastRaft(),
		ApplyTimeout: 10 * time.Second,
		ForwardAddr:  "127.0.0.1:0",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })
	require.NoError(t, restored.WaitReady(10*time.Second))
	engine := sqle.NewDefault(restored)
	t.Cleanup(func() { _ = engine.Close() })
	restored.SetReplicaQuery(func(ctx *gmsql.Context, query string) error {
		_, iter, _, err := engine.Query(ctx, query)
		if err != nil {
			return err
		}
		_, err = gmsql.RowIterToRows(ctx, iter)
		return err
	})

	replay, ok := parseAdmin(fmt.Sprintf("RESTORE META BINLOG FROM '%s' AFTER %d", filepath.Join(backup, "binlog"), meta.Index))
	require.True(t, ok)
	require.NoError(t, finishAdmin(restored, replay, discardResult))
	require.Equal(t, []gmsql.Row{{"ada"}, {"bea"}}, queryNames(t, restored, engine))

	peerAddr, peerTrans := raft.NewInmemTransportWithTimeout(raft.ServerAddress("node-1"), 2*time.Second)
	restoredTrans.Connect(peerAddr, peerTrans)
	peerTrans.Connect(restoredAddr, restoredTrans)
	peer, err := openCluster(filepath.Join(t.TempDir(), "peer"), cluster.ClusterOptions{
		ID:           "node-1",
		Advertise:    "node-1",
		RaftDir:      filepath.Join(t.TempDir(), "peer-raft"),
		Bootstrap:    false,
		ServerUUID:   "11111111-1111-1111-1111-111111111111",
		Transport:    peerTrans,
		Config:       fastRaft(),
		ApplyTimeout: 10 * time.Second,
		ForwardAddr:  "127.0.0.1:0",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })

	add, ok := parseAdmin("META ADD VOTER 'node-1' 'node-1'")
	require.True(t, ok)
	require.NoError(t, finishAdmin(restored, add, discardResult))
	waitStoreCaughtUp(t, restored, peer)
	peerEngine := sqle.NewDefault(peer)
	t.Cleanup(func() { _ = peerEngine.Close() })
	require.Equal(t, []gmsql.Row{{"ada"}, {"bea"}}, queryNames(t, peer, peerEngine))
}

func TestMetaBackupForwardsToLeader(t *testing.T) {
	nodes := startForwardCluster(t, 2)
	var follower *forwardNode
	for _, node := range nodes {
		if !node.store.IsLeader() {
			follower = node
			break
		}
	}
	require.NotNil(t, follower)
	backup := t.TempDir()
	query := "BACKUP META TO '" + backup + "'"
	cmd, ok := parseAdmin(query)
	require.True(t, ok)
	h := &partHandler{meta: follower.store}
	require.NoError(t, h.metaAdmin(&mysql.Conn{}, query, cmd, discardResult))
	_, err := os.Stat(filepath.Join(backup, "state.bin"))
	require.NoError(t, err)
}

func TestMetaStatusStaysLocal(t *testing.T) {
	nodes := startForwardCluster(t, 2)
	var follower *forwardNode
	for _, node := range nodes {
		if !node.store.IsLeader() {
			follower = node
			break
		}
	}
	require.NotNil(t, follower)
	cmd, ok := parseAdmin("SHOW META STATUS")
	require.True(t, ok)
	var role string
	h := &partHandler{meta: follower.store}
	require.NoError(t, h.metaAdmin(&mysql.Conn{}, "SHOW META STATUS", cmd, func(res *sqltypes.Result, _ bool) error {
		role = res.Rows[0][0].ToString()
		return nil
	}))
	require.Equal(t, "follower", role)
}

func TestMetaStatementRequiresCatalog(t *testing.T) {
	nodes := startForwardCluster(t, 1)
	conn := openMySQL(t, nodes[0].addr)
	_, err := conn.ExecContext(t.Context(), "BACKUP META TO '/tmp/hardhatdb-meta-backup'")
	require.Error(t, err)
	require.Contains(t, err.Error(), "meta catalog is not configured")
}

func discardResult(*sqltypes.Result, bool) error { return nil }

func queryNames(t *testing.T, st *hardhatdb.Store, engine *sqle.Engine) []gmsql.Row {
	t.Helper()
	ctx := newCtx(st)
	ctx.SetCurrentDatabase("mydb")
	return runQuery(t, ctx, engine, "SELECT name FROM t ORDER BY id")
}

func copyFiles(t *testing.T, src, dst string) {
	t.Helper()
	require.NoError(t, os.RemoveAll(dst))
	require.NoError(t, os.MkdirAll(dst, 0o755))
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(src, entry.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dst, entry.Name()), raw, 0o644))
	}
}
