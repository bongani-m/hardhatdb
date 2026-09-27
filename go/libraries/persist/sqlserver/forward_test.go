package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	vtmysql "github.com/dolthub/vitess/go/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/bongani-m/persist/go/libraries/persist/cluster"
	persist "github.com/bongani-m/persist/go/libraries/persist/sqle"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	gmsql "github.com/dolthub/go-mysql-server/sql"
)

func TestClassifyForward(t *testing.T) {
	require.Equal(t, actBegin, classify("BEGIN", false))
	require.Equal(t, actLocal, classify("SELECT 1", false))
	require.Equal(t, actLocal, classify("SHOW TABLES", false))
	require.Equal(t, actLocal, classify("EXPLAIN SELECT 1", false))
	require.Equal(t, actForward, classify("INSERT INTO t VALUES (1)", false))
	require.Equal(t, actForward, classify("SELECT * FROM t FOR UPDATE", false))
	require.Equal(t, actForward, classify("CALL p()", false))
	require.Equal(t, actEnd, classify("COMMIT", true))
	require.Equal(t, actEnd, classify("ROLLBACK", true))
	require.Equal(t, actForward, classify("SELECT 1", true))
	require.Equal(t, actLocal, classify("COMMIT", false))
}

func TestFollowerForwardsWrites(t *testing.T) {
	nodes := startForwardCluster(t, 3)
	leader := waitTestLeader(t, nodes)
	leaderConn := openMySQL(t, leader.addr)
	execSQL(t, leaderConn, "CREATE DATABASE mydb")
	execSQL(t, leaderConn, "CREATE TABLE mydb.t (id BIGINT PRIMARY KEY, name TEXT)")
	for _, node := range nodes {
		waitStoreCaughtUp(t, leader.store, node.store)
	}

	var follower *forwardNode
	for _, node := range nodes {
		if node != leader {
			follower = node
			break
		}
	}
	conn := openMySQL(t, follower.addr)
	execSQL(t, conn, "INSERT INTO mydb.t VALUES (1, 'ada')")
	require.Equal(t, "ada", queryName(t, conn, 1))

	execSQL(t, conn, "BEGIN")
	execSQL(t, conn, "INSERT INTO mydb.t VALUES (2, 'bea')")
	require.Equal(t, "bea", queryName(t, conn, 2))
	execSQL(t, conn, "COMMIT")
	require.Equal(t, "bea", queryName(t, conn, 2))

	stmt, err := conn.PrepareContext(context.Background(), "INSERT INTO mydb.t (id, name) VALUES (?, ?)")
	require.NoError(t, err)
	_, err = stmt.Exec(4, "dee")
	require.NoError(t, err)
	require.NoError(t, stmt.Close())
	require.Equal(t, "dee", queryName(t, conn, 4))

	require.NoError(t, leader.store.TransferLeadership())
	newLeader := waitTestLeaderExcept(t, nodes, leader)
	execSQL(t, leaderConn, "INSERT INTO mydb.t VALUES (3, 'cy')")
	require.Equal(t, "cy", queryName(t, openMySQL(t, newLeader.addr), 3))
	require.Equal(t, "cy", queryName(t, leaderConn, 3))
}

type forwardNode struct {
	store *persist.Store
	addr  string
	srv   *server.Server
}

func startForwardCluster(t *testing.T, n int) []*forwardNode {
	t.Helper()
	dir := cluster.NewForwardDir()
	addrs := make([]raft.ServerAddress, n)
	trans := make([]*raft.InmemTransport, n)
	for i := 0; i < n; i++ {
		addr, tr := raft.NewInmemTransportWithTimeout(raft.ServerAddress(fmt.Sprintf("node-%d", i)), 2*time.Second)
		addrs[i] = addr
		trans[i] = tr
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				trans[i].Connect(addrs[j], trans[j])
			}
		}
	}

	nodes := make([]*forwardNode, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node-%d", i)
		gate := &leadershipGate{}
		store, err := openCluster(filepath.Join(t.TempDir(), id, "gms"), cluster.ClusterOptions{
			ID:           id,
			Advertise:    string(addrs[i]),
			RaftDir:      filepath.Join(t.TempDir(), id, "raft"),
			Bootstrap:    i == 0,
			ServerUUID:   "11111111-1111-1111-1111-111111111111",
			Transport:    trans[i],
			Config:       fastRaft(),
			ApplyTimeout: 10 * time.Second,
			ForwardAddr:  "127.0.0.1:0",
			ForwardDir:   dir,
			OnLeadership: func(isLeader bool) {
				gate.set(isLeader)
			},
		})
		require.NoError(t, err)
		engine := sqle.NewDefault(store)
		gate.mu.Lock()
		gate.engine = engine
		engine.ReadOnly.Store(!store.IsLeader())
		gate.mu.Unlock()
		store.SetForwardExec(newLeaderExec(engine, store).Exec)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		srv, err := server.NewServerWithHandler(server.Config{
			Protocol: "tcp",
			Address:  ln.Addr().String(),
			Listener: ln,
		}, engine, gmsql.NewContext, persist.NewSessionBuilder(store), nil, func(h vtmysql.Handler) (vtmysql.Handler, error) {
			inner, ok := h.(*server.Handler)
			if !ok {
				return nil, fmt.Errorf("unexpected handler %T", h)
			}
			return newForwardHandler(inner, store), nil
		})
		require.NoError(t, err)
		go func() { _ = srv.Start() }()
		node := &forwardNode{store: store, addr: ln.Addr().String(), srv: srv}
		nodes[i] = node
		t.Cleanup(func() {
			_ = node.srv.Close()
			_ = node.store.Close()
		})
	}
	require.NoError(t, nodes[0].store.WaitReady(10*time.Second))
	for i := 1; i < n; i++ {
		require.NoError(t, nodes[0].store.AddVoter(fmt.Sprintf("node-%d", i), string(addrs[i])))
		waitStoreCaughtUp(t, nodes[0].store, nodes[i].store)
	}
	return nodes
}

func fastRaft() *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LogLevel = "ERROR"
	cfg.HeartbeatTimeout = 100 * time.Millisecond
	cfg.ElectionTimeout = 100 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	return cfg
}

func waitTestLeader(t *testing.T, nodes []*forwardNode) *forwardNode {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.store.IsLeader() {
				return node
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}

func waitTestLeaderExcept(t *testing.T, nodes []*forwardNode, old *forwardNode) *forwardNode {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node != old && node.store.IsLeader() {
				return node
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("leadership did not move")
	return nil
}

func waitStoreCaughtUp(t *testing.T, leader, follower *persist.Store) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		last, err := leader.LastIndex()
		require.NoError(t, err)
		if last > 0 && follower.AppliedIndex() >= last {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	last, _ := leader.LastIndex()
	t.Fatalf("follower applied %d, leader last %d", follower.AppliedIndex(), last)
}

func openMySQL(t *testing.T, addr string) *sql.Conn {
	t.Helper()
	cfg := mysql.Config{
		User:                 "root",
		Net:                  "tcp",
		Addr:                 addr,
		AllowNativePasswords: true,
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func execSQL(t *testing.T, conn *sql.Conn, query string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := conn.ExecContext(ctx, query)
	require.NoError(t, err)
}

func TestRaftStatusAndRemove(t *testing.T) {
	nodes := startForwardCluster(t, 3)
	var leader, follower, other *forwardNode
	for _, node := range nodes {
		if node.store.IsLeader() {
			leader = node
			break
		}
	}
	require.NotNil(t, leader)
	for _, node := range nodes {
		if node == leader {
			continue
		}
		if follower == nil {
			follower = node
			continue
		}
		other = node
	}
	require.NotNil(t, follower)
	require.NotNil(t, other)

	conn := openMySQL(t, follower.addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var role, leaderAddr, commit, applied, lag string
	require.NoError(t, conn.QueryRowContext(ctx, "SHOW RAFT STATUS").Scan(&role, &leaderAddr, &commit, &applied, &lag))
	require.Equal(t, "follower", role)
	require.NotEmpty(t, leaderAddr)

	execSQL(t, conn, "RAFT REMOVE SERVER '"+other.store.NodeID()+"'")
	execSQL(t, openMySQL(t, leader.addr), "CREATE DATABASE IF NOT EXISTS mydb")
	execSQL(t, openMySQL(t, leader.addr), "CREATE TABLE IF NOT EXISTS mydb.kept (id bigint primary key, name varchar(32))")
	execSQL(t, openMySQL(t, follower.addr), "INSERT INTO mydb.kept VALUES (1, 'ada')")
	got := queryKept(t, openMySQL(t, leader.addr))
	require.Equal(t, "ada", got)
}

func queryKept(t *testing.T, conn *sql.Conn) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var name string
	err := conn.QueryRowContext(ctx, "SELECT name FROM mydb.kept WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	return name
}

func queryName(t *testing.T, conn *sql.Conn, id int) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var name string
	err := conn.QueryRowContext(ctx, "SELECT name FROM mydb.t WHERE id = ?", id).Scan(&name)
	require.NoError(t, err)
	return name
}
