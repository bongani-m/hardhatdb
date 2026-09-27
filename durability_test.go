package persist

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestValueLogStaysSmall(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gms")
	store, err := Open(dir)
	require.NoError(t, err)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, store)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))
	payload := bytes.Repeat([]byte("x"), 1<<20+1)
	require.NoError(t, store.badgerDB().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("vlog-probe"), payload)
	}))
	matches, err := filepath.Glob(filepath.Join(dir, "*.vlog"))
	require.NoError(t, err)
	require.NotEmpty(t, matches)
	for _, name := range matches {
		info, err := os.Stat(name)
		require.NoError(t, err)
		require.LessOrEqual(t, info.Size(), int64(128<<20))
	}
	store.rewriteValueLog()
	require.NoError(t, store.Close())

	store, err = Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	ctx = sql.NewContext(context.Background())
	rows := readRows(t, ctx, tableNamed(t, ctx, store))
	require.Equal(t, "ada", rows[0][1])
	var got []byte
	err = store.badgerDB().View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("vlog-probe"))
		if err != nil {
			return err
		}
		got, err = item.ValueCopy(nil)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestDatabaseReplicationFilter(t *testing.T) {
	src := replicaSource{DoDBs: []string{"app"}}
	require.True(t, src.allowsTable("app", "t"))
	require.False(t, src.allowsTable("other", "t"))
	src = replicaSource{IgnoreDBs: []string{"mysql"}}
	require.False(t, src.allowsTable("mysql", "user"))
	require.True(t, src.allowsTable("app", "t"))
	src = replicaSource{DoDBs: []string{"app"}, IgnoreTables: []string{"app.secret"}}
	require.False(t, src.allowsTable("app", "secret"))
	require.True(t, src.allowsTable("app", "public"))
}

func TestUnsupportedRowsEvent(t *testing.T) {
	partial := make([]byte, 19)
	partial[4] = 39
	require.True(t, unsupportedRowsEvent(mysql.NewMariadbBinlogEvent(partial)))
	heartbeat := make([]byte, 19)
	heartbeat[4] = 27
	require.False(t, unsupportedRowsEvent(mysql.NewMariadbBinlogEvent(heartbeat)))
}

func TestRaftPlaintextRefusedOffLoopback(t *testing.T) {
	_, err := OpenCluster(filepath.Join(t.TempDir(), "gms"), ClusterOptions{
		ID:        "n1",
		Bind:      "10.1.1.1:7001",
		Advertise: "10.1.1.1:7001",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "GMS_RAFT_TLS_CERT")
}

func TestRaftTLSRejectsAnonymousDial(t *testing.T) {
	cfg, _ := testRaftCert(t)
	addr1 := freeAddr(t)
	addr2 := freeAddr(t)
	dir := t.TempDir()
	open := func(id, addr string, bootstrap bool) *Store {
		t.Helper()
		store, err := OpenCluster(filepath.Join(dir, id, "gms"), ClusterOptions{
			ID:          id,
			Bind:        addr,
			Advertise:   addr,
			RaftDir:     filepath.Join(dir, id, "raft"),
			Bootstrap:   bootstrap,
			ServerUUID:  testServerUUID,
			TLS:         cfg,
			ForwardAddr: "127.0.0.1:0",
			Config:      testRaftConfig(),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close() })
		return store
	}
	leader := open("n1", addr1, true)
	require.NoError(t, leader.WaitReady(10*time.Second))
	follower := open("n2", addr2, false)
	require.NoError(t, leader.AddVoter("n2", addr2))
	waitCaughtUp(t, leader, follower)

	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))
	waitRows(t, follower, 1)

	pool := x509.NewCertPool()
	pool.AddCert(cfg.Certificates[0].Leaf)
	conn, err := tls.Dial("tcp", addr1, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
	if err == nil {
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, err = conn.Read(make([]byte, 8))
		_ = conn.Close()
	}
	require.Error(t, err)
	require.NotContains(t, err.Error(), "timeout")

	client, err := dialForward(leader.cluster.forwardLn.Addr().String(), cfg, 2*time.Second)
	require.NoError(t, err)
	client.Close("n1", 1)
}

func TestClusterPartitionHeals(t *testing.T) {
	mem := newMemCluster(t, 3)
	stores := make([]*Store, 3)
	stores[0] = mem.open(t, 0, true, nil)
	require.NoError(t, stores[0].WaitReady(10*time.Second))
	for i := 1; i < 3; i++ {
		stores[i] = mem.open(t, i, false, nil)
		require.NoError(t, stores[0].AddVoter(nodeName(i), string(mem.addrs[i])))
		waitCaughtUp(t, stores[0], stores[i])
	}
	leader := waitLeader(t, stores)
	ctx := sql.NewContext(context.Background())
	table := kvTable(t, ctx, leader)
	require.NoError(t, insertRows(ctx, table, sql.NewRow(int64(1), "ada")))
	for _, store := range stores {
		waitRows(t, store, 1)
	}

	follower := -1
	for i, store := range stores {
		if store != leader {
			follower = i
			break
		}
	}
	for i := 0; i < 3; i++ {
		if i == follower {
			continue
		}
		mem.trans[follower].Disconnect(mem.addrs[i])
		mem.trans[i].Disconnect(mem.addrs[follower])
	}
	require.NoError(t, insertRows(ctx, tableNamed(t, ctx, leader), sql.NewRow(int64(2), "bea")))
	time.Sleep(300 * time.Millisecond)
	rows := readRows(t, ctx, tableNamed(t, ctx, stores[follower]))
	require.Len(t, rows, 1)

	for i := 0; i < 3; i++ {
		if i == follower {
			continue
		}
		mem.trans[follower].Connect(mem.addrs[i], mem.trans[i])
		mem.trans[i].Connect(mem.addrs[follower], mem.trans[follower])
	}
	waitRows(t, stores[follower], 2)
	rows = readRows(t, ctx, tableNamed(t, ctx, stores[follower]))
	require.Equal(t, "bea", rows[1][1])
}

func nodeName(i int) string {
	return fmt.Sprintf("node-%d", i)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func testRaftCert(t *testing.T) (*tls.Config, *x509.Certificate) {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	cfg := &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{der},
			PrivateKey:  key,
			Leaf:        cert,
		}},
		ClientCAs:  pool,
		RootCAs:    pool,
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS12,
	}
	return cfg, caCert
}

func TestCertFilesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg, ca := testRaftCert(t)
	key := cfg.Certificates[0].PrivateKey.(*rsa.PrivateKey)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ca.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "server.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cfg.Certificates[0].Certificate[0]}), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "server.key"), pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600))
	loaded, err := LoadRaftTLS(filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key"), filepath.Join(dir, "ca.crt"))
	require.NoError(t, err)
	require.NotNil(t, loaded)
}
