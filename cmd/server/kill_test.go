package main

import (
	"context"
	"database/sql"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if os.Getenv("GMS_KILL_CHILD") == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}

func TestSigkillRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gms")
	certFile, keyFile := writeTestCert(t, t.TempDir())
	tlsName := strings.ReplaceAll(t.Name(), "/", "-")
	require.NoError(t, mysql.RegisterTLSConfig(tlsName, trustCert(t, certFile)))
	port := freePort(t)
	cmd := startPersist(t, dir, port, certFile, keyFile)
	db := openPersist(t, port, tlsName)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var email string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT email FROM mytable WHERE id = 1").Scan(&email))
	require.Equal(t, "janedeo@gmail.com", email)
	_, err := db.ExecContext(ctx, "UPDATE mytable SET email = ? WHERE id = 1", "killed@example.com")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	require.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait()

	cmd = startPersist(t, dir, port, certFile, keyFile)
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	db = openPersist(t, port, tlsName)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.QueryRowContext(ctx, "SELECT email FROM mytable WHERE id = 1").Scan(&email))
	require.Equal(t, "killed@example.com", email)
}

func TestSigtermRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gms")
	certFile, keyFile := writeTestCert(t, t.TempDir())
	tlsName := strings.ReplaceAll(t.Name(), "/", "-")
	require.NoError(t, mysql.RegisterTLSConfig(tlsName, trustCert(t, certFile)))
	port := freePort(t)
	cmd := startPersist(t, dir, port, certFile, keyFile)
	db := openPersist(t, port, tlsName)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, "UPDATE mytable SET email = ? WHERE id = 1", "stopped@example.com")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	waitErr := cmd.Wait()
	require.NoError(t, waitErr)

	cmd = startPersist(t, dir, port, certFile, keyFile)
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	db = openPersist(t, port, tlsName)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var email string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT email FROM mytable WHERE id = 1").Scan(&email))
	require.Equal(t, "stopped@example.com", email)
}

func TestShowRaftStatusStandalone(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gms")
	certFile, keyFile := writeTestCert(t, t.TempDir())
	tlsName := strings.ReplaceAll(t.Name(), "/", "-")
	require.NoError(t, mysql.RegisterTLSConfig(tlsName, trustCert(t, certFile)))
	port := freePort(t)
	cmd := startPersist(t, dir, port, certFile, keyFile)
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	}()
	db := openPersist(t, port, tlsName)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var role, leader, commit, applied, lag string
	require.NoError(t, db.QueryRowContext(ctx, "SHOW RAFT STATUS").Scan(&role, &leader, &commit, &applied, &lag))
	require.Equal(t, "standalone", role)
	require.Equal(t, "0", commit)
	require.Equal(t, "0", applied)
	require.Equal(t, "0", lag)
}

func startPersist(t *testing.T, dir, port, certFile, keyFile string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	env := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "GMS_") {
			continue
		}
		env = append(env, entry)
	}
	cmd.Env = append(env,
		"GMS_KILL_CHILD=1",
		"GMS_DATA="+dir,
		"GMS_MYSQL_HOST=127.0.0.1",
		"GMS_MYSQL_PORT="+port,
		"GMS_BOOTSTRAP_PASSWORD=secret",
		"GMS_SEED_EXAMPLE=1",
		"GMS_TLS_CERT="+certFile,
		"GMS_TLS_KEY="+keyFile,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return cmd
}

func openPersist(t *testing.T, port, tlsName string) *sql.DB {
	t.Helper()
	cfg := mysql.Config{
		User:                 "root",
		Passwd:               "secret",
		Net:                  "tcp",
		Addr:                 net.JoinHostPort("127.0.0.1", port),
		DBName:               "mydb",
		AllowNativePasswords: true,
		TLSConfig:            tlsName,
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	deadline := time.Now().Add(20 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		last = db.PingContext(ctx)
		cancel()
		if last == nil {
			return db
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server on %s did not accept connections: %v", port, last)
	return nil
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	require.NoError(t, ln.Close())
	return port
}
