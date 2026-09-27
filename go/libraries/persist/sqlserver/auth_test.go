package sqlserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	persist "github.com/bongani-m/persist/go/libraries/persist/sqle"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	gsql "github.com/dolthub/go-mysql-server/sql"
)

func TestBootstrapRequiresPassword(t *testing.T) {
	store, err := persist.Open(filepath.Join(t.TempDir(), "gms"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	engine := sqle.NewDefault(store)
	ctx := gsql.NewContext(context.Background())
	err = enableAuth(ctx, store, engine, bootstrapAccount{User: "root", Host: "%"})
	require.Error(t, err)
}

func TestLoadServerTLS(t *testing.T) {
	cfg, err := loadServerTLS("", "")
	require.NoError(t, err)
	require.Nil(t, cfg)
	_, err = loadServerTLS("cert.pem", "")
	require.Error(t, err)
	_, err = loadServerTLS("", "key.pem")
	require.Error(t, err)
}

func TestListenerAuthAndTLS(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCert(t, dir)
	store, err := persist.Open(filepath.Join(dir, "gms"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	engine := sqle.NewDefault(store)
	ctx := gsql.NewContext(context.Background())
	require.NoError(t, enableAuth(ctx, store, engine, bootstrapAccount{
		User:     "root",
		Password: "s3cret",
		Host:     "%",
	}))
	tlsCfg, err := loadServerTLS(certFile, keyFile)
	require.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv, err := server.NewServer(server.Config{
		Listener:               ln,
		TLSConfig:              tlsCfg,
		RequireSecureTransport: true,
	}, engine, gsql.NewContext, persist.NewSessionBuilder(store), nil)
	require.NoError(t, err)
	go func() { _ = srv.Start() }()
	t.Cleanup(func() { _ = srv.Close() })

	addr := ln.Addr().String()
	tlsName := strings.ReplaceAll(t.Name(), "/", "-")
	require.NoError(t, mysql.RegisterTLSConfig(tlsName, trustCert(t, certFile)))

	require.Error(t, pingMySQL(addr, "s3cret", ""))
	require.Error(t, pingMySQL(addr, "wrong", tlsName))
	require.NoError(t, pingMySQL(addr, "s3cret", tlsName))
}

func pingMySQL(addr, password, tlsName string) error {
	cfg := mysql.Config{
		User:                 "root",
		Passwd:               password,
		Net:                  "tcp",
		Addr:                 addr,
		AllowNativePasswords: true,
		TLSConfig:            tlsName,
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

func trustCert(t *testing.T, certFile string) *tls.Config {
	t.Helper()
	pemBytes, err := os.ReadFile(certFile)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pemBytes))
	return &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}
}

func writeTestCert(t *testing.T, dir string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		DNSNames:     []string{"localhost", "n1", "n2", "n3"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	return certPath, keyPath
}
