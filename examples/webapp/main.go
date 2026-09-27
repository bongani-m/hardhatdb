// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command examplewebapp is a CRUD API for the HardhatDB server.
// Start that server first, then run this program:
//
//	go run .
//
//	curl -s localhost:8080/people?size=2
//	curl -s 'localhost:8080/people?name=Jane'
//
// MYSQL_ADDRS is every MySQL address, comma-separated. Reads and writes use
// any of them. A broken connection tries the next address. Leave it unset to
// use MYSQL_HOST and MYSQL_PORT (default localhost:3306):
//
//	MYSQL_ADDRS=127.0.0.1:3306,127.0.0.1:3307,127.0.0.1:3308 go run .
//
// Another connection can still see an older copy. A write whose connection
// breaks before a result comes back is sent to the next address. Connections
// use TLS and trust ../../certs/ca.crt. Set MYSQL_TLS_CA to another PEM
// file, or to off for a plaintext server. The password matches the compose
// cluster default.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

func main() {
	if ca, ok := tlsCAPath(); ok {
		if err := registerMySQLTLS(ca); err != nil {
			log.Fatal(err)
		}
		log.Printf("MySQL TLS CA %s", ca)
	}
	addrs := mysqlAddrs()
	nodes, err := openMySQL(addrs)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		for _, db := range nodes {
			db.Close()
		}
	}()
	log.Printf("MySQL %s", strings.Join(addrs, ", "))

	addr := env("HTTP_ADDR", ":8080")
	log.Printf("API listening on %s", addr)
	if err := http.ListenAndServe(addr, NewHandler(NewMySQLStore(nodes...))); err != nil {
		log.Fatal(err)
	}
}

// mysqlAddrs is every node. MYSQL_ADDRS overrides MYSQL_HOST and MYSQL_PORT.
func mysqlAddrs() []string {
	if addrs := splitAddrs(os.Getenv("MYSQL_ADDRS")); len(addrs) > 0 {
		return addrs
	}
	return []string{net.JoinHostPort(env("MYSQL_HOST", "localhost"), env("MYSQL_PORT", "3306"))}
}

// openMySQL opens a pool for each address. One live node is enough to start.
func openMySQL(addrs []string) ([]*sql.DB, error) {
	nodes := make([]*sql.DB, 0, len(addrs))
	alive := 0
	var last error
	for _, addr := range addrs {
		db, err := sql.Open("mysql", mysqlDSN(addr))
		if err != nil {
			closeDBs(nodes)
			return nil, err
		}
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(2)
		nodes = append(nodes, db)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err != nil {
			log.Printf("MySQL %s is down: %v", addr, err)
			last = err
			continue
		}
		alive++
	}
	if alive == 0 {
		closeDBs(nodes)
		return nil, fmt.Errorf("no MySQL node accepted a connection: %w", last)
	}
	return nodes, nil
}

func closeDBs(nodes []*sql.DB) {
	for _, db := range nodes {
		db.Close()
	}
}

func mysqlDSN(addr string) string {
	cfg := mysql.Config{
		User:                 env("MYSQL_USER", "root"),
		Passwd:               env("MYSQL_PASSWORD", "dev-only-change-me"),
		Net:                  "tcp",
		Addr:                 addr,
		DBName:               env("MYSQL_DB", "mydb"),
		ParseTime:            true,
		Loc:                  time.UTC,
		AllowNativePasswords: true,
		Timeout:              2 * time.Second,
	}
	if _, ok := tlsCAPath(); ok {
		cfg.TLSConfig = "gms"
	}
	return cfg.FormatDSN()
}

// tlsCAPath is the PEM file the client uses to verify the server.
// The default is the dev certificate at the repo root. MYSQL_TLS_CA=off skips TLS.
func tlsCAPath() (string, bool) {
	if ca, ok := os.LookupEnv("MYSQL_TLS_CA"); ok {
		if ca == "" || ca == "off" {
			return "", false
		}
		return ca, true
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return filepath.Join("..", "..", "certs", "ca.crt"), true
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "certs", "ca.crt"), true
}

// registerMySQLTLS trusts the server certificate signed by the PEM file at caPath.
func registerMySQLTLS(caPath string) error {
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return fmt.Errorf("MYSQL_TLS_CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return fmt.Errorf("MYSQL_TLS_CA: no certificates in %s", caPath)
	}
	return mysql.RegisterTLSConfig("gms", &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	})
}

// splitAddrs parses a comma-separated host:port list. Empty input is no addresses.
func splitAddrs(raw string) []string {
	if raw == "" {
		return nil
	}
	var addrs []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			addrs = append(addrs, part)
		}
	}
	return addrs
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
