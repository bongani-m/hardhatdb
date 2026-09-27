package sqlserver

import (
	"crypto/tls"
	"fmt"
	"os"

	persist "github.com/bongani-m/persist/go/libraries/persist/sqle"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
)

// bootstrapAccount is the first login, created only when this store has no
// privilege blob yet. Password must be set for that first boot. Later boots
// load the blob and ignore the password.
type bootstrapAccount struct {
	User     string
	Password string
	Host     string
}

func accountFromEnv() bootstrapAccount {
	return bootstrapAccount{
		User:     envOr("GMS_BOOTSTRAP_USER", "root"),
		Password: os.Getenv("GMS_BOOTSTRAP_PASSWORD"),
		Host:     envOr("GMS_BOOTSTRAP_HOST", "%"),
	}
}

// enableAuth turns on the mysql privilege database and points it at the
// Badger blob. A follower with no blob yet still enables the database, so
// logins fail until the leader's accounts replicate. A disabled database
// would accept every user.
func enableAuth(ctx *sql.Context, store *persist.Store, engine *sqle.Engine, account bootstrapAccount) error {
	db := engine.Analyzer.Catalog.MySQLDb
	db.SetPersister(store.AttachPrivileges(db))
	loaded, err := store.LoadPrivileges(ctx)
	if err != nil {
		return err
	}
	if loaded {
		return nil
	}
	if store.Replicating() && !store.IsLeader() {
		db.SetEnabled(true)
		return nil
	}
	if account.Password == "" {
		return fmt.Errorf("GMS_BOOTSTRAP_PASSWORD is required to create the first account")
	}
	if account.User == "" {
		account.User = "root"
	}
	if account.Host == "" {
		account.Host = "%"
	}
	ed := db.Editor()
	defer ed.Close()
	db.AddSuperUser(ed, account.User, account.Host, account.Password)
	return db.Persist(ctx, ed)
}

// loadServerTLS builds the listener TLS config. Both paths empty means
// plaintext. One path set is an error.
func loadServerTLS(certFile, keyFile string) (*tls.Config, error) {
	if certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("GMS_TLS_CERT and GMS_TLS_KEY must both be set")
	}
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
