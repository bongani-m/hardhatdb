package sqlserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/cluster"
	hardhatdb "github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/sqle"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/eventscheduler"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Persistent MySQL server. Rows live in a Badger directory and are still there
// after this process exits.
//
//	HARDHATDB_SEED_EXAMPLE=1 HARDHATDB_BOOTSTRAP_PASSWORD=secret go run ./cmd/hardhatdb sql-server
//	mysql --host=127.0.0.1 --port=3306 --user=root --password=secret mydb --execute="SELECT name, email FROM accounts;"
//
// Example clients connect to this server over the MySQL protocol.
// Set HARDHATDB_DATA to choose the directory. The default is data/hardhatdb.
//
// Cluster mode is off unless HARDHATDB_RAFT_ADDR is set. One node bootstraps:
//
//	HARDHATDB_NODE_ID=n1 HARDHATDB_RAFT_ADDR=127.0.0.1:7001 HARDHATDB_RAFT_BOOTSTRAP=1 \
//	  HARDHATDB_SERVER_UUID=11111111-1111-1111-1111-111111111111 \
//	  HARDHATDB_RAFT_PEERS=n1=127.0.0.1:7001,n2=127.0.0.1:7002 go run ./cmd/hardhatdb sql-server
//
// Other nodes use the same HARDHATDB_SERVER_UUID and HARDHATDB_RAFT_PEERS, their own
// HARDHATDB_NODE_ID and HARDHATDB_RAFT_ADDR, and leave HARDHATDB_RAFT_BOOTSTRAP unset. Start
// them with the bootstrap node so the group can elect a leader.
//
//	docker compose up --build
//
// Then, from the host:
//
//	mysql --host=127.0.0.1 --port=3306 --user=root --password=secret mydb --execute="SELECT name, email FROM accounts;"
//	mysql --host=127.0.0.1 --port=3307 --user=root --password=secret mydb --execute="SELECT name, email FROM accounts;"
//
// HARDHATDB_MYSQL_HOST defaults to localhost. Set it to 0.0.0.0 to accept connections
// from other containers and from published host ports. HARDHATDB_MYSQL_PORT overrides
// 3306. HARDHATDB_RAFT_ADVERTISE is the address other nodes dial; HARDHATDB_RAFT_ADDR is
// the address this process binds.

const (
	accountsTable = "accounts"
	notesTable    = "notes"
)

var (
	dbName  = "mydb"
	address = "localhost"
	port    = 3306
)

func Serve() {
	limits, err := loadLimits()
	if err != nil {
		log.Fatal(err)
	}
	queryTimeout = limits.exec
	if host := os.Getenv("HARDHATDB_MYSQL_HOST"); host != "" {
		address = host
	}
	if raw := os.Getenv("HARDHATDB_MYSQL_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 65535 {
			log.Fatalf("HARDHATDB_MYSQL_PORT: %q", raw)
		}
		port = parsed
	}
	path := os.Getenv("HARDHATDB_DATA")
	if path == "" {
		path = "data/hardhatdb"
	}
	part, err := loadPartConfig()
	if err != nil {
		log.Fatal(err)
	}
	gate := &leadershipGate{}
	var leader *leaderExec
	var metaExec *leaderExec
	var meta *hardhatdb.Store
	var groups *groupHost
	if part != nil {
		metaPath := os.Getenv("HARDHATDB_META_DATA")
		if metaPath == "" {
			metaPath = "data/meta"
		}
		meta, err = openMeta(metaPath, func(isLeader bool) {
			if !isLeader && metaExec != nil {
				metaExec.dropAll()
			}
		})
		if err != nil {
			log.Fatalf("open meta %s: %v", metaPath, err)
		}
		defer meta.Close()
		nodeID := os.Getenv("HARDHATDB_NODE_ID")
		if nodeID == "" {
			nodeID = os.Getenv("HARDHATDB_RAFT_ADDR")
		}
		raftTLS, err := raftTLSFromEnv()
		if err != nil {
			log.Fatalf("raft tls: %v", err)
		}
		groups = newGroupHost(nodeID, filepath.Dir(path), raftTLS)
	}
	store, err := openStore(path, func(isLeader bool) {
		if part != nil {
			if !isLeader && leader != nil {
				leader.dropAll()
			}
			return
		}
		gate.set(isLeader)
		if !isLeader && leader != nil {
			leader.dropAll()
		}
	})
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	defer store.Close()
	if groups != nil {
		defer groups.close(store)
	}

	ctx := sql.NewContext(context.Background())
	if seedExample() && (!store.Replicating() || store.IsLeader()) {
		if err := ensureExample(ctx, store); err != nil {
			log.Fatalf("seed %s: %v", dbName, err)
		}
	}

	engine := sqle.NewDefault(store)
	defer func() { _ = engine.Close() }()
	if err := engine.InitializeEventScheduler(func() (*sql.Context, error) {
		return sql.NewContext(context.Background(), sql.WithSession(hardhatdb.NewSession(sql.NewBaseSession(), store))), nil
	}, eventscheduler.SchedulerOn, 0); err != nil {
		log.Fatalf("events: %v", err)
	}
	if store.Replicating() {
		engine.Analyzer.Catalog.BinlogPrimaryController = store
		engine.Analyzer.Catalog.BinlogReplicaController = store
		store.SetReplicaQuery(func(ctx *sql.Context, query string) error {
			_, iter, _, err := engine.Query(ctx, query)
			if err != nil {
				return err
			}
			_, err = sql.RowIterToRows(ctx, iter)
			return err
		})
	}
	if part == nil {
		gate.bind(engine, store.Replicating(), store.IsLeader())
	}
	if store.Replicating() {
		leader = newLeaderExec(engine, store)
		store.SetForwardExec(leader.Exec)
	}
	if groups != nil && leader != nil {
		groups.add(store.GroupID(), store, engine, leader)
		if err := groups.adopt(meta); err != nil {
			log.Fatalf("groups: %v", err)
		}
		if err := groups.recover(meta); err != nil {
			log.Fatalf("recover: %v", err)
		}
	}
	authStore := store
	if meta != nil {
		metaEngine := sqle.NewDefault(meta)
		defer func() { _ = metaEngine.Close() }()
		metaExec = newLeaderExec(metaEngine, meta)
		meta.SetForwardExec(metaExec.Exec)
		authStore = meta
	}
	if err := enableAuth(ctx, authStore, engine, accountFromEnv()); err != nil {
		log.Fatalf("auth: %v", err)
	}
	if err := enableUpstream(store); err != nil {
		log.Fatalf("upstream replica: %v", err)
	}
	tlsConfig, err := loadServerTLS(os.Getenv("HARDHATDB_TLS_CERT"), os.Getenv("HARDHATDB_TLS_KEY"))
	if err != nil {
		log.Fatalf("tls: %v", err)
	}
	queries := &countingMetric{}
	queryErrs := &countingMetric{}
	config := server.Config{
		Protocol:               "tcp",
		Address:                fmt.Sprintf("%s:%d", address, port),
		TLSConfig:              tlsConfig,
		RequireSecureTransport: tlsConfig != nil,
		MaxConnections:         limits.maxConns,
		ConnReadTimeout:        limits.read,
		ConnWriteTimeout:       limits.write,
		QueryCounter:           queries,
		QueryErrorCounter:      queryErrs,
	}
	var metricsSrv *http.Server
	if addr := strings.TrimSpace(os.Getenv("HARDHATDB_METRICS_ADDR")); addr != "" {
		metricsSrv, err = startMetrics(addr, store, queries, queryErrs)
		if err != nil {
			log.Fatalf("metrics: %v", err)
		}
		log.Printf("metrics listening on %s", addr)
	}
	s, err := server.NewServerWithHandler(config, engine, sql.NewContext, hardhatdb.NewSessionBuilder(store), nil, func(h mysql.Handler) (mysql.Handler, error) {
		inner, ok := h.(*server.Handler)
		if !ok {
			return nil, fmt.Errorf("hardhatdb: unexpected mysql handler %T", h)
		}
		// Followers forward writes. A standalone process only intercepts
		// Raft admin statements, and leaves every other statement on the
		// engine handler.
		if part != nil {
			return newPartHandler(newForwardHandler(inner, store), meta, part, groups), nil
		}
		if store.Replicating() {
			return newForwardHandler(inner, store), nil
		}
		return newAdminHandler(inner, store), nil
	})
	if err != nil {
		log.Fatal(err)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		log.Printf("shutting down")
		if metricsSrv != nil {
			stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = metricsSrv.Shutdown(stopCtx)
			cancel()
		}
		if err := s.Close(); err != nil {
			log.Printf("close listener: %v", err)
		}
	}()
	log.Printf("persistent MySQL listening on %s, data file %s", config.Address, path)
	if err = s.Start(); err != nil {
		log.Printf("server: %v", err)
	}
	waitSessions(engine, limits.shutdown)
}

// queryTimeout is the per-statement deadline. Zero means no deadline.
var queryTimeout time.Duration

func withQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if queryTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, queryTimeout)
}

// waitSessions blocks until the process list is empty or the timeout passes.
func waitSessions(engine *sqle.Engine, timeout time.Duration) {
	if engine == nil || engine.ProcessList == nil || timeout <= 0 {
		return
	}
	deadline := time.Now().Add(timeout)
	for {
		n := len(engine.ProcessList.Processes())
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			log.Printf("shutdown: %d sessions still open", n)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// leadershipGate turns the SQL engine read-only while this process is not
// the Raft primary. The engine does not exist until after the store opens.
type leadershipGate struct {
	mu     sync.Mutex
	engine *sqle.Engine
}

func (g *leadershipGate) set(isLeader bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.engine != nil {
		g.engine.ReadOnly.Store(!isLeader)
	}
}

func (g *leadershipGate) bind(engine *sqle.Engine, replicating, leader bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.engine = engine
	if replicating {
		engine.ReadOnly.Store(!leader)
	}
}

func openMeta(path string, onLeadership func(bool)) (*hardhatdb.Store, error) {
	peers, err := cluster.ParsePeers(os.Getenv("HARDHATDB_META_PEERS"))
	if err != nil {
		return nil, err
	}
	nonvoters, err := cluster.ParsePeers(os.Getenv("HARDHATDB_META_NONVOTERS"))
	if err != nil {
		return nil, err
	}
	addr := os.Getenv("HARDHATDB_META_ADDR")
	if addr == "" {
		return nil, fmt.Errorf("HARDHATDB_META_ADDR is empty")
	}
	id := os.Getenv("HARDHATDB_NODE_ID")
	if id == "" {
		id = addr
	}
	advertise := os.Getenv("HARDHATDB_META_ADVERTISE")
	if advertise == "" {
		advertise = addr
	}
	raftDir := os.Getenv("HARDHATDB_META_DIR")
	if raftDir == "" {
		raftDir = "data/meta-raft"
	}
	bootstrap := os.Getenv("HARDHATDB_META_BOOTSTRAP") == "1" || strings.EqualFold(os.Getenv("HARDHATDB_META_BOOTSTRAP"), "true")
	tlsConfig, err := raftTLSFromEnv()
	if err != nil {
		return nil, err
	}
	store, err := openCluster(path, cluster.ClusterOptions{
		ID:           id,
		Bind:         addr,
		Advertise:    advertise,
		RaftDir:      raftDir,
		Peers:        peers,
		Nonvoters:    nonvoters,
		Bootstrap:    bootstrap,
		ServerUUID:   os.Getenv("HARDHATDB_META_UUID"),
		OnLeadership: onLeadership,
		TLS:          tlsConfig,
		ForwardAddr:  os.Getenv("HARDHATDB_META_FORWARD"),
	})
	if err != nil {
		return nil, err
	}
	if store.Bootstrapped() {
		if err := store.WaitReady(30 * time.Second); err != nil {
			store.Close()
			return nil, err
		}
	}
	if err := store.WaitCaughtUp(30 * time.Second); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

func raftTLSFromEnv() (*tls.Config, error) {
	if os.Getenv("HARDHATDB_RAFT_TLS_CERT") == "" && os.Getenv("HARDHATDB_RAFT_TLS_KEY") == "" && os.Getenv("HARDHATDB_RAFT_TLS_CA") == "" {
		return nil, nil
	}
	return cluster.LoadRaftTLS(os.Getenv("HARDHATDB_RAFT_TLS_CERT"), os.Getenv("HARDHATDB_RAFT_TLS_KEY"), os.Getenv("HARDHATDB_RAFT_TLS_CA"))
}

func openStore(path string, onLeadership func(bool)) (*hardhatdb.Store, error) {
	addr := os.Getenv("HARDHATDB_RAFT_ADDR")
	if addr == "" {
		return hardhatdb.Open(path)
	}
	id := os.Getenv("HARDHATDB_NODE_ID")
	if id == "" {
		id = addr
	}
	peers, err := cluster.ParsePeers(os.Getenv("HARDHATDB_RAFT_PEERS"))
	if err != nil {
		return nil, err
	}
	bootstrap := os.Getenv("HARDHATDB_RAFT_BOOTSTRAP") == "1" || strings.EqualFold(os.Getenv("HARDHATDB_RAFT_BOOTSTRAP"), "true")
	maxBytes, err := parseBinlogMax(os.Getenv("HARDHATDB_BINLOG_MAX_SIZE"))
	if err != nil {
		return nil, err
	}
	tlsConfig, err := raftTLSFromEnv()
	if err != nil {
		return nil, err
	}
	store, err := openCluster(path, cluster.ClusterOptions{
		ID:             id,
		Bind:           addr,
		Advertise:      os.Getenv("HARDHATDB_RAFT_ADVERTISE"),
		RaftDir:        os.Getenv("HARDHATDB_RAFT_DIR"),
		Peers:          peers,
		Bootstrap:      bootstrap,
		ServerUUID:     os.Getenv("HARDHATDB_SERVER_UUID"),
		BinlogMaxBytes: maxBytes,
		OnLeadership:   onLeadership,
		TLS:            tlsConfig,
		ForwardAddr:    os.Getenv("HARDHATDB_FORWARD_ADDR"),
	})
	if err != nil {
		return nil, err
	}
	// WaitReady only after this process created the group. A restart with
	// HARDHATDB_RAFT_BOOTSTRAP still set joins as a follower; waiting to become
	// leader times out and exits the node.
	if store.Bootstrapped() {
		if err := store.WaitReady(30 * time.Second); err != nil {
			store.Close()
			return nil, err
		}
	}
	if err := store.WaitCaughtUp(30 * time.Second); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

func parseBinlogMax(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("HARDHATDB_BINLOG_MAX_SIZE: %q", raw)
	}
	return n, nil
}

func enableUpstream(store *hardhatdb.Store) error {
	host := strings.TrimSpace(os.Getenv("HARDHATDB_SOURCE_HOST"))
	if host == "" {
		return nil
	}
	port := 3306
	if raw := os.Getenv("HARDHATDB_SOURCE_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 65535 {
			return fmt.Errorf("HARDHATDB_SOURCE_PORT: %q", raw)
		}
		port = parsed
	}
	store.EnableUpstream(host, uint16(port), os.Getenv("HARDHATDB_SOURCE_USER"), os.Getenv("HARDHATDB_SOURCE_PASSWORD"))
	return nil
}

// ensureExample creates mydb.accounts and mydb.notes when accounts is missing.
// accounts.id is the shard column. Each account is inserted with two notes.
func ensureExample(ctx *sql.Context, store *hardhatdb.Store) error {
	if !store.HasDatabase(ctx, dbName) {
		if err := store.CreateDatabase(ctx, dbName); err != nil {
			return err
		}
	}
	db, err := store.Database(ctx, dbName)
	if err != nil {
		return err
	}
	_, ok, err := db.GetTableInsensitive(ctx, accountsTable)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	accountsSchema, err := accountsSchema()
	if err != nil {
		return err
	}
	if err := db.(sql.TableCreator).CreateTable(ctx, accountsTable, accountsSchema, sql.Collation_Default, ""); err != nil {
		return err
	}
	accounts, err := exampleTable(ctx, db, accountsTable)
	if err != nil {
		return err
	}
	if err := accounts.CreateIndex(ctx, sql.IndexDef{
		Name:       "accounts_email",
		Columns:    []sql.IndexColumn{{Name: "email"}},
		Constraint: sql.IndexConstraint_Unique,
	}); err != nil {
		return err
	}
	if err := accounts.CreateIndex(ctx, sql.IndexDef{
		Name:    "accounts_status_created",
		Columns: []sql.IndexColumn{{Name: "status"}, {Name: "created_at"}},
	}); err != nil {
		return err
	}
	notesSchema, err := notesSchema()
	if err != nil {
		return err
	}
	if err := db.(sql.TableCreator).CreateTable(ctx, notesTable, notesSchema, sql.Collation_Default, ""); err != nil {
		return err
	}
	notes, err := exampleTable(ctx, db, notesTable)
	if err != nil {
		return err
	}
	if err := notes.CreateIndex(ctx, sql.IndexDef{
		Name:    "notes_account",
		Columns: []sql.IndexColumn{{Name: "account_id"}},
	}); err != nil {
		return err
	}
	if err := notes.AddForeignKey(ctx, sql.ForeignKeyConstraint{
		Name:           "notes_account_fk",
		Database:       dbName,
		Table:          notesTable,
		Columns:        []string{"account_id"},
		ParentDatabase: dbName,
		ParentTable:    accountsTable,
		ParentColumns:  []string{"id"},
		OnUpdate:       sql.ForeignKeyReferentialAction_Restrict,
		OnDelete:       sql.ForeignKeyReferentialAction_Restrict,
		IsResolved:     true,
	}); err != nil {
		return err
	}
	created := time.Unix(0, 1667304000000001000).UTC()
	if err := insertExampleRows(ctx, accounts, accountRows(created)); err != nil {
		return err
	}
	return insertExampleRows(ctx, notes, noteRows(created))
}

func exampleTable(ctx *sql.Context, db sql.Database, name string) (interface {
	sql.IndexAlterableTable
	sql.ForeignKeyTable
	sql.InsertableTable
}, error) {
	table, ok, err := db.GetTableInsensitive(ctx, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("table %s was not created", name)
	}
	indexed, ok := table.(interface {
		sql.IndexAlterableTable
		sql.ForeignKeyTable
		sql.InsertableTable
	})
	if !ok {
		return nil, fmt.Errorf("table %s cannot store indexes", name)
	}
	return indexed, nil
}

func accountsSchema() (sql.PrimaryKeySchema, error) {
	email, err := varcharColumn(accountsTable, "email")
	if err != nil {
		return sql.PrimaryKeySchema{}, err
	}
	name, err := varcharColumn(accountsTable, "name")
	if err != nil {
		return sql.PrimaryKeySchema{}, err
	}
	return sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: accountsTable, PrimaryKey: true, AutoIncrement: true},
		email,
		name,
		{Name: "status", Type: types.Int8, Nullable: false, Source: accountsTable},
		{Name: "tags", Type: types.JSON, Nullable: false, Source: accountsTable},
		{Name: "created_at", Type: types.MustCreateDatetimeType(query.Type_DATETIME, 6), Nullable: false, Source: accountsTable},
	}), nil
}

func notesSchema() (sql.PrimaryKeySchema, error) {
	body, err := varcharColumn(notesTable, "body")
	if err != nil {
		return sql.PrimaryKeySchema{}, err
	}
	return sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: notesTable, PrimaryKey: true, AutoIncrement: true},
		{Name: "account_id", Type: types.Int64, Nullable: false, Source: notesTable},
		body,
		{Name: "created_at", Type: types.MustCreateDatetimeType(query.Type_DATETIME, 6), Nullable: false, Source: notesTable},
	}), nil
}

func varcharColumn(table, name string) (*sql.Column, error) {
	typ, err := types.CreateString(query.Type_VARCHAR, 255, sql.Collation_Default)
	if err != nil {
		return nil, err
	}
	return &sql.Column{Name: name, Type: typ, Nullable: false, Source: table}, nil
}

func insertExampleRows(ctx *sql.Context, table sql.InsertableTable, rows []sql.Row) error {
	inserter := table.Inserter(ctx)
	inserter.StatementBegin(ctx)
	for _, row := range rows {
		if err := inserter.Insert(ctx, row); err != nil {
			_ = inserter.DiscardChanges(ctx, err)
			_ = inserter.Close(ctx)
			return err
		}
	}
	if err := inserter.StatementComplete(ctx); err != nil {
		return err
	}
	return inserter.Close(ctx)
}

func accountRows(created time.Time) []sql.Row {
	rows := make([]sql.Row, len(seedAccounts))
	for i, account := range seedAccounts {
		rows[i] = sql.NewRow(account.id, account.email, account.name, account.status, types.MustJSON(account.tags), created)
	}
	return rows
}

func noteRows(created time.Time) []sql.Row {
	rows := make([]sql.Row, len(seedNotes))
	for i, note := range seedNotes {
		rows[i] = sql.NewRow(note.id, note.accountID, note.body, created)
	}
	return rows
}

var seedAccounts = []struct {
	id     int64
	email  string
	name   string
	status int8
	tags   string
}{
	{1, "ada@example.com", "Ada Lovelace", 1, `["demo"]`},
	{2, "grace@example.com", "Grace Hopper", 1, `["demo","compiler"]`},
	{3, "alan@example.com", "Alan Turing", 0, `[]`},
	{4, "katherine@example.com", "Katherine Johnson", 1, `["orbit"]`},
}

var seedNotes = []struct {
	id        int64
	accountID int64
	body      string
}{
	{1, 1, "Wrote the first program"},
	{2, 1, "Cluster demo"},
	{3, 2, "A compiler is a program"},
	{4, 2, "Second note"},
	{5, 3, "Can machines think"},
	{6, 3, "Second note"},
	{7, 4, "Calculated the trajectory"},
	{8, 4, "Second note"},
}
