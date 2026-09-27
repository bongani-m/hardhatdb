package persist

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/vttls"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
)

// sourcePasswordFile is the local replica password. It is not written to Raft.
const sourcePasswordFile = "source.password"

const replicaServerID uint32 = 2

type sourceGTIDCtx struct{}

func withSourceGTID(ctx *sql.Context, gtid string) *sql.Context {
	if ctx == nil || gtid == "" {
		return ctx
	}
	return ctx.WithContext(context.WithValue(ctx, sourceGTIDCtx{}, gtid))
}

func sourceGTID(ctx *sql.Context) string {
	if ctx == nil || ctx.Context == nil {
		return ""
	}
	gtid, _ := ctx.Value(sourceGTIDCtx{}).(string)
	return gtid
}

// replicaSource is the upstream MySQL this cluster follows. Password is local.
type replicaSource struct {
	Host         string   `json:"host"`
	User         string   `json:"user"`
	Port         uint16   `json:"port"`
	Running      bool     `json:"running"`
	DoTables     []string `json:"do_tables,omitempty"`
	IgnoreTables []string `json:"ignore_tables,omitempty"`
	WildDo       []string `json:"wild_do,omitempty"`
	WildIgnore   []string `json:"wild_ignore,omitempty"`
	DoDBs        []string `json:"do_dbs,omitempty"`
	IgnoreDBs    []string `json:"ignore_dbs,omitempty"`
	Password     string   `json:"-"`
}

// binlogStream is one upstream dump. Close unblocks ReadEvent.
type binlogStream interface {
	ReadEvent() (mysql.BinlogEvent, error)
	Close() error
}

// binlogDial opens a dump at executed. Tests replace it.
type binlogDial func(ctx context.Context, host string, port uint16, user, password string, executed mysql.GTIDSet) (binlogStream, error)

// replicaQuery runs one upstream statement on the leader.
type replicaQuery func(ctx *sql.Context, query string) error

// replicaState is the leader's upstream applier.
type replicaState struct {
	mu              sync.Mutex
	upstream        *replicaSource
	upstreamStopped bool
	closed          bool
	alive           bool
	cancel          context.CancelFunc
	done            chan struct{}
	stream          binlogStream
	dial            binlogDial
	query           replicaQuery
	ioState         string
	sqlState        string
	lastIOErr       string
	lastSQLErr      string
	lastIOAt        *time.Time
	lastSQLAt       *time.Time
	sourceUUID      string
	lastSourceUnix  uint32
	sqlIdle         bool
}

func (s *Store) replica() *replicaState {
	s.replOnce.Do(func() {
		s.repl = &replicaState{
			ioState:  binlogreplication.ReplicaIoNotRunning,
			sqlState: binlogreplication.ReplicaSqlNotRunning,
		}
	})
	return s.repl
}

func (s *Store) onLeadership(isLeader bool) {
	s.clearAutoRanges()
	if !isLeader {
		s.haltReplica(false)
		return
	}
	s.kickReplica()
}

// EnableUpstream follows host while this node is the primary. Every node in
// the group needs the same host, user, and password so a promotion can dial.
func (s *Store) EnableUpstream(host string, port uint16, user, password string) {
	if port == 0 {
		port = 3306
	}
	st := s.replica()
	st.mu.Lock()
	st.upstream = &replicaSource{Host: host, Port: port, User: user, Password: password, Running: true}
	st.upstreamStopped = false
	st.mu.Unlock()
	if password != "" {
		_ = s.writeSourcePassword(password)
	}
	if s.IsLeader() {
		s.kickReplica()
	}
}

// SetReplicaQuery runs upstream Query events. Row events do not use it.
func (s *Store) SetReplicaQuery(fn func(ctx *sql.Context, query string) error) {
	st := s.replica()
	st.mu.Lock()
	st.query = fn
	st.mu.Unlock()
}

// SetBinlogDial replaces the MySQL dump connection. Tests use it.
func (s *Store) SetBinlogDial(d binlogDial) {
	st := s.replica()
	st.mu.Lock()
	st.dial = d
	st.mu.Unlock()
}

func (s *Store) kickReplica() {
	st := s.replica()
	st.mu.Lock()
	if st.closed || st.alive || !s.IsLeader() {
		st.mu.Unlock()
		return
	}
	if _, ok := s.replicaConfig(st); !ok {
		st.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	st.cancel = cancel
	st.done = done
	st.alive = true
	st.mu.Unlock()
	go s.replicaLoop(ctx, done)
}

func (s *Store) haltReplica(permanent bool) {
	st := s.replica()
	st.mu.Lock()
	if permanent {
		st.closed = true
	}
	cancel := st.cancel
	done := st.done
	stream := st.stream
	st.cancel = nil
	st.done = nil
	st.stream = nil
	st.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if stream != nil {
		_ = stream.Close()
	}
	if done != nil {
		<-done
	}
}

func (s *Store) stopReplica() {
	s.haltReplica(true)
}

func (s *Store) replicaLoop(ctx context.Context, done chan struct{}) {
	st := s.replica()
	defer func() {
		st.mu.Lock()
		closed := st.closed
		st.alive = false
		if st.done == done {
			st.done = nil
		}
		st.mu.Unlock()
		close(done)
		s.setReplicaIdle()
		if !closed && s.IsLeader() {
			s.kickReplica()
		}
	}()
	for {
		if ctx.Err() != nil {
			return
		}
		cfg, ok := s.replicaConfig(nil)
		if !ok {
			return
		}
		if cfg.Port == 0 {
			cfg.Port = 3306
		}
		s.setReplicaIO(binlogreplication.ReplicaIoConnecting, "")
		executed, err := s.loadSourceGTID()
		if err != nil {
			s.setReplicaSQL(binlogreplication.ReplicaSqlNotRunning, err.Error())
			if !sleepCtx(ctx, 2*time.Second) {
				return
			}
			continue
		}
		stream, err := s.dialUpstream(ctx, cfg, executed)
		if err != nil {
			s.setReplicaIO(binlogreplication.ReplicaIoConnecting, err.Error())
			if !sleepCtx(ctx, 2*time.Second) {
				return
			}
			continue
		}
		st.mu.Lock()
		st.stream = stream
		st.mu.Unlock()
		s.setReplicaIO(binlogreplication.ReplicaIoRunning, "")
		s.setReplicaSQL(binlogreplication.ReplicaSqlRunning, "")
		err = s.consumeUpstream(ctx, stream)
		_ = stream.Close()
		st.mu.Lock()
		if st.stream == stream {
			st.stream = nil
		}
		st.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.setReplicaSQL(binlogreplication.ReplicaSqlNotRunning, err.Error())
		}
		if !sleepCtx(ctx, 2*time.Second) {
			return
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Store) dialUpstream(ctx context.Context, cfg replicaSource, executed mysql.GTIDSet) (binlogStream, error) {
	st := s.replica()
	st.mu.Lock()
	dial := st.dial
	st.mu.Unlock()
	if dial == nil {
		dial = dialMySQL
	}
	return dial(ctx, cfg.Host, cfg.Port, cfg.User, cfg.Password, executed)
}

func dialMySQL(ctx context.Context, host string, port uint16, user, password string, executed mysql.GTIDSet) (binlogStream, error) {
	if executed == nil {
		executed = mysql.Mysql56GTIDSet{}
	}
	conn, err := mysql.Connect(ctx, &mysql.ConnParams{
		Host:             host,
		Port:             int(port),
		Uname:            user,
		Pass:             password,
		SslMode:          vttls.Disabled,
		ConnectTimeoutMs: 4000,
	})
	if err != nil {
		return nil, err
	}
	_, _ = conn.ExecuteFetch("set @master_binlog_checksum=@@global.binlog_checksum", 1, false)
	if err := conn.SendBinlogDumpCommand(replicaServerID, mysql.Position{GTIDSet: executed}); err != nil {
		conn.Close()
		return nil, err
	}
	return &mysqlBinlogStream{conn: conn}, nil
}

type mysqlBinlogStream struct {
	once sync.Once
	conn *mysql.Conn
}

func (m *mysqlBinlogStream) ReadEvent() (mysql.BinlogEvent, error) {
	return m.conn.ReadBinlogEvent()
}

func (m *mysqlBinlogStream) Close() error {
	m.once.Do(func() { m.conn.Close() })
	return nil
}

// replicaConfig reports the source to dial. held is non-nil when the caller
// already owns held.mu.
func (s *Store) replicaConfig(held *replicaState) (replicaSource, bool) {
	src, err := s.loadReplicaSource()
	if err != nil {
		return replicaSource{}, false
	}
	pass := s.readPasswordFile()
	if src.Host != "" {
		if !src.Running {
			return replicaSource{}, false
		}
		src.Password = pass
		if src.Password == "" {
			src.Password = s.memoryPassword(held)
		}
		return src, true
	}
	var up *replicaSource
	var stopped bool
	if held != nil {
		up = held.upstream
		stopped = held.upstreamStopped
	} else {
		st := s.replica()
		st.mu.Lock()
		up = st.upstream
		stopped = st.upstreamStopped
		st.mu.Unlock()
	}
	if up == nil || stopped || up.Host == "" {
		return replicaSource{}, false
	}
	copy := *up
	if pass != "" {
		copy.Password = pass
	}
	return copy, true
}

func (s *Store) memoryPassword(held *replicaState) string {
	if held != nil {
		if held.upstream != nil {
			return held.upstream.Password
		}
		return ""
	}
	st := s.replica()
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.upstream != nil {
		return st.upstream.Password
	}
	return ""
}

func (s *Store) loadReplicaSource() (replicaSource, error) {
	var src replicaSource
	err := s.view(func(tx *kvTx) error {
		raw := tx.root().Get(keyReplicaSource)
		if len(raw) == 0 {
			return nil
		}
		return json.Unmarshal(append([]byte(nil), raw...), &src)
	})
	return src, err
}

func (s *Store) saveReplicaSource(src replicaSource) error {
	raw, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return s.commitGTID("", "", func(tx *kvTx) error {
		return tx.root().Put(keyReplicaSource, raw)
	})
}

func (s *Store) loadSourceGTID() (mysql.Mysql56GTIDSet, error) {
	return s.loadGTIDKey(keySourceGTID)
}

func (s *Store) loadRetrievedGTID() (mysql.Mysql56GTIDSet, error) {
	return s.loadGTIDKey(keyRetrievedGTID)
}

func (s *Store) loadGTIDKey(key []byte) (mysql.Mysql56GTIDSet, error) {
	var raw []byte
	err := s.view(func(tx *kvTx) error {
		raw = append([]byte(nil), tx.root().Get(key)...)
		return nil
	})
	if err != nil || len(raw) == 0 {
		return mysql.Mysql56GTIDSet{}, err
	}
	set, err := mysql.ParseMysql56GTIDSet(string(raw))
	if err != nil {
		return nil, err
	}
	parsed, ok := set.(mysql.Mysql56GTIDSet)
	if !ok {
		return nil, fmt.Errorf("persist: gtid set %T", set)
	}
	return parsed, nil
}

// noteRetrievedGTID records a GTID the IO thread has read, before the SQL thread applies it.
func (s *Store) noteRetrievedGTID(gtid mysql.GTID) error {
	set, err := s.loadRetrievedGTID()
	if err != nil {
		return err
	}
	if len(set) == 0 {
		set, err = s.loadSourceGTID()
		if err != nil {
			return err
		}
	}
	next, ok := set.AddGTID(gtid).(mysql.Mysql56GTIDSet)
	if !ok {
		return fmt.Errorf("persist: cannot record retrieved gtid %v", gtid)
	}
	return s.commitGTID("", "", func(tx *kvTx) error {
		return tx.root().Put(keyRetrievedGTID, []byte(next.String()))
	})
}

func (s *Store) noteSourceTime(unix uint32) {
	if unix == 0 {
		return
	}
	st := s.replica()
	st.mu.Lock()
	st.lastSourceUnix = unix
	st.mu.Unlock()
}

func (s *Store) setReplicaSQLIdle(idle bool) {
	st := s.replica()
	st.mu.Lock()
	st.sqlIdle = idle
	st.mu.Unlock()
}

func (s *Store) passwordPath() string {
	if s.raftDir == "" {
		return ""
	}
	return filepath.Join(s.raftDir, sourcePasswordFile)
}

func (s *Store) writeSourcePassword(password string) error {
	path := s.passwordPath()
	if path == "" {
		return fmt.Errorf("persist: no directory for the source password")
	}
	if err := os.WriteFile(path, []byte(password), 0o600); err != nil {
		return err
	}
	return os.Chmod(path, 0o600)
}

func (s *Store) readPasswordFile() string {
	path := s.passwordPath()
	if path == "" {
		return ""
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(raw)
}

func (s *Store) setReplicaIO(state, errText string) {
	st := s.replica()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.ioState = state
	st.lastIOErr = errText
	if errText != "" {
		now := time.Now()
		st.lastIOAt = &now
	}
}

func (s *Store) setReplicaSQL(state, errText string) {
	st := s.replica()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.sqlState = state
	st.lastSQLErr = errText
	if errText != "" {
		now := time.Now()
		st.lastSQLAt = &now
	}
}

func (s *Store) setReplicaIdle() {
	s.setReplicaIO(binlogreplication.ReplicaIoNotRunning, "")
	s.setReplicaSQL(binlogreplication.ReplicaSqlNotRunning, "")
}

func (s *Store) noteSourceUUID(gtid mysql.GTID) {
	mysqlGTID, ok := gtid.(mysql.Mysql56GTID)
	if !ok {
		return
	}
	st := s.replica()
	st.mu.Lock()
	st.sourceUUID = mysqlGTID.Server.String()
	st.mu.Unlock()
}

var _ binlogreplication.BinlogReplicaController = (*Store)(nil)

// SetReplicationSourceOptions implements binlogreplication.BinlogReplicaController.
func (s *Store) SetReplicationSourceOptions(_ *sql.Context, options []binlogreplication.ReplicationOption) error {
	src, err := s.loadReplicaSource()
	if err != nil {
		return err
	}
	var password string
	var havePassword bool
	for _, option := range options {
		switch strings.ToUpper(option.Name) {
		case "SOURCE_HOST":
			src.Host, err = optionString(option)
		case "SOURCE_USER":
			src.User, err = optionString(option)
		case "SOURCE_PASSWORD":
			password, err = optionString(option)
			havePassword = err == nil
		case "SOURCE_PORT":
			var port int
			port, err = optionInt(option)
			src.Port = uint16(port)
		case "SOURCE_AUTO_POSITION":
			var on int
			on, err = optionInt(option)
			if err == nil && on < 1 {
				err = fmt.Errorf("persist: SOURCE_AUTO_POSITION cannot be disabled")
			}
		default:
			err = fmt.Errorf("persist: unknown replication source option: %s", option.Name)
		}
		if err != nil {
			return err
		}
	}
	if havePassword {
		if err := s.writeSourcePassword(password); err != nil {
			return err
		}
	}
	return s.saveReplicaSource(src)
}

// SetReplicationFilterOptions implements binlogreplication.BinlogReplicaController.
func (s *Store) SetReplicationFilterOptions(_ *sql.Context, options []binlogreplication.ReplicationOption) error {
	src, err := s.loadReplicaSource()
	if err != nil {
		return err
	}
	for _, option := range options {
		switch strings.ToUpper(option.Name) {
		case "REPLICATE_DO_TABLE":
			src.DoTables, err = optionTables(option)
		case "REPLICATE_IGNORE_TABLE":
			src.IgnoreTables, err = optionTables(option)
		case "REPLICATE_WILD_DO_TABLE":
			src.WildDo, err = optionStringList(option)
		case "REPLICATE_WILD_IGNORE_TABLE":
			src.WildIgnore, err = optionStringList(option)
		case "REPLICATE_DO_DB":
			src.DoDBs, err = optionStringList(option)
		case "REPLICATE_IGNORE_DB":
			src.IgnoreDBs, err = optionStringList(option)
		case "REPLICATE_REWRITE_DB":
			err = fmt.Errorf("persist: unsupported replication filter: %s", option.Name)
		default:
			err = fmt.Errorf("persist: unsupported replication filter: %s", option.Name)
		}
		if err != nil {
			return err
		}
	}
	return s.saveReplicaSource(src)
}

// StartReplica implements binlogreplication.BinlogReplicaController.
func (s *Store) StartReplica(*sql.Context) error {
	src, err := s.loadReplicaSource()
	if err != nil {
		return err
	}
	if src.Host == "" {
		st := s.replica()
		st.mu.Lock()
		if st.upstream != nil {
			src.Host = st.upstream.Host
			src.Port = st.upstream.Port
			src.User = st.upstream.User
		}
		st.mu.Unlock()
	}
	if src.Host == "" {
		return fmt.Errorf("persist: server is not configured as a replica; fix with CHANGE REPLICATION SOURCE TO")
	}
	src.Running = true
	if err := s.saveReplicaSource(src); err != nil {
		return err
	}
	st := s.replica()
	st.mu.Lock()
	st.upstreamStopped = false
	st.mu.Unlock()
	s.kickReplica()
	return nil
}

// StopReplica implements binlogreplication.BinlogReplicaController.
func (s *Store) StopReplica(*sql.Context) error {
	src, err := s.loadReplicaSource()
	if err != nil {
		return err
	}
	if src.Host != "" {
		src.Running = false
		if err := s.saveReplicaSource(src); err != nil {
			return err
		}
	}
	st := s.replica()
	st.mu.Lock()
	st.upstreamStopped = true
	st.mu.Unlock()
	s.haltReplica(false)
	s.setReplicaIdle()
	return nil
}

// ResetReplica implements binlogreplication.BinlogReplicaController.
func (s *Store) ResetReplica(_ *sql.Context, resetAll bool) error {
	st := s.replica()
	st.mu.Lock()
	alive := st.alive
	st.mu.Unlock()
	if alive {
		return fmt.Errorf("persist: stop replica before reset")
	}
	st.mu.Lock()
	st.lastIOErr = ""
	st.lastSQLErr = ""
	st.lastIOAt = nil
	st.lastSQLAt = nil
	if resetAll {
		st.upstream = nil
		st.upstreamStopped = true
	}
	st.mu.Unlock()
	if !resetAll {
		return nil
	}
	if path := s.passwordPath(); path != "" {
		_ = os.Remove(path)
	}
	return s.commitGTID("", "", func(tx *kvTx) error {
		if err := tx.root().Delete(keyReplicaSource); err != nil {
			return err
		}
		return tx.root().Delete(keyRetrievedGTID)
	})
}

// GetReplicaStatus implements binlogreplication.BinlogReplicaController.
func (s *Store) GetReplicaStatus(*sql.Context) (*binlogreplication.ReplicaStatus, error) {
	src, err := s.loadReplicaSource()
	if err != nil {
		return nil, err
	}
	st := s.replica()
	st.mu.Lock()
	up := st.upstream
	status := binlogreplication.ReplicaStatus{
		ReplicaIoRunning:      st.ioState,
		ReplicaSqlRunning:     st.sqlState,
		LastIoError:           st.lastIOErr,
		LastSqlError:          st.lastSQLErr,
		LastIoErrorTimestamp:  st.lastIOAt,
		LastSqlErrorTimestamp: st.lastSQLAt,
		SourceServerUuid:      st.sourceUUID,
		ConnectRetry:          5,
		AutoPosition:          true,
	}
	sqlRunning := st.sqlState == binlogreplication.ReplicaSqlRunning
	idle := st.sqlIdle
	lastUnix := st.lastSourceUnix
	st.mu.Unlock()
	if sqlRunning {
		var lag int64
		if !idle && lastUnix > 0 {
			lag = time.Now().Unix() - int64(lastUnix)
			if lag < 0 {
				lag = 0
			}
		}
		status.SecondsBehindSource = &lag
	}
	if src.Host == "" && (up == nil || up.Host == "") {
		return nil, nil
	}
	if src.Host == "" && up != nil {
		src.Host = up.Host
		src.Port = up.Port
		src.User = up.User
	}
	executed, err := s.loadSourceGTID()
	if err != nil {
		return nil, err
	}
	retrieved, err := s.loadRetrievedGTID()
	if err != nil {
		return nil, err
	}
	status.SourceHost = src.Host
	status.SourceUser = src.User
	status.SourcePort = uint(src.Port)
	status.ExecutedGtidSet = executed.String()
	status.RetrievedGtidSet = retrieved.String()
	status.ReplicateDoTables = append([]string(nil), src.DoTables...)
	status.ReplicateIgnoreTables = append([]string(nil), src.IgnoreTables...)
	status.ReplicateWildDoTables = append([]string(nil), src.WildDo...)
	status.ReplicateWildIgnoreTables = append([]string(nil), src.WildIgnore...)
	status.ReplicateDoDBs = append([]string(nil), src.DoDBs...)
	status.ReplicateIgnoreDBs = append([]string(nil), src.IgnoreDBs...)
	if status.ReplicaIoRunning == "" {
		status.ReplicaIoRunning = binlogreplication.ReplicaIoNotRunning
	}
	if status.ReplicaSqlRunning == "" {
		status.ReplicaSqlRunning = binlogreplication.ReplicaSqlNotRunning
	}
	return &status, nil
}

func optionString(option binlogreplication.ReplicationOption) (string, error) {
	value, ok := option.Value.(string)
	if !ok {
		return "", fmt.Errorf("persist: %s expects a string", option.Name)
	}
	return value, nil
}

func optionInt(option binlogreplication.ReplicationOption) (int, error) {
	switch value := option.Value.(type) {
	case int:
		return value, nil
	case int8:
		return int(value), nil
	case int16:
		return int(value), nil
	case int32:
		return int(value), nil
	case int64:
		return int(value), nil
	case uint16:
		return int(value), nil
	case uint32:
		return int(value), nil
	case uint64:
		return int(value), nil
	default:
		return 0, fmt.Errorf("persist: %s expects an integer", option.Name)
	}
}

func optionTables(option binlogreplication.ReplicationOption) ([]string, error) {
	tables, ok := option.Value.([]sql.UnresolvedTable)
	if !ok {
		return nil, fmt.Errorf("persist: %s expects a table list", option.Name)
	}
	out := make([]string, len(tables))
	for i, table := range tables {
		db := ""
		if table.Database() != nil {
			db = table.Database().Name()
		}
		out[i] = db + "." + table.Name()
	}
	return out, nil
}

func optionStringList(option binlogreplication.ReplicationOption) ([]string, error) {
	values, ok := option.Value.([]string)
	if !ok {
		return nil, fmt.Errorf("persist: %s expects a string list", option.Name)
	}
	if err := binlogreplication.ValidateWildcardTablePatterns(values); err != nil {
		return nil, err
	}
	return append([]string(nil), values...), nil
}

func (src replicaSource) allowsTable(db, table string) bool {
	if len(src.DoDBs) > 0 && !listHasFold(src.DoDBs, db) {
		return false
	}
	if listHasFold(src.IgnoreDBs, db) {
		return false
	}
	name := db + "." + table
	if len(src.DoTables) > 0 || len(src.WildDo) > 0 {
		if !listHasFold(src.DoTables, name) && !wildHas(src.WildDo, name) {
			return false
		}
	}
	if listHasFold(src.IgnoreTables, name) || wildHas(src.WildIgnore, name) {
		return false
	}
	return true
}

func listHasFold(list []string, name string) bool {
	for _, item := range list {
		if strings.EqualFold(item, name) {
			return true
		}
	}
	return false
}

func wildHas(patterns []string, name string) bool {
	for _, pattern := range patterns {
		if likeMatch(pattern, name) {
			return true
		}
	}
	return false
}

func likeMatch(pattern, value string) bool {
	var b strings.Builder
	b.WriteString("(?i)^")
	for _, r := range pattern {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteByte('$')
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false
	}
	return re.MatchString(value)
}

type replicaTxn struct {
	ctx        *sql.Context
	sess       *Session
	gtidText   string
	skip       bool
	sawRows    bool
	gtidStored bool
}

func (s *Store) beginReplicaTxn(gtid mysql.GTID) (*replicaTxn, error) {
	s.noteSourceUUID(gtid)
	set, err := s.loadSourceGTID()
	if err != nil {
		return nil, err
	}
	if set.ContainsGTID(gtid) {
		return &replicaTxn{skip: true}, nil
	}
	next, ok := set.AddGTID(gtid).(mysql.Mysql56GTIDSet)
	if !ok {
		return nil, fmt.Errorf("persist: cannot record source gtid %v", gtid)
	}
	sess := NewSession(sql.NewBaseSession(), s)
	ctx := withSourceGTID(sql.NewContext(context.Background(), sql.WithSession(sess)), next.String())
	ctx.SetIgnoreAutoCommit(true)
	return &replicaTxn{ctx: ctx, sess: sess, gtidText: next.String()}, nil
}

func (t *replicaTxn) query(s *Store, statement string) error {
	if t == nil || t.skip {
		return nil
	}
	trimmed := strings.TrimSpace(statement)
	if strings.EqualFold(trimmed, "BEGIN") || strings.EqualFold(trimmed, "COMMIT") {
		return nil
	}
	st := s.replica()
	st.mu.Lock()
	queryFn := st.query
	st.mu.Unlock()
	if queryFn == nil {
		return fmt.Errorf("persist: replica query executor is not set")
	}
	sess := NewSession(sql.NewBaseSession(), s)
	ctx := withSourceGTID(sql.NewContext(context.Background(), sql.WithSession(sess)), t.gtidText)
	if err := queryFn(ctx, statement); err != nil {
		return err
	}
	t.gtidStored = true
	return nil
}

func (t *replicaTxn) finish(s *Store) error {
	if t == nil || t.skip {
		return nil
	}
	if t.sawRows {
		return t.sess.CommitTransaction(t.ctx, nil)
	}
	if t.gtidStored {
		return nil
	}
	return s.commitGTID("", t.gtidText, func(tx *kvTx) error { return nil })
}

// ReplayBinlog applies an existing binlog file through the replica row applier.
// Point-in-time restore installs a Raft snapshot first, then calls this for the
// events that follow that snapshot. GTIDs already executed are skipped.
func (s *Store) ReplayBinlog(path string) error {
	events, format, err := readBinlogFile(path)
	if err != nil {
		return err
	}
	applier := &binlogApply{format: format, tables: map[uint64]*mysql.TableMap{}}
	for _, ev := range events {
		if err := applier.event(s, ev); err != nil {
			return err
		}
	}
	if applier.txn != nil {
		return applier.txn.finish(s)
	}
	return nil
}

type binlogApply struct {
	format mysql.BinlogFormat
	tables map[uint64]*mysql.TableMap
	txn    *replicaTxn
}

func (s *Store) consumeUpstream(ctx context.Context, stream binlogStream) error {
	applier := &binlogApply{tables: map[uint64]*mysql.TableMap{}}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.setReplicaSQLIdle(true)
		ev, err := stream.ReadEvent()
		s.setReplicaSQLIdle(false)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if err := applier.event(s, ev); err != nil {
			return err
		}
	}
}

func (a *binlogApply) event(s *Store, ev mysql.BinlogEvent) error {
	if ev == nil || !ev.IsValid() {
		return nil
	}
	if ev.IsFormatDescription() {
		format, err := ev.Format()
		if err != nil {
			return err
		}
		a.format = format
		return nil
	}
	if a.format.ChecksumAlgorithm != 0 {
		stripped, _, err := ev.StripChecksum(a.format)
		if err != nil {
			return err
		}
		ev = stripped
	}
	s.noteSourceTime(ev.Timestamp())
	if ev.IsPreviousGTIDs() || ev.IsRotate() {
		return nil
	}
	if ev.IsGTID() {
		if a.txn != nil {
			if err := a.txn.finish(s); err != nil {
				return err
			}
		}
		gtid, _, err := ev.GTID(a.format)
		if err != nil {
			return err
		}
		if err := s.noteRetrievedGTID(gtid); err != nil {
			return err
		}
		a.txn, err = s.beginReplicaTxn(gtid)
		return err
	}
	if a.txn == nil || a.txn.skip {
		return nil
	}
	switch {
	case ev.IsQuery():
		q, err := ev.Query(a.format)
		if err != nil {
			return err
		}
		if q.Database != "" && a.txn.sess != nil {
			a.txn.sess.SetCurrentDatabase(q.Database)
		}
		return a.txn.query(s, q.SQL)
	case ev.IsTableMap():
		tm, err := ev.TableMap(a.format)
		if err != nil {
			return err
		}
		a.tables[ev.TableID(a.format)] = tm
		return nil
	case ev.IsWriteRows(), ev.IsUpdateRows(), ev.IsDeleteRows():
		tm := a.tables[ev.TableID(a.format)]
		if tm == nil {
			return fmt.Errorf("persist: missing table map for binlog rows")
		}
		parsed, err := ev.Rows(a.format, tm)
		if err != nil {
			return err
		}
		return a.txn.applyRows(s, tm, ev, parsed)
	case ev.IsXID():
		if err := a.txn.finish(s); err != nil {
			return err
		}
		a.txn = nil
	default:
		if unsupportedRowsEvent(ev) {
			return fmt.Errorf("persist: unsupported binlog rows event: %s", ev.TypeName())
		}
	}
	return nil
}

// unsupportedRowsEvent reports row images this applier cannot apply.
// Heartbeat, rotate, and previous-GTID events are ignored by the caller.
func unsupportedRowsEvent(ev mysql.BinlogEvent) bool {
	raw := ev.Bytes()
	if len(raw) < 5 {
		return false
	}
	switch raw[4] {
	case 20, 21, 22: // v0 write, update, and delete rows
		return true
	case 39: // PARTIAL_UPDATE_ROWS_EVENT
		return true
	default:
		return false
	}
}

func (t *replicaTxn) applyRows(s *Store, tm *mysql.TableMap, ev mysql.BinlogEvent, parsed mysql.Rows) error {
	src, err := s.loadReplicaSource()
	if err != nil {
		return err
	}
	if !src.allowsTable(tm.Database, tm.Name) {
		return nil
	}
	db, err := s.Database(t.ctx, tm.Database)
	if err != nil {
		return err
	}
	table, ok, err := db.GetTableInsensitive(t.ctx, tm.Name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("persist: replica table %s.%s does not exist", tm.Database, tm.Name)
	}
	schema := table.Schema(t.ctx)
	switch {
	case ev.IsWriteRows():
		inserter := table.(sql.InsertableTable).Inserter(t.ctx)
		inserter.StatementBegin(t.ctx)
		for _, row := range parsed.Rows {
			values, err := decodeBinlogImage(schema, row.Data, parsed.DataColumns, row.NullColumns)
			if err != nil {
				_ = inserter.DiscardChanges(t.ctx, err)
				_ = inserter.Close(t.ctx)
				return err
			}
			if err := inserter.Insert(t.ctx, values); err != nil {
				_ = inserter.DiscardChanges(t.ctx, err)
				_ = inserter.Close(t.ctx)
				return err
			}
		}
		t.sawRows = true
		return inserter.Close(t.ctx)
	case ev.IsDeleteRows():
		deleter := table.(sql.DeletableTable).Deleter(t.ctx)
		deleter.StatementBegin(t.ctx)
		for _, row := range parsed.Rows {
			values, err := decodeBinlogImage(schema, row.Identify, parsed.IdentifyColumns, row.NullIdentifyColumns)
			if err != nil {
				_ = deleter.DiscardChanges(t.ctx, err)
				_ = deleter.Close(t.ctx)
				return err
			}
			if err := deleter.Delete(t.ctx, values); err != nil {
				_ = deleter.DiscardChanges(t.ctx, err)
				_ = deleter.Close(t.ctx)
				return err
			}
		}
		t.sawRows = true
		return deleter.Close(t.ctx)
	case ev.IsUpdateRows():
		updater := table.(sql.UpdatableTable).Updater(t.ctx)
		updater.StatementBegin(t.ctx)
		for _, row := range parsed.Rows {
			before, err := decodeBinlogImage(schema, row.Identify, parsed.IdentifyColumns, row.NullIdentifyColumns)
			if err != nil {
				_ = updater.DiscardChanges(t.ctx, err)
				_ = updater.Close(t.ctx)
				return err
			}
			after, err := decodeBinlogImage(schema, row.Data, parsed.DataColumns, row.NullColumns)
			if err != nil {
				_ = updater.DiscardChanges(t.ctx, err)
				_ = updater.Close(t.ctx)
				return err
			}
			if err := updater.Update(t.ctx, before, after); err != nil {
				_ = updater.DiscardChanges(t.ctx, err)
				_ = updater.Close(t.ctx)
				return err
			}
		}
		t.sawRows = true
		return updater.Close(t.ctx)
	default:
		return fmt.Errorf("persist: unsupported binlog rows event")
	}
}

func decodeBinlogImage(schema sql.Schema, data []byte, present, nulls mysql.Bitmap) (sql.Row, error) {
	row := make(sql.Row, len(schema))
	pos := 0
	bit := 0
	for i, col := range schema {
		if present.Count() > 0 && (i >= present.Count() || !present.Bit(i)) {
			continue
		}
		isNull := bit < nulls.Count() && nulls.Bit(bit)
		bit++
		if isNull {
			continue
		}
		value, n, err := decodeBinlogValue(col, data[pos:])
		if err != nil {
			return nil, err
		}
		row[i] = value
		pos += n
	}
	return row, nil
}

func decodeBinlogValue(col *sql.Column, data []byte) (interface{}, int, error) {
	width := 0
	var value interface{}
	switch col.Type.Type() {
	case query.Type_INT8, query.Type_UINT8:
		width = 1
	case query.Type_INT16, query.Type_UINT16:
		width = 2
	case query.Type_INT24, query.Type_UINT24:
		width = 3
	case query.Type_INT32, query.Type_UINT32:
		width = 4
	case query.Type_INT64, query.Type_UINT64:
		width = 8
	case query.Type_FLOAT32:
		width = 4
	case query.Type_FLOAT64:
		width = 8
	default:
		return decodeBinlogText(col, data)
	}
	if len(data) < width {
		return nil, 0, io.ErrUnexpectedEOF
	}
	switch col.Type.Type() {
	case query.Type_INT8:
		value = int8(data[0])
	case query.Type_UINT8:
		value = uint8(data[0])
	case query.Type_INT16:
		value = int16(binary.LittleEndian.Uint16(data))
	case query.Type_UINT16:
		value = binary.LittleEndian.Uint16(data)
	case query.Type_INT24:
		value = int32(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)
	case query.Type_UINT24:
		value = uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
	case query.Type_INT32:
		value = int32(binary.LittleEndian.Uint32(data))
	case query.Type_UINT32:
		value = binary.LittleEndian.Uint32(data)
	case query.Type_INT64:
		value = int64(binary.LittleEndian.Uint64(data))
	case query.Type_UINT64:
		value = binary.LittleEndian.Uint64(data)
	case query.Type_FLOAT32:
		value = math.Float32frombits(binary.LittleEndian.Uint32(data))
	case query.Type_FLOAT64:
		value = math.Float64frombits(binary.LittleEndian.Uint64(data))
	}
	converted, _, err := col.Type.Convert(context.Background(), value)
	if err != nil {
		return value, width, nil
	}
	return converted, width, nil
}

func decodeBinlogText(col *sql.Column, data []byte) (interface{}, int, error) {
	n := columnLengthBytes(col)
	if n < 1 {
		n = 1
	}
	if n > 4 {
		n = 4
	}
	if len(data) < n {
		return nil, 0, io.ErrUnexpectedEOF
	}
	var length int
	switch n {
	case 1:
		length = int(data[0])
	case 2:
		length = int(binary.LittleEndian.Uint16(data))
	case 3:
		length = int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)
	default:
		length = int(binary.LittleEndian.Uint32(data))
	}
	if length < 0 || len(data) < n+length {
		return nil, 0, io.ErrUnexpectedEOF
	}
	raw := data[n : n+length]
	converted, _, err := col.Type.Convert(context.Background(), string(raw))
	if err != nil {
		converted, _, err = col.Type.Convert(context.Background(), raw)
		if err != nil {
			return string(raw), n + length, nil
		}
	}
	return converted, n + length, nil
}
