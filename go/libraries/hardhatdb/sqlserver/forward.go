package sqlserver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/cluster"
	hardhatdb "github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/sqle"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type forwardAction int

const (
	actLocal forwardAction = iota
	actForward
	actBegin
	actEnd
)

// forwardHandler sends writes from a follower to the current leader.
// The engine stays read-only on a follower, so a write cannot commit locally.
type forwardHandler struct {
	*server.Handler
	store *hardhatdb.Store
	mu    sync.Mutex
	conns map[uint32]*fwdState
}

type fwdState struct {
	pinned bool
	wait   uint64
	client *cluster.ForwardClient
	leader string
}

func newForwardHandler(inner *server.Handler, store *hardhatdb.Store) *forwardHandler {
	return &forwardHandler{
		Handler: inner,
		store:   store,
		conns:   make(map[uint32]*fwdState),
	}
}

func (h *forwardHandler) ConnectionClosed(c *mysql.Conn) {
	h.dropState(c)
	h.Handler.ConnectionClosed(c)
}

func (h *forwardHandler) ComResetConnection(c *mysql.Conn) error {
	h.dropState(c)
	return h.Handler.ComResetConnection(c)
}

func (h *forwardHandler) ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error {
	return h.dispatch(ctx, c, query, nil, callback)
}

func (h *forwardHandler) ComMultiQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) (string, error) {
	first, rest := splitFirst(query)
	if err := h.dispatch(ctx, c, first, nil, callback); err != nil {
		return "", err
	}
	return rest, nil
}

func (h *forwardHandler) ComStmtExecute(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	// A leader executes the prepared statement it already has. Copying the
	// binds is only needed when the statement is forwarded.
	if !h.store.Replicating() || h.store.IsLeader() {
		return h.execLocal(ctx, c, prepare, callback)
	}
	var binds []cluster.ForwardBind
	for name, bind := range prepare.BindVars {
		binds = append(binds, cluster.ForwardBind{
			Name:  name,
			Type:  int32(bind.Type),
			Value: append([]byte(nil), bind.Value...),
		})
	}
	return h.dispatch(ctx, c, prepare.PrepareStmt, binds, func(res *sqltypes.Result, _ bool) error {
		return callback(res)
	})
}

// execLocal runs a prepared statement on this node. A transaction that was
// forwarded to the previous leader cannot continue here.
func (h *forwardHandler) execLocal(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	if cmd, ok := parseAdmin(prepare.PrepareStmt); ok {
		return h.handleAdmin(c, prepare.PrepareStmt, cmd, func(res *sqltypes.Result, _ bool) error {
			return callback(res)
		})
	}
	if h.store.Replicating() {
		st := h.state(c)
		if st.pinned {
			h.dropState(c)
			return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "hardhatdb: leader changed during transaction")
		}
	}
	return h.Handler.ComStmtExecute(ctx, c, prepare, callback)
}

func (h *forwardHandler) dispatch(ctx context.Context, c *mysql.Conn, query string, binds []cluster.ForwardBind, callback mysql.ResultSpoolFn) error {
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	if cmd, ok := parseAdmin(query); ok {
		return h.handleAdmin(c, query, cmd, callback)
	}
	st := h.state(c)
	if !h.store.Replicating() || h.store.IsLeader() {
		if h.store.Replicating() && st.pinned {
			h.dropState(c)
			return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "hardhatdb: leader changed during transaction")
		}
		return h.local(ctx, c, query, binds, callback)
	}
	if st.wait > 0 && !st.pinned {
		if err := h.store.WaitApplied(st.wait, h.store.ApplyTimeout()); err != nil {
			return err
		}
		st.wait = 0
	}
	switch classify(query, st.pinned) {
	case actLocal:
		return h.local(ctx, c, query, binds, callback)
	case actBegin:
		reply, err := h.forward(c, query, binds, true, false)
		if err != nil {
			return err
		}
		st.pinned = true
		return spoolReply(reply, callback)
	case actEnd:
		if !st.pinned {
			return h.local(ctx, c, query, binds, callback)
		}
		reply, err := h.forward(c, query, binds, true, true)
		st.pinned = false
		if err != nil {
			return err
		}
		st.wait = reply.Index
		h.applyReply(c, reply)
		return spoolReply(reply, callback)
	default:
		hold := st.pinned
		reply, err := h.forward(c, query, binds, hold, false)
		if err != nil {
			if st.pinned {
				h.dropState(c)
			}
			return err
		}
		if !hold {
			st.wait = reply.Index
		}
		h.applyReply(c, reply)
		return spoolReply(reply, callback)
	}
}

func (h *forwardHandler) handleAdmin(c *mysql.Conn, query string, cmd adminCmd, callback mysql.ResultSpoolFn) error {
	if cmd.kind == adminStatus || h.store.IsLeader() || !h.store.Replicating() {
		return runAdminLocal(h.store, cmd, callback)
	}
	reply, err := h.forward(c, query, nil, false, false)
	if err != nil {
		return err
	}
	return spoolReply(reply, callback)
}

func (h *forwardHandler) local(ctx context.Context, c *mysql.Conn, query string, binds []cluster.ForwardBind, callback mysql.ResultSpoolFn) error {
	if len(binds) == 0 {
		return h.Handler.ComQuery(ctx, c, query, callback)
	}
	prepare := &mysql.PrepareData{PrepareStmt: query, BindVars: bindMap(binds)}
	return h.Handler.ComStmtExecute(ctx, c, prepare, func(res *sqltypes.Result) error {
		return callback(res, false)
	})
}

func (h *forwardHandler) forward(c *mysql.Conn, query string, binds []cluster.ForwardBind, hold, release bool) (cluster.ForwardReply, error) {
	st := h.state(c)
	deadline := time.Now().Add(h.store.ApplyTimeout())
	req := cluster.ForwardRequest{
		Hold:     hold,
		Release:  release,
		Node:     h.store.NodeID(),
		Session:  uint64(c.ConnectionID),
		User:     c.User,
		Host:     remoteHost(c),
		Database: currentDB(h.Handler, c),
		Query:    query,
		Vars:     userVars(h.Handler, c),
		Binds:    binds,
	}
	var last error
	for {
		remain := time.Until(deadline)
		if remain <= 0 {
			if last == nil {
				last = fmt.Errorf("hardhatdb: leader is unavailable")
			}
			return cluster.ForwardReply{}, last
		}
		leader, err := h.leaderAddr()
		if err != nil {
			last = err
		} else if st.pinned && st.leader != "" && st.leader != leader {
			h.dropState(c)
			return cluster.ForwardReply{}, mysql.NewSQLError(mysql.ERUnknownError, "HY000", "hardhatdb: leader changed during transaction")
		} else {
			if st.client == nil || st.leader != leader {
				if st.client != nil {
					st.client.Close(h.store.NodeID(), uint64(c.ConnectionID))
					st.client = nil
				}
				client, err := h.store.DialForward()
				if err != nil {
					last = err
				} else {
					st.client = client
					st.leader = leader
				}
			}
			if st.client != nil {
				// The request is encoded inside Exec. A failure after that
				// must not be retried: the leader may already have applied it.
				reply, err := st.client.Exec(req, remain)
				if err != nil {
					st.client.Close(h.store.NodeID(), uint64(c.ConnectionID))
					st.client = nil
					if errors.Is(err, io.EOF) {
						err = mysql.NewSQLError(mysql.ERUnknownError, "HY000", "hardhatdb: leader connection closed")
					}
					return cluster.ForwardReply{}, err
				}
				if reply.Err != "" {
					return cluster.ForwardReply{}, mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
				}
				return reply, nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *forwardHandler) leaderAddr() (string, error) {
	return h.store.LeaderForwardAddr()
}

func spoolReply(reply cluster.ForwardReply, callback mysql.ResultSpoolFn) error {
	res := &sqltypes.Result{
		RowsAffected: reply.RowsAffected,
		InsertID:     reply.InsertID,
		Info:         reply.Info,
	}
	if len(reply.Fields) > 0 {
		res.Fields = make([]*querypb.Field, len(reply.Fields))
		for i, field := range reply.Fields {
			res.Fields[i] = &querypb.Field{
				Name:         field.Name,
				OrgName:      field.OrgName,
				Table:        field.Table,
				OrgTable:     field.OrgTable,
				Database:     field.Database,
				Type:         querypb.Type(field.Type),
				Charset:      field.Charset,
				ColumnLength: field.ColumnLength,
				Flags:        field.Flags,
				Decimals:     field.Decimals,
			}
		}
		res.Rows = make([][]sqltypes.Value, len(reply.Rows))
		for i, row := range reply.Rows {
			res.Rows[i] = make([]sqltypes.Value, len(row))
			for j, cell := range row {
				if cell.Null {
					res.Rows[i][j] = sqltypes.NULL
					continue
				}
				res.Rows[i][j] = sqltypes.MakeTrusted(querypb.Type(cell.Type), cell.Raw)
			}
		}
	}
	return callback(res, false)
}

func (h *forwardHandler) applyReply(c *mysql.Conn, reply cluster.ForwardReply) {
	sess, ok := h.Handler.ConnectionSession(c).(*hardhatdb.Session)
	if !ok || sess == nil {
		return
	}
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	if reply.Database != "" {
		sess.SetCurrentDatabase(reply.Database)
	}
	for _, v := range reply.Vars {
		_ = applyForwardVar(ctx, sess, v)
	}
}

func (h *forwardHandler) state(c *mysql.Conn) *fwdState {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.conns[c.ConnectionID]
	if st == nil {
		st = &fwdState{}
		h.conns[c.ConnectionID] = st
	}
	return st
}

func (h *forwardHandler) dropState(c *mysql.Conn) {
	h.mu.Lock()
	st := h.conns[c.ConnectionID]
	delete(h.conns, c.ConnectionID)
	h.mu.Unlock()
	if st != nil {
		h.dropClient(c, st)
	}
}

func (h *forwardHandler) dropClient(c *mysql.Conn, st *fwdState) {
	if st.client != nil {
		session := uint64(0)
		if c != nil {
			session = uint64(c.ConnectionID)
		}
		st.client.Close(h.store.NodeID(), session)
		st.client = nil
	}
	st.leader = ""
}

func isReadQuery(query string) bool {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return false
	}
	sel, ok := stmt.(*sqlparser.Select)
	return ok && !hasLock(sel.Lock) && sel.Into == nil
}

func classify(query string, pinned bool) forwardAction {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		if pinned {
			return actForward
		}
		return actForward
	}
	if pinned {
		switch stmt.(type) {
		case *sqlparser.Commit, *sqlparser.Rollback:
			return actEnd
		default:
			return actForward
		}
	}
	switch n := stmt.(type) {
	case *sqlparser.Begin:
		return actBegin
	case *sqlparser.Commit, *sqlparser.Rollback:
		return actLocal
	case *sqlparser.Select:
		if hasLock(n.Lock) || n.Into != nil {
			return actForward
		}
		return actLocal
	case *sqlparser.Show, *sqlparser.Set, *sqlparser.Use, *sqlparser.Explain:
		return actLocal
	case *sqlparser.SetOp:
		if !hasLock(n.Lock) && n.Into == nil && selectLocal(n.Left) && selectLocal(n.Right) {
			return actLocal
		}
		return actForward
	default:
		return actForward
	}
}

func hasLock(lock *sqlparser.Lock) bool {
	return lock != nil && lock.Type != ""
}

func selectLocal(stmt any) bool {
	switch n := stmt.(type) {
	case *sqlparser.Select:
		return !hasLock(n.Lock) && n.Into == nil
	case *sqlparser.SetOp:
		return !hasLock(n.Lock) && n.Into == nil && selectLocal(n.Left) && selectLocal(n.Right)
	case *sqlparser.ParenSelect:
		return !hasLock(n.Lock) && selectLocal(n.Select)
	default:
		return false
	}
}

func splitFirst(query string) (string, string) {
	pieces, err := sqlparser.SplitStatementToPieces(query)
	if err != nil || len(pieces) == 0 {
		return query, ""
	}
	first := pieces[0]
	rest := strings.TrimSpace(query[len(first):])
	rest = strings.TrimPrefix(rest, ";")
	return first, strings.TrimSpace(rest)
}

func currentDB(h *server.Handler, c *mysql.Conn) string {
	sess := h.ConnectionSession(c)
	if sess == nil {
		return ""
	}
	return sess.GetCurrentDatabase()
}

func userVars(h *server.Handler, c *mysql.Conn) []cluster.ForwardVar {
	sess, ok := h.ConnectionSession(c).(*hardhatdb.Session)
	if !ok || sess == nil {
		return nil
	}
	snap := sess.UserVariableSnapshot()
	out := make([]cluster.ForwardVar, 0, len(snap))
	for name, val := range snap {
		out = append(out, encodeForwardVar(name, val))
	}
	return out
}

func encodeForwardVar(name string, val sql.TypedValue) cluster.ForwardVar {
	v := cluster.ForwardVar{Name: name}
	switch x := val.Value.(type) {
	case nil:
		v.Kind = 'n'
	case int:
		v.Kind, v.Int = 'i', int64(x)
	case int8:
		v.Kind, v.Int = 'i', int64(x)
	case int16:
		v.Kind, v.Int = 'i', int64(x)
	case int32:
		v.Kind, v.Int = 'i', int64(x)
	case int64:
		v.Kind, v.Int = 'i', x
	case uint:
		v.Kind, v.Int = 'i', int64(x)
	case uint8:
		v.Kind, v.Int = 'i', int64(x)
	case uint16:
		v.Kind, v.Int = 'i', int64(x)
	case uint32:
		v.Kind, v.Int = 'i', int64(x)
	case uint64:
		v.Kind, v.Int = 'i', int64(x)
	case float32:
		v.Kind, v.Float = 'f', float64(x)
	case float64:
		v.Kind, v.Float = 'f', x
	case string:
		v.Kind, v.Text = 's', x
	case []byte:
		v.Kind, v.Raw = 'b', append([]byte(nil), x...)
	default:
		v.Kind, v.Text = 's', fmt.Sprint(x)
	}
	return v
}

func applyForwardVar(ctx *sql.Context, sess *hardhatdb.Session, v cluster.ForwardVar) error {
	switch v.Kind {
	case 'n':
		return sess.SetUserVariable(ctx, v.Name, nil, types.Null)
	case 'i':
		return sess.SetUserVariable(ctx, v.Name, v.Int, types.Int64)
	case 'f':
		return sess.SetUserVariable(ctx, v.Name, v.Float, types.Float64)
	case 'b':
		return sess.SetUserVariable(ctx, v.Name, append([]byte(nil), v.Raw...), types.Blob)
	default:
		return sess.SetUserVariable(ctx, v.Name, v.Text, types.LongText)
	}
}

func bindMap(binds []cluster.ForwardBind) map[string]*querypb.BindVariable {
	out := make(map[string]*querypb.BindVariable, len(binds))
	for _, bind := range binds {
		out[bind.Name] = &querypb.BindVariable{Type: querypb.Type(bind.Type), Value: bind.Value}
	}
	return out
}

func remoteHost(c *mysql.Conn) string {
	if c.Conn == nil || c.RemoteAddr() == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(c.RemoteAddr().String())
	if err != nil {
		return c.RemoteAddr().String()
	}
	return host
}

type leaderExec struct {
	mu       sync.Mutex
	engine   *sqle.Engine
	store    *hardhatdb.Store
	sessions map[string]*heldSession
	// command handles group open, split, and replica moves. It runs on
	// followers as well as the leader. PREPARE stays on the held session.
	command func(string) (cluster.ForwardReply, bool)
}

type heldSession struct {
	ctx  *sql.Context
	sess *hardhatdb.Session
}

func newLeaderExec(engine *sqle.Engine, store *hardhatdb.Store) *leaderExec {
	return &leaderExec{
		engine:   engine,
		store:    store,
		sessions: make(map[string]*heldSession),
	}
}

func (l *leaderExec) Exec(req cluster.ForwardRequest) cluster.ForwardReply {
	if req.Close {
		l.drop(req)
		return cluster.ForwardReply{}
	}
	if l.command != nil {
		if reply, ok := l.command(req.Query); ok {
			return reply
		}
	}
	// Status and plain reads are served from the local copy. A follower has
	// the log. Writes still require the leader.
	if cmd, ok := parseAdmin(req.Query); ok && cmd.kind == adminStatus {
		reply, err := runAdmin(l.store, cmd)
		if err != nil {
			return cluster.ForwardReply{Err: err.Error()}
		}
		return reply
	}
	if !l.store.IsLeader() && !isReadQuery(req.Query) {
		return cluster.ForwardReply{Err: "hardhatdb: not the leader"}
	}
	key := req.Node + "/" + fmt.Sprint(req.Session)
	l.mu.Lock()
	held := l.sessions[key]
	if held == nil {
		sess := hardhatdb.NewSession(sql.NewBaseSession(), l.store)
		sess.SetClient(sql.Client{User: req.User, Address: req.Host})
		ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
		held = &heldSession{ctx: ctx, sess: sess}
		if req.Hold {
			l.sessions[key] = held
		}
	}
	l.mu.Unlock()

	reply := l.run(held, req)
	if !req.Hold || req.Release {
		l.drop(req)
	}
	if reply.Err == "" {
		reply.Index = l.store.AppliedIndex()
		reply.Database = held.sess.GetCurrentDatabase()
		reply.Vars = snapshotVars(held.sess)
	}
	return reply
}

func (l *leaderExec) run(held *heldSession, req cluster.ForwardRequest) cluster.ForwardReply {
	if reply, ok := l.metaCmd(req.Query); ok {
		return reply
	}
	if kind, id, n, ok := parseTxCmd(req.Query); ok {
		return l.txCmd(held, kind, id, n)
	}
	if req.Database != "" {
		held.sess.SetCurrentDatabase(req.Database)
	}
	for _, v := range req.Vars {
		if err := applyForwardVar(held.ctx, held.sess, v); err != nil {
			return cluster.ForwardReply{Err: err.Error()}
		}
	}
	if cmd, ok := parseAdmin(req.Query); ok {
		reply, err := runAdmin(l.store, cmd)
		if err != nil {
			return cluster.ForwardReply{Err: err.Error()}
		}
		return reply
	}
	if p, kr, ok := parseRangeRecord(req.Query); ok {
		if err := l.store.SavePlacement(p); err != nil {
			return cluster.ForwardReply{Err: err.Error()}
		}
		if err := l.store.PutRange(kr); err != nil {
			return cluster.ForwardReply{Err: err.Error()}
		}
		return cluster.ForwardReply{}
	}
	qctx := held.ctx
	if queryTimeout > 0 {
		parent, cancel := context.WithTimeout(held.ctx, queryTimeout)
		defer cancel()
		copied := *held.ctx
		copied.Context = parent
		qctx = &copied
	}
	exprs, err := server.BindingsToExprs(bindMap(req.Binds))
	if err != nil {
		return cluster.ForwardReply{Err: err.Error()}
	}
	schema, iter, _, err := l.engine.QueryWithBindings(qctx, req.Query, nil, exprs, nil)
	if err != nil {
		return cluster.ForwardReply{Err: err.Error()}
	}
	var reply cluster.ForwardReply
	if len(schema) > 0 && !types.IsOkResultSchema(schema) {
		reply.Fields = forwardFields(qctx, schema)
	}
	for {
		row, err := iter.Next(qctx)
		if err != nil {
			_ = iter.Close(qctx)
			if errors.Is(err, io.EOF) {
				break
			}
			return cluster.ForwardReply{Err: err.Error()}
		}
		if types.IsOkResult(row) {
			ok := row[0].(types.OkResult)
			reply.RowsAffected = ok.RowsAffected
			reply.InsertID = ok.InsertID
			if ok.Info != nil {
				reply.Info = ok.Info.String()
			}
			continue
		}
		vals, err := server.RowToSQL(qctx, schema, row, nil, nil)
		if err != nil {
			_ = iter.Close(qctx)
			return cluster.ForwardReply{Err: err.Error()}
		}
		cells := make([]cluster.ForwardCell, len(vals))
		for i, val := range vals {
			if val.IsNull() {
				cells[i] = cluster.ForwardCell{Null: true}
				continue
			}
			cells[i] = cluster.ForwardCell{Type: int32(val.Type()), Raw: append([]byte(nil), val.ToBytes()...)}
		}
		reply.Rows = append(reply.Rows, cells)
	}
	return reply
}

func (l *leaderExec) drop(req cluster.ForwardRequest) {
	key := req.Node + "/" + fmt.Sprint(req.Session)
	l.mu.Lock()
	delete(l.sessions, key)
	l.mu.Unlock()
}

func (l *leaderExec) metaCmd(query string) (cluster.ForwardReply, bool) {
	q := strings.TrimSpace(query)
	if strings.EqualFold(q, "NEXT COMMIT") {
		n, err := l.store.NextCommit()
		if err != nil {
			return cluster.ForwardReply{Err: err.Error()}, true
		}
		return cluster.ForwardReply{InsertID: n}, true
	}
	fields, err := splitAdmin(q)
	if err == nil && len(fields) == 3 && strings.EqualFold(fields[0], "RANGE") && strings.EqualFold(fields[1], "OP") {
		raw, err := base64.RawURLEncoding.DecodeString(fields[2])
		if err != nil {
			return cluster.ForwardReply{Err: "hardhatdb: range op is malformed"}, true
		}
		var op hardhatdb.RangeOp
		if err := json.Unmarshal(raw, &op); err != nil {
			return cluster.ForwardReply{Err: err.Error()}, true
		}
		if err := l.store.ApplyRangeOp(op); err != nil {
			return cluster.ForwardReply{Err: err.Error()}, true
		}
		return cluster.ForwardReply{}, true
	}
	if err != nil || len(fields) != 4 || !strings.EqualFold(fields[0], "SAVE") || !strings.EqualFold(fields[1], "DECISION") {
		return cluster.ForwardReply{}, false
	}
	n, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil || fields[2] == "" {
		return cluster.ForwardReply{Err: "hardhatdb: decision is malformed"}, true
	}
	if err := l.store.SaveDecision(hardhatdb.TxnDecision{ID: fields[2], Commit: n}); err != nil {
		return cluster.ForwardReply{Err: err.Error()}, true
	}
	return cluster.ForwardReply{}, true
}

func (l *leaderExec) txCmd(held *heldSession, kind, id string, commitNo uint64) cluster.ForwardReply {
	var err error
	switch kind {
	case "prepare":
		err = held.sess.PrepareTransaction(held.ctx, id)
	case "commit":
		err = l.store.CommitPrepared(id, commitNo)
	case "abort":
		err = held.sess.AbortPrepared(id)
	default:
		err = fmt.Errorf("hardhatdb: unknown transaction command %s", kind)
	}
	if err != nil {
		return cluster.ForwardReply{Err: err.Error()}
	}
	return cluster.ForwardReply{}
}

// parseTxCmd recognizes PREPARE TX, COMMIT TX, and ABORT TX.
func parseTxCmd(q string) (kind, id string, commitNo uint64, ok bool) {
	fields, err := splitAdmin(q)
	if err != nil || len(fields) < 3 || !strings.EqualFold(fields[1], "TX") {
		return "", "", 0, false
	}
	switch strings.ToUpper(fields[0]) {
	case "PREPARE", "ABORT":
		if len(fields) != 3 || fields[2] == "" {
			return "", "", 0, false
		}
		return strings.ToLower(fields[0]), fields[2], 0, true
	case "COMMIT":
		if len(fields) != 4 {
			return "", "", 0, false
		}
		n, err := strconv.ParseUint(fields[3], 10, 64)
		if err != nil || fields[2] == "" {
			return "", "", 0, false
		}
		return "commit", fields[2], n, true
	default:
		return "", "", 0, false
	}
}

// parseRangeRecord recognizes SHARD TABLE ... RANGE GROUP ... PEER, including
// optional CHECK, START, and END. The meta leader stores that record.
func parseRangeRecord(query string) (hardhatdb.TablePlacement, hardhatdb.KeyRange, bool) {
	p, kr, ranged, ok := parseShardDetail(query)
	if !ok || !ranged || kr.Group == "" {
		return hardhatdb.TablePlacement{}, hardhatdb.KeyRange{}, false
	}
	return p, kr, true
}

func (l *leaderExec) dropAll() {
	l.mu.Lock()
	l.sessions = make(map[string]*heldSession)
	l.mu.Unlock()
}

func snapshotVars(sess *hardhatdb.Session) []cluster.ForwardVar {
	snap := sess.UserVariableSnapshot()
	out := make([]cluster.ForwardVar, 0, len(snap))
	for name, val := range snap {
		out = append(out, encodeForwardVar(name, val))
	}
	return out
}

func forwardFields(ctx *sql.Context, schema sql.Schema) []cluster.ForwardField {
	fields := server.SchemaToFields(ctx, schema)
	out := make([]cluster.ForwardField, len(fields))
	for i, field := range fields {
		out[i] = cluster.ForwardField{
			Name:         field.Name,
			OrgName:      field.OrgName,
			Table:        field.Table,
			OrgTable:     field.OrgTable,
			Database:     field.Database,
			Type:         int32(field.Type),
			Charset:      field.Charset,
			ColumnLength: field.ColumnLength,
			Flags:        field.Flags,
			Decimals:     field.Decimals,
		}
	}
	return out
}
