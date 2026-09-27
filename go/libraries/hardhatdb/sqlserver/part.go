package sqlserver

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/cluster"
	hardhatdb "github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/sqle"
)

// partConfig is how to reach one member of each data group. Schema statements
// fan out over Contacts before any range exists.
type partConfig struct {
	Contacts []string
}

// loadPartConfig reads the data-group contact list. A process without
// HARDHATDB_META_ADDR is a single Raft group.
func loadPartConfig() (*partConfig, error) {
	if strings.TrimSpace(os.Getenv("HARDHATDB_META_ADDR")) == "" {
		return nil, nil
	}
	contacts, err := parseShardContacts(os.Getenv("HARDHATDB_SHARD_FORWARD"))
	if err != nil {
		return nil, err
	}
	return &partConfig{Contacts: contacts}, nil
}

func parseShardContacts(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("HARDHATDB_SHARD_FORWARD is empty")
	}
	var contacts []string
	seen := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		idxRaw, addr, ok := strings.Cut(part, "=")
		if !ok || addr == "" {
			return nil, fmt.Errorf("HARDHATDB_SHARD_FORWARD: %q must be index=host:port", part)
		}
		if _, err := strconv.Atoi(idxRaw); err != nil {
			return nil, fmt.Errorf("HARDHATDB_SHARD_FORWARD: index %q", idxRaw)
		}
		if _, ok := seen[addr]; ok {
			return nil, fmt.Errorf("HARDHATDB_SHARD_FORWARD: duplicate %s", addr)
		}
		seen[addr] = struct{}{}
		contacts = append(contacts, addr)
	}
	if len(contacts) < 2 {
		return nil, fmt.Errorf("HARDHATDB_SHARD_FORWARD: want at least two groups")
	}
	return contacts, nil
}

type partPin struct {
	inTx   bool
	groups []string
	begun  map[string]bool
}

// partHandler routes a statement to the shard that owns its key.
// Catalog changes go to meta and to every shard. The engine-wide read-only
// gate stays off: this node can lead its shard and follow meta.
type partHandler struct {
	*forwardHandler
	meta    *hardhatdb.Store
	cfg     *partConfig
	groups  *groupHost
	splitMu sync.Mutex
	mu      sync.Mutex
	pins    map[uint32]*partPin
	// clients is one forward connection per MySQL session per remote address.
	// A process-wide client would queue every session on one socket.
	clients map[uint32]map[string]*fwdConn
	places  placeCache
}

// fwdConn is a pooled forward connection and the node id used to close it.
type fwdConn struct {
	client *cluster.ForwardClient
	node   string
}

// placeCache is the placement list last read at a meta FSM index.
type placeCache struct {
	index uint64
	list  []hardhatdb.TablePlacement
	ok    bool
}

func newPartHandler(inner *forwardHandler, meta *hardhatdb.Store, cfg *partConfig, groups *groupHost) mysql.Handler {
	h := &partHandler{
		forwardHandler: inner,
		meta:           meta,
		cfg:            cfg,
		groups:         groups,
		pins:           make(map[uint32]*partPin),
		clients:        make(map[uint32]map[string]*fwdConn),
	}
	if groups != nil {
		groups.split = h.doSplit
		groups.move = h.movePeer
		go h.resumeSplits()
		if raw := strings.TrimSpace(os.Getenv("HARDHATDB_RANGE_SPLIT_BYTES")); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err == nil && n > 0 {
				go h.watchSplits(n)
			}
		}
	}
	return h
}

func (h *partHandler) ConnectionClosed(c *mysql.Conn) {
	h.dropSession(c.ConnectionID)
	h.forwardHandler.ConnectionClosed(c)
}

func (h *partHandler) ComResetConnection(c *mysql.Conn) error {
	h.dropSession(c.ConnectionID)
	return h.forwardHandler.ComResetConnection(c)
}

func (h *partHandler) dropSession(id uint32) {
	h.mu.Lock()
	delete(h.pins, id)
	by := h.clients[id]
	delete(h.clients, id)
	h.mu.Unlock()
	for _, conn := range by {
		conn.client.Close(conn.node, uint64(id))
	}
}

func (h *partHandler) ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error {
	return h.dispatch(ctx, c, query, nil, callback)
}

func (h *partHandler) ComMultiQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) (string, error) {
	first, rest := splitFirst(query)
	if err := h.dispatch(ctx, c, first, nil, callback); err != nil {
		return "", err
	}
	return rest, nil
}

func (h *partHandler) ComStmtExecute(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
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

func (h *partHandler) dispatch(ctx context.Context, c *mysql.Conn, query string, binds []cluster.ForwardBind, callback mysql.ResultSpoolFn) error {
	if _, ok := parseAdmin(query); ok {
		return h.forwardHandler.dispatch(ctx, c, query, binds, callback)
	}
	places, err := h.placements()
	if err != nil {
		return err
	}
	pin := h.pin(c)
	ranges, err := h.meta.Ranges()
	if err != nil {
		return err
	}
	if dbName, table, at, ok := parseSplitRange(query); ok {
		if err := h.handleSplit(c, dbName, table, at); err != nil {
			return err
		}
		return callback(&sqltypes.Result{}, false)
	}
	if add, group, id, raft, forward, ok := parseRangeMove(query); ok {
		if err := h.handleMove(c, add, group, id, raft, forward); err != nil {
			return err
		}
		return callback(&sqltypes.Result{}, false)
	}
	route, err := RouteQuery(query, currentDB(h.Handler, c), binds, places, ranges)
	if err != nil {
		return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", err.Error())
	}
	switch route.Kind {
	case RoutePlace:
		if err := h.savePlacement(c, query, route); err != nil {
			return err
		}
		return callback(&sqltypes.Result{}, false)
	case RouteDDL:
		if err := h.broadcastDDL(c, query); err != nil {
			return err
		}
		return callback(&sqltypes.Result{}, false)
	case RouteScatter:
		reply, err := h.scatterGroups(c, route.Groups, query, binds)
		if err != nil {
			return err
		}
		return spoolReply(reply, callback)
	case RouteShard:
		if len(route.Groups) == 1 && route.Groups[0] == h.store.GroupID() && len(pin.groups) == 0 {
			if err := h.rejectDups(c, route); err != nil {
				return err
			}
			return h.forwardHandler.dispatch(ctx, c, query, binds, callback)
		}
		return h.execRanged(c, pin, route, query, binds, callback)
	default:
		switch txKind(query) {
		case txBegin:
			pin.inTx = true
			pin.groups = nil
			pin.begun = map[string]bool{}
		case txEnd:
			groups := append([]string(nil), pin.groups...)
			pin.inTx = false
			pin.groups = nil
			pin.begun = map[string]bool{}
			if len(groups) > 0 {
				if err := h.finishRanges(c, query, groups); err != nil {
					return err
				}
			}
		}
		return h.forwardHandler.dispatch(ctx, c, query, binds, callback)
	}
}

func (h *partHandler) pin(c *mysql.Conn) *partPin {
	h.mu.Lock()
	defer h.mu.Unlock()
	pin := h.pins[c.ConnectionID]
	if pin == nil {
		pin = &partPin{begun: map[string]bool{}}
		h.pins[c.ConnectionID] = pin
	}
	return pin
}

func (h *partHandler) placements() ([]hardhatdb.TablePlacement, error) {
	index := h.meta.FSMApplied()
	h.mu.Lock()
	if h.places.ok && h.places.index == index {
		list := append([]hardhatdb.TablePlacement(nil), h.places.list...)
		h.mu.Unlock()
		return list, nil
	}
	h.mu.Unlock()

	list, err := h.meta.Placements()
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	if h.meta.FSMApplied() == index {
		h.places = placeCache{
			index: index,
			list:  append([]hardhatdb.TablePlacement(nil), list...),
			ok:    true,
		}
	}
	h.mu.Unlock()
	return list, nil
}

func (h *partHandler) savePlacement(c *mysql.Conn, query string, route Route) error {
	return h.saveRange(c, query, route.Place, route.Span, route.SpanSet)
}

// broadcastDDL applies query to meta, then to every data-group leader.
func (h *partHandler) broadcastDDL(c *mysql.Conn, query string) error {
	if err := h.execMeta(c, query); err != nil {
		return err
	}
	local := h.store.LocalForwardAddr()
	for _, contact := range h.cfg.Contacts {
		addr, err := h.leaderForward(c, contact)
		if err != nil {
			return err
		}
		reply, err := h.call(c, h.store, addr, h.request(c, h.store, query, nil))
		if err != nil {
			return err
		}
		if reply.Err != "" {
			return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
		}
		if local != "" && addr == local && reply.Index > 0 {
			if err := h.store.WaitApplied(reply.Index, h.store.ApplyTimeout()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *partHandler) execMeta(c *mysql.Conn, query string) error {
	reply, err := h.call(c, h.meta, "", h.request(c, h.meta, query, nil))
	if err != nil {
		return err
	}
	if reply.Err != "" {
		return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
	}
	if !h.meta.IsLeader() && reply.Index > 0 {
		return h.meta.WaitApplied(reply.Index, h.meta.ApplyTimeout())
	}
	return nil
}

// checkBatch is how many unique values one scatter probes. A seed insert of
// 100 rows fits in one query.
const checkBatch = 200

type checkQuery struct {
	Column string
	SQL    string
}

// checkQueries builds one lookup per column. A value repeated in this
// statement is a duplicate before any shard is asked.
func checkQueries(place hardhatdb.TablePlacement, checks []routedValue) ([]checkQuery, *routedValue) {
	if len(checks) == 0 {
		return nil, nil
	}
	type group struct {
		vals []routedValue
		seen map[string]struct{}
	}
	var order []string
	groups := make(map[string]*group)
	for _, check := range checks {
		g := groups[check.Column]
		if g == nil {
			g = &group{seen: make(map[string]struct{})}
			groups[check.Column] = g
			order = append(order, check.Column)
		}
		if _, ok := g.seen[check.Text]; ok {
			dup := check
			return nil, &dup
		}
		g.seen[check.Text] = struct{}{}
		g.vals = append(g.vals, check)
	}
	var queries []checkQuery
	for _, column := range order {
		vals := groups[column].vals
		for from := 0; from < len(vals); from += checkBatch {
			to := from + checkBatch
			if to > len(vals) {
				to = len(vals)
			}
			var b strings.Builder
			b.WriteString("SELECT ")
			b.WriteString(column)
			b.WriteString(" FROM ")
			b.WriteString(place.DB)
			b.WriteByte('.')
			b.WriteString(place.Table)
			b.WriteString(" WHERE ")
			b.WriteString(column)
			b.WriteString(" IN (")
			for i, v := range vals[from:to] {
				if i > 0 {
					b.WriteByte(',')
				}
				b.WriteString(sqlLiteral(v))
			}
			b.WriteByte(')')
			queries = append(queries, checkQuery{Column: column, SQL: b.String()})
		}
	}
	return queries, nil
}

func sqlLiteral(v routedValue) string {
	if !v.Quote {
		return v.Text
	}
	return "'" + strings.ReplaceAll(v.Text, "'", "''") + "'"
}

func (h *partHandler) rejectDups(c *mysql.Conn, route Route) error {
	queries, dup := checkQueries(route.Place, route.Checks)
	if dup != nil {
		return dupEntry(dup.Text, dup.Column)
	}
	for _, q := range queries {
		ids := groupsOf(h.rangeList(), route.Place)
		reply, err := h.scatterGroups(c, ids, q.SQL, nil)
		if err != nil {
			return err
		}
		if len(reply.Rows) == 0 || len(reply.Rows[0]) == 0 || reply.Rows[0][0].Null {
			continue
		}
		return dupEntry(string(reply.Rows[0][0].Raw), q.Column)
	}
	return nil
}

func dupEntry(text, column string) error {
	return mysql.NewSQLError(mysql.ERDupEntry, "23000", "Duplicate entry '%s' for key '%s'", text, column)
}

// leaderForward resolves the leader's forward address from one member of the group.
func (h *partHandler) leaderForward(c *mysql.Conn, contact string) (string, error) {
	reply, err := h.call(c, h.store, contact, queryReq(h.store, "SHOW RAFT STATUS", nil))
	if err != nil {
		return "", err
	}
	if reply.Err != "" {
		return "", fmt.Errorf("%s", reply.Err)
	}
	if len(reply.Rows) == 0 || len(reply.Rows[0]) < 2 || len(reply.Rows[0][1].Raw) == 0 {
		return "", fmt.Errorf("hardhatdb: shard leader is unknown")
	}
	leader := string(reply.Rows[0][1].Raw)
	host, _, err := net.SplitHostPort(leader)
	if err != nil {
		return "", err
	}
	_, port, err := net.SplitHostPort(contact)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, port), nil
}

// call runs req on addr. An empty addr is the store's current leader. The
// local forward listener runs in-process. Any other address reuses the
// session's pooled connection.
func (h *partHandler) call(c *mysql.Conn, store *hardhatdb.Store, addr string, req cluster.ForwardRequest) (cluster.ForwardReply, error) {
	if addr == "" {
		var err error
		addr, err = store.LeaderForwardAddr()
		if err != nil {
			return cluster.ForwardReply{}, err
		}
	}
	if local := store.LocalForwardAddr(); local != "" && addr == local {
		return store.ExecForward(req)
	}
	return h.execPooled(c, store, addr, req)
}

func (h *partHandler) execPooled(c *mysql.Conn, store *hardhatdb.Store, addr string, req cluster.ForwardRequest) (cluster.ForwardReply, error) {
	id := uint32(0)
	if c != nil {
		id = c.ConnectionID
	}
	conn, err := h.clientFor(id, store, addr)
	if err != nil {
		return cluster.ForwardReply{}, err
	}
	reply, err := conn.client.Exec(req, store.ApplyTimeout())
	if err != nil {
		h.retire(id, addr)
		return cluster.ForwardReply{}, err
	}
	return reply, nil
}

func (h *partHandler) clientFor(id uint32, store *hardhatdb.Store, addr string) (*fwdConn, error) {
	h.mu.Lock()
	if by := h.clients[id]; by != nil {
		if conn := by[addr]; conn != nil {
			h.mu.Unlock()
			return conn, nil
		}
	}
	h.mu.Unlock()

	client, err := store.DialForwardAddr(addr)
	if err != nil {
		return nil, err
	}
	conn := &fwdConn{client: client, node: store.NodeID()}

	h.mu.Lock()
	if h.clients[id] == nil {
		h.clients[id] = make(map[string]*fwdConn)
	}
	if existing := h.clients[id][addr]; existing != nil {
		h.mu.Unlock()
		client.Close(conn.node, uint64(id))
		return existing, nil
	}
	h.clients[id][addr] = conn
	h.mu.Unlock()
	return conn, nil
}

func (h *partHandler) retire(id uint32, addr string) {
	h.mu.Lock()
	by := h.clients[id]
	var conn *fwdConn
	if by != nil {
		conn = by[addr]
		delete(by, addr)
	}
	h.mu.Unlock()
	if conn != nil {
		conn.client.Close(conn.node, uint64(id))
	}
}

func checkedReply(reply cluster.ForwardReply, err error) (cluster.ForwardReply, error) {
	if err != nil {
		return cluster.ForwardReply{}, err
	}
	if reply.Err != "" {
		return cluster.ForwardReply{}, mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
	}
	return reply, nil
}

// scatterCalls runs one hop per addr at the same time. localAddr is executed
// with local and is not passed to remote. Rows are merged in addr order.
func scatterCalls(addrs []string, localAddr string, local, remote func(addr string) (cluster.ForwardReply, error)) (cluster.ForwardReply, error) {
	return scatterFanout(addrs, func(addr string) (cluster.ForwardReply, error) {
		if localAddr != "" && addr == localAddr {
			return local(addr)
		}
		return remote(addr)
	})
}

func scatterFanout(addrs []string, call func(addr string) (cluster.ForwardReply, error)) (cluster.ForwardReply, error) {
	replies := make([]cluster.ForwardReply, len(addrs))
	errs := make([]error, len(addrs))
	var wg sync.WaitGroup
	for i, addr := range addrs {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			reply, err := call(addr)
			if err != nil {
				errs[i] = err
				return
			}
			replies[i] = reply
		}(i, addr)
	}
	wg.Wait()
	var merged cluster.ForwardReply
	for i := range addrs {
		if errs[i] != nil {
			return cluster.ForwardReply{}, errs[i]
		}
		merged = mergeReply(merged, replies[i])
	}
	return merged, nil
}

func queryReq(store *hardhatdb.Store, query string, binds []cluster.ForwardBind) cluster.ForwardRequest {
	return cluster.ForwardRequest{
		Node:  store.NodeID(),
		Query: query,
		Binds: binds,
	}
}

func (h *partHandler) request(c *mysql.Conn, store *hardhatdb.Store, query string, binds []cluster.ForwardBind) cluster.ForwardRequest {
	req := queryReq(store, query, binds)
	if c == nil {
		return req
	}
	req.Session = uint64(c.ConnectionID)
	req.User = c.User
	req.Host = remoteHost(c)
	req.Database = currentDB(h.Handler, c)
	return req
}

func mergeReply(base, extra cluster.ForwardReply) cluster.ForwardReply {
	if len(base.Fields) == 0 {
		base.Fields = extra.Fields
	}
	base.Rows = append(base.Rows, extra.Rows...)
	base.RowsAffected += extra.RowsAffected
	return base
}

type txMark int

const (
	txNone txMark = iota
	txBegin
	txEnd
)

func txKind(query string) txMark {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return txNone
	}
	switch stmt.(type) {
	case *sqlparser.Begin:
		return txBegin
	case *sqlparser.Commit, *sqlparser.Rollback:
		return txEnd
	default:
		return txNone
	}
}
