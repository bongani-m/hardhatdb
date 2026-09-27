package persist

import (
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"net"
	"sync"
	"time"
)

const defaultForwardPort = "7002"

// ForwardDir maps a Raft advertise address to that node's forward listener.
// Tests share one directory when each node listens on an ephemeral port.
type ForwardDir struct {
	mu    sync.Mutex
	addrs map[string]string
}

// NewForwardDir returns an empty peer directory.
func NewForwardDir() *ForwardDir {
	return &ForwardDir{addrs: make(map[string]string)}
}

// Set records the forward address for a Raft advertise address.
func (d *ForwardDir) Set(raftAddr, forwardAddr string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.addrs[raftAddr] = forwardAddr
	d.mu.Unlock()
}

// Get returns the forward address for a Raft advertise address.
func (d *ForwardDir) Get(raftAddr string) (string, bool) {
	if d == nil {
		return "", false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	addr, ok := d.addrs[raftAddr]
	return addr, ok
}

// ForwardVar is one user variable sent with a forwarded statement.
type ForwardVar struct {
	Name  string
	Kind  byte
	Int   int64
	Float float64
	Text  string
	Raw   []byte
}

// ForwardBind is one prepared-statement parameter.
type ForwardBind struct {
	Name  string
	Type  int32
	Value []byte
}

// ForwardField is one result column.
type ForwardField struct {
	Name         string
	OrgName      string
	Table        string
	OrgTable     string
	Database     string
	Type         int32
	Charset      uint32
	ColumnLength uint32
	Flags        uint32
	Decimals     uint32
}

// ForwardCell is one result value. Null is set instead of an empty Raw.
type ForwardCell struct {
	Null bool
	Type int32
	Raw  []byte
}

// ForwardRequest is one statement, or a request to drop a leader session.
type ForwardRequest struct {
	Close    bool
	Hold     bool
	Release  bool
	Node     string
	Session  uint64
	User     string
	Host     string
	Database string
	Query    string
	Vars     []ForwardVar
	Binds    []ForwardBind
}

// ForwardReply is the leader's result. Index is the Raft index that contains
// the commit, so the follower can wait before serving a later local read.
type ForwardReply struct {
	Err          string
	Index        uint64
	RowsAffected uint64
	InsertID     uint64
	Info         string
	Database     string
	Vars         []ForwardVar
	Fields       []ForwardField
	Rows         [][]ForwardCell
}

// ForwardExec runs one request on the leader. The persist package does not
// import the SQL engine; the server registers this callback.
type ForwardExec func(ForwardRequest) ForwardReply

func (c *cluster) setForwardExec(fn ForwardExec) {
	c.execMu.Lock()
	c.exec = fn
	c.execMu.Unlock()
}

func (c *cluster) forwardExec(req ForwardRequest) ForwardReply {
	c.execMu.RLock()
	fn := c.exec
	c.execMu.RUnlock()
	if fn == nil {
		return ForwardReply{Err: "persist: write forwarding is not configured"}
	}
	return fn(req)
}

func (c *cluster) serveForward(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go c.handleForward(conn)
	}
}

func (c *cluster) handleForward(conn net.Conn) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)
	var session ForwardRequest
	held := false
	defer func() {
		if held {
			c.forwardExec(ForwardRequest{Close: true, Node: session.Node, Session: session.Session})
		}
	}()
	for {
		var req ForwardRequest
		if err := dec.Decode(&req); err != nil {
			return
		}
		if err := conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
			return
		}
		if req.Close {
			reply := c.forwardExec(req)
			held = false
			_ = enc.Encode(reply)
			return
		}
		session = req
		held = req.Hold && !req.Release
		reply := c.forwardExec(req)
		if req.Release {
			held = false
		}
		if err := enc.Encode(reply); err != nil {
			return
		}
	}
}

func listenForward(addr string, cfg *tls.Config) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return ln, nil
	}
	return tls.NewListener(ln, cfg), nil
}

// ForwardClient is one follower's connection to the current leader.
type ForwardClient struct {
	addr   string
	conn   net.Conn
	enc    *gob.Encoder
	dec    *gob.Decoder
	mu     sync.Mutex
	closed bool
}

// forwardDialTimeout bounds one TCP connect. A dead leader used to hold the
// dial for the whole apply timeout, so a statement started during an election
// failed long after the new leader was serving.
const forwardDialTimeout = 250 * time.Millisecond

func dialForward(addr string, cfg *tls.Config, timeout time.Duration) (*ForwardClient, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn net.Conn
	var err error
	if cfg == nil {
		conn, err = dialer.Dial("tcp", addr)
	} else {
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, cfg)
	}
	if err != nil {
		return nil, err
	}
	return &ForwardClient{
		addr: addr,
		conn: conn,
		enc:  gob.NewEncoder(conn),
		dec:  gob.NewDecoder(conn),
	}, nil
}

// Exec sends one statement and waits for the leader's result.
func (f *ForwardClient) Exec(req ForwardRequest, timeout time.Duration) (ForwardReply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ForwardReply{}, fmt.Errorf("persist: forward connection is closed")
	}
	if err := f.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return ForwardReply{}, err
	}
	if err := f.enc.Encode(req); err != nil {
		return ForwardReply{}, err
	}
	var reply ForwardReply
	if err := f.dec.Decode(&reply); err != nil {
		return ForwardReply{}, err
	}
	return reply, nil
}

// Close tells the leader to drop the session, then closes the socket.
func (f *ForwardClient) Close(node string, session uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	_ = f.conn.SetDeadline(time.Now().Add(2 * time.Second))
	_ = f.enc.Encode(ForwardRequest{Close: true, Node: node, Session: session})
	var reply ForwardReply
	_ = f.dec.Decode(&reply)
	_ = f.conn.Close()
}

// Addr is the leader forward address this client dialed.
func (f *ForwardClient) Addr() string { return f.addr }

func (c *cluster) leaderForwardAddr() (string, error) {
	leader := string(c.raft.Leader())
	if leader == "" {
		return "", fmt.Errorf("persist: not the leader; leader is unknown")
	}
	if addr, ok := c.forwardDir.Get(leader); ok {
		return addr, nil
	}
	host, _, err := net.SplitHostPort(leader)
	if err != nil || c.forwardPort == "" {
		return "", fmt.Errorf("persist: leader %s has no forward address", leader)
	}
	return net.JoinHostPort(host, c.forwardPort), nil
}

// LeaderForwardAddr is the address of the current leader's forward listener.
func (s *Store) LeaderForwardAddr() (string, error) {
	if s.cluster == nil {
		return "", fmt.Errorf("persist: store is not replicating")
	}
	return s.cluster.leaderForwardAddr()
}

// DialForward opens a connection to the current leader's forward listener.
func (s *Store) DialForward() (*ForwardClient, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("persist: store is not replicating")
	}
	addr, err := s.cluster.leaderForwardAddr()
	if err != nil {
		return nil, err
	}
	return s.DialForwardAddr(addr)
}

// DialForwardAddr opens a forward connection to addr using this store's TLS.
func (s *Store) DialForwardAddr(addr string) (*ForwardClient, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("persist: store is not replicating")
	}
	timeout := forwardDialTimeout
	if s.ApplyTimeout() < timeout {
		timeout = s.ApplyTimeout()
	}
	return dialForward(addr, s.cluster.tls, timeout)
}

// LocalForwardAddr is the address this node's forward listener accepts.
// An empty string means this process has no forward listener.
func (s *Store) LocalForwardAddr() string {
	if s.cluster == nil || s.cluster.forwardLn == nil {
		return ""
	}
	return s.cluster.forwardLn.Addr().String()
}

// ExecForward runs req on this node's registered forward handler.
func (s *Store) ExecForward(req ForwardRequest) (ForwardReply, error) {
	if s.cluster == nil {
		return ForwardReply{}, fmt.Errorf("persist: store is not replicating")
	}
	return s.cluster.forwardExec(req), nil
}

// SetForwardExec registers the leader's statement runner.
func (s *Store) SetForwardExec(fn ForwardExec) {
	if s.cluster == nil {
		return
	}
	s.cluster.setForwardExec(fn)
}

// ApplyTimeout is how long a SQL commit waits for the Raft quorum.
func (s *Store) ApplyTimeout() time.Duration {
	if s.cluster == nil || s.cluster.timeout <= 0 {
		return 10 * time.Second
	}
	return s.cluster.timeout
}

// WaitApplied blocks until this node has applied index, or the timeout elapses.
func (s *Store) WaitApplied(index uint64, timeout time.Duration) error {
	if s.cluster == nil || index == 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		if s.AppliedIndex() >= index {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("persist: timed out waiting for raft index %d", index)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// NodeID is this process's Raft server id.
func (s *Store) NodeID() string {
	if s.cluster == nil {
		return ""
	}
	return string(s.cluster.id)
}
