package store

import (
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"net"
	"sync"
	"time"
)

func init() {
	// Forward messages written before the package split use the original names.
	gob.RegisterName("github.com/bongani-m/persist.ForwardVar", ForwardVar{})
	gob.RegisterName("github.com/bongani-m/persist.ForwardBind", ForwardBind{})
	gob.RegisterName("github.com/bongani-m/persist.ForwardField", ForwardField{})
	gob.RegisterName("github.com/bongani-m/persist.ForwardCell", ForwardCell{})
	gob.RegisterName("github.com/bongani-m/persist.ForwardRequest", ForwardRequest{})
	gob.RegisterName("github.com/bongani-m/persist.ForwardReply", ForwardReply{})
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

// ForwardExec runs one request on the leader.
type ForwardExec func(ForwardRequest) ForwardReply

// ForwardClient is one follower's connection to the current leader.
type ForwardClient struct {
	addr   string
	conn   net.Conn
	enc    *gob.Encoder
	dec    *gob.Decoder
	mu     sync.Mutex
	closed bool
}

// ForwardDialTimeout bounds one TCP connect. A dead leader used to hold the
// dial for the whole apply timeout, so a statement started during an election
// failed long after the new leader was serving.
const ForwardDialTimeout = 250 * time.Millisecond

// DialForwardClient opens a forward connection.
func DialForwardClient(addr string, cfg *tls.Config, timeout time.Duration) (*ForwardClient, error) {
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
