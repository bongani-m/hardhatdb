package cluster

import (
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bongani-m/persist/go/store"
)

const defaultForwardPort = "7002"

type (
	ForwardVar     = store.ForwardVar
	ForwardBind    = store.ForwardBind
	ForwardField   = store.ForwardField
	ForwardCell    = store.ForwardCell
	ForwardRequest = store.ForwardRequest
	ForwardReply   = store.ForwardReply
	ForwardExec    = store.ForwardExec
	ForwardClient  = store.ForwardClient
)

// DialForwardClient opens a forward connection.
func DialForwardClient(addr string, cfg *tls.Config, timeout time.Duration) (*ForwardClient, error) {
	return store.DialForwardClient(addr, cfg, timeout)
}

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

func (c *Group) setForwardExec(fn ForwardExec) {
	c.execMu.Lock()
	c.exec = fn
	c.execMu.Unlock()
}

func (c *Group) forwardExec(req ForwardRequest) ForwardReply {
	c.execMu.RLock()
	fn := c.exec
	c.execMu.RUnlock()
	if fn == nil {
		return ForwardReply{Err: "persist: write forwarding is not configured"}
	}
	return fn(req)
}

func (c *Group) serveForward(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go c.handleForward(conn)
	}
}

func (c *Group) handleForward(conn net.Conn) {
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

func (c *Group) leaderForwardAddr() (string, error) {
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
func (c *Group) LeaderForwardAddr() (string, error) {
	if c == nil {
		return "", fmt.Errorf("persist: store is not replicating")
	}
	return c.leaderForwardAddr()
}

// DialForward opens a connection to the current leader's forward listener.
func (c *Group) DialForward() (*ForwardClient, error) {
	if c == nil {
		return nil, fmt.Errorf("persist: store is not replicating")
	}
	addr, err := c.leaderForwardAddr()
	if err != nil {
		return nil, err
	}
	return c.DialForwardAddr(addr)
}

// DialForwardAddr opens a forward connection to addr using this store's TLS.
func (c *Group) DialForwardAddr(addr string) (*ForwardClient, error) {
	if c == nil {
		return nil, fmt.Errorf("persist: store is not replicating")
	}
	timeout := store.ForwardDialTimeout
	if c.ApplyTimeout() < timeout {
		timeout = c.ApplyTimeout()
	}
	return DialForwardClient(addr, c.tls, timeout)
}

// LocalForwardAddr is the address this node's forward listener accepts.
// An empty string means this process has no forward listener.
func (c *Group) LocalForwardAddr() string {
	if c == nil || c.forwardLn == nil {
		return ""
	}
	return c.forwardLn.Addr().String()
}

// ExecForward runs req on this node's registered forward handler.
func (c *Group) ExecForward(req ForwardRequest) (ForwardReply, error) {
	if c == nil {
		return ForwardReply{}, fmt.Errorf("persist: store is not replicating")
	}
	return c.forwardExec(req), nil
}

// SetForwardExec registers the leader's statement runner.
func (c *Group) SetForwardExec(fn ForwardExec) {
	if c == nil {
		return
	}
	c.setForwardExec(fn)
}

// ApplyTimeout is how long a SQL commit waits for the Raft quorum.
func (c *Group) ApplyTimeout() time.Duration {
	if c == nil || c.timeout <= 0 {
		return 10 * time.Second
	}
	return c.timeout
}

// WaitApplied blocks until this node has applied index, or the timeout elapses.
func (c *Group) WaitApplied(index uint64, timeout time.Duration) error {
	if c == nil || index == 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		if c.AppliedIndex() >= index {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("persist: timed out waiting for raft index %d", index)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// NodeID is this process's Raft server id.
func (c *Group) NodeID() string {
	if c == nil {
		return ""
	}
	return string(c.id)
}
