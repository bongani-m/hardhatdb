package sqlserver

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/cluster"
	hardhatdb "github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/sqle"
	sqle "github.com/dolthub/go-mysql-server"
)

// groupHost is the set of Raft groups this process stores. The primary group
// is opened at startup. A split adds a directory under groups/<id>.
type groupHost struct {
	mu     sync.Mutex
	nodeID string
	root   string
	tls    *tls.Config
	groups map[string]*hostedGroup
	split  func(db, table string, at []byte) error
	move   func(add bool, group, id, raft, forward string) error
}

type hostedGroup struct {
	id     string
	store  *hardhatdb.Store
	engine *sqle.Engine
	exec   *leaderExec
}

type openSpec struct {
	GroupID   string
	NodeID    string
	Bootstrap bool
	Bind      string
	Forward   string
	Peers     []cluster.Peer
	Strict    bool
}

func newGroupHost(nodeID, root string, tlsConfig *tls.Config) *groupHost {
	return &groupHost{
		nodeID: nodeID,
		root:   root,
		tls:    tlsConfig,
		groups: make(map[string]*hostedGroup),
	}
}

func (g *groupHost) add(id string, store *hardhatdb.Store, engine *sqle.Engine, exec *leaderExec) {
	if exec != nil {
		exec.command = g.command
	}
	g.mu.Lock()
	g.groups[id] = &hostedGroup{id: id, store: store, engine: engine, exec: exec}
	g.mu.Unlock()
}

func (g *groupHost) get(id string) *hostedGroup {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.groups[id]
}

func (g *groupHost) list() []*hostedGroup {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*hostedGroup, 0, len(g.groups))
	for _, hosted := range g.groups {
		out = append(out, hosted)
	}
	return out
}

// close shuts down groups this process opened after the primary store.
func (g *groupHost) close(primary *hardhatdb.Store) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, hosted := range g.groups {
		if hosted.store == primary {
			continue
		}
		if hosted.engine != nil {
			_ = hosted.engine.Close()
		}
		hosted.store.Close()
		delete(g.groups, id)
	}
}

func (g *groupHost) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.groups)
}

// adopt opens every group whose peer list names this node.
func (g *groupHost) adopt(meta *hardhatdb.Store) error {
	list, err := meta.Ranges()
	if err != nil {
		return err
	}
	for _, r := range list {
		if err := g.ensure(r.Group, r.Peers, false); err != nil {
			return err
		}
		if r.State == hardhatdb.RangeCopying {
			if err := g.ensure(r.RightGroup, r.RightPeers, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *groupHost) recover(meta *hardhatdb.Store) error {
	g.mu.Lock()
	var stores []*hardhatdb.Store
	for _, hosted := range g.groups {
		stores = append(stores, hosted.store)
	}
	g.mu.Unlock()
	return hardhatdb.RecoverTwoPhase(meta, stores)
}

func (g *groupHost) ensure(group string, peers []hardhatdb.RangePeer, strict bool) error {
	if group == "" || g.get(group) != nil {
		return nil
	}
	var me *hardhatdb.RangePeer
	raftPeers := make([]cluster.Peer, 0, len(peers))
	for _, p := range peers {
		raftPeers = append(raftPeers, cluster.Peer{ID: p.ID, Address: p.Raft})
		if p.ID == g.nodeID {
			cp := p
			me = &cp
		}
	}
	if me == nil {
		return nil
	}
	return g.open(openSpec{
		GroupID: group, NodeID: g.nodeID, Bind: me.Raft, Forward: me.Forward,
		Peers: raftPeers, Strict: strict,
	})
}

func (g *groupHost) open(spec openSpec) error {
	if spec.NodeID != g.nodeID {
		return fmt.Errorf("hardhatdb: open group is for %s", spec.NodeID)
	}
	if g.get(spec.GroupID) != nil {
		return nil
	}
	dir := filepath.Join(g.root, "groups", spec.GroupID)
	var exec *leaderExec
	store, err := openCluster(filepath.Join(dir, "hardhatdb"), cluster.ClusterOptions{
		ID:           g.nodeID,
		Bind:         spec.Bind,
		Advertise:    spec.Bind,
		RaftDir:      filepath.Join(dir, "raft"),
		Peers:        spec.Peers,
		Bootstrap:    spec.Bootstrap,
		ServerUUID:   spec.GroupID,
		GroupID:      spec.GroupID,
		TLS:          g.tls,
		ForwardAddr:  spec.Forward,
		ApplyTimeout: 10 * time.Second,
		OnLeadership: func(isLeader bool) {
			if !isLeader && exec != nil {
				exec.dropAll()
			}
		},
	})
	if err != nil {
		return err
	}
	if spec.Bootstrap {
		if err := store.WaitReady(30 * time.Second); err != nil {
			store.Close()
			return err
		}
	} else if err := store.WaitCaughtUp(30 * time.Second); err != nil {
		if spec.Strict {
			store.Close()
			return err
		}
		log.Printf("group %s is waiting for raft: %v", spec.GroupID, err)
	}
	engine := sqle.NewDefault(store)
	exec = newLeaderExec(engine, store)
	exec.command = g.command
	store.SetForwardExec(exec.Exec)
	g.mu.Lock()
	if existing := g.groups[spec.GroupID]; existing != nil {
		g.mu.Unlock()
		store.Close()
		_ = engine.Close()
		return nil
	}
	g.groups[spec.GroupID] = &hostedGroup{id: spec.GroupID, store: store, engine: engine, exec: exec}
	g.mu.Unlock()
	return nil
}

func (g *groupHost) command(query string) (cluster.ForwardReply, bool) {
	if spec, ok := parseOpenGroup(query); ok {
		if spec.NodeID != g.nodeID {
			return cluster.ForwardReply{Err: "hardhatdb: open group was sent to " + g.nodeID}, true
		}
		if err := g.open(spec); err != nil {
			return cluster.ForwardReply{Err: err.Error()}, true
		}
		return cluster.ForwardReply{}, true
	}
	if db, table, at, ok := parseSplitRange(query); ok {
		if g.split == nil {
			return cluster.ForwardReply{Err: "hardhatdb: split is not configured"}, true
		}
		if err := g.split(db, table, hardhatdb.EncodeIntKey(at)); err != nil {
			return cluster.ForwardReply{Err: err.Error()}, true
		}
		return cluster.ForwardReply{}, true
	}
	if add, group, id, raft, forward, ok := parseRangeMove(query); ok {
		if g.move == nil {
			return cluster.ForwardReply{Err: "hardhatdb: range move is not configured"}, true
		}
		if err := g.move(add, group, id, raft, forward); err != nil {
			return cluster.ForwardReply{Err: err.Error()}, true
		}
		return cluster.ForwardReply{}, true
	}
	return cluster.ForwardReply{}, false
}

func parseOpenGroup(query string) (openSpec, bool) {
	fields, err := splitAdmin(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";")))
	if err != nil || len(fields) != 8 {
		return openSpec{}, false
	}
	if !strings.EqualFold(fields[0], "OPEN") || !strings.EqualFold(fields[1], "GROUP") {
		return openSpec{}, false
	}
	boot, err := strconv.Atoi(fields[4])
	if err != nil || (boot != 0 && boot != 1) {
		return openSpec{}, false
	}
	peers, err := cluster.ParsePeers(fields[7])
	if err != nil {
		return openSpec{}, false
	}
	return openSpec{
		GroupID: fields[2], NodeID: fields[3], Bootstrap: boot == 1,
		Bind: fields[5], Forward: fields[6], Peers: peers, Strict: true,
	}, true
}

func openGroupSQL(groupID string, peer hardhatdb.RangePeer, bootstrap bool, peers []hardhatdb.RangePeer) string {
	boot := "0"
	if bootstrap {
		boot = "1"
	}
	parts := make([]string, len(peers))
	for i, p := range peers {
		parts[i] = p.ID + "=" + p.Raft
	}
	return fmt.Sprintf("OPEN GROUP %s %s %s %s %s %s", groupID, peer.ID, boot, peer.Raft, peer.Forward, strings.Join(parts, ","))
}

func shiftPeers(peers []hardhatdb.RangePeer, delta int) ([]hardhatdb.RangePeer, error) {
	out := make([]hardhatdb.RangePeer, len(peers))
	for i, p := range peers {
		raft, err := shiftPort(p.Raft, delta)
		if err != nil {
			return nil, err
		}
		fwd, err := shiftPort(p.Forward, delta)
		if err != nil {
			return nil, err
		}
		out[i] = hardhatdb.RangePeer{ID: p.ID, Raft: raft, Forward: fwd}
	}
	return out, nil
}

func shiftPort(addr string, delta int) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return "", err
	}
	n += delta
	if n < 1 || n > 65535 {
		return "", fmt.Errorf("hardhatdb: port %d is out of range", n)
	}
	return net.JoinHostPort(host, strconv.Itoa(n)), nil
}

func parseSplitRange(query string) (db, table string, at int64, ok bool) {
	fields, err := splitAdmin(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";")))
	if err != nil || len(fields) != 5 {
		return "", "", 0, false
	}
	if !strings.EqualFold(fields[0], "SPLIT") || !strings.EqualFold(fields[1], "RANGE") || !strings.EqualFold(fields[3], "AT") {
		return "", "", 0, false
	}
	db, table, ok = splitQual(fields[2])
	if !ok {
		return "", "", 0, false
	}
	at, err = strconv.ParseInt(fields[4], 10, 64)
	if err != nil {
		return "", "", 0, false
	}
	return db, table, at, true
}

// parseRangeMove recognizes RANGE ADD VOTER and RANGE REMOVE SERVER.
func parseRangeMove(query string) (add bool, group, id, raft, forward string, ok bool) {
	fields, err := splitAdmin(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";")))
	if err != nil || len(fields) < 4 {
		return false, "", "", "", "", false
	}
	if !strings.EqualFold(fields[0], "RANGE") {
		return false, "", "", "", "", false
	}
	if len(fields) == 7 && strings.EqualFold(fields[1], "ADD") && strings.EqualFold(fields[2], "VOTER") {
		if fields[3] == "" || fields[4] == "" || fields[5] == "" || fields[6] == "" {
			return false, "", "", "", "", false
		}
		return true, fields[3], fields[4], fields[5], fields[6], true
	}
	if len(fields) == 4 && strings.EqualFold(fields[1], "REMOVE") && strings.EqualFold(fields[2], "SERVER") {
		if fields[3] == "" {
			return false, "", "", "", "", false
		}
		// group id is not in this short form. The caller uses a 5-field form.
		return false, "", "", "", "", false
	}
	if len(fields) == 5 && strings.EqualFold(fields[1], "REMOVE") && strings.EqualFold(fields[2], "SERVER") {
		if fields[3] == "" || fields[4] == "" {
			return false, "", "", "", "", false
		}
		return false, fields[3], fields[4], "", "", true
	}
	return false, "", "", "", "", false
}
