package persist

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// pendingCap is how many recorded batches may wait for Raft at once.
// Past that, commit waits until an apply frees a slot.
const pendingCap = 32

var errClusterClosed = errors.New("persist: cluster shut down")

// Peer is one voter in the Raft group.
type Peer struct {
	ID      string
	Address string
}

// ClusterOptions configures a single-primary Raft group for one Badger
// directory. Leave Transport nil to listen on Bind. Tests pass an in-memory
// transport and set Advertise to that transport's address.
type ClusterOptions struct {
	ID         string
	Bind       string
	Advertise  string
	RaftDir    string
	Peers      []Peer
	Bootstrap  bool
	ServerUUID string
	Transport  raft.Transport
	Config     *raft.Config
	// ApplyTimeout bounds how long a SQL commit waits for the Raft quorum.
	ApplyTimeout time.Duration
	// BinlogMaxBytes rolls the binlog after a transaction crosses this size.
	// Zero uses the default of 1 GiB.
	BinlogMaxBytes uint64
	// OnLeadership runs after this node gains or loses leadership. It is
	// called from a goroutine that is not the Raft thread.
	OnLeadership func(isLeader bool)
	// TLS is the mutual TLS config for Raft and the forward listener.
	// A non-loopback bind without TLS is refused.
	TLS *tls.Config
	// Nonvoters replicate the log and do not vote. A bootstrap configuration
	// includes them with raft.Nonvoter so the quorum stays the voters.
	Nonvoters []Peer
	// ForwardAddr is this node's write-forward listener. Empty uses port 7002
	// on the advertise host when this process opens its own TCP transport.
	ForwardAddr string
	// GroupID names the range group stored in this directory. Empty uses
	// ServerUUID, which every member of the primary group already shares.
	GroupID string
	// ForwardDir publishes ephemeral forward addresses. Production leaves it
	// nil and derives the leader's address from the Raft host and port 7002.
	ForwardDir *ForwardDir
}

// ParsePeers parses "id=host:port,id2=host:port". An empty string is no peers.
func ParsePeers(raw string) ([]Peer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var peers []Peer
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("persist: peer %q must be id=host:port", part)
		}
		peers = append(peers, Peer{ID: id, Address: addr})
	}
	return peers, nil
}

// OpenCluster opens a Badger directory and joins or bootstraps a Raft group
// that replicates its commits. The Raft log is stored beside the Badger
// directory, not inside it.
func OpenCluster(path string, opts ClusterOptions) (*Store, error) {
	if opts.ID == "" {
		return nil, fmt.Errorf("persist: cluster node id is empty")
	}
	if opts.Advertise == "" {
		opts.Advertise = opts.Bind
	}
	if opts.Transport == nil && opts.Bind == "" {
		return nil, fmt.Errorf("persist: cluster bind address is empty")
	}
	if opts.RaftDir == "" {
		opts.RaftDir = filepath.Join(filepath.Dir(path), "raft-"+opts.ID)
	}
	if opts.ApplyTimeout <= 0 {
		opts.ApplyTimeout = 10 * time.Second
	}
	serverUUID, err := loadServerUUID(opts.RaftDir, opts.ServerUUID)
	if err != nil {
		return nil, err
	}
	opts.ServerUUID = serverUUID

	// The Raft log is the commit record. Badger is fsynced on snapshot and shutdown.
	store, err := OpenWithOptions(path, OpenOptions{NoSync: true})
	if err != nil {
		return nil, err
	}
	store.noteFSMApplied(store.readRaftApplied())
	bin, err := openBinlog(filepath.Join(opts.RaftDir, "binlog"), opts.ServerUUID, opts.BinlogMaxBytes)
	if err != nil {
		store.Close()
		return nil, err
	}
	store.bin = bin
	store.raftDir = opts.RaftDir
	store.groupID = opts.GroupID
	if store.groupID == "" {
		store.groupID = opts.ServerUUID
	}

	c, err := startCluster(store, opts)
	if err != nil {
		bin.close()
		store.bin = nil
		store.Close()
		return nil, err
	}
	store.cluster = c
	// A snapshot index can sit on a configuration entry, which does not write
	// the Badger applied key. Count that index as finished so startup does not
	// wait for a log Raft will not replay.
	store.noteFSMApplied(store.AppliedIndex())
	go c.watchLeadership(opts.OnLeadership)
	return store, nil
}

func loadServerUUID(dir, id string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "server.uuid")
	if id == "" {
		raw, err := os.ReadFile(path)
		if err == nil {
			id = strings.TrimSpace(string(raw))
		}
	}
	if id == "" {
		id = uuid.NewString()
	}
	if _, err := uuid.Parse(id); err != nil {
		return "", fmt.Errorf("persist: server uuid: %w", err)
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o644); err != nil {
		return "", err
	}
	return id, nil
}

// queuedCommit is one recorded statement waiting for its Raft result.
type queuedCommit struct {
	id    uint64
	batch replBatch
	done  chan error
	// proposed is set when the batch leaves the queue for Raft. abandonFrom
	// leaves a proposed batch for the waiter that called Apply, and still
	// fails batches that are only recorded.
	proposed bool
}

type cluster struct {
	store     *Store
	id        raft.ServerID
	raft      *raft.Raft
	wal       *raftWAL
	bolt      *raftboltdb.BoltStore
	transport raft.Transport
	timeout   time.Duration
	tls       *tls.Config

	forwardLn   net.Listener
	forwardPort string
	forwardDir  *ForwardDir
	advertise   string
	execMu      sync.RWMutex
	exec        ForwardExec

	// recordMu serializes recording. It is not held while Raft waits, so the
	// next statement can record against batches still in flight.
	recordMu sync.Mutex
	cond     *sync.Cond
	inflight []*queuedCommit
	queue    []*queuedCommit
	nextID   uint64
	stopped  bool
	exited   chan struct{}
	// gate, if set, runs on the proposer before each group is sent to Raft.
	// Tests hold it to observe a commit that is recorded but not yet applied.
	gate func()
	// bootstrapped is true only when this process created the Raft group.
	// A restart that already has state leaves it false.
	bootstrapped bool
}

func startCluster(store *Store, opts ClusterOptions) (*cluster, error) {
	cfg := opts.Config
	if cfg == nil {
		cfg = raft.DefaultConfig()
		cfg.LogLevel = "ERROR"
	}
	cfg.LocalID = raft.ServerID(opts.ID)
	// Buffer applyCh so the leader can fsync and replicate a group in one
	// AppendEntries. The apply timeout still bounds how long Apply waits for a
	// free slot. A restart keeps the Badger directory; the snapshot index is
	// still recorded, and a joining replica installs a snapshot over RPC.
	cfg.BatchApplyCh = true
	cfg.NoSnapshotRestoreOnStart = true

	transport := opts.Transport
	var err error
	if transport == nil {
		if opts.TLS == nil && !loopbackHost(opts.Bind) {
			return nil, fmt.Errorf("persist: raft on %s requires GMS_RAFT_TLS_CERT, GMS_RAFT_TLS_KEY, and GMS_RAFT_TLS_CA", opts.Bind)
		}
		var addr *net.TCPAddr
		addr, err = net.ResolveTCPAddr("tcp", opts.Advertise)
		if err != nil {
			return nil, err
		}
		if opts.TLS == nil {
			transport, err = raft.NewTCPTransport(opts.Bind, addr, 3, 10*time.Second, os.Stderr)
		} else {
			transport, err = newTLSTransport(opts.Bind, addr, opts.TLS)
		}
		if err != nil {
			return nil, err
		}
	}

	if err := os.MkdirAll(opts.RaftDir, 0o755); err != nil {
		closeTransport(transport)
		return nil, err
	}
	bolt, err := raftboltdb.NewBoltStore(filepath.Join(opts.RaftDir, "raft.db"))
	if err != nil {
		closeTransport(transport)
		return nil, err
	}
	wal, err := openRaftWAL(opts.RaftDir, bolt)
	if err != nil {
		bolt.Close()
		closeTransport(transport)
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStore(opts.RaftDir, 2, os.Stderr)
	if err != nil {
		wal.Close()
		bolt.Close()
		closeTransport(transport)
		return nil, err
	}

	bootstrapped := false
	if opts.Bootstrap {
		has, err := raft.HasExistingState(wal, bolt, snaps)
		if err != nil {
			wal.Close()
			bolt.Close()
			closeTransport(transport)
			return nil, err
		}
		if has {
			log.Printf("persist: GMS_RAFT_BOOTSTRAP is set and this node already has Raft state; ignoring bootstrap")
		} else {
			err = raft.BootstrapCluster(cfg, wal, bolt, snaps, transport, raft.Configuration{
				Servers: opts.servers(),
			})
			if err != nil {
				wal.Close()
				bolt.Close()
				closeTransport(transport)
				return nil, err
			}
			bootstrapped = true
		}
	}

	r, err := raft.NewRaft(cfg, &storeFSM{store: store}, wal, bolt, snaps, transport)
	if err != nil {
		wal.Close()
		bolt.Close()
		closeTransport(transport)
		return nil, err
	}
	c := &cluster{
		store:        store,
		id:           raft.ServerID(opts.ID),
		raft:         r,
		wal:          wal,
		bolt:         bolt,
		transport:    transport,
		timeout:      opts.ApplyTimeout,
		tls:          opts.TLS,
		forwardDir:   opts.ForwardDir,
		advertise:    opts.Advertise,
		exited:       make(chan struct{}),
		nextID:       newBatchEpoch(),
		bootstrapped: bootstrapped,
	}
	if err := c.listenForward(opts); err != nil {
		r.Shutdown()
		closeTransport(transport)
		wal.Close()
		bolt.Close()
		return nil, err
	}
	c.cond = sync.NewCond(&store.mu)
	go c.proposeLoop()
	return c, nil
}

func (c *cluster) listenForward(opts ClusterOptions) error {
	listen, port, ok := forwardListenAddr(opts)
	if !ok {
		return nil
	}
	ln, err := listenForward(listen, opts.TLS)
	if err != nil {
		return err
	}
	_, actual, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		ln.Close()
		return err
	}
	c.forwardLn = ln
	if port == "0" || port == "" {
		c.forwardPort = actual
	} else {
		c.forwardPort = port
	}
	opts.ForwardDir.Set(opts.Advertise, ln.Addr().String())
	go c.serveForward(ln)
	return nil
}

func forwardListenAddr(opts ClusterOptions) (listen, port string, ok bool) {
	if opts.ForwardAddr != "" {
		_, port, err := net.SplitHostPort(opts.ForwardAddr)
		if err != nil {
			return "", "", false
		}
		return opts.ForwardAddr, port, true
	}
	if opts.Transport != nil {
		return "", "", false
	}
	host, _, err := net.SplitHostPort(opts.Advertise)
	if err != nil || host == "" {
		return "", "", false
	}
	return net.JoinHostPort(host, defaultForwardPort), defaultForwardPort, true
}

// watchLeadership reports leadership changes. LeaderCh is a one-slot channel,
// so the current state is read once before the loop.
func (c *cluster) watchLeadership(hook func(bool)) {
	var have bool
	var last bool
	notify := func(isLeader bool) {
		if have && last == isLeader {
			return
		}
		have = true
		last = isLeader
		log.Printf("persist: leadership isLeader=%v leader=%s", isLeader, c.raft.Leader())
		if hook != nil {
			hook(isLeader)
		}
		c.store.onLeadership(isLeader)
	}
	if c.raft.State() == raft.Leader {
		notify(true)
	}
	ch := c.raft.LeaderCh()
	for {
		select {
		case <-c.exited:
			return
		case isLeader, ok := <-ch:
			if !ok {
				return
			}
			notify(isLeader)
		}
	}
}

// newBatchEpoch keeps in-flight ids from matching commands replayed out of an
// older log. noteApplied ignores id 0, so an epoch of 0 becomes 1.
func newBatchEpoch() uint64 {
	var seed uint64
	if err := binary.Read(rand.Reader, binary.LittleEndian, &seed); err != nil || seed == 0 {
		return 1
	}
	return seed
}

func (o ClusterOptions) servers() []raft.Server {
	nonvoter := map[string]bool{}
	for _, peer := range o.Nonvoters {
		nonvoter[peer.ID] = true
	}
	seen := map[string]bool{}
	var out []raft.Server
	add := func(id, addr string, suffrage raft.ServerSuffrage) {
		if id == "" || addr == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, raft.Server{
			ID:       raft.ServerID(id),
			Address:  raft.ServerAddress(addr),
			Suffrage: suffrage,
		})
	}
	self := raft.Voter
	if nonvoter[o.ID] {
		self = raft.Nonvoter
	}
	add(o.ID, o.Advertise, self)
	for _, peer := range o.Peers {
		suf := raft.Voter
		if nonvoter[peer.ID] {
			suf = raft.Nonvoter
		}
		add(peer.ID, peer.Address, suf)
	}
	for _, peer := range o.Nonvoters {
		add(peer.ID, peer.Address, raft.Nonvoter)
	}
	return out
}

// beginRecord waits for a free in-flight slot and returns the writes the next
// statement must see. The caller holds recordMu and does not hold the store lock.
func (c *cluster) beginRecord() ([]kvOp, error) {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	for {
		if c.stopped {
			return nil, errClusterClosed
		}
		if c.raft.State() != raft.Leader {
			return nil, c.notLeader()
		}
		if len(c.inflight) < pendingCap {
			break
		}
		c.cond.Wait()
	}
	return snapshotOps(c.inflight), nil
}

func snapshotOps(pending []*queuedCommit) []kvOp {
	n := 0
	for _, q := range pending {
		if q.batch.Phase != phaseApply {
			continue
		}
		n += len(q.batch.Ops)
	}
	ops := make([]kvOp, 0, n)
	for _, q := range pending {
		if q.batch.Phase != phaseApply {
			continue
		}
		ops = append(ops, q.batch.Ops...)
	}
	return ops
}

// enqueue assigns a batch id and hands it to the proposer. The caller holds recordMu.
func (c *cluster) enqueue(batch replBatch) (chan error, error) {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	if c.stopped {
		return nil, errClusterClosed
	}
	if c.raft.State() != raft.Leader {
		return nil, c.notLeader()
	}
	c.nextID++
	if c.nextID == 0 {
		c.nextID = 1
	}
	batch.ID = c.nextID
	q := &queuedCommit{
		id:    batch.ID,
		batch: batch,
		done:  make(chan error, 1),
	}
	c.inflight = append(c.inflight, q)
	c.queue = append(c.queue, q)
	c.cond.Broadcast()
	return q.done, nil
}

// proposeLoop hands each recorded group to Raft once the previous group's
// Apply calls have been accepted. A goroutine waits on those futures, so the
// next group can join the same quorum flush. Raft coalesces the entries
// already in applyCh into one AppendEntries round trip.
func (c *cluster) proposeLoop() {
	defer close(c.exited)
	for {
		group := c.takeQueue()
		if group == nil {
			return
		}
		c.submitGroup(group)
	}
}

func (c *cluster) takeQueue() []*queuedCommit {
	c.store.mu.Lock()
	for len(c.queue) == 0 && !c.stopped {
		c.cond.Wait()
	}
	if c.stopped {
		queued := c.detachQueuedLocked()
		c.store.mu.Unlock()
		for _, q := range queued {
			q.done <- errClusterClosed
		}
		return nil
	}
	group := c.queue
	c.queue = nil
	for _, q := range group {
		q.proposed = true
	}
	c.store.mu.Unlock()
	return group
}

// submitGroup encodes the group and waits for Raft in the background.
// The caller returns to the queue as soon as every Apply has been accepted.
func (c *cluster) submitGroup(group []*queuedCommit) {
	if c.gate != nil {
		c.gate()
	}
	futures := make([]raft.ApplyFuture, len(group))
	for i, q := range group {
		payload, err := encodeBatch(q.batch)
		if err != nil {
			go c.finishProposed(group, futures, i, err)
			return
		}
		futures[i] = c.raft.Apply(payload, c.timeout)
	}
	go c.finishProposed(group, futures, len(group), nil)
}

// finishProposed waits for futures that were sent. A Raft error from index
// failAt, or an encode error there, fails that batch and every later one,
// including batches recorded after this group.
func (c *cluster) finishProposed(group []*queuedCommit, futures []raft.ApplyFuture, failAt int, failErr error) {
	results := make([]error, len(group))
	firstRaftFail := failAt
	if failAt < len(group) && failErr != nil {
		results[failAt] = failErr
	}
	for i := 0; i < len(group) && i < failAt; i++ {
		err := futures[i].Error()
		if err != nil {
			results[i] = err
			if firstRaftFail > i {
				firstRaftFail = i
				failErr = err
			}
			continue
		}
		if resp, ok := futures[i].Response().(error); ok && resp != nil {
			results[i] = resp
		}
	}
	if firstRaftFail < len(group) {
		notified := make(map[uint64]bool, len(group)-firstRaftFail)
		for i := firstRaftFail; i < len(group); i++ {
			notified[group[i].id] = true
			err := results[i]
			if err == nil {
				err = failErr
			}
			group[i].done <- err
		}
		c.abandonFrom(group[firstRaftFail].id, failErr, notified)
		for i := 0; i < firstRaftFail; i++ {
			group[i].done <- results[i]
		}
		return
	}
	for i, q := range group {
		q.done <- results[i]
	}
}

// abandonFrom drops id and every in-flight batch recorded after it.
// alreadyNotified batches are left for the caller to wake.
func (c *cluster) abandonFrom(id uint64, err error, alreadyNotified map[uint64]bool) {
	var dropped []*queuedCommit
	c.store.mu.Lock()
	drop := false
	kept := make([]*queuedCommit, 0, len(c.inflight))
	for _, q := range c.inflight {
		if q.id == id {
			drop = true
		}
		if !drop {
			kept = append(kept, q)
			continue
		}
		// A later group already accepted by Apply reports its own Raft result.
		// This group, and anything still only recorded, is failed here.
		if q.proposed && !alreadyNotified[q.id] {
			kept = append(kept, q)
			continue
		}
		dropped = append(dropped, q)
	}
	if drop {
		c.inflight = kept
		skip := make(map[uint64]bool, len(dropped))
		for _, q := range dropped {
			skip[q.id] = true
		}
		queued := make([]*queuedCommit, 0, len(c.queue))
		for _, q := range c.queue {
			if !skip[q.id] {
				queued = append(queued, q)
			}
		}
		c.queue = queued
		c.cond.Broadcast()
	}
	c.store.mu.Unlock()
	for _, q := range dropped {
		if alreadyNotified[q.id] {
			continue
		}
		q.done <- err
	}
}

// detachQueuedLocked removes batches still waiting to be proposed. The store lock is held.
func (c *cluster) detachQueuedLocked() []*queuedCommit {
	queued := c.queue
	c.queue = nil
	if len(queued) == 0 {
		return nil
	}
	skip := make(map[uint64]bool, len(queued))
	for _, q := range queued {
		skip[q.id] = true
	}
	kept := make([]*queuedCommit, 0, len(c.inflight))
	for _, q := range c.inflight {
		if !skip[q.id] {
			kept = append(kept, q)
		}
	}
	c.inflight = kept
	c.cond.Broadcast()
	return queued
}

// noteApplied drops the leader's in-flight batch once its writes are in Badger.
// id 0 is a log entry that this process did not propose.
func (s *Store) noteApplied(id uint64) {
	if id == 0 || s.cluster == nil {
		return
	}
	c := s.cluster
	s.mu.Lock()
	for i, q := range c.inflight {
		if q.id != id {
			continue
		}
		copy(c.inflight[i:], c.inflight[i+1:])
		c.inflight[len(c.inflight)-1] = nil
		c.inflight = c.inflight[:len(c.inflight)-1]
		c.cond.Broadcast()
		break
	}
	s.mu.Unlock()
}

func (c *cluster) notLeader() error {
	addr := c.raft.Leader()
	if addr == "" {
		return fmt.Errorf("persist: not the leader; leader is unknown")
	}
	return fmt.Errorf("persist: not the leader; leader is %s", addr)
}

func (c *cluster) shutdown() error {
	if c.forwardLn != nil {
		_ = c.forwardLn.Close()
	}
	c.store.mu.Lock()
	c.stopped = true
	c.cond.Broadcast()
	c.store.mu.Unlock()
	var err error
	if c.raft != nil {
		err = c.raft.Shutdown().Error()
	}
	<-c.exited
	closeTransport(c.transport)
	if c.wal != nil {
		if cerr := c.wal.Close(); err == nil {
			err = cerr
		}
	}
	if c.bolt != nil {
		if cerr := c.bolt.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

// GroupID is the range group stored in this directory.
func (s *Store) GroupID() string {
	if s.groupID != "" {
		return s.groupID
	}
	return s.NodeID()
}

// RaftAddr is the address other nodes dial for this process.
func (s *Store) RaftAddr() string {
	if s.cluster == nil {
		return ""
	}
	return s.cluster.advertise
}

// Voters returns the voting members of this Raft group.
func (s *Store) Voters() ([]Peer, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("persist: store is not replicating")
	}
	fut := s.cluster.raft.GetConfiguration()
	if err := fut.Error(); err != nil {
		return nil, err
	}
	var out []Peer
	for _, srv := range fut.Configuration().Servers {
		if srv.Suffrage != raft.Voter {
			continue
		}
		out = append(out, Peer{ID: string(srv.ID), Address: string(srv.Address)})
	}
	return out, nil
}

// IsLeader reports whether this process currently accepts SQL writes.
func (s *Store) IsLeader() bool {
	return s.cluster != nil && s.cluster.raft.State() == raft.Leader
}

// Leader is the advertised address of the current primary. It is empty when
// the group has not elected one.
func (s *Store) Leader() string {
	if s.cluster == nil {
		return ""
	}
	return string(s.cluster.raft.Leader())
}

// Replicating reports whether commits go through Raft.
func (s *Store) Replicating() bool {
	return s.cluster != nil
}

// WaitReady waits until this node is the leader. A bootstrap node calls it
// before serving writes.
func (s *Store) WaitReady(timeout time.Duration) error {
	if s.cluster == nil {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.IsLeader() {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("persist: timed out waiting for leadership")
}

// WaitCaughtUp waits until this node has applied every committed Raft entry.
// CommitIndex stays 0 until a quorum entry arrives (the leader's noop, or a
// follower's first AppendEntries). An uncommitted log tail is left unapplied.
func (s *Store) WaitCaughtUp(timeout time.Duration) error {
	if s.cluster == nil {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		commit := s.cluster.raft.CommitIndex()
		if commit > 0 && s.caughtUpTo(commit) {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("persist: timed out waiting for raft apply")
}

// caughtUpTo reports that index is committed on this node and the FSM has
// finished it. Raft's AppliedIndex moves when a batch is queued, which is
// before privileges and rows are visible.
func (s *Store) caughtUpTo(index uint64) bool {
	if s.cluster == nil {
		return true
	}
	if index == 0 || s.cluster.raft.AppliedIndex() < index {
		return false
	}
	return atomic.LoadUint64(&s.fsmApplied) >= s.cluster.fsmTarget(index)
}

// fsmTarget is the newest index at or before index that the FSM observes.
// A noop is not delivered to Apply.
func (c *cluster) fsmTarget(index uint64) uint64 {
	for idx := index; idx > 0; idx-- {
		var lg raft.Log
		if err := c.wal.GetLog(idx, &lg); err != nil {
			return index
		}
		if lg.Type != raft.LogNoop {
			return idx
		}
	}
	return 0
}

// AddVoter adds a replica that is already running at addr.
func (s *Store) AddVoter(id, addr string) error {
	if s.cluster == nil {
		return fmt.Errorf("persist: store is not replicating")
	}
	return s.cluster.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, s.cluster.timeout).Error()
}

// AddNonvoter adds a replica that replicates the log and does not vote.
func (s *Store) AddNonvoter(id, addr string) error {
	if s.cluster == nil {
		return fmt.Errorf("persist: store is not replicating")
	}
	return s.cluster.raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(addr), 0, s.cluster.timeout).Error()
}

// RemoveServer drops a voter from the group. id is the Raft server id.
func (s *Store) RemoveServer(id string) error {
	if s.cluster == nil {
		return fmt.Errorf("persist: store is not replicating")
	}
	if id == "" {
		return fmt.Errorf("persist: server id is empty")
	}
	return s.cluster.raft.RemoveServer(raft.ServerID(id), 0, s.cluster.timeout).Error()
}

// RaftStatus is this process's view of the group. A standalone store reports
// role "standalone" and zero indexes.
type RaftStatus struct {
	Role    string
	Leader  string
	Commit  uint64
	Applied uint64
	Lag     uint64
}

// Status returns the current Raft role and how far this node has applied.
func (s *Store) Status() RaftStatus {
	if s.cluster == nil || s.cluster.raft == nil {
		return RaftStatus{Role: "standalone"}
	}
	commit := s.cluster.raft.CommitIndex()
	applied := atomic.LoadUint64(&s.fsmApplied)
	lag := uint64(0)
	if commit > applied {
		lag = commit - applied
	}
	return RaftStatus{
		Role:    strings.ToLower(s.cluster.raft.State().String()),
		Leader:  string(s.cluster.raft.Leader()),
		Commit:  commit,
		Applied: applied,
		Lag:     lag,
	}
}

// Ready reports that the store is open and, on a cluster node, that this
// process has applied the commit index.
func (s *Store) Ready() bool {
	if s.badgerDB() == nil {
		return false
	}
	if s.cluster == nil || s.cluster.raft == nil {
		return true
	}
	commit := s.cluster.raft.CommitIndex()
	if commit == 0 {
		return false
	}
	return s.caughtUpTo(commit)
}

// Bootstrapped reports whether this process created a new Raft group.
// A restart that ignores GMS_RAFT_BOOTSTRAP returns false: that node joins
// the existing group and must not wait to become leader.
func (s *Store) Bootstrapped() bool {
	return s.cluster != nil && s.cluster.bootstrapped
}

// Snapshot asks Raft to compact the log into a Badger backup. A replica that
// joins after the log is truncated installs that backup.
func (s *Store) Snapshot() error {
	if s.cluster == nil {
		return fmt.Errorf("persist: store is not replicating")
	}
	return s.cluster.raft.Snapshot().Error()
}

// TransferLeadership moves the primary to another voter.
func (s *Store) TransferLeadership() error {
	if s.cluster == nil {
		return fmt.Errorf("persist: store is not replicating")
	}
	return s.cluster.raft.LeadershipTransfer().Error()
}

// LastIndex is the newest Raft log index, including configuration entries.
func (s *Store) LastIndex() (uint64, error) {
	if s.cluster == nil {
		return 0, fmt.Errorf("persist: store is not replicating")
	}
	return s.cluster.raft.LastIndex(), nil
}

func closeTransport(transport raft.Transport) {
	closer, ok := transport.(io.Closer)
	if ok && closer != nil {
		_ = closer.Close()
	}
}

// AppliedIndex is the newest index applied into this node's Badger.
func (s *Store) AppliedIndex() uint64 {
	if s.cluster == nil {
		return 0
	}
	return s.cluster.raft.AppliedIndex()
}
