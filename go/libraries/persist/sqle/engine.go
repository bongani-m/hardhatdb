package sqle

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/bongani-m/persist/go/store"
	"github.com/dgraph-io/badger/v4"
)

// raftNode is the Raft group attached by sqlserver. The cluster package
// implements it. This package does not import cluster.
type raftNode interface {
	IsLeader() bool
	Leader() string
	Replicating() bool
	WaitReady(time.Duration) error
	WaitCaughtUp(time.Duration) error
	AddVoter(id, addr string) error
	AddNonvoter(id, addr string) error
	RemoveServer(id string) error
	Status() store.RaftStatus
	Ready() bool
	Bootstrapped() bool
	Snapshot() error
	TransferLeadership() error
	LastIndex() (uint64, error)
	AppliedIndex() uint64
	GroupID() string
	RaftAddr() string
	Shutdown() error
	NodeID() string
	CommitMarked(statement, gtid string, mode marked, record func(snap []kvOp) (replBatch, bool, error)) error
	InFlight(id uint64) bool
	Voters() ([]store.Peer, error)
	LeaderForwardAddr() (string, error)
	ExecForward(req store.ForwardRequest) (store.ForwardReply, error)
	DialForward() (*store.ForwardClient, error)
	DialForwardAddr(addr string) (*store.ForwardClient, error)
	SetForwardExec(fn store.ForwardExec)
	ApplyTimeout() time.Duration
	WaitApplied(index uint64, timeout time.Duration) error
	LocalForwardAddr() string
}

// Attach binds a Raft group opened by cluster.Start.
func (s *Store) Attach(g raftNode) {
	s.group = g
}

func (s *Store) Lock()   { s.mu.Lock() }
func (s *Store) Unlock() { s.mu.Unlock() }

func (s *Store) Badger() *badger.DB { return s.badgerDB() }

func (s *Store) SyncData() error { return s.syncData() }

func (s *Store) InstallBackup(r io.Reader) error {
	return s.data.InstallBackup(r)
}

func (s *Store) NoteFSMApplied(index uint64) { s.noteFSMApplied(index) }

func (s *Store) ReadRaftApplied() uint64 { return s.readRaftApplied() }

func (s *Store) RaftDir() string { return s.raftDir }

// PrepareCluster opens the binlog beside the Raft directory before the group starts.
func (s *Store) PrepareCluster(raftDir, groupID, serverUUID string, binlogMax uint64) error {
	s.noteFSMApplied(s.readRaftApplied())
	bin, err := openBinlog(filepath.Join(raftDir, "binlog"), serverUUID, binlogMax)
	if err != nil {
		return err
	}
	s.bin = bin
	s.raftDir = raftDir
	s.groupID = groupID
	if s.groupID == "" {
		s.groupID = serverUUID
	}
	return nil
}

// AbortCluster closes a binlog opened by PrepareCluster when Raft fails to start.
func (s *Store) AbortCluster() {
	if s.bin != nil {
		s.bin.close()
		s.bin = nil
	}
}

func (s *Store) OnLeadership(isLeader bool) { s.onLeadership(isLeader) }

func (s *Store) PrivilegeProposed(id uint64) bool { return s.privilegeProposed(id) }

func (s *Store) SavePrepared(index uint64, batch store.ReplBatch) error {
	return s.savePrepared(index, batch)
}

func (s *Store) DropPrepared(index uint64, id string) error {
	return s.dropPrepared(index, id)
}

func (s *Store) TakePrepared(index uint64, id string) (store.ReplBatch, error) {
	return s.takePrepared(index, id)
}

func (s *Store) ApplyOps(index uint64, ops []store.KVOp) error {
	return s.applyOpsAt(index, ops)
}

func (s *Store) ReloadPrivileges(batch store.ReplBatch, local bool) error {
	return s.reloadPrivileges(batch, local)
}

func (s *Store) AppendBinlog(index uint64, batch store.ReplBatch) error {
	return s.appendBinlog(index, batch)
}

func (s *Store) RotateBinlog() error { return s.rotateBinlog() }

func (s *Store) ReloadPrivilegesFromDisk() error { return s.reloadPrivilegesFromDisk() }

func (s *Store) IsLeader() bool {
	return s.group != nil && s.group.IsLeader()
}

func (s *Store) Leader() string {
	if s.group == nil {
		return ""
	}
	return s.group.Leader()
}

func (s *Store) Replicating() bool {
	return s.group != nil && s.group.Replicating()
}

func (s *Store) WaitReady(timeout time.Duration) error {
	return s.group.WaitReady(timeout)
}

func (s *Store) WaitCaughtUp(timeout time.Duration) error {
	return s.group.WaitCaughtUp(timeout)
}

func (s *Store) AddVoter(id, addr string) error { return s.group.AddVoter(id, addr) }

func (s *Store) AddNonvoter(id, addr string) error { return s.group.AddNonvoter(id, addr) }

func (s *Store) RemoveServer(id string) error { return s.group.RemoveServer(id) }

func (s *Store) Status() store.RaftStatus {
	if s.group == nil {
		return store.RaftStatus{Role: "standalone"}
	}
	return s.group.Status()
}

func (s *Store) Ready() bool {
	return s.group != nil && s.group.Ready()
}

func (s *Store) Bootstrapped() bool {
	return s.group != nil && s.group.Bootstrapped()
}

func (s *Store) Snapshot() error {
	if s.group == nil {
		return nil
	}
	return s.group.Snapshot()
}

func (s *Store) TransferLeadership() error { return s.group.TransferLeadership() }

func (s *Store) LastIndex() (uint64, error) {
	if s.group == nil {
		return 0, nil
	}
	return s.group.LastIndex()
}

func (s *Store) AppliedIndex() uint64 {
	if s.group == nil {
		return 0
	}
	return s.group.AppliedIndex()
}

func (s *Store) GroupID() string {
	if s.group != nil {
		if id := s.group.GroupID(); id != "" {
			return id
		}
	}
	return s.groupID
}

func (s *Store) RaftAddr() string {
	if s.group == nil {
		return ""
	}
	return s.group.RaftAddr()
}

func (s *Store) NodeID() string {
	if s.group == nil {
		return ""
	}
	return s.group.NodeID()
}

func (s *Store) Voters() ([]store.Peer, error) {
	if s.group == nil {
		return nil, fmt.Errorf("persist: store is not replicating")
	}
	return s.group.Voters()
}

func (s *Store) LeaderForwardAddr() (string, error) {
	if s.group == nil {
		return "", fmt.Errorf("persist: store is not replicating")
	}
	return s.group.LeaderForwardAddr()
}

func (s *Store) ExecForward(req store.ForwardRequest) (store.ForwardReply, error) {
	if s.group == nil {
		return store.ForwardReply{}, fmt.Errorf("persist: store is not replicating")
	}
	return s.group.ExecForward(req)
}

func (s *Store) DialForward() (*store.ForwardClient, error) {
	if s.group == nil {
		return nil, fmt.Errorf("persist: store is not replicating")
	}
	return s.group.DialForward()
}

func (s *Store) DialForwardAddr(addr string) (*store.ForwardClient, error) {
	if s.group == nil {
		return nil, fmt.Errorf("persist: store is not replicating")
	}
	return s.group.DialForwardAddr(addr)
}

func (s *Store) SetForwardExec(fn store.ForwardExec) {
	if s.group != nil {
		s.group.SetForwardExec(fn)
	}
}

func (s *Store) ApplyTimeout() time.Duration {
	if s.group == nil {
		return 10 * time.Second
	}
	return s.group.ApplyTimeout()
}

func (s *Store) WaitApplied(index uint64, timeout time.Duration) error {
	if s.group == nil {
		return nil
	}
	return s.group.WaitApplied(index, timeout)
}

func (s *Store) LocalForwardAddr() string {
	if s.group == nil {
		return ""
	}
	return s.group.LocalForwardAddr()
}
