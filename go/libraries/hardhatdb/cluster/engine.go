package cluster

import (
	"io"
	"sync"

	"github.com/bongani-m/hardhatdb/go/store"
	"github.com/dgraph-io/badger/v4"
)

// Engine is the SQL store Raft applies into. sqle.Store implements it.
// The cluster package does not import sqle.
type Engine interface {
	sync.Locker
	Close() error
	Badger() *badger.DB
	Path() string
	RaftDir() string
	SyncData() error
	WriteBackup(w io.Writer) error
	InstallBackup(r io.Reader) error
	NoteFSMApplied(index uint64)
	ReadRaftApplied() uint64
	FSMApplied() uint64
	PrepareCluster(raftDir, groupID, serverUUID string, binlogMax uint64) error
	AbortCluster()
	OnLeadership(isLeader bool)
	PrivilegeProposed(id uint64) bool
	SavePrepared(index uint64, batch store.ReplBatch) error
	DropPrepared(index uint64, id string) error
	TakePrepared(index uint64, id string) (store.ReplBatch, error)
	ApplyOps(index uint64, ops []store.KVOp) error
	ApplyOpsRun(indexes []uint64, groups [][]store.KVOp) error
	ReloadPrivileges(batch store.ReplBatch, local bool) error
	AppendBinlog(index uint64, batch store.ReplBatch) error
	QueueBinlog(index uint64, batch store.ReplBatch, write, rotate bool) error
	FlushBinlog(index uint64) error
	BinlogWatermark() uint64
	RotateBinlog() error
	ReloadPrivilegesFromDisk() error
	CaptureSnapshot(dir string) (store.SnapFile, error)
	CommitSnapVersion(version uint64) error
	SnapBase() string
}
