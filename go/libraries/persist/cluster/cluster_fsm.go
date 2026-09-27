package cluster

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/bongani-m/persist/go/store"
	"github.com/hashicorp/raft"
)

// storeFSM applies committed key/value batches and catches a new replica up
// from a Badger backup.
type storeFSM struct {
	store Engine
	group *Group
}

var _ raft.BatchingFSM = (*storeFSM)(nil)

func (f *storeFSM) Apply(log *raft.Log) interface{} {
	resp := f.applyOne(log)
	if log.Type == raft.LogCommand || log.Type == raft.LogConfiguration {
		f.store.NoteFSMApplied(log.Index)
	}
	return resp
}

// ApplyBatch applies each entry in its own Badger transaction. A later
// failure does not roll back an earlier entry. The Raft log is already
// durable; Badger is fsynced on snapshot and shutdown, and a crash replays
// any batch that did not reach disk.
func (f *storeFSM) ApplyBatch(logs []*raft.Log) []interface{} {
	out := make([]interface{}, len(logs))
	var max uint64
	for i, log := range logs {
		out[i] = f.applyOne(log)
		if log.Index > max {
			max = log.Index
		}
	}
	if max > 0 {
		f.store.NoteFSMApplied(max)
	}
	return out
}

func (f *storeFSM) applyOne(log *raft.Log) interface{} {
	if log.Type != raft.LogCommand {
		return nil
	}
	batch, err := store.DecodeBatch(log.Data)
	if err != nil {
		return err
	}
	// Decide before noteApplied drops this batch from the leader's in-flight list.
	local := f.store.PrivilegeProposed(batch.ID)
	note := func(id uint64) {
		if f.group != nil {
			f.group.noteApplied(id)
		}
	}
	switch batch.Phase {
	case store.PhasePrepare:
		err = f.store.SavePrepared(log.Index, batch)
		note(batch.ID)
		if err != nil {
			return err
		}
		return nil
	case store.PhaseAbort:
		err = f.store.DropPrepared(log.Index, batch.PrepareID)
		note(batch.ID)
		if err != nil {
			return err
		}
		return nil
	case store.PhaseCommit:
		stored, err := f.store.TakePrepared(log.Index, batch.PrepareID)
		note(batch.ID)
		if err != nil {
			return err
		}
		if err := f.store.ReloadPrivileges(stored, local); err != nil {
			return err
		}
		if len(stored.Ops) > 0 {
			if err := f.store.AppendBinlog(log.Index, stored); err != nil {
				return err
			}
		}
		return nil
	default:
		err = f.store.ApplyOps(log.Index, batch.Ops)
	}
	note(batch.ID)
	if err != nil {
		return err
	}
	if err := f.store.ReloadPrivileges(batch, local); err != nil {
		return err
	}
	if err := f.store.AppendBinlog(log.Index, batch); err != nil {
		return err
	}
	if batch.Rotate {
		if err := f.store.RotateBinlog(); err != nil {
			return err
		}
	}
	return nil
}

func (f *storeFSM) Snapshot() (raft.FSMSnapshot, error) {
	if err := f.store.SyncData(); err != nil {
		return nil, err
	}
	dir := f.store.RaftDir()
	if dir == "" {
		dir = os.TempDir()
	}
	file, err := os.CreateTemp(dir, "snap-")
	if err != nil {
		return nil, err
	}
	path := file.Name()
	fail := func(err error) (raft.FSMSnapshot, error) {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	// Leave room for the version prefix, then write it once Backup returns it.
	if _, err := file.Seek(8, io.SeekStart); err != nil {
		return fail(err)
	}
	version, err := f.store.Badger().Backup(file, 0)
	if err != nil {
		return fail(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	if err := binary.Write(file, binary.LittleEndian, version); err != nil {
		return fail(err)
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return &storeSnapshot{path: path}, nil
}

// SnapshotNow writes a Badger backup the way the FSM does.
func SnapshotNow(eng Engine) (raft.FSMSnapshot, error) {
	return (&storeFSM{store: eng}).Snapshot()
}

// SnapshotPath is the file a snapshot will stream from.
func SnapshotPath(snap raft.FSMSnapshot) string {
	return snap.(*storeSnapshot).path
}

func (f *storeFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if err := f.store.InstallBackup(rc); err != nil {
		return err
	}
	f.store.NoteFSMApplied(f.store.ReadRaftApplied())
	return f.store.ReloadPrivilegesFromDisk()
}

type storeSnapshot struct {
	path string
}

func (s *storeSnapshot) Persist(sink raft.SnapshotSink) error {
	file, err := os.Open(s.path)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	defer file.Close()
	if _, err := io.Copy(sink, file); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *storeSnapshot) Release() {
	if s.path != "" {
		_ = os.Remove(s.path)
		s.path = ""
	}
}
