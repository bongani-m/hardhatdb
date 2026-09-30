package cluster

import (
	"io"
	"os"
	"path/filepath"

	"github.com/bongani-m/hardhatdb/go/store"
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
	if log.Type == raft.LogConfiguration {
		_ = f.store.QueueBinlog(log.Index, store.ReplBatch{}, false, false)
	}
	return resp
}

// ApplyBatch writes a contiguous run of ordinary commits in one Badger
// transaction. Prepare, commit, and abort stay on their own transactions.
// A later failure does not roll back an earlier run. The Raft log is already
// durable; Badger is fsynced on snapshot and shutdown, and a crash replays
// any batch that did not reach disk.
func (f *storeFSM) ApplyBatch(logs []*raft.Log) []interface{} {
	out := make([]interface{}, len(logs))
	var max uint64
	for i := 0; i < len(logs); {
		log := logs[i]
		if log.Type == raft.LogCommand || log.Type == raft.LogConfiguration {
			if log.Index > max {
				max = log.Index
			}
		}
		if log.Type != raft.LogCommand {
			if log.Type == raft.LogConfiguration {
				_ = f.store.QueueBinlog(log.Index, store.ReplBatch{}, false, false)
			}
			i++
			continue
		}
		batch, err := store.DecodeBatch(log.Data)
		if err != nil || batch.Phase != store.PhaseApply {
			out[i] = f.applyDecoded(log, batch, err)
			i++
			continue
		}
		run := []applyItem{{log: log, batch: batch, local: f.store.PrivilegeProposed(batch.ID)}}
		j := i + 1
		for j < len(logs) && logs[j].Type == raft.LogCommand {
			next, err := store.DecodeBatch(logs[j].Data)
			if err != nil || next.Phase != store.PhaseApply {
				break
			}
			run = append(run, applyItem{log: logs[j], batch: next, local: f.store.PrivilegeProposed(next.ID)})
			if logs[j].Index > max {
				max = logs[j].Index
			}
			j++
		}
		f.applyRun(out, i, run)
		i = j
	}
	if max > 0 {
		f.store.NoteFSMApplied(max)
	}
	return out
}

type applyItem struct {
	log   *raft.Log
	batch store.ReplBatch
	local bool
}

func (f *storeFSM) applyRun(out []interface{}, at int, run []applyItem) {
	indexes := make([]uint64, len(run))
	groups := make([][]store.KVOp, len(run))
	for i, item := range run {
		indexes[i] = item.log.Index
		groups[i] = item.batch.Ops
	}
	err := f.store.ApplyOpsRun(indexes, groups)
	for i, item := range run {
		f.note(item.batch.ID)
		if err != nil {
			out[at+i] = err
			continue
		}
		if perr := f.store.ReloadPrivileges(item.batch, item.local); perr != nil {
			out[at+i] = perr
			continue
		}
		if qerr := f.store.QueueBinlog(item.log.Index, item.batch, true, item.batch.Rotate); qerr != nil {
			out[at+i] = qerr
		}
	}
}

func (f *storeFSM) note(id uint64) {
	if f.group != nil {
		f.group.noteApplied(id)
	}
}

func (f *storeFSM) applyOne(log *raft.Log) interface{} {
	if log.Type != raft.LogCommand {
		return nil
	}
	batch, err := store.DecodeBatch(log.Data)
	return f.applyDecoded(log, batch, err)
}

func (f *storeFSM) applyDecoded(log *raft.Log, batch store.ReplBatch, decErr error) interface{} {
	if decErr != nil {
		return decErr
	}
	// Decide before noteApplied drops this batch from the leader's in-flight list.
	local := f.store.PrivilegeProposed(batch.ID)
	switch batch.Phase {
	case store.PhasePrepare:
		err := f.store.SavePrepared(log.Index, batch)
		f.note(batch.ID)
		if err != nil {
			return err
		}
		return f.store.QueueBinlog(log.Index, store.ReplBatch{}, false, false)
	case store.PhaseAbort:
		err := f.store.DropPrepared(log.Index, batch.PrepareID)
		f.note(batch.ID)
		if err != nil {
			return err
		}
		return f.store.QueueBinlog(log.Index, store.ReplBatch{}, false, false)
	case store.PhaseCommit:
		stored, err := f.store.TakePrepared(log.Index, batch.PrepareID)
		f.note(batch.ID)
		if err != nil {
			return err
		}
		if err := f.store.ReloadPrivileges(stored, local); err != nil {
			return err
		}
		write := len(stored.Ops) > 0
		return f.store.QueueBinlog(log.Index, stored, write, false)
	default:
		err := f.store.ApplyOps(log.Index, batch.Ops)
		f.note(batch.ID)
		if err != nil {
			return err
		}
		if err := f.store.ReloadPrivileges(batch, local); err != nil {
			return err
		}
		return f.store.QueueBinlog(log.Index, batch, true, batch.Rotate)
	}
}

func (f *storeFSM) Snapshot() (raft.FSMSnapshot, error) {
	if err := f.store.FlushBinlog(f.store.FSMApplied()); err != nil {
		return nil, err
	}
	if err := f.store.SyncData(); err != nil {
		return nil, err
	}
	dir := f.store.RaftDir()
	if dir == "" {
		dir = os.TempDir()
	}
	snap, err := f.store.CaptureSnapshot(dir)
	if err != nil {
		return nil, err
	}
	return &storeSnapshot{
		path:    snap.Path,
		full:    snap.Full,
		base:    f.store.SnapBase(),
		version: snap.Version,
		commit:  f.store.CommitSnapVersion,
	}, nil
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
	path    string
	full    bool
	base    string
	version uint64
	commit  func(uint64) error
}

func (s *storeSnapshot) Persist(sink raft.SnapshotSink) error {
	full := s.path
	var cleanup func()
	if !s.full && s.base != "" {
		if _, err := os.Stat(s.base); err == nil {
			f, err := os.CreateTemp(filepath.Dir(s.base), "snap-full-")
			if err != nil {
				_ = sink.Cancel()
				return err
			}
			if err := store.MaterializeFull(s.base, s.path, f); err != nil {
				_ = f.Close()
				_ = os.Remove(f.Name())
				_ = sink.Cancel()
				return err
			}
			if err := f.Close(); err != nil {
				_ = os.Remove(f.Name())
				_ = sink.Cancel()
				return err
			}
			full = f.Name()
			cleanup = func() { _ = os.Remove(full) }
		}
	}
	if cleanup != nil {
		defer cleanup()
	}
	file, err := os.Open(full)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	defer file.Close()
	if _, err := io.Copy(sink, file); err != nil {
		_ = sink.Cancel()
		return err
	}
	if err := sink.Close(); err != nil {
		return err
	}
	if s.base != "" {
		if err := copySnapshot(full, s.base); err != nil {
			return err
		}
	}
	if s.commit != nil {
		return s.commit(s.version)
	}
	return nil
}

func copySnapshot(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := to + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, to)
}

func (s *storeSnapshot) Release() {
	if s.path != "" {
		_ = os.Remove(s.path)
		s.path = ""
	}
}
