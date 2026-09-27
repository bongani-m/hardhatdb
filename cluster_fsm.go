package persist

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/hashicorp/raft"
)

// storeFSM applies committed key/value batches and catches a new replica up
// from a Badger backup.
type storeFSM struct {
	store *Store
}

var _ raft.BatchingFSM = (*storeFSM)(nil)

func (f *storeFSM) Apply(log *raft.Log) interface{} {
	resp := f.applyOne(log)
	if log.Type == raft.LogCommand || log.Type == raft.LogConfiguration {
		f.store.noteFSMApplied(log.Index)
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
		f.store.noteFSMApplied(max)
	}
	return out
}

func (f *storeFSM) applyOne(log *raft.Log) interface{} {
	if log.Type != raft.LogCommand {
		return nil
	}
	batch, err := decodeBatch(log.Data)
	if err != nil {
		return err
	}
	// Decide before noteApplied drops this batch from the leader's in-flight list.
	local := f.store.privilegeProposed(batch.ID)
	switch batch.Phase {
	case phasePrepare:
		err = f.store.savePrepared(log.Index, batch)
		f.store.noteApplied(batch.ID)
		if err != nil {
			return err
		}
		return nil
	case phaseAbort:
		err = f.store.dropPrepared(log.Index, batch.PrepareID)
		f.store.noteApplied(batch.ID)
		if err != nil {
			return err
		}
		return nil
	case phaseCommit:
		stored, err := f.store.takePrepared(log.Index, batch.PrepareID)
		f.store.noteApplied(batch.ID)
		if err != nil {
			return err
		}
		if err := f.store.reloadPrivileges(stored, local); err != nil {
			return err
		}
		if len(stored.Ops) > 0 {
			if err := f.store.appendBinlog(log.Index, stored); err != nil {
				return err
			}
		}
		return nil
	default:
		err = f.store.applyOpsAt(log.Index, batch.Ops)
	}
	f.store.noteApplied(batch.ID)
	if err != nil {
		return err
	}
	if err := f.store.reloadPrivileges(batch, local); err != nil {
		return err
	}
	if err := f.store.appendBinlog(log.Index, batch); err != nil {
		return err
	}
	if batch.Rotate {
		if err := f.store.rotateBinlog(); err != nil {
			return err
		}
	}
	return nil
}

func (f *storeFSM) Snapshot() (raft.FSMSnapshot, error) {
	if err := f.store.syncData(); err != nil {
		return nil, err
	}
	dir := f.store.raftDir
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
	version, err := f.store.badgerDB().Backup(file, 0)
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

func (f *storeFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if err := f.store.installBackup(rc); err != nil {
		return err
	}
	f.store.noteFSMApplied(f.store.readRaftApplied())
	// The backup replaced the directory. Memory still has the old accounts
	// unless this process has not attached a privilege database yet.
	return f.store.reloadPrivilegesFromDisk()
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

// installBackup replaces the open Badger directory with the keys in a full
// backup. Load merges into whatever is already open, so the replacement
// starts from an empty sibling directory and is renamed into place only
// after Load succeeds.
func (s *Store) installBackup(r io.Reader) error {
	var version uint64
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return err
	}
	restorePath := s.path + ".restore"
	if err := os.RemoveAll(restorePath); err != nil {
		return err
	}
	loaded, err := openBadger(restorePath, s.syncWrites)
	if err != nil {
		return err
	}
	if err := loaded.Load(r, 256); err != nil {
		loaded.Close()
		_ = os.RemoveAll(restorePath)
		return err
	}
	if err := loaded.Close(); err != nil {
		_ = os.RemoveAll(restorePath)
		return err
	}

	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	if err := s.db.Close(); err != nil {
		if reopened, oerr := openBadger(s.path, s.syncWrites); oerr == nil {
			s.db = reopened
		}
		_ = os.RemoveAll(restorePath)
		return err
	}
	oldPath := s.path + ".old"
	_ = os.RemoveAll(oldPath)
	if err := os.Rename(s.path, oldPath); err != nil {
		if reopened, oerr := openBadger(s.path, s.syncWrites); oerr == nil {
			s.db = reopened
		}
		_ = os.RemoveAll(restorePath)
		return err
	}
	if err := os.Rename(restorePath, s.path); err != nil {
		_ = os.Rename(oldPath, s.path)
		if reopened, oerr := openBadger(s.path, s.syncWrites); oerr == nil {
			s.db = reopened
		}
		return err
	}
	reopened, err := openBadger(s.path, s.syncWrites)
	if err != nil {
		return err
	}
	s.db = reopened
	_ = os.RemoveAll(oldPath)
	return nil
}

// recoverRestoreDirs finishes or discards a snapshot install that stopped
// between the two renames. A sibling is kept only when the live directory
// was already moved aside, which happens after Load has succeeded.
func recoverRestoreDirs(path string) error {
	old := path + ".old"
	restore := path + ".restore"
	live := pathExists(path)
	hasOld := pathExists(old)
	hasRestore := pathExists(restore)
	if !live && hasOld && hasRestore {
		if err := os.Rename(restore, path); err != nil {
			return err
		}
		live = true
		if err := os.RemoveAll(old); err != nil {
			return err
		}
		hasOld = false
	} else if !live && hasOld {
		if err := os.Rename(old, path); err != nil {
			return err
		}
		live = true
		hasOld = false
	}
	if live && hasRestore {
		if err := os.RemoveAll(restore); err != nil {
			return err
		}
	}
	if live && hasOld {
		if err := os.RemoveAll(old); err != nil {
			return err
		}
	}
	return nil
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
