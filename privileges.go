package persist

import (
	"bytes"
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

var (
	bucketPrivileges = []byte("mysql_privileges")
	keyPrivilegeData = []byte("data")
)

// privilegeDataKey is the Badger key of the serialized mysql privilege tables.
func privilegeDataKey() []byte {
	bucket := entryKey(nil, bucketPrivileges, kindBucket)
	return entryKey(bucket, keyPrivilegeData, kindValue)
}

// privilegePersister writes the flatbuffer from MySQLDb.Persist into Badger.
// A replicating store proposes that write through Raft. The proposer has
// already updated its in-memory accounts, so reload skips this process while
// the commit is in flight.
type privilegePersister struct {
	store *Store
}

var _ mysql_db.MySQLDbPersistence = privilegePersister{}

// AttachPrivileges keeps db in sync with the privilege blob in this store.
// The returned persister is what MySQLDb should use for later account changes.
func (s *Store) AttachPrivileges(db *mysql_db.MySQLDb) mysql_db.MySQLDbPersistence {
	s.privMu.Lock()
	s.privDB = db
	s.privMu.Unlock()
	return privilegePersister{store: s}
}

// Persist implements mysql_db.MySQLDbPersistence. The statement is left empty
// so the binlog does not emit an event for this key.
func (p privilegePersister) Persist(ctx *sql.Context, data []byte) error {
	p.store.privMu.Lock()
	p.store.privSkip++
	p.store.privMu.Unlock()
	defer func() {
		p.store.privMu.Lock()
		p.store.privSkip--
		p.store.privMu.Unlock()
	}()
	return p.store.commit("", func(tx *kvTx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketPrivileges)
		if err != nil {
			return err
		}
		return bucket.Put(keyPrivilegeData, data)
	})
}

// LoadPrivileges reads the blob into the attached database.
// found is false when this store has never persisted accounts.
func (s *Store) LoadPrivileges(ctx *sql.Context) (found bool, err error) {
	s.privMu.Lock()
	defer s.privMu.Unlock()
	if s.privDB == nil {
		return false, fmt.Errorf("persist: privilege database is not attached")
	}
	data, ok, err := s.readPrivileges()
	if err != nil || !ok {
		return false, err
	}
	if err := s.privDB.LoadData(ctx, data); err != nil {
		return false, err
	}
	return true, nil
}

// privilegeProposed reports whether this process proposed the batch.
// Call it before noteApplied, which drops the batch from the in-flight list.
func (s *Store) privilegeProposed(id uint64) bool {
	if id == 0 || s.cluster == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.cluster.inflight {
		if q.id == id {
			return true
		}
	}
	return false
}

// reloadPrivileges applies a committed privilege blob on nodes that did not
// propose it. The proposer already holds the MySQLDb editor lock inside Persist.
func (s *Store) reloadPrivileges(batch replBatch, local bool) error {
	data, ok := privilegeBytes(batch.Ops)
	if !ok {
		return nil
	}
	s.privMu.Lock()
	defer s.privMu.Unlock()
	if local || s.privSkip > 0 || s.privDB == nil {
		return nil
	}
	return s.overwritePrivilegesLocked(data)
}

// reloadPrivilegesFromDisk loads whatever blob is currently stored.
// Snapshot restore calls this after the directory has been replaced.
func (s *Store) reloadPrivilegesFromDisk() error {
	s.privMu.Lock()
	defer s.privMu.Unlock()
	if s.privSkip > 0 || s.privDB == nil {
		return nil
	}
	data, ok, err := s.readPrivileges()
	if err != nil || !ok {
		return err
	}
	return s.overwritePrivilegesLocked(data)
}

func (s *Store) overwritePrivilegesLocked(data []byte) error {
	ctx := sql.NewContext(context.Background())
	ed := s.privDB.Editor()
	defer ed.Close()
	if err := s.privDB.OverwriteUsersAndGrantData(ctx, ed, data); err != nil {
		return err
	}
	s.privDB.SetEnabled(true)
	return nil
}

func (s *Store) readPrivileges() ([]byte, bool, error) {
	var data []byte
	var ok bool
	err := s.view(func(tx *kvTx) error {
		bucket := tx.Bucket(bucketPrivileges)
		if bucket == nil {
			return nil
		}
		val := bucket.Get(keyPrivilegeData)
		if val == nil {
			return nil
		}
		data = append([]byte(nil), val...)
		ok = true
		return nil
	})
	return data, ok, err
}

func privilegeBytes(ops []kvOp) ([]byte, bool) {
	key := privilegeDataKey()
	var data []byte
	found := false
	for _, op := range ops {
		if op.Delete || !bytes.Equal(op.Key, key) {
			continue
		}
		data = append([]byte(nil), op.Value...)
		found = true
	}
	return data, found
}
