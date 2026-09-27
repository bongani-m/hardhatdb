// Package store is the Badger directory and the key encoding under it.
// It does not speak SQL and it does not run Raft.
package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// valueLogFileSize is the Badger value-log file size. Badger mmaps each file
// at twice this size, so 64 MiB keeps a fresh directory near 128 MiB instead
// of the default 2 GiB.
const valueLogFileSize int64 = 64 << 20

// DB is one Badger directory. A snapshot restore swaps the handle under mu.
type DB struct {
	mu         sync.RWMutex
	db         *badger.DB
	path       string
	syncWrites bool
	gcStop     chan struct{}
	gcDone     chan struct{}
}

// Open opens or creates the Badger directory at path. An existing file at
// path is refused: bbolt files are not migrated.
func Open(path string, syncWrites bool) (*DB, error) {
	if err := RecoverRestoreDirs(path); err != nil {
		return nil, err
	}
	db, err := openBadger(path, syncWrites)
	if err != nil {
		return nil, err
	}
	return &DB{db: db, path: path, syncWrites: syncWrites}, nil
}

func openBadger(path string, syncWrites bool) (*badger.DB, error) {
	info, err := os.Stat(path)
	if err == nil && !info.IsDir() {
		return nil, fmt.Errorf("persist: %s is a file, not a Badger directory (bbolt files are not migrated)", path)
	}
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(path).
		WithSyncWrites(syncWrites).
		WithLoggingLevel(badger.WARNING).
		WithValueLogFileSize(valueLogFileSize)
	return badger.Open(opts)
}

// Path returns the Badger directory path.
func (d *DB) Path() string {
	if d == nil {
		return ""
	}
	return d.path
}

// SyncWrites reports whether each commit fsyncs.
func (d *DB) SyncWrites() bool {
	return d != nil && d.syncWrites
}

// Badger returns the open database. A snapshot restore swaps it under mu.
func (d *DB) Badger() *badger.DB {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.db
}

// Close stops value-log GC and closes Badger.
func (d *DB) Close() error {
	if d == nil {
		return nil
	}
	d.StopGC()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db == nil {
		return nil
	}
	err := d.db.Close()
	d.db = nil
	return err
}

// Sync fsyncs Badger when commits themselves do not.
func (d *DB) Sync() error {
	if d == nil || d.syncWrites {
		return nil
	}
	db := d.Badger()
	if db == nil {
		return nil
	}
	return db.Sync()
}

// StartGC rewrites stale value-log files until StopGC.
func (d *DB) StartGC() {
	if d == nil || d.gcStop != nil {
		return
	}
	d.gcStop = make(chan struct{})
	d.gcDone = make(chan struct{})
	go func() {
		defer close(d.gcDone)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-d.gcStop:
				return
			case <-ticker.C:
				d.rewriteValueLog()
			}
		}
	}()
}

// StopGC waits for the value-log rewriter to finish.
func (d *DB) StopGC() {
	if d == nil || d.gcStop == nil {
		return
	}
	close(d.gcStop)
	<-d.gcDone
	d.gcStop = nil
}

func (d *DB) RewriteValueLog() { d.rewriteValueLog() }

func (d *DB) rewriteValueLog() {
	db := d.Badger()
	if db == nil {
		return
	}
	for {
		err := db.RunValueLogGC(0.5)
		if err != nil {
			return
		}
	}
}

// InstallBackup replaces the open directory with the keys in a full backup.
// Load merges into whatever is already open, so the replacement starts from
// an empty sibling directory and is renamed into place only after Load succeeds.
func (d *DB) InstallBackup(r io.Reader) error {
	var version uint64
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return err
	}
	restorePath := d.path + ".restore"
	if err := os.RemoveAll(restorePath); err != nil {
		return err
	}
	loaded, err := openBadger(restorePath, d.syncWrites)
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

	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.db.Close(); err != nil {
		if reopened, oerr := openBadger(d.path, d.syncWrites); oerr == nil {
			d.db = reopened
		}
		_ = os.RemoveAll(restorePath)
		return err
	}
	oldPath := d.path + ".old"
	_ = os.RemoveAll(oldPath)
	if err := os.Rename(d.path, oldPath); err != nil {
		if reopened, oerr := openBadger(d.path, d.syncWrites); oerr == nil {
			d.db = reopened
		}
		_ = os.RemoveAll(restorePath)
		return err
	}
	if err := os.Rename(restorePath, d.path); err != nil {
		_ = os.Rename(oldPath, d.path)
		if reopened, oerr := openBadger(d.path, d.syncWrites); oerr == nil {
			d.db = reopened
		}
		return err
	}
	reopened, err := openBadger(d.path, d.syncWrites)
	if err != nil {
		return err
	}
	d.db = reopened
	_ = os.RemoveAll(oldPath)
	return nil
}

// RecoverRestoreDirs finishes or discards a snapshot install that stopped
// between the two renames. A sibling is kept only when the live directory
// was already moved aside, which happens after Load has succeeded.
func RecoverRestoreDirs(path string) error {
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
