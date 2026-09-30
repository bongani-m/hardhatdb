// Package store is the Badger directory and the key encoding under it.
// It does not speak SQL and it does not run Raft.
package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// valueLogFileSize is the Badger value-log file size. Badger mmaps each file
// at twice this size, so 64 MiB keeps a fresh directory near 128 MiB instead
// of the default 2 GiB.
const valueLogFileSize int64 = 64 << 20

// snapMagic marks a snapshot written by CaptureSnapshot. Older backups start
// with a Badger version word instead, which InstallBackup still accepts.
const snapMagic uint64 = 0x31424848

// SnapFile is one point-in-time Badger backup. Full is set when Since is zero.
// Version is the token the next incremental snapshot passes as since.
type SnapFile struct {
	Path    string
	Full    bool
	Since   uint64
	Version uint64
}

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
		return nil, fmt.Errorf("hardhatdb: %s is a file, not a Badger directory (bbolt files are not migrated)", path)
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

// WriteBackup writes the stream InstallBackup reads: an 8-byte little-endian
// header, then a full backup. The header is zero; the Badger stream follows.
// w does not need to be seekable.
func (d *DB) WriteBackup(w io.Writer) error {
	db := d.Badger()
	if db == nil {
		return fmt.Errorf("hardhatdb: database is closed")
	}
	var hdr [8]byte
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := db.Backup(w, 0)
	return err
}

// CaptureSnapshot writes one snapshot file in dir. The first snapshot for a
// directory is a full backup. Later snapshots are the delta since the version
// committed by the previous snapshot.
func (d *DB) CaptureSnapshot(dir string) (SnapFile, error) {
	db := d.Badger()
	if db == nil {
		return SnapFile{}, fmt.Errorf("hardhatdb: database is closed")
	}
	if err := d.Sync(); err != nil {
		return SnapFile{}, err
	}
	f, err := os.CreateTemp(dir, "snap-")
	if err != nil {
		return SnapFile{}, err
	}
	since := d.readSnapVersion()
	version, err := writeSnap(f, db, since)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return SnapFile{}, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return SnapFile{}, err
	}
	// SinceTs skips versions less than or equal to this token, so the returned
	// max version is the right since value for the next snapshot.
	return SnapFile{Path: f.Name(), Full: since == 0, Since: since, Version: version}, nil
}

// CommitSnapVersion records the since token for the next incremental snapshot.
func (d *DB) CommitSnapVersion(version uint64) error {
	return os.WriteFile(d.snapVerPath(), []byte(strconv.FormatUint(version, 10)), 0o644)
}

func (d *DB) snapVerPath() string { return d.path + ".snapver" }

func (d *DB) readSnapVersion() uint64 {
	raw, err := os.ReadFile(d.snapVerPath())
	if err != nil {
		return 0
	}
	n, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func writeSnap(f *os.File, db *badger.DB, since uint64) (uint64, error) {
	kind := uint64(1)
	if since != 0 {
		kind = 2
	}
	hdr := make([]byte, 32)
	binary.LittleEndian.PutUint64(hdr[0:8], snapMagic)
	binary.LittleEndian.PutUint64(hdr[8:16], kind)
	binary.LittleEndian.PutUint64(hdr[16:24], since)
	if _, err := f.Write(hdr); err != nil {
		return 0, err
	}
	version, err := db.Backup(f, since)
	if err != nil {
		return 0, err
	}
	binary.LittleEndian.PutUint64(hdr[24:32], version)
	if _, err := f.WriteAt(hdr, 0); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	return version, nil
}

// MaterializeFull loads a full base and a delta into a scratch directory and
// writes one full snapshot. An empty joiner can install that image.
func MaterializeFull(base, delta string, w io.Writer) error {
	dir, err := os.MkdirTemp("", "hardhatdb-snap-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := openBadger(dir, false)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := loadSnap(db, base); err != nil {
		return err
	}
	if err := loadSnap(db, delta); err != nil {
		return err
	}
	out, err := os.CreateTemp("", "hardhatdb-full-")
	if err != nil {
		return err
	}
	name := out.Name()
	defer os.Remove(name)
	if _, err := writeSnap(out, db, 0); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	return err
}

func loadSnap(db *badger.DB, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var word uint64
	if err := binary.Read(f, binary.LittleEndian, &word); err != nil {
		return err
	}
	if word == snapMagic {
		var skip [24]byte
		if _, err := io.ReadFull(f, skip[:]); err != nil {
			return err
		}
	}
	return db.Load(f, 256)
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
	var word uint64
	if err := binary.Read(r, binary.LittleEndian, &word); err != nil {
		return err
	}
	if word == snapMagic {
		var kind, since, version uint64
		if err := binary.Read(r, binary.LittleEndian, &kind); err != nil {
			return err
		}
		if err := binary.Read(r, binary.LittleEndian, &since); err != nil {
			return err
		}
		if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
			return err
		}
		if kind == 2 {
			local := d.readSnapVersion()
			if local != since {
				return fmt.Errorf("hardhatdb: snapshot base %d does not match local %d", since, local)
			}
			db := d.Badger()
			if db == nil {
				return fmt.Errorf("hardhatdb: database is closed")
			}
			if err := db.Load(r, 256); err != nil {
				return err
			}
			return d.CommitSnapVersion(version)
		}
		if err := d.installFull(r); err != nil {
			return err
		}
		return d.CommitSnapVersion(version)
	}
	return d.installFull(r)
}

func (d *DB) installFull(r io.Reader) error {
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
