package sqle

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// backupMeta is the cutoff recorded beside state.bin.
type backupMeta struct {
	Index uint64 `json:"index"`
	UUID  string `json:"uuid"`
}

// BackupTo writes a backup of this directory into dir. A replicating store
// snapshots on the Raft thread first, then copies that snapshot and the binlog.
// A standalone store writes a Badger backup and records index 0. dir is
// created when it is missing and refused when it already holds files.
func (s *Store) BackupTo(dir string) (index uint64, err error) {
	created, err := prepareBackupDir(dir)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dir)
			if !created {
				_ = os.Mkdir(dir, 0o755)
			}
		}
	}()
	var uuid string
	if s.group != nil {
		if err = s.group.Snapshot(); err != nil {
			return 0, err
		}
		var rc io.ReadCloser
		index, rc, err = s.group.LatestSnapshot()
		if err != nil {
			return 0, err
		}
		err = writeState(filepath.Join(dir, "state.bin"), rc)
		_ = rc.Close()
		if err != nil {
			return 0, err
		}
		uuid = s.serverUUID
		if err = copyDirFiles(filepath.Join(s.raftDir, "binlog"), filepath.Join(dir, "binlog")); err != nil {
			return 0, err
		}
	} else {
		if err = s.syncData(); err != nil {
			return 0, err
		}
		var f *os.File
		f, err = os.Create(filepath.Join(dir, "state.bin"))
		if err != nil {
			return 0, err
		}
		err = s.WriteBackup(f)
		closeErr := f.Close()
		if err != nil {
			return 0, err
		}
		if closeErr != nil {
			return 0, closeErr
		}
	}
	if err = writeBackupMeta(dir, backupMeta{Index: index, UUID: uuid}); err != nil {
		return 0, err
	}
	return index, nil
}

func prepareBackupDir(dir string) (created bool, err error) {
	if dir == "" {
		return false, fmt.Errorf("hardhatdb: backup directory is empty")
	}
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return false, err
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("hardhatdb: %s is not a directory", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	if len(entries) > 0 {
		return false, fmt.Errorf("hardhatdb: backup directory %s is not empty", dir)
	}
	return false, nil
}

func writeState(path string, r io.Reader) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, r)
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func writeBackupMeta(dir string, meta backupMeta) error {
	raw, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(filepath.Join(dir, "meta"), raw, 0o644)
}

func readBackupMeta(dir string) (backupMeta, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "meta"))
	if err != nil {
		return backupMeta{}, err
	}
	var meta backupMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return backupMeta{}, err
	}
	return meta, nil
}

func copyDirFiles(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := copyFile(filepath.Join(src, entry.Name()), filepath.Join(dst, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}
