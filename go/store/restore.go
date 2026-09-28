package store

import (
	"fmt"
	"os"
	"path/filepath"
)

// RestoreBackup loads state.bin from a backup directory into an empty data
// directory. The server for that directory is stopped. A directory that
// already exists is refused.
func RestoreBackup(from, data string) error {
	if from == "" || data == "" {
		return fmt.Errorf("hardhatdb: restore requires a backup directory and a data directory")
	}
	if _, err := os.Stat(data); err == nil {
		return fmt.Errorf("hardhatdb: data directory %s already exists", data)
	} else if !os.IsNotExist(err) {
		return err
	}
	src, err := os.Open(filepath.Join(from, "state.bin"))
	if err != nil {
		return err
	}
	defer src.Close()
	db, err := Open(data, true)
	if err != nil {
		removeRestore(data)
		return err
	}
	if err := db.InstallBackup(src); err != nil {
		_ = db.Close()
		removeRestore(data)
		return err
	}
	if err := db.Close(); err != nil {
		removeRestore(data)
		return err
	}
	return nil
}

func removeRestore(data string) {
	_ = os.RemoveAll(data)
	_ = os.RemoveAll(data + ".restore")
	_ = os.RemoveAll(data + ".old")
}
