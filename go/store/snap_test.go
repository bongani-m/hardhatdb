package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestSnapshotDeltaInstall(t *testing.T) {
	dir := t.TempDir()
	src, err := Open(filepath.Join(dir, "src"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if err := src.Badger().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("a"), []byte("1"))
	}); err != nil {
		t.Fatal(err)
	}
	full, err := src.CaptureSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(full.Path)
	if !full.Full {
		t.Fatal("first snapshot should be complete")
	}
	if err := src.CommitSnapVersion(full.Version); err != nil {
		t.Fatal(err)
	}

	dst, err := Open(filepath.Join(dir, "dst"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	f, err := os.Open(full.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := dst.InstallBackup(f); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()

	if err := src.Badger().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("b"), []byte("2"))
	}); err != nil {
		t.Fatal(err)
	}
	delta, err := src.CaptureSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(delta.Path)
	if delta.Full {
		t.Fatal("second snapshot should be a delta")
	}
	empty, err := Open(filepath.Join(dir, "empty"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	df, err := os.Open(delta.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := empty.InstallBackup(df); err == nil {
		df.Close()
		t.Fatal("empty database accepted a delta")
	}
	df.Close()

	df, err = os.Open(delta.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if err := dst.InstallBackup(df); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	err = dst.Badger().View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			got[string(it.Item().Key())] = string(val)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got["a"] != "1" || got["b"] != "2" {
		t.Fatalf("keys = %v", got)
	}

	var buf bytes.Buffer
	if err := MaterializeFull(full.Path, delta.Path, &buf); err != nil {
		t.Fatal(err)
	}
	if buf.Len() < 32 {
		t.Fatalf("full image is %d bytes", buf.Len())
	}
}
