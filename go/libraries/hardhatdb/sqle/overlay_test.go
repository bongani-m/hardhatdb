package sqle

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestOverlayReadsAndScan(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLoggingLevel(badger.ERROR))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bucketName := []byte("rows")
	if err := db.Update(func(txn *badger.Txn) error {
		tx := &kvTx{txn: txn}
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		if err := b.Put([]byte("a"), []byte("1")); err != nil {
			return err
		}
		if err := b.Put([]byte("b"), []byte("2")); err != nil {
			return err
		}
		return b.PutRaw([]byte("k"), []byte("row"))
	}); err != nil {
		t.Fatal(err)
	}

	bucket := entryKey(nil, bucketName, kindBucket)
	keyA := entryKey(bucket, []byte("a"), kindValue)
	keyB := entryKey(bucket, []byte("b"), kindValue)
	keyC := entryKey(bucket, []byte("c"), kindValue)
	rawB := append(append(append([]byte{}, bucket...), rawMark...), []byte("k")...)
	rawNew := append(append(append([]byte{}, bucket...), rawMark...), []byte("z")...)

	overlay := newKVOverlay([]kvOp{
		{Key: keyA, Delete: true},
		{Key: keyB, Value: []byte("9")},
		{Key: keyC, Value: []byte("3")},
		{Key: rawB, Delete: true},
		{Key: rawNew, Value: []byte("new")},
	})

	err = db.Update(func(txn *badger.Txn) error {
		rec := &recordingTxn{Txn: txn}
		tx := &kvTx{txn: rec, overlay: overlay}
		b := tx.Bucket(bucketName)
		if b == nil {
			t.Fatal("bucket missing")
		}
		if got := b.Get([]byte("a")); got != nil {
			t.Fatalf("deleted key a = %q", got)
		}
		if got := string(b.Get([]byte("b"))); got != "9" {
			t.Fatalf("key b = %q", got)
		}
		if got := string(b.Get([]byte("c"))); got != "3" {
			t.Fatalf("key c = %q", got)
		}
		if got := b.GetRaw([]byte("k")); got != nil {
			t.Fatalf("deleted raw key = %q", got)
		}
		if got := string(b.GetRaw([]byte("z"))); got != "new" {
			t.Fatalf("raw z = %q", got)
		}

		var names []string
		if err := b.ForEach(func(k, v []byte) error {
			names = append(names, string(k)+":"+string(v))
			return nil
		}); err != nil {
			return err
		}
		if len(names) != 2 || names[0] != "b:9" || names[1] != "c:3" {
			t.Fatalf("foreach = %v", names)
		}

		it := b.rawIter(false)
		defer it.Close()
		var raws []string
		for it.Rewind(); it.Valid(); it.Next() {
			val, err := it.Value()
			if err != nil {
				return err
			}
			raws = append(raws, string(it.Key())+":"+string(val))
		}
		if len(raws) != 1 || raws[0] != "z:new" {
			t.Fatalf("raw scan = %v", raws)
		}

		if err := rec.Set(keyB, []byte("7")); err != nil {
			return err
		}
		if got := string(b.Get([]byte("b"))); got != "7" {
			t.Fatalf("local write = %q", got)
		}
		return errReplicate
	})
	if err != errReplicate {
		t.Fatal(err)
	}
}
