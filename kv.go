package persist

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/dolthub/go-mysql-server/sql"
)

// rawMark prefixes row and index keys stored in key order. Ten 0xFF bytes
// overflow a uvarint, so these keys are not bucket entries, and a constant
// prefix keeps their relative order equal to the memcomparable key.
var rawMark = bytes.Repeat([]byte{0xFF}, binary.MaxVarintLen64)

const (
	kindValue  byte = 0
	kindBucket byte = 1
)

// openBadger opens or creates a Badger directory. An existing file at path is
// refused: bbolt files are not migrated.
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

// valueLogFileSize is the Badger value-log file size. Badger mmaps each file
// at twice this size, so 64 MiB keeps a fresh directory near 128 MiB instead
// of the default 2 GiB.
const valueLogFileSize int64 = 64 << 20

func (s *Store) startValueLogGC() {
	s.gcStop = make(chan struct{})
	s.gcDone = make(chan struct{})
	go func() {
		defer close(s.gcDone)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-s.gcStop:
				return
			case <-ticker.C:
				s.rewriteValueLog()
			}
		}
	}()
}

func (s *Store) stopValueLogGC() {
	if s.gcStop == nil {
		return
	}
	close(s.gcStop)
	<-s.gcDone
	s.gcStop = nil
}

// rewriteValueLog discards stale value-log files until Badger has nothing left to rewrite.
func (s *Store) rewriteValueLog() {
	db := s.badgerDB()
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

// view runs fn in a read-only transaction.
func (s *Store) view(fn func(tx *kvTx) error) error {
	return s.badgerDB().View(func(txn *badger.Txn) error {
		return fn(&kvTx{txn: txn})
	})
}

// rowView runs fn against the session snapshot when this is a consistent read.
// current forces the latest commit, which locking reads and writers use.
func (s *Store) rowView(ctx context.Context, current bool, fn func(tx *kvTx) error) error {
	if !current {
		if txn := snapshotTxnFrom(ctx); txn != nil {
			return fn(&kvTx{txn: txn})
		}
	}
	return s.view(fn)
}

// update runs fn in a read-write transaction. Writers are serialized: Badger
// does not retry a conflicting commit, and this store read-modify-writes
// sequences and JSON blobs the way a single bbolt writer did.
func (s *Store) update(fn func(tx *kvTx) error) error {
	return s.commit("", fn)
}

// updateQuery is update, and it keeps the statement text for a catalog-only
// binlog event when this store is replicating.
func (s *Store) updateQuery(ctx *sql.Context, fn func(tx *kvTx) error) error {
	statement := ""
	if ctx != nil {
		statement = ctx.Query()
	}
	err := s.commitGTID(statement, sourceGTID(ctx), fn)
	if err == nil {
		if sess, ok := sessionFrom(ctx); ok {
			sess.refreshSnapshot()
		}
	}
	return err
}

// kvTxn is the Badger transaction surface this store uses. A recording
// transaction wraps it while a cluster commit is being built.
type kvTxn interface {
	Set(key, val []byte) error
	Delete(key []byte) error
	Get(key []byte) (*badger.Item, error)
	NewIterator(opt badger.IteratorOptions) *badger.Iterator
}

// kvTx is one Badger transaction. Nested buckets are prefixes, not a separate
// type in the database.
type kvTx struct {
	txn kvTxn
	// rotate asks the commit path to roll the binlog after this batch.
	rotate bool
}

func (tx *kvTx) root() *kvBucket {
	return &kvBucket{tx: tx}
}

func (tx *kvTx) Bucket(name []byte) *kvBucket {
	return tx.root().Bucket(name)
}

func (tx *kvTx) CreateBucketIfNotExists(name []byte) (*kvBucket, error) {
	return tx.root().CreateBucketIfNotExists(name)
}

// kvBucket is a prefix of the flat key space. Its own sentinel key is prefix,
// and the 8-byte sequence lives in that sentinel. The root bucket has an empty
// prefix and no sentinel.
type kvBucket struct {
	tx     *kvTx
	prefix []byte
}

func (b *kvBucket) Get(key []byte) []byte {
	val, err := b.tx.get(entryKey(b.prefix, key, kindValue))
	if err != nil {
		return nil
	}
	return val
}

func (b *kvBucket) Put(key, value []byte) error {
	if value == nil {
		value = []byte{}
	}
	// Badger keeps the slices until the transaction ends.
	return b.tx.txn.Set(entryKey(b.prefix, key, kindValue), append([]byte(nil), value...))
}

func (b *kvBucket) Delete(key []byte) error {
	stored := entryKey(b.prefix, key, kindValue)
	val, err := b.tx.get(stored)
	if err != nil || val == nil {
		return err
	}
	return b.tx.txn.Delete(stored)
}

func (b *kvBucket) Bucket(name []byte) *kvBucket {
	key := entryKey(b.prefix, name, kindBucket)
	val, err := b.tx.get(key)
	if err != nil || val == nil {
		return nil
	}
	return &kvBucket{tx: b.tx, prefix: key}
}

func (b *kvBucket) CreateBucket(name []byte) (*kvBucket, error) {
	if b.Bucket(name) != nil {
		return nil, fmt.Errorf("persist: bucket %q already exists", name)
	}
	if existing := b.Get(name); existing != nil {
		return nil, fmt.Errorf("persist: key %q is not a bucket", name)
	}
	key := entryKey(b.prefix, name, kindBucket)
	var seq [8]byte
	if err := b.tx.txn.Set(key, append([]byte(nil), seq[:]...)); err != nil {
		return nil, err
	}
	return &kvBucket{tx: b.tx, prefix: key}, nil
}

func (b *kvBucket) CreateBucketIfNotExists(name []byte) (*kvBucket, error) {
	if existing := b.Bucket(name); existing != nil {
		return existing, nil
	}
	return b.CreateBucket(name)
}

func (b *kvBucket) DeleteBucket(name []byte) error {
	key := entryKey(b.prefix, name, kindBucket)
	val, err := b.tx.get(key)
	if err != nil {
		return err
	}
	if val == nil {
		return fmt.Errorf("persist: bucket %q does not exist", name)
	}
	return b.tx.deletePrefix(key)
}

// ForEach visits immediate keys and child buckets. A child bucket is reported
// with a nil value, matching bbolt, so callers can tell buckets from keys.
func (b *kvBucket) ForEach(fn func(k, v []byte) error) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = b.prefix
	it := b.tx.txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if bytesEqual(key, b.prefix) {
			continue
		}
		if len(key) < len(b.prefix) || !bytesEqual(key[:len(b.prefix)], b.prefix) {
			continue
		}
		name, kind, n, ok := parseComponent(key[len(b.prefix):])
		if !ok || n != len(key)-len(b.prefix) {
			continue
		}
		name = append([]byte(nil), name...)
		if kind == kindBucket {
			if err := fn(name, nil); err != nil {
				return err
			}
			continue
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if val == nil {
			val = []byte{}
		}
		if err := fn(name, val); err != nil {
			return err
		}
	}
	return nil
}

func (b *kvBucket) Sequence() uint64 {
	val, err := b.tx.get(b.prefix)
	if err != nil || len(val) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(val)
}

func (b *kvBucket) SetSequence(seq uint64) error {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], seq)
	return b.tx.txn.Set(append([]byte(nil), b.prefix...), append([]byte(nil), raw[:]...))
}

func (b *kvBucket) rawFull(key []byte) []byte {
	full := make([]byte, 0, len(b.prefix)+len(rawMark)+len(key))
	full = append(full, b.prefix...)
	full = append(full, rawMark...)
	full = append(full, key...)
	return full
}

func (b *kvBucket) GetRaw(key []byte) []byte {
	val, err := b.tx.get(b.rawFull(key))
	if err != nil {
		return nil
	}
	return val
}

func (b *kvBucket) PutRaw(key, value []byte) error {
	if value == nil {
		value = []byte{}
	}
	return b.tx.txn.Set(b.rawFull(key), append([]byte(nil), value...))
}

func (b *kvBucket) DeleteRaw(key []byte) error {
	stored := b.rawFull(key)
	val, err := b.tx.get(stored)
	if err != nil || val == nil {
		return err
	}
	return b.tx.txn.Delete(stored)
}

func (b *kvBucket) forEachRaw(fn func(k, v []byte) error) error {
	it := b.rawIter(false)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		val, err := it.Value()
		if err != nil {
			return err
		}
		if err := fn(it.Key(), val); err != nil {
			return err
		}
	}
	return nil
}

// rawIter walks keys stored with PutRaw, in memcomparable order.
type rawIter struct {
	it     *badger.Iterator
	prefix []byte
}

func (b *kvBucket) rawIter(reverse bool) *rawIter {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = append(append([]byte(nil), b.prefix...), rawMark...)
	opts.Reverse = reverse
	return &rawIter{it: b.tx.txn.NewIterator(opts), prefix: opts.Prefix}
}

func (it *rawIter) Seek(key []byte) {
	target := append(append([]byte(nil), it.prefix...), key...)
	it.it.Seek(target)
}

// seekEnd positions a reverse iterator on the last key in the prefix. Rewind
// seeks the prefix itself, and a reverse seek stops on the key before that
// prefix, which is outside it.
func (it *rawIter) seekEnd() {
	end := prefixEnd(it.prefix)
	if end == nil {
		end = append(append([]byte(nil), it.prefix...), 0)
	}
	it.it.Seek(end)
}

func (it *rawIter) Rewind() { it.it.Rewind() }

func (it *rawIter) Valid() bool { return it.it.ValidForPrefix(it.prefix) }

func (it *rawIter) Next() { it.it.Next() }

func (it *rawIter) Key() []byte {
	full := it.it.Item().Key()
	return append([]byte(nil), full[len(it.prefix):]...)
}

func (it *rawIter) Value() ([]byte, error) {
	return it.it.Item().ValueCopy(nil)
}

func (it *rawIter) Close() { it.it.Close() }

func (b *kvBucket) NextSequence() (uint64, error) {
	seq := b.Sequence()
	if seq == math.MaxUint64 {
		return 0, fmt.Errorf("persist: sequence overflow")
	}
	seq++
	if err := b.SetSequence(seq); err != nil {
		return 0, err
	}
	return seq, nil
}

func (tx *kvTx) get(key []byte) ([]byte, error) {
	item, err := tx.txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (tx *kvTx) deletePrefix(prefix []byte) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := tx.txn.NewIterator(opts)
	var keys [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	it.Close()
	for _, key := range keys {
		if err := tx.txn.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// entryKey encodes one path component as uvarint(len) || name || kind.
// A bucket's children are stored under its sentinel key, so a prefix scan of
// that sentinel yields the bucket and everything inside it.
func entryKey(prefix, name []byte, kind byte) []byte {
	var n [binary.MaxVarintLen64]byte
	nn := binary.PutUvarint(n[:], uint64(len(name)))
	key := make([]byte, 0, len(prefix)+nn+len(name)+1)
	key = append(key, prefix...)
	key = append(key, n[:nn]...)
	key = append(key, name...)
	key = append(key, kind)
	return key
}

func parseComponent(rest []byte) (name []byte, kind byte, n int, ok bool) {
	length, k := binary.Uvarint(rest)
	if k <= 0 || k >= len(rest) {
		return nil, 0, 0, false
	}
	if length > uint64(len(rest)-k-1) {
		return nil, 0, 0, false
	}
	nameEnd := k + int(length)
	kind = rest[nameEnd]
	if kind != kindValue && kind != kindBucket {
		return nil, 0, 0, false
	}
	return rest[k:nameEnd], kind, nameEnd + 1, true
}
