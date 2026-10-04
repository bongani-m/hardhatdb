package sqle

import (
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// Staged chunks live outside the user keyspace. A commit marker publishes
// them. A crash before that marker leaves them invisible.
var (
	stageCursorPrefix = []byte("stageCursor\x00")
	stageMetaPrefix   = []byte("stageMeta\x00")
)

func stageDataPrefix(id string) []byte {
	return append(append([]byte("staged\x00"), id...), 0)
}

func stageDataKey(id string, seq uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], seq)
	return append(stageDataPrefix(id), buf[:]...)
}

func stageCursorKey(id string) []byte {
	return append(append([]byte(nil), stageCursorPrefix...), id...)
}

func stageMetaKey(id string) []byte {
	return append(append([]byte(nil), stageMetaPrefix...), id...)
}

func (s *Store) saveStage(index uint64, batch replBatch) error {
	raw, err := encodeBatch(batch)
	if err != nil {
		return err
	}
	return s.badgerDB().Update(func(txn *badger.Txn) error {
		if err := txn.Set(stageDataKey(batch.PrepareID, batch.Chunk), raw); err != nil {
			return err
		}
		return putApplied(txn, index)
	})
}

func (s *Store) saveStageMeta(index uint64, batch replBatch) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], batch.ChunkCount)
	return s.badgerDB().Update(func(txn *badger.Txn) error {
		if err := txn.Set(stageMetaKey(batch.PrepareID), buf[:]); err != nil {
			return err
		}
		return putApplied(txn, index)
	})
}

func (s *Store) hasStageMeta(id string) (bool, error) {
	var ok bool
	err := s.badgerDB().View(func(txn *badger.Txn) error {
		_, err := txn.Get(stageMetaKey(id))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		ok = true
		return nil
	})
	return ok, err
}

type stageCursor struct {
	Next   uint64
	Index  uint64
	Marker replBatch
}

func encodeCursor(c stageCursor) ([]byte, error) {
	raw, err := encodeBatch(c.Marker)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 16+len(raw))
	binary.BigEndian.PutUint64(buf[0:8], c.Next)
	binary.BigEndian.PutUint64(buf[8:16], c.Index)
	copy(buf[16:], raw)
	return buf, nil
}

func decodeCursor(raw []byte) (stageCursor, error) {
	if len(raw) < 16 {
		return stageCursor{}, errors.New("hardhatdb: short stage cursor")
	}
	c := stageCursor{
		Next:  binary.BigEndian.Uint64(raw[0:8]),
		Index: binary.BigEndian.Uint64(raw[8:16]),
	}
	marker, err := decodeBatch(raw[16:])
	if err != nil {
		return stageCursor{}, err
	}
	c.Marker = marker
	return c, nil
}

func (s *Store) readCursor(id string) (stageCursor, bool, error) {
	var c stageCursor
	var ok bool
	err := s.badgerDB().View(func(txn *badger.Txn) error {
		item, err := txn.Get(stageCursorKey(id))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		c, err = decodeCursor(raw)
		if err != nil {
			return err
		}
		ok = true
		return nil
	})
	return c, ok, err
}

func (s *Store) loadStage(id string, seq uint64) (replBatch, bool, error) {
	var batch replBatch
	var ok bool
	err := s.badgerDB().View(func(txn *badger.Txn) error {
		item, err := txn.Get(stageDataKey(id, seq))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		batch, err = decodeBatch(raw)
		if err != nil {
			return err
		}
		ok = true
		return nil
	})
	return batch, ok, err
}

func (s *Store) hasStages(id string) (bool, error) {
	var ok bool
	err := s.badgerDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = stageDataPrefix(id)
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		ok = it.Valid()
		return nil
	})
	return ok, err
}

// publishStaged copies staged chunks into the user keyspace while holding the
// publish lock, then drops the staged copies. A cursor in the same transaction
// as each chunk lets a crash resume without exposing a second copy of a row.
func (s *Store) publishStaged(index uint64, marker replBatch) (replBatch, error) {
	id := marker.PrepareID
	if id == "" {
		return replBatch{}, errors.New("hardhatdb: stage id is empty")
	}
	cur, haveCur, err := s.readCursor(id)
	if err != nil {
		return replBatch{}, err
	}
	if haveCur && marker.ChunkCount == 0 {
		marker = cur.Marker
	}
	if index == 0 && haveCur {
		index = cur.Index
	}
	count := marker.ChunkCount
	if count == 0 {
		count, err = s.stageCount(id)
		if err != nil {
			return replBatch{}, err
		}
	}
	next := uint64(0)
	if haveCur {
		next = cur.Next
	}
	present, err := s.hasStages(id)
	if err != nil {
		return replBatch{}, err
	}
	if !present && !haveCur {
		return replBatch{Statement: marker.Statement, Unix: marker.Unix, Rotate: marker.Rotate, PrepareID: id}, nil
	}

	chunks := make([]replBatch, count)
	var ops []kvOp
	for seq := uint64(0); seq < count; seq++ {
		chunk, ok, err := s.loadStage(id, seq)
		if err != nil {
			return replBatch{}, err
		}
		if !ok {
			return replBatch{}, errors.New("hardhatdb: missing staged chunk")
		}
		chunks[seq] = chunk
		ops = append(ops, chunk.Ops...)
	}

	s.publish.Lock()
	defer s.publish.Unlock()

	marker.ChunkCount = count
	for seq := next; seq < count; seq++ {
		raw, err := encodeCursor(stageCursor{Next: seq + 1, Index: index, Marker: marker})
		if err != nil {
			return replBatch{}, err
		}
		err = s.badgerDB().Update(func(txn *badger.Txn) error {
			if err := applyOpsTxn(txn, chunks[seq].Ops); err != nil {
				return err
			}
			return txn.Set(stageCursorKey(id), raw)
		})
		if err != nil {
			return replBatch{}, err
		}
	}
	if err := s.deleteStageKeys(index, id); err != nil {
		return replBatch{}, err
	}
	out := marker
	out.Ops = ops
	out.Phase = phaseApply
	return out, nil
}

func (s *Store) stageCount(id string) (uint64, error) {
	var n uint64
	err := s.badgerDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = stageDataPrefix(id)
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			n++
		}
		return nil
	})
	return n, err
}

func (s *Store) deleteStageKeys(index uint64, id string) error {
	prefix := stageDataPrefix(id)
	for {
		var keys [][]byte
		err := s.badgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid() && len(keys) < 128; it.Next() {
				keys = append(keys, append([]byte(nil), it.Item().Key()...))
			}
			return nil
		})
		if err != nil {
			return err
		}
		last := len(keys) == 0
		err = s.badgerDB().Update(func(txn *badger.Txn) error {
			for _, key := range keys {
				if err := txn.Delete(key); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
			}
			if !last {
				return nil
			}
			for _, key := range [][]byte{stageCursorKey(id), stageMetaKey(id), preparedStorageKey(id)} {
				if err := txn.Delete(key); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
			}
			return putApplied(txn, index)
		})
		if err != nil {
			return err
		}
		if last {
			return nil
		}
	}
}

// dropStaged removes staged chunks and any prepared blob. A cursor means a
// publish is in progress, so the chunks stay for the resume and only the
// applied index moves.
func (s *Store) dropStaged(index uint64, id string) error {
	_, ok, err := s.readCursor(id)
	if err != nil {
		return err
	}
	if ok {
		return s.badgerDB().Update(func(txn *badger.Txn) error {
			return putApplied(txn, index)
		})
	}
	return s.deleteStageKeys(index, id)
}

// stageIDs lists transactions that have at least one staged chunk.
func (s *Store) stageIDs() ([]string, error) {
	prefix := []byte("staged\x00")
	seen := map[string]struct{}{}
	err := s.badgerDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			rest := key[len(prefix):]
			i := indexByte(rest, 0)
			if i <= 0 || len(rest) < i+1+8 {
				continue
			}
			seen[string(rest[:i])] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids, nil
}

func indexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
}

func (s *Store) cursorIDs() ([]string, error) {
	var ids []string
	err := s.badgerDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = stageCursorPrefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			ids = append(ids, string(key[len(stageCursorPrefix):]))
		}
		return nil
	})
	return ids, err
}

// recoverStages finishes a publish that crashed and drops staged chunks that
// never reached a marker. A prepare marker (stage meta, no cursor) is left
// for the later commit.
func (s *Store) recoverStages() error {
	ids, err := s.cursorIDs()
	if err != nil {
		return err
	}
	for _, id := range ids {
		cur, ok, err := s.readCursor(id)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if _, err := s.publishStaged(cur.Index, cur.Marker); err != nil {
			return err
		}
	}
	ids, err = s.stageIDs()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, ok, err := s.readCursor(id); err != nil {
			return err
		} else if ok {
			continue
		}
		meta, err := s.hasStageMeta(id)
		if err != nil {
			return err
		}
		if meta {
			continue
		}
		if err := s.dropStaged(0, id); err != nil {
			return err
		}
	}
	return nil
}

// abortOrphanStages proposes an abort for staged transactions that have no
// commit cursor and no prepare marker. The leader calls it after the log is
// applied, so a committed marker has already published or written its meta.
func (s *Store) abortOrphanStages() error {
	ids, err := s.stageIDs()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, ok, err := s.readCursor(id); err != nil {
			return err
		} else if ok {
			continue
		}
		meta, err := s.hasStageMeta(id)
		if err != nil {
			return err
		}
		if meta {
			continue
		}
		if s.group != nil && s.group.StageActive(id) {
			continue
		}
		if s.group == nil {
			if err := s.dropStaged(0, id); err != nil {
				return err
			}
			continue
		}
		if err := s.AbortPrepared(id); err != nil {
			return err
		}
	}
	return nil
}
