package sqle

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"time"

	"github.com/bongani-m/persist/go/store"
	"github.com/dgraph-io/badger/v4"
)

// errReplicate aborts the Badger transaction that computed a commit. The
// recorded key/value operations are proposed to Raft and applied only after
// a quorum persists them.
var errReplicate = errors.New("persist: replicate batch")

type kvOp = store.KVOp
type rowChange = store.RowChange
type replBatch = store.ReplBatch
type marked = store.Marked

const (
	phaseApply   = store.PhaseApply
	phasePrepare = store.PhasePrepare
	phaseCommit  = store.PhaseCommit
	phaseAbort   = store.PhaseAbort
)

// recordingTxn copies every Set and Delete while the real transaction still
// runs, so the batch matches what this commit would have written.
type recordingTxn struct {
	*badger.Txn
	ops []kvOp
}

func (t *recordingTxn) Set(key, val []byte) error {
	t.ops = append(t.ops, kvOp{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), val...),
	})
	return t.Txn.Set(key, val)
}

func (t *recordingTxn) Delete(key []byte) error {
	t.ops = append(t.ops, kvOp{
		Key:    append([]byte(nil), key...),
		Delete: true,
	})
	return t.Txn.Delete(key)
}

// noteRow tags the key/value op just recorded as the table row. The caller
// records that row op immediately before this.
func (tx *kvTx) noteRow(ch rowChange) {
	rec, ok := tx.txn.(*recordingTxn)
	if !ok || rec == nil || len(rec.ops) == 0 {
		return
	}
	op := &rec.ops[len(rec.ops)-1]
	op.Row = true
	op.Database = ch.Database
	op.Table = ch.Table
	op.Op = ch.Op
	if !opSchemaNoted(rec.ops[:len(rec.ops)-1], ch.Database, ch.Table) {
		op.Schema = append([]byte(nil), ch.Schema...)
	}
	if len(ch.Before) > 0 {
		op.Before = append([]byte(nil), ch.Before...)
	}
}

// opSchemaNoted reports that this batch already stored the schema for the table.
// Later row ops leave Schema empty; the binlog builder reuses the first copy.
func opSchemaNoted(ops []kvOp, database, table string) bool {
	for _, op := range ops {
		if op.Row && op.Database == database && op.Table == table && len(op.Schema) > 0 {
			return true
		}
	}
	return false
}

// batchRowChanges returns the row images for a log entry. Older entries
// stored them in Rows. Newer entries tag the row ops and leave Rows empty.
func batchRowChanges(batch replBatch) []rowChange {
	if len(batch.Rows) > 0 {
		return batch.Rows
	}
	var rows []rowChange
	for _, op := range batch.Ops {
		if !op.Row {
			continue
		}
		rows = append(rows, rowChange{
			Database: op.Database,
			Table:    op.Table,
			Schema:   op.Schema,
			Op:       op.Op,
			Before:   op.Before,
			After:    op.Value,
		})
	}
	return rows
}

// commit runs fn as the single writer. Without a cluster it commits directly.
// With a cluster the leader records the writes, rolls them back, and queues
// the batch. The lock is not held while Raft waits for a quorum, so the next
// statement can record against the in-flight batches.
func (s *Store) commit(statement string, fn func(tx *kvTx) error) error {
	return s.commitGTID(statement, "", fn)
}

// commitGTID is commit, and it stores gtid in the same batch when gtid is set.
// A transaction that only asks to rotate the binlog is replicated too.
func (s *Store) commitGTID(statement, gtid string, fn func(tx *kvTx) error) error {
	if s.group == nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		var rotate bool
		err := s.badgerDB().Update(func(txn *badger.Txn) error {
			tx := &kvTx{txn: txn}
			if err := fn(tx); err != nil {
				return err
			}
			rotate = tx.rotate
			return putSourceGTID(tx, gtid)
		})
		if err != nil {
			return err
		}
		if rotate {
			return s.rotateBinlog()
		}
		return nil
	}
	return s.commitMarked(statement, gtid, marked{}, fn)
}

func (s *Store) commitMarked(statement, gtid string, mode marked, fn func(tx *kvTx) error) error {
	return s.group.CommitMarked(statement, gtid, mode, func(snap []kvOp) (replBatch, bool, error) {
		rec := &recordingTxn{}
		var rotate bool
		err := s.badgerDB().Update(func(txn *badger.Txn) error {
			if err := replayOps(txn, snap); err != nil {
				return err
			}
			rec.Txn = txn
			tx := &kvTx{txn: rec}
			if err := fn(tx); err != nil {
				return err
			}
			if err := putSourceGTID(tx, gtid); err != nil {
				return err
			}
			rotate = tx.rotate
			if len(rec.ops) == 0 && !rotate && mode.Phase == phaseApply {
				return nil
			}
			return errReplicate
		})
		if err != nil && !errors.Is(err, errReplicate) {
			return replBatch{}, false, err
		}
		if !errors.Is(err, errReplicate) {
			return replBatch{}, false, nil
		}
		return replBatch{
			Ops:       rec.ops,
			Statement: statement,
			Unix:      uint32(time.Now().Unix()),
			Rotate:    rotate,
			Phase:     mode.Phase,
			PrepareID: mode.PrepareID,
			CommitNo:  mode.CommitNo,
		}, true, nil
	})
}

func putSourceGTID(tx *kvTx, gtid string) error {
	if gtid == "" {
		return nil
	}
	return tx.root().Put(keySourceGTID, []byte(gtid))
}

// replayOps installs in-flight writes into txn so fn sees them. The sets go
// to the Badger transaction directly and are not recorded as new operations.
// The transaction still rolls back; Raft apply is what lands them.
func replayOps(txn *badger.Txn, ops []kvOp) error {
	for _, op := range ops {
		if op.Delete {
			if err := txn.Delete(op.Key); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			continue
		}
		if err := txn.Set(op.Key, op.Value); err != nil {
			return err
		}
	}
	return nil
}

func encodeBatch(batch replBatch) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(batch); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeBatch(raw []byte) (replBatch, error) {
	var batch replBatch
	err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&batch)
	return batch, err
}

// applyOpsAt writes a committed batch into the local Badger and records index
// in that same transaction. Puts and deletes are applied in order and are
// safe to repeat for the same Raft index.
func (s *Store) applyOpsAt(index uint64, ops []kvOp) error {
	return s.badgerDB().Update(func(txn *badger.Txn) error {
		for _, op := range ops {
			if op.Delete {
				if err := txn.Delete(op.Key); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				continue
			}
			if err := txn.Set(op.Key, op.Value); err != nil {
				return err
			}
		}
		if index == 0 {
			return nil
		}
		var raw [8]byte
		binary.BigEndian.PutUint64(raw[:], index)
		return txn.Set(entryKey(nil, keyRaftApplied, kindValue), raw[:])
	})
}
