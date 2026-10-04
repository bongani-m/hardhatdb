package sqle

import (
	"errors"
	"time"

	"github.com/bongani-m/hardhatdb/go/store"
	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

// errReplicate aborts the Badger transaction that computed a commit. The
// recorded key/value operations are proposed to Raft and applied only after
// a quorum persists them.
var errReplicate = errors.New("hardhatdb: replicate batch")

type kvOp = store.KVOp
type rowChange = store.RowChange
type replBatch = store.ReplBatch
type marked = store.Marked

const (
	phaseApply   = store.PhaseApply
	phasePrepare = store.PhasePrepare
	phaseCommit  = store.PhaseCommit
	phaseAbort   = store.PhaseAbort
	phaseStage   = store.PhaseStage
)

// recordingTxn copies every Set and Delete while the real transaction still
// runs, so the batch matches what this commit would have written.
type recordingTxn struct {
	*badger.Txn
	ops []kvOp
	// idx is the latest op for each key this statement wrote. Those keys win
	// over the in-flight overlay. Keys already sealed into an earlier chunk
	// are removed so iterators read them from the overlay.
	idx map[string]int
	// before runs before a write is recorded. It spills a full chunk.
	before func(kvOp) error
}

func (t *recordingTxn) note(op kvOp) {
	if t.idx == nil {
		t.idx = make(map[string]int)
	}
	t.idx[string(op.Key)] = len(t.ops)
	t.ops = append(t.ops, op)
}

func (t *recordingTxn) lookup(key []byte) (kvOp, bool) {
	if t == nil || t.idx == nil {
		return kvOp{}, false
	}
	i, ok := t.idx[string(key)]
	if !ok {
		return kvOp{}, false
	}
	return t.ops[i], true
}

func (t *recordingTxn) Set(key, val []byte) error {
	op := kvOp{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), val...),
	}
	if t.before != nil {
		if err := t.before(op); err != nil {
			return err
		}
	}
	t.note(op)
	if t.Txn == nil {
		return nil
	}
	return t.Txn.Set(key, val)
}

func (t *recordingTxn) Delete(key []byte) error {
	op := kvOp{
		Key:    append([]byte(nil), key...),
		Delete: true,
	}
	if t.before != nil {
		if err := t.before(op); err != nil {
			return err
		}
	}
	t.note(op)
	if t.Txn == nil {
		return nil
	}
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
// With a cluster the leader records the writes against the in-flight overlay,
// rolls the local transaction back, and queues the batch. The lock is not held
// while Raft waits for a quorum, so the next statement can record against the
// batches still in flight.
func (s *Store) commit(statement string, fn func(tx *kvTx) error) error {
	return s.commitGTID(statement, "", fn)
}

// commitGTID is commit, and it stores gtid in the same batch when gtid is set.
// A transaction that only asks to rotate the binlog is replicated too.
func (s *Store) commitGTID(statement, gtid string, fn func(tx *kvTx) error) error {
	if s.group == nil {
		return s.commitLocal(statement, gtid, fn)
	}
	return s.commitMarked(statement, gtid, marked{}, fn)
}

func (s *Store) commitLocal(statement, gtid string, fn func(tx *kvTx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ops, rotate, err := s.capture(nil, gtid, fn, nil)
	if err != nil {
		return err
	}
	if len(ops) == 0 && !rotate {
		return nil
	}
	if s.oneEntry(ops, "") {
		err = s.badgerDB().Update(func(txn *badger.Txn) error {
			return applyOpsTxn(txn, ops)
		})
		if err != nil {
			return err
		}
		if rotate {
			return s.rotateBinlog()
		}
		return nil
	}
	id := uuid.NewString()
	parts := store.ChunkOps(ops, store.EntryLimit, id)
	for i, part := range parts {
		if err := s.saveStage(0, replBatch{
			Ops:       part,
			Unix:      uint32(time.Now().Unix()),
			Phase:     phaseStage,
			PrepareID: id,
			Chunk:     uint64(i),
		}); err != nil {
			_ = s.dropStaged(0, id)
			return err
		}
	}
	_, err = s.publishStaged(0, replBatch{
		Statement:  statement,
		Unix:       uint32(time.Now().Unix()),
		Rotate:     rotate,
		Phase:      phaseCommit,
		PrepareID:  id,
		ChunkCount: uint64(len(parts)),
	})
	if err != nil {
		return err
	}
	if rotate {
		return s.rotateBinlog()
	}
	return nil
}

func (s *Store) commitMarked(statement, gtid string, mode marked, fn func(tx *kvTx) error) error {
	return s.group.CommitEmitted(func(snap []kvOp, emit func(replBatch) error) error {
		return s.emitRecorded(statement, gtid, mode, snap, emit, fn)
	})
}

// emitRecorded records fn and proposes either one entry or staged chunks
// plus a marker. spill hands each sealed chunk to emit so Raft can replicate
// it before the rest of the statement is recorded.
func (s *Store) emitRecorded(statement, gtid string, mode marked, snap []kvOp, emit func(replBatch) error, fn func(tx *kvTx) error) error {
	id := mode.PrepareID
	var seq uint64
	ops, rotate, err := s.capture(snap, gtid, fn, func(part []kvOp) error {
		if id == "" {
			id = uuid.NewString()
		}
		err := emit(replBatch{
			Ops:       part,
			Unix:      uint32(time.Now().Unix()),
			Phase:     phaseStage,
			PrepareID: id,
			Chunk:     seq,
		})
		seq++
		return err
	})
	if err != nil {
		return err
	}
	if seq == 0 && len(ops) == 0 && !rotate && mode.Phase == phaseApply {
		return nil
	}
	unix := uint32(time.Now().Unix())
	if seq == 0 {
		return emit(replBatch{
			Ops:       ops,
			Statement: statement,
			Unix:      unix,
			Rotate:    rotate,
			Phase:     mode.Phase,
			PrepareID: mode.PrepareID,
			CommitNo:  mode.CommitNo,
		})
	}
	if len(ops) > 0 {
		if id == "" {
			id = uuid.NewString()
		}
		if err := emit(replBatch{
			Ops:       ops,
			Unix:      unix,
			Phase:     phaseStage,
			PrepareID: id,
			Chunk:     seq,
		}); err != nil {
			return err
		}
		seq++
	}
	phase := phaseCommit
	if mode.Phase == phasePrepare {
		phase = phasePrepare
	}
	text := statement
	if store.StagedLen(nil, id)+len(text) > entryLimit() {
		text = ""
	}
	return emit(replBatch{
		Statement:  text,
		Unix:       unix,
		Rotate:     rotate,
		Phase:      phase,
		PrepareID:  id,
		CommitNo:   mode.CommitNo,
		ChunkCount: seq,
	})
}

// capture runs fn against discarded Badger transactions. When the open
// transaction would pass the Raft or Badger budget, those ops are sealed into
// the overlay and, if spill is set, handed off. The returned ops are the
// unsealed tail when spill is set, and every op when it is not.
func (s *Store) capture(snap []kvOp, gtid string, fn func(tx *kvTx) error, spill func([]kvOp) error) ([]kvOp, bool, error) {
	overlay := newKVOverlay(snap)
	if overlay == nil {
		overlay = &kvOverlay{}
	}
	rec := &recordingTxn{idx: map[string]int{}}
	var held bool
	release := func() {
		if rec.Txn != nil {
			rec.Txn.Discard()
			rec.Txn = nil
		}
		if held {
			s.publish.RUnlock()
			held = false
		}
	}
	defer release()
	open := func() error {
		if rec.Txn != nil {
			return nil
		}
		s.publish.RLock()
		held = true
		rec.Txn = s.badgerDB().NewTransaction(true)
		return nil
	}
	if err := open(); err != nil {
		return nil, false, err
	}
	txnOps := 0
	txnWire := 0
	limit := entryLimit()
	rec.before = func(op kvOp) error {
		add := store.OpWire(op)
		if txnOps > 0 && (stagedBytes(txnOps+1, txnWire+add) > limit || s.txnFull(txnOps, txnWire)) {
			part := append([]kvOp(nil), rec.ops[len(rec.ops)-txnOps:]...)
			sealOps(overlay, rec, part)
			release()
			if spill != nil {
				if err := spill(part); err != nil {
					return err
				}
			}
			if err := open(); err != nil {
				return err
			}
			txnOps = 0
			txnWire = 0
		}
		txnWire += add
		txnOps++
		return nil
	}
	tx := &kvTx{txn: rec, overlay: overlay}
	if err := fn(tx); err != nil {
		return nil, false, err
	}
	if err := putSourceGTID(tx, gtid); err != nil {
		return nil, false, err
	}
	rotate := tx.rotate
	var tail []kvOp
	if spill == nil {
		tail = append([]kvOp(nil), rec.ops...)
	} else if txnOps > 0 {
		tail = append([]kvOp(nil), rec.ops[len(rec.ops)-txnOps:]...)
	}
	release()
	return tail, rotate, nil
}

func sealOps(overlay *kvOverlay, rec *recordingTxn, part []kvOp) {
	if overlay.byKey == nil {
		overlay.byKey = make(map[string]kvOp, len(part))
	}
	for _, op := range part {
		overlay.byKey[string(op.Key)] = op
		delete(rec.idx, string(op.Key))
	}
}

func stagedBytes(n, wire int) int {
	return store.StagedLen(nil, "stage") - 1 + uvarintLen(uint64(n)) + wire
}

func (s *Store) txnFull(ops, wire int) bool {
	db := s.badgerDB()
	if db == nil {
		return false
	}
	if int64(ops+1) >= db.MaxBatchCount() {
		return true
	}
	return int64(wire) >= db.MaxBatchSize()*8/10
}

func uvarintLen(n uint64) int {
	l := 1
	for n >= 0x80 {
		n >>= 7
		l++
	}
	return l
}

func entryLimit() int {
	if store.EntryLimit > 0 {
		return store.EntryLimit
	}
	return 512 * 1024
}

func (s *Store) oneEntry(ops []kvOp, id string) bool {
	if store.StagedLen(ops, id) > entryLimit() {
		return false
	}
	db := s.badgerDB()
	if db == nil {
		return true
	}
	if int64(len(ops)) >= db.MaxBatchCount() {
		return false
	}
	var size int64
	for _, op := range ops {
		size += int64(len(op.Key) + len(op.Value) + len(op.Before) + len(op.Schema) + 32)
		if size >= db.MaxBatchSize() {
			return false
		}
	}
	return true
}

func putSourceGTID(tx *kvTx, gtid string) error {
	if gtid == "" {
		return nil
	}
	return tx.root().Put(keySourceGTID, []byte(gtid))
}

func encodeBatch(batch replBatch) ([]byte, error) {
	return store.EncodeBatch(batch)
}

func decodeBatch(raw []byte) (replBatch, error) {
	return store.DecodeBatch(raw)
}

func applyOpsTxn(txn *badger.Txn, ops []kvOp) error {
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

// applyOpsAt writes a committed batch into the local Badger and records index
// in that same transaction. Puts and deletes are applied in order and are
// safe to repeat for the same Raft index.
func (s *Store) applyOpsAt(index uint64, ops []kvOp) error {
	return s.applyOpsRun([]uint64{index}, [][]kvOp{ops})
}

// applyOpsRun writes a contiguous run of committed batches in one transaction.
// The stored Raft index is the last entry in the run.
func (s *Store) applyOpsRun(indexes []uint64, groups [][]kvOp) error {
	if len(indexes) == 0 {
		return nil
	}
	last := indexes[len(indexes)-1]
	return s.badgerDB().Update(func(txn *badger.Txn) error {
		for _, ops := range groups {
			if err := applyOpsTxn(txn, ops); err != nil {
				return err
			}
		}
		return putApplied(txn, last)
	})
}
