package persist

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// ErrTwoPhaseCrash is returned when a commit stops between prepare and the
// moment every range has applied the decision. Recovery then commits every
// prepared range or aborts every one of them.
var ErrTwoPhaseCrash = errors.New("persist: two-phase commit interrupted")

var (
	keyCommitSeq = []byte("commitSeq")
	keyDecisions = []byte("txnDecisions")
)

// TxnDecision is the meta group's record of a cross-range transaction.
// Commit is zero when the decision is to abort.
type TxnDecision struct {
	ID     string `json:"id"`
	Commit uint64 `json:"commit"`
}

func preparedStorageKey(id string) []byte {
	return append(append([]byte("prepared\x00"), id...), 0)
}

// PrepareEdits records pending as a prepared Raft batch. The keys stay hidden
// until CommitPrepared. id is the transaction id shared by every range.
func (s *Store) PrepareEdits(id string, pending map[tableRef][]edit, reads []rowImage) error {
	if id == "" {
		return fmt.Errorf("persist: prepare id is empty")
	}
	if s.cluster == nil {
		return fmt.Errorf("persist: prepare requires raft")
	}
	return s.cluster.commitMarked("", "", marked{phase: phasePrepare, prepareID: id}, func(tx *kvTx) error {
		if err := checkReads(tx, reads); err != nil {
			return err
		}
		for ref, edits := range pending {
			if err := applyEdits(tx, ref, edits); err != nil {
				return err
			}
		}
		return nil
	})
}

// CommitPrepared makes a prepared batch visible and drops the prepare record.
// A missing record has already been committed or aborted.
func (s *Store) CommitPrepared(id string, commitNo uint64) error {
	if id == "" {
		return fmt.Errorf("persist: prepare id is empty")
	}
	if s.cluster == nil {
		return fmt.Errorf("persist: prepare requires raft")
	}
	return s.cluster.commitMarked("", "", marked{phase: phaseCommit, prepareID: id, commitNo: commitNo}, func(tx *kvTx) error {
		return nil
	})
}

// AbortPrepared drops a prepared batch without applying it.
func (s *Store) AbortPrepared(id string) error {
	if id == "" {
		return fmt.Errorf("persist: prepare id is empty")
	}
	if s.cluster == nil {
		return s.dropPrepared(0, id)
	}
	return s.cluster.commitMarked("", "", marked{phase: phaseAbort, prepareID: id}, func(tx *kvTx) error {
		return nil
	})
}

// PreparedIDs lists transactions this store has prepared and not finished.
func (s *Store) PreparedIDs() ([]string, error) {
	prefix := []byte("prepared\x00")
	var ids []string
	err := s.badgerDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			id := string(key[len(prefix) : len(key)-1])
			ids = append(ids, id)
		}
		return nil
	})
	return ids, err
}

// NextCommit allocates a monotonic commit number. Meta is the usual caller.
func (s *Store) NextCommit() (uint64, error) {
	var n uint64
	err := s.update(func(tx *kvTx) error {
		raw := tx.root().Get(keyCommitSeq)
		if len(raw) == 8 {
			n = binary.BigEndian.Uint64(raw)
		}
		n++
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], n)
		return tx.root().Put(keyCommitSeq, buf[:])
	})
	return n, err
}

// SaveDecision replicates the outcome of one cross-range transaction.
func (s *Store) SaveDecision(d TxnDecision) error {
	if d.ID == "" {
		return fmt.Errorf("persist: decision id is empty")
	}
	return s.update(func(tx *kvTx) error {
		list, err := readDecisions(tx)
		if err != nil {
			return err
		}
		replaced := false
		for i := range list {
			if list[i].ID == d.ID {
				list[i] = d
				replaced = true
				break
			}
		}
		if !replaced {
			list = append(list, d)
		}
		raw, err := json.Marshal(list)
		if err != nil {
			return err
		}
		return tx.root().Put(keyDecisions, raw)
	})
}

// Decision reads one transaction outcome. ok is false when meta has no record.
func (s *Store) Decision(id string) (TxnDecision, bool, error) {
	var list []TxnDecision
	err := s.view(func(tx *kvTx) error {
		var err error
		list, err = readDecisions(tx)
		return err
	})
	if err != nil {
		return TxnDecision{}, false, err
	}
	for _, d := range list {
		if d.ID == id {
			return d, true, nil
		}
	}
	return TxnDecision{}, false, nil
}

func readDecisions(tx *kvTx) ([]TxnDecision, error) {
	raw := tx.root().Get(keyDecisions)
	if len(raw) == 0 {
		return nil, nil
	}
	var list []TxnDecision
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	return list, nil
}

// FinishTwoPhase writes the meta decision and commits every store.
// crash "prepare" stops before the decision. crash "decision" stops after
// the decision is durable and before any range applies it.
func FinishTwoPhase(meta *Store, id string, stores []*Store, crash string) error {
	if crash == "prepare" {
		return ErrTwoPhaseCrash
	}
	n, err := meta.NextCommit()
	if err != nil {
		return err
	}
	if err := meta.SaveDecision(TxnDecision{ID: id, Commit: n}); err != nil {
		return err
	}
	if crash == "decision" {
		return ErrTwoPhaseCrash
	}
	for _, st := range stores {
		if err := st.CommitPrepared(id, n); err != nil {
			return err
		}
	}
	return nil
}

// AbortTwoPhase records an abort and drops every prepared batch.
func AbortTwoPhase(meta *Store, id string, stores []*Store) error {
	if err := meta.SaveDecision(TxnDecision{ID: id, Commit: 0}); err != nil {
		return err
	}
	for _, st := range stores {
		if err := st.AbortPrepared(id); err != nil {
			return err
		}
	}
	return nil
}

// RecoverTwoPhase finishes prepared transactions from the meta decision.
// A missing decision aborts every copy. A commit number commits every copy
// that still has the prepare record.
func RecoverTwoPhase(meta *Store, stores []*Store) error {
	seen := map[string]struct{}{}
	for _, st := range stores {
		ids, err := st.PreparedIDs()
		if err != nil {
			return err
		}
		for _, id := range ids {
			seen[id] = struct{}{}
		}
	}
	for id := range seen {
		d, ok, err := meta.Decision(id)
		if err != nil {
			return err
		}
		for _, st := range stores {
			if !ok || d.Commit == 0 {
				if err := st.AbortPrepared(id); err != nil {
					return err
				}
				continue
			}
			if err := st.CommitPrepared(id, d.Commit); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) savePrepared(index uint64, batch replBatch) error {
	raw, err := encodeBatch(batch)
	if err != nil {
		return err
	}
	return s.badgerDB().Update(func(txn *badger.Txn) error {
		if err := txn.Set(preparedStorageKey(batch.PrepareID), raw); err != nil {
			return err
		}
		return putApplied(txn, index)
	})
}

func (s *Store) dropPrepared(index uint64, id string) error {
	return s.badgerDB().Update(func(txn *badger.Txn) error {
		if err := txn.Delete(preparedStorageKey(id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return putApplied(txn, index)
	})
}

// takePrepared applies a prepared batch and removes it. A missing record
// has already been finished; the Raft index is still recorded.
func (s *Store) takePrepared(index uint64, id string) (replBatch, error) {
	var stored replBatch
	err := s.badgerDB().Update(func(txn *badger.Txn) error {
		item, err := txn.Get(preparedStorageKey(id))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return putApplied(txn, index)
		}
		if err != nil {
			return err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		stored, err = decodeBatch(raw)
		if err != nil {
			return err
		}
		for _, op := range stored.Ops {
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
		if err := txn.Delete(preparedStorageKey(id)); err != nil {
			return err
		}
		return putApplied(txn, index)
	})
	return stored, err
}

func putApplied(txn *badger.Txn, index uint64) error {
	if index == 0 {
		return nil
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], index)
	return txn.Set(entryKey(nil, keyRaftApplied, kindValue), raw[:])
}
