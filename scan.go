package persist

import (
	"bytes"
	"context"
	"io"
	"sort"

	"github.com/dgraph-io/badger/v4"

	"github.com/dolthub/go-mysql-server/sql"
)

// keySpan is a half-open range of memcomparable keys: start inclusive, end exclusive.
type keySpan struct {
	start []byte
	end   []byte
}

func keyInSpan(key []byte, span keySpan) bool {
	if span.start != nil && bytes.Compare(key, span.start) < 0 {
		return false
	}
	if span.end != nil && bytes.Compare(key, span.end) >= 0 {
		return false
	}
	return true
}

type overlayEntry struct {
	key  []byte
	row  sql.Row
	tomb bool
}

// buildOverlay folds edits into the newest row for each key. A tombstone
// suppresses a committed row that this session deleted or moved.
func buildOverlay(edits []edit) []overlayEntry {
	index := make(map[string]int)
	var entries []overlayEntry
	set := func(key []byte, row sql.Row, tomb bool) {
		if i, ok := index[string(key)]; ok {
			entries[i].row = row
			entries[i].tomb = tomb
			return
		}
		index[string(key)] = len(entries)
		entries = append(entries, overlayEntry{key: append([]byte(nil), key...), row: row, tomb: tomb})
	}
	for _, ed := range edits {
		switch ed.op {
		case opDelete:
			set(ed.key, nil, true)
		case opUpdate:
			if len(ed.oldKey) > 0 && !bytesEqual(ed.oldKey, ed.key) {
				set(ed.oldKey, nil, true)
			}
			set(ed.key, ed.row, false)
		case opInsert:
			set(ed.key, ed.row, false)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	return entries
}

// mergeIter walks a rows bucket in key order and applies pending edits as it goes.
type mergeIter struct {
	ctx      context.Context
	schema   sql.Schema
	iter     *rawIter
	discard  func()
	overlay  []overlayEntry
	oi       int
	span     keySpan
	reverse  bool
	ref      tableRef
	diskKey  []byte
	diskRow  sql.Row
	diskRaw  []byte
	diskOn   bool
	diskDone bool
	closed   bool
	err      error
}

func (it *mergeIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		row, err := it.nextStored(ctx)
		if err != nil {
			return nil, err
		}
		if sess, ok := sessionFrom(ctx); ok {
			skip, err := sess.observeRow(ctx, it.ref, row.key, row.raw)
			if err != nil {
				return nil, err
			}
			if skip {
				continue
			}
		}
		return row.row, nil
	}
}

func (it *mergeIter) nextStored(ctx context.Context) (storedRow, error) {
	if it.err != nil {
		return storedRow{}, it.err
	}
	if ctx == nil {
		ctx = it.ctx
	}
	for {
		if err := it.pullDisk(ctx); err != nil {
			it.err = err
			return storedRow{}, err
		}
		it.skipOverlay()
		over := it.oi >= 0 && it.oi < len(it.overlay)
		if !it.diskOn && !over {
			return storedRow{}, io.EOF
		}
		if over && (!it.diskOn || it.overlayFirst()) {
			entry := it.overlay[it.oi]
			it.stepOverlay()
			if it.diskOn && bytesEqual(it.diskKey, entry.key) {
				it.diskOn = false
			}
			if entry.tomb {
				continue
			}
			return storedRow{key: entry.key, row: entry.row}, nil
		}
		row := storedRow{key: it.diskKey, row: it.diskRow, raw: it.diskRaw}
		it.diskOn = false
		return row, nil
	}
}

func (it *mergeIter) overlayFirst() bool {
	cmp := bytes.Compare(it.overlay[it.oi].key, it.diskKey)
	if it.reverse {
		return cmp >= 0
	}
	return cmp <= 0
}

func (it *mergeIter) stepOverlay() {
	if it.reverse {
		it.oi--
		return
	}
	it.oi++
}

func (it *mergeIter) skipOverlay() {
	for it.oi >= 0 && it.oi < len(it.overlay) && !keyInSpan(it.overlay[it.oi].key, it.span) {
		if it.reverse {
			if it.span.start != nil && bytes.Compare(it.overlay[it.oi].key, it.span.start) < 0 {
				it.oi = -1
				return
			}
			it.oi--
			continue
		}
		if it.span.end != nil && bytes.Compare(it.overlay[it.oi].key, it.span.end) >= 0 {
			it.oi = len(it.overlay)
			return
		}
		it.oi++
	}
}

func (it *mergeIter) pullDisk(ctx context.Context) error {
	if it.diskOn || it.diskDone {
		return nil
	}
	for it.iter.Valid() {
		key := it.iter.Key()
		if it.reverse {
			if it.span.end != nil && bytes.Compare(key, it.span.end) >= 0 {
				it.iter.Next()
				continue
			}
			if it.span.start != nil && bytes.Compare(key, it.span.start) < 0 {
				it.diskDone = true
				return nil
			}
		} else {
			if it.span.end != nil && bytes.Compare(key, it.span.end) >= 0 {
				it.diskDone = true
				return nil
			}
			if it.span.start != nil && bytes.Compare(key, it.span.start) < 0 {
				it.iter.Next()
				continue
			}
		}
		val, err := it.iter.Value()
		if err != nil {
			return err
		}
		row, err := decodeRow(ctx, it.schema, val)
		if err != nil {
			return err
		}
		it.diskKey = key
		it.diskRow = row
		it.diskRaw = val
		it.diskOn = true
		it.iter.Next()
		return nil
	}
	it.diskDone = true
	return nil
}

func (it *mergeIter) Close(*sql.Context) error {
	if it.closed {
		return nil
	}
	it.closed = true
	if it.iter != nil {
		it.iter.Close()
		if sess := sessionOf(it.ctx); sess != nil {
			sess.noteIteratorClosed(it.iter.it)
		}
	}
	if it.discard != nil {
		it.discard()
	}
	return nil
}

func (s *Store) mergeRows(ctx context.Context, t *Table, edits []edit, span keySpan, reverse bool) (*mergeIter, error) {
	tx, discard := s.readFor(ctx, wantsCurrentRead(ctx))
	rows := rowsBucket(tx, t.dbName, t.name)
	if rows == nil {
		discard()
		return nil, sql.ErrTableNotFound.New(t.name)
	}
	iter := rows.rawIter(reverse)
	if sess := sessionOf(ctx); sess != nil && sess.snapshotTxn() == tx.txn {
		if txn, ok := tx.txn.(*badger.Txn); ok {
			sess.trackIterator(iter.it, txn)
		}
	}
	if reverse {
		if span.end != nil {
			iter.Seek(span.end)
		} else {
			iter.seekEnd()
		}
	} else if span.start != nil {
		iter.Seek(span.start)
	} else {
		iter.Rewind()
	}
	overlay := buildOverlay(edits)
	oi := 0
	if reverse {
		oi = len(overlay) - 1
	}
	return &mergeIter{
		ctx:     ctx,
		schema:  t.meta.schema,
		iter:    iter,
		discard: discard,
		overlay: overlay,
		oi:      oi,
		span:    span,
		reverse: reverse,
		ref:     t.ref(),
	}, nil
}

func (s *Store) readTx() (*kvTx, func()) {
	txn := s.badgerDB().NewTransaction(false)
	return &kvTx{txn: txn}, func() { txn.Discard() }
}

// readFor returns the session snapshot for a consistent read. current, or a
// session with no snapshot, opens a short-lived transaction.
func (s *Store) readFor(ctx context.Context, current bool) (*kvTx, func()) {
	if !current {
		if txn := snapshotTxnFrom(ctx); txn != nil {
			return &kvTx{txn: txn}, func() {}
		}
	}
	return s.readTx()
}
