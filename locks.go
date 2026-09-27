package persist

import (
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

type lockMode int

const (
	lockShared lockMode = iota
	lockExclusive
)

// heldLock is one grant. A session keeps these until commit, rollback, or disconnect.
type heldLock struct {
	ref   tableRef
	key   string
	table bool
	mode  lockMode
}

type lockWaiter struct {
	sess *Session
	mode lockMode
	wake chan struct{}
}

type rowSlot struct {
	shared    map[*Session]int
	exclusive *Session
	exCount   int
	waiters   []*lockWaiter
}

type tableSlot struct {
	readers map[*Session]int
	writer  *Session
	wrCount int
	waiters []*lockWaiter
}

// lockTable is the process-wide row and table lock set for one store.
type lockTable struct {
	mu      sync.Mutex
	rows    map[rowLockKey]*rowSlot
	tables  map[tableRef]*tableSlot
	waiting map[*Session]*Session
}

type rowLockKey struct {
	ref tableRef
	key string
}

func newLockTable() *lockTable {
	return &lockTable{
		rows:    make(map[rowLockKey]*rowSlot),
		tables:  make(map[tableRef]*tableSlot),
		waiting: make(map[*Session]*Session),
	}
}

func (s *Store) locksFor() *lockTable {
	s.lockOnce.Do(func() {
		s.rowLock = newLockTable()
	})
	return s.rowLock
}

// acquire blocks until key is granted, the wait times out, or a deadlock is found.
// skip reports that SKIP LOCKED should omit the row instead of waiting.
// ok is false when the row was skipped.
func (lt *lockTable) acquire(sess *Session, ref tableRef, key []byte, mode lockMode, nowait, skip bool, timeout time.Duration) (bool, error) {
	return lt.acquireKey(sess, rowLockKey{ref: ref, key: string(key)}, mode, nowait, skip, timeout)
}

// acquireTable locks the whole table. write is LOCK TABLES WRITE.
func (lt *lockTable) acquireTable(sess *Session, ref tableRef, write, nowait bool, timeout time.Duration) error {
	mode := lockShared
	if write {
		mode = lockExclusive
	}
	ok, err := lt.acquireKey(sess, rowLockKey{ref: ref, key: ""}, mode, nowait, false, timeout)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrLockNowait.New()
	}
	return nil
}

func (lt *lockTable) acquireKey(sess *Session, key rowLockKey, mode lockMode, nowait, skip bool, timeout time.Duration) (bool, error) {
	if timeout <= 0 {
		timeout = 50 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	lt.mu.Lock()
	for {
		other := lt.blocker(sess, key, mode)
		if other == nil {
			lt.grant(sess, key, mode)
			delete(lt.waiting, sess)
			lt.mu.Unlock()
			return true, nil
		}
		if skip {
			delete(lt.waiting, sess)
			lt.mu.Unlock()
			return false, nil
		}
		if nowait {
			delete(lt.waiting, sess)
			lt.mu.Unlock()
			return false, sql.ErrLockNowait.New()
		}
		if lt.cycle(sess, other) {
			delete(lt.waiting, sess)
			lt.mu.Unlock()
			return false, sql.ErrLockDeadlock.New("row lock")
		}
		lt.waiting[sess] = other
		w := &lockWaiter{sess: sess, mode: mode, wake: make(chan struct{})}
		lt.enqueue(key, w)
		lt.mu.Unlock()
		var timedOut bool
		select {
		case <-w.wake:
		case <-timer.C:
			timedOut = true
		}
		lt.mu.Lock()
		lt.dequeue(key, w)
		if timedOut {
			delete(lt.waiting, sess)
			lt.mu.Unlock()
			return false, sql.ErrLockWaitTimeout.New()
		}
	}
}

func (lt *lockTable) blocker(sess *Session, key rowLockKey, mode lockMode) *Session {
	if other := lt.tableLockBlocker(sess, key.ref, mode); other != nil {
		return other
	}
	if key.key == "" {
		if mode != lockExclusive {
			return nil
		}
		// LOCK TABLES WRITE waits for row locks held by other sessions.
		for rowKey, row := range lt.rows {
			if rowKey.ref != key.ref {
				continue
			}
			if row.exclusive != nil && row.exclusive != sess {
				return row.exclusive
			}
			for other := range row.shared {
				if other != sess {
					return other
				}
			}
		}
		return nil
	}
	slot := lt.rows[key]
	if slot == nil {
		return nil
	}
	if slot.exclusive != nil && slot.exclusive != sess {
		return slot.exclusive
	}
	if mode == lockExclusive {
		for other := range slot.shared {
			if other != sess {
				return other
			}
		}
	}
	return nil
}

// tableLockBlocker reports a session that holds a table lock conflicting with mode.
func (lt *lockTable) tableLockBlocker(sess *Session, ref tableRef, mode lockMode) *Session {
	slot := lt.tables[ref]
	if slot == nil {
		return nil
	}
	if slot.writer != nil && slot.writer != sess {
		return slot.writer
	}
	if mode == lockExclusive {
		for other := range slot.readers {
			if other != sess {
				return other
			}
		}
	}
	return nil
}

func (lt *lockTable) grant(sess *Session, key rowLockKey, mode lockMode) {
	if key.key == "" {
		slot := lt.tables[key.ref]
		if slot == nil {
			slot = &tableSlot{readers: make(map[*Session]int)}
			lt.tables[key.ref] = slot
		}
		if mode == lockExclusive {
			slot.writer = sess
			slot.wrCount++
			return
		}
		slot.readers[sess]++
		return
	}
	slot := lt.rows[key]
	if slot == nil {
		slot = &rowSlot{shared: make(map[*Session]int)}
		lt.rows[key] = slot
	}
	if mode == lockExclusive {
		slot.exclusive = sess
		slot.exCount++
		return
	}
	slot.shared[sess]++
}

func (lt *lockTable) enqueue(key rowLockKey, w *lockWaiter) {
	if key.key == "" {
		slot := lt.tables[key.ref]
		if slot == nil {
			slot = &tableSlot{readers: make(map[*Session]int)}
			lt.tables[key.ref] = slot
		}
		slot.waiters = append(slot.waiters, w)
		return
	}
	slot := lt.rows[key]
	if slot == nil {
		slot = &rowSlot{shared: make(map[*Session]int)}
		lt.rows[key] = slot
	}
	slot.waiters = append(slot.waiters, w)
}

func (lt *lockTable) dequeue(key rowLockKey, w *lockWaiter) {
	if key.key == "" {
		slot := lt.tables[key.ref]
		if slot == nil {
			return
		}
		slot.waiters = dropWaiter(slot.waiters, w)
		return
	}
	slot := lt.rows[key]
	if slot == nil {
		return
	}
	slot.waiters = dropWaiter(slot.waiters, w)
}

func dropWaiter(waiters []*lockWaiter, w *lockWaiter) []*lockWaiter {
	for i, cur := range waiters {
		if cur == w {
			return append(waiters[:i], waiters[i+1:]...)
		}
	}
	return waiters
}

func (lt *lockTable) cycle(sess, other *Session) bool {
	seen := map[*Session]bool{}
	for other != nil {
		if other == sess {
			return true
		}
		if seen[other] {
			return false
		}
		seen[other] = true
		other = lt.waiting[other]
	}
	return false
}

func (lt *lockTable) release(sess *Session, held []heldLock) {
	if len(held) == 0 {
		return
	}
	lt.mu.Lock()
	defer lt.mu.Unlock()
	delete(lt.waiting, sess)
	wake := map[rowLockKey]struct{}{}
	tables := map[tableRef]struct{}{}
	for _, h := range held {
		if h.table {
			lt.releaseTable(sess, h)
			tables[h.ref] = struct{}{}
			continue
		}
		key := rowLockKey{ref: h.ref, key: h.key}
		lt.releaseRow(sess, key, h.mode)
		wake[key] = struct{}{}
		tables[h.ref] = struct{}{}
	}
	for key := range wake {
		if slot := lt.rows[key]; slot != nil {
			lt.wake(slot.waiters)
			slot.waiters = nil
			if slot.exclusive == nil && len(slot.shared) == 0 {
				delete(lt.rows, key)
			}
		}
	}
	for ref := range tables {
		if slot := lt.tables[ref]; slot != nil {
			lt.wake(slot.waiters)
			slot.waiters = nil
			if slot.writer == nil && len(slot.readers) == 0 {
				delete(lt.tables, ref)
			}
		}
		for key, slot := range lt.rows {
			if key.ref != ref || slot == nil || len(slot.waiters) == 0 {
				continue
			}
			lt.wake(slot.waiters)
			slot.waiters = nil
		}
	}
}

func (lt *lockTable) releaseRow(sess *Session, key rowLockKey, mode lockMode) {
	slot := lt.rows[key]
	if slot == nil {
		return
	}
	if mode == lockExclusive {
		if slot.exclusive == sess {
			slot.exCount--
			if slot.exCount <= 0 {
				slot.exclusive = nil
				slot.exCount = 0
			}
		}
		return
	}
	if n := slot.shared[sess]; n > 1 {
		slot.shared[sess] = n - 1
		return
	}
	delete(slot.shared, sess)
}

func (lt *lockTable) releaseTable(sess *Session, h heldLock) {
	slot := lt.tables[h.ref]
	if slot == nil {
		return
	}
	if h.mode == lockExclusive {
		if slot.writer == sess {
			slot.wrCount--
			if slot.wrCount <= 0 {
				slot.writer = nil
				slot.wrCount = 0
			}
		}
		return
	}
	if n := slot.readers[sess]; n > 1 {
		slot.readers[sess] = n - 1
		return
	}
	delete(slot.readers, sess)
}

func (lt *lockTable) wake(waiters []*lockWaiter) {
	for _, w := range waiters {
		select {
		case <-w.wake:
		default:
			close(w.wake)
		}
	}
}

// awaitWaiting blocks until sess is recorded as waiting for a lock.
// Tests use it to order a deadlock without a sleep.
func (lt *lockTable) awaitWaiting(sess *Session) {
	for {
		lt.mu.Lock()
		_, ok := lt.waiting[sess]
		lt.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
}
