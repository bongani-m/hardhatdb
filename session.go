package persist

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
)

const (
	isoReadUncommitted = "READ-UNCOMMITTED"
	isoReadCommitted   = "READ-COMMITTED"
	isoRepeatableRead  = "REPEATABLE-READ"
	isoSerializable    = "SERIALIZABLE"
)

// rowImage is a primary key and the on-disk bytes a locking read observed.
type rowImage struct {
	ref tableRef
	key []byte
	raw []byte
}

// savepoint marks how far a transaction had progressed. Rollback truncates
// pending edits, open editors, and locking reads back to these lengths.
type savepoint struct {
	name    string
	pending map[tableRef]int
	editors map[*editor]int
	reads   int
}

// Session tracks an uncommitted transaction. Autocommit statements write in
// editor.Close. BEGIN keeps those edits here until COMMIT or ROLLBACK.
type Session struct {
	*sql.BaseSession
	store   *Store
	mu      sync.Mutex
	pending map[tableRef][]edit
	// open editors have not closed yet. A statement can write one table through
	// more than one editor (INSERT ... ON DUPLICATE KEY UPDATE), and each has
	// to see the others' buffered rows.
	open       map[tableRef][]*editor
	reads      []rowImage
	savepoints []savepoint
	locking    bool
	lockNowait bool
	lockSkip   bool
	// snap is the repeatable-read view. Read committed leaves it nil and each
	// read opens its own transaction.
	snap *badger.Txn
	iso  string
	inTx bool
	held []heldLock
	// iters are Badger iterators and the transaction they read. A commit
	// replaces snap, and the old transaction stays until its iterators close.
	iters []trackedIter
}

type trackedIter struct {
	it  *badger.Iterator
	txn *badger.Txn
}

var _ sql.Session = (*Session)(nil)
var _ sql.TransactionSession = (*Session)(nil)
var _ sql.LockingReadSession = (*Session)(nil)
var _ sql.LockingReadModeSession = (*Session)(nil)
var _ sql.LifecycleAwareSession = (*Session)(nil)

// NewSession returns a session that commits into store.
func NewSession(base *sql.BaseSession, store *Store) *Session {
	return &Session{
		BaseSession: base,
		store:       store,
		pending:     make(map[tableRef][]edit),
	}
}

// NewSessionBuilder builds sessions for server.NewServer.
func NewSessionBuilder(store *Store) func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
	return func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		host := ""
		user := ""
		mysqlUser, ok := conn.UserData.(sql.MysqlConnectionUser)
		if ok {
			host = mysqlUser.Host
			user = mysqlUser.User
		}
		client := sql.Client{Address: host, User: user, Capabilities: conn.Capabilities}
		base := sql.NewBaseSessionWithClientServer(addr, client, conn.ConnectionID)
		return NewSession(base, store), nil
	}
}

type transaction struct {
	readOnly bool
}

func (t *transaction) String() string {
	if t.readOnly {
		return "badger read-only transaction"
	}
	return "badger transaction"
}

func (t *transaction) IsReadOnly() bool { return t.readOnly }

// StartTransaction implements sql.TransactionSession. Repeatable read and
// serializable keep one Badger snapshot until commit or rollback.
func (s *Session) StartTransaction(ctx *sql.Context, characteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	s.releaseLocks()
	s.discardSnap()
	iso := sessionIsolation(ctx, s)
	readOnly := characteristic == sql.ReadOnly || sessionReadOnly(ctx, s)
	s.mu.Lock()
	s.iso = iso
	s.inTx = true
	s.mu.Unlock()
	if iso != isoReadCommitted && iso != isoReadUncommitted {
		s.mu.Lock()
		s.snap = s.store.badgerDB().NewTransaction(false)
		s.mu.Unlock()
	}
	return &transaction{readOnly: readOnly}, nil
}

// SetLockingRead implements sql.LockingReadSession. The next scan locks each
// on-disk row it yields.
func (s *Session) SetLockingRead(on bool) {
	s.mu.Lock()
	s.locking = on
	if !on {
		s.lockNowait = false
		s.lockSkip = false
	}
	s.mu.Unlock()
}

// SetLockingReadMode implements sql.LockingReadModeSession.
func (s *Session) SetLockingReadMode(nowait, skip bool) {
	s.mu.Lock()
	s.lockNowait = nowait
	s.lockSkip = skip
	s.mu.Unlock()
}

// CommandBegin implements sql.LifecycleAwareSession.
func (s *Session) CommandBegin() error { return nil }

// CommandEnd releases locks taken by an autocommit statement.
func (s *Session) CommandEnd() {
	s.mu.Lock()
	explicit := s.inTx
	s.mu.Unlock()
	if !explicit {
		s.releaseLocks()
	}
}

// SessionEnd releases locks and the snapshot when the connection closes.
func (s *Session) SessionEnd() {
	s.abort()
	s.unlockTables()
}

// CommitTransaction writes every edit buffered since BEGIN. A row image that
// no longer matches disk aborts the transaction and drops the buffered edits.
func (s *Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	s.mu.Lock()
	pending := s.pending
	reads := s.reads
	points := s.savepoints
	s.pending = make(map[tableRef][]edit)
	s.reads = nil
	s.savepoints = nil
	s.mu.Unlock()
	gtid := sourceGTID(ctx)
	if len(pending) == 0 && len(reads) == 0 && gtid == "" {
		s.finishTx()
		return nil
	}
	statement := ""
	if ctx != nil {
		statement = ctx.Query()
	}
	if err := s.store.applyAll(pending, reads, statement, gtid); err != nil {
		if sql.ErrLockDeadlock.Is(err) {
			s.abort()
			return err
		}
		s.mu.Lock()
		s.restore(pending)
		s.reads = append(reads, s.reads...)
		s.savepoints = points
		s.mu.Unlock()
		return err
	}
	s.finishTx()
	return nil
}

// PrepareTransaction records the buffered edits as a hidden Raft batch.
// Other sessions do not see them until CommitPrepared applies that batch.
func (s *Session) PrepareTransaction(ctx *sql.Context, id string) error {
	s.mu.Lock()
	pending := s.pending
	reads := s.reads
	s.pending = make(map[tableRef][]edit)
	s.reads = nil
	s.savepoints = nil
	s.mu.Unlock()
	if len(pending) == 0 && len(reads) == 0 {
		s.finishTx()
		return nil
	}
	if err := s.store.PrepareEdits(id, pending, reads); err != nil {
		s.mu.Lock()
		s.restore(pending)
		s.reads = append(reads, s.reads...)
		s.mu.Unlock()
		return err
	}
	s.finishTx()
	return nil
}

// AbortPrepared drops a prepared batch, if one exists, and the buffered edits.
func (s *Session) AbortPrepared(id string) error {
	if s.store.Replicating() {
		if err := s.store.AbortPrepared(id); err != nil {
			return err
		}
	}
	s.abort()
	return nil
}

// Rollback drops edits buffered since BEGIN. Writes that already committed
// (autocommit statements and TRUNCATE) stay on disk.
func (s *Session) Rollback(ctx *sql.Context, transaction sql.Transaction) error {
	s.mu.Lock()
	s.pending = make(map[tableRef][]edit)
	s.reads = nil
	s.savepoints = nil
	s.clearOpenEditors()
	s.mu.Unlock()
	s.finishTx()
	return nil
}

// CreateSavepoint records the buffered edits and open editors. Repeating a
// name replaces that mark and drops marks recorded after it.
func (s *Session) CreateSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i := s.savepointIndex(name); i >= 0 {
		s.savepoints = s.savepoints[:i]
	}
	s.savepoints = append(s.savepoints, s.capture(name))
	return nil
}

// RollbackToSavepoint truncates edits, open editors, and locking reads back
// to the named mark and drops marks recorded after it.
func (s *Session) RollbackToSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.savepointIndex(name)
	if i < 0 {
		return sql.ErrSavepointDoesNotExist.New(name)
	}
	sp := s.savepoints[i]
	s.savepoints = s.savepoints[:i+1]
	s.restorePending(sp.pending)
	s.restoreEditors(sp.editors)
	if sp.reads < len(s.reads) {
		s.reads = s.reads[:sp.reads]
	}
	return nil
}

// ReleaseSavepoint drops the named mark and every mark recorded after it.
// The edits stay.
func (s *Session) ReleaseSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.savepointIndex(name)
	if i < 0 {
		return sql.ErrSavepointDoesNotExist.New(name)
	}
	s.savepoints = s.savepoints[:i]
	return nil
}

func (s *Session) savepointIndex(name string) int {
	for i, sp := range s.savepoints {
		if sp.name == name {
			return i
		}
	}
	return -1
}

func (s *Session) capture(name string) savepoint {
	pending := make(map[tableRef]int, len(s.pending))
	for ref, edits := range s.pending {
		pending[ref] = len(edits)
	}
	editors := make(map[*editor]int)
	for _, eds := range s.open {
		for _, ed := range eds {
			editors[ed] = len(ed.edits)
		}
	}
	return savepoint{name: name, pending: pending, editors: editors, reads: len(s.reads)}
}

func (s *Session) restorePending(marks map[tableRef]int) {
	for ref, edits := range s.pending {
		n, ok := marks[ref]
		if !ok {
			delete(s.pending, ref)
			continue
		}
		if n < len(edits) {
			s.pending[ref] = edits[:n]
		}
	}
}

func (s *Session) restoreEditors(marks map[*editor]int) {
	for _, eds := range s.open {
		for _, ed := range eds {
			n, ok := marks[ed]
			if !ok {
				ed.edits = nil
				ed.mark = 0
				continue
			}
			if n < len(ed.edits) {
				ed.edits = ed.edits[:n]
			}
			if ed.mark > len(ed.edits) {
				ed.mark = len(ed.edits)
			}
		}
	}
}

func (s *Session) clearOpenEditors() {
	for _, eds := range s.open {
		for _, ed := range eds {
			ed.edits = nil
			ed.mark = 0
		}
	}
}

func (s *Session) noteLockedRead(ctx *sql.Context, ref tableRef, key, raw []byte) {
	if len(raw) == 0 || ctx == nil || !ctx.GetIgnoreAutoCommit() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.locking {
		return
	}
	s.reads = append(s.reads, rowImage{
		ref: ref,
		key: append([]byte(nil), key...),
		raw: append([]byte(nil), raw...),
	})
}

func (s *Session) add(ref tableRef, edits []edit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[ref] = append(s.pending[ref], edits...)
}

func (s *Session) edits(ref tableRef) []edit {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.pending[ref]
	out := make([]edit, len(src))
	copy(out, src)
	return out
}

func (s *Session) track(e *editor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open == nil {
		s.open = make(map[tableRef][]*editor)
	}
	ref := e.table.ref()
	s.open[ref] = append(s.open[ref], e)
}

func (s *Session) untrack(e *editor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref := e.table.ref()
	eds := s.open[ref]
	for i, ed := range eds {
		if ed != e {
			continue
		}
		s.open[ref] = append(eds[:i], eds[i+1:]...)
		return
	}
}

func (s *Session) openEditsExcept(ref tableRef, self *editor) []edit {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []edit
	for _, ed := range s.open[ref] {
		if ed == self {
			continue
		}
		out = append(out, ed.edits...)
	}
	return out
}

func (s *Session) openEdits(ref tableRef) []edit {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []edit
	for _, ed := range s.open[ref] {
		out = append(out, ed.edits...)
	}
	return out
}

// lookupOpen reports the newest view of key in every open editor except self.
func (s *Session) lookupOpen(self *editor, key []byte) (sql.Row, bool, bool) {
	s.mu.Lock()
	eds := append([]*editor(nil), s.open[self.table.ref()]...)
	s.mu.Unlock()
	var row sql.Row
	var ok, decided bool
	for _, ed := range eds {
		if ed == self {
			continue
		}
		if next, found, hit := lookupEdits(ed.edits, key); hit {
			row, ok, decided = next, found, true
		}
	}
	return row, ok, decided
}

func (s *Session) clear(ref tableRef) {
	s.mu.Lock()
	delete(s.pending, ref)
	s.mu.Unlock()
}

func (s *Session) rename(from, to tableRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	edits, ok := s.pending[from]
	if !ok {
		return
	}
	delete(s.pending, from)
	s.pending[to] = append(edits, s.pending[to]...)
}

// restore puts pending edits back in front of anything buffered after a failed commit.
func (s *Session) restore(pending map[tableRef][]edit) {
	for ref, edits := range pending {
		s.pending[ref] = append(edits, s.pending[ref]...)
	}
}

// finishTx drops the snapshot and every lock. The transaction is over.
func (s *Session) finishTx() {
	s.mu.Lock()
	s.inTx = false
	s.mu.Unlock()
	s.releaseLocks()
	s.discardSnap()
}

// abort rolls the in-memory transaction back. Deadlock uses it so waiters proceed.
func (s *Session) abort() {
	s.mu.Lock()
	s.pending = make(map[tableRef][]edit)
	s.reads = nil
	s.savepoints = nil
	s.clearOpenEditors()
	s.inTx = false
	s.mu.Unlock()
	s.releaseLocks()
	s.discardSnap()
}

func (s *Session) trackIterator(it *badger.Iterator, txn *badger.Txn) {
	if it == nil || txn == nil {
		return
	}
	s.mu.Lock()
	s.iters = append(s.iters, trackedIter{it: it, txn: txn})
	s.mu.Unlock()
}

// noteIteratorClosed drops a finished scan and discards a transaction that
// refreshSnapshot replaced, once nothing is still reading it.
func (s *Session) noteIteratorClosed(it *badger.Iterator) {
	if it == nil {
		return
	}
	s.mu.Lock()
	var txn *badger.Txn
	kept := s.iters[:0]
	for _, cur := range s.iters {
		if cur.it == it {
			txn = cur.txn
			continue
		}
		kept = append(kept, cur)
	}
	s.iters = kept
	for _, cur := range kept {
		if cur.txn == txn {
			txn = nil
			break
		}
	}
	current := s.snap
	s.mu.Unlock()
	if txn != nil && txn != current {
		txn.Discard()
	}
}

// refreshSnapshot reopens the read snapshot after this session commits.
// DDL and autocommit writes land in Badger immediately, and a snapshot taken
// at BEGIN would keep hiding those rows from the rest of the statement.
// An open scan keeps its transaction until the iterator closes.
func (s *Session) refreshSnapshot() {
	s.mu.Lock()
	if !s.inTx || s.snap == nil {
		s.mu.Unlock()
		return
	}
	old := s.snap
	s.snap = s.store.badgerDB().NewTransaction(false)
	used := false
	for _, cur := range s.iters {
		if cur.txn == old {
			used = true
			break
		}
	}
	s.mu.Unlock()
	if !used {
		old.Discard()
	}
}

func (s *Session) discardSnap() {
	s.mu.Lock()
	iters := s.iters
	s.iters = nil
	txn := s.snap
	s.snap = nil
	s.mu.Unlock()
	seen := map[*badger.Txn]struct{}{}
	if txn != nil {
		seen[txn] = struct{}{}
	}
	for _, cur := range iters {
		cur.it.Close()
		if cur.txn != nil {
			seen[cur.txn] = struct{}{}
		}
	}
	for old := range seen {
		old.Discard()
	}
}

func (s *Session) snapshotTxn() *badger.Txn {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.locking || s.iso == isoSerializable {
		return nil
	}
	return s.snap
}

// currentRead reports that this statement must see the latest commit.
func (s *Session) currentRead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.locking || s.iso == isoSerializable
}

// observeRow locks a row for FOR UPDATE or serializable and records its image.
// skip is true when SKIP LOCKED omits the row.
func (s *Session) observeRow(ctx *sql.Context, ref tableRef, key, raw []byte) (bool, error) {
	s.mu.Lock()
	locking := s.locking
	serial := s.iso == isoSerializable
	nowait := s.lockNowait
	skipLocked := s.lockSkip
	s.mu.Unlock()
	if !locking && !serial {
		return false, nil
	}
	if len(key) == 0 {
		return false, nil
	}
	mode := lockShared
	if locking {
		mode = lockExclusive
	}
	ok, err := s.store.locksFor().acquire(s, ref, key, mode, nowait, skipLocked, lockWait(ctx))
	if sql.ErrLockDeadlock.Is(err) {
		s.abort()
		return false, err
	}
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	s.noteHeld(ref, key, mode, false)
	if locking {
		s.noteLockedRead(ctx, ref, key, raw)
	}
	return false, nil
}

// lockExclusive waits for an exclusive row lock. Writers call it before they edit.
func (s *Session) lockExclusive(ctx *sql.Context, ref tableRef, key []byte) error {
	if len(key) == 0 {
		return nil
	}
	_, err := s.store.locksFor().acquire(s, ref, key, lockExclusive, false, false, lockWait(ctx))
	if sql.ErrLockDeadlock.Is(err) {
		s.abort()
		return err
	}
	if err != nil {
		return err
	}
	s.noteHeld(ref, key, lockExclusive, false)
	return nil
}

// lockTable waits for a LOCK TABLES lock.
func (s *Session) lockTable(ctx *sql.Context, ref tableRef, write bool) error {
	err := s.store.locksFor().acquireTable(s, ref, write, false, lockWait(ctx))
	if sql.ErrLockDeadlock.Is(err) {
		s.abort()
		return err
	}
	if err != nil {
		return err
	}
	mode := lockShared
	if write {
		mode = lockExclusive
	}
	s.noteHeld(ref, nil, mode, true)
	return nil
}

// unlockTables releases table locks held by this session and leaves row locks.
func (s *Session) unlockTables() {
	s.mu.Lock()
	var drop []heldLock
	var keep []heldLock
	for _, h := range s.held {
		if h.table {
			drop = append(drop, h)
			continue
		}
		keep = append(keep, h)
	}
	s.held = keep
	s.mu.Unlock()
	if s.store != nil {
		s.store.locksFor().release(s, drop)
	}
}

func (s *Session) noteHeld(ref tableRef, key []byte, mode lockMode, table bool) {
	s.mu.Lock()
	s.held = append(s.held, heldLock{ref: ref, key: string(key), mode: mode, table: table})
	s.mu.Unlock()
}

// releaseLocks drops row locks and keeps LOCK TABLES locks.
func (s *Session) releaseLocks() {
	s.releaseHeld(false)
}

// releaseAllLocks drops row locks and LOCK TABLES locks.
func (s *Session) releaseAllLocks() {
	s.releaseHeld(true)
}

func (s *Session) releaseHeld(includeTable bool) {
	s.mu.Lock()
	var drop, keep []heldLock
	for _, h := range s.held {
		if h.table && !includeTable {
			keep = append(keep, h)
			continue
		}
		drop = append(drop, h)
	}
	s.held = keep
	s.mu.Unlock()
	if s.store != nil && len(drop) > 0 {
		s.store.locksFor().release(s, drop)
	}
}

func sessionOf(ctx context.Context) *Session {
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		return nil
	}
	sess, ok := sessionFrom(sqlCtx)
	if !ok {
		return nil
	}
	return sess
}

func snapshotTxnFrom(ctx context.Context) *badger.Txn {
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		return nil
	}
	sess, ok := sessionFrom(sqlCtx)
	if !ok {
		return nil
	}
	return sess.snapshotTxn()
}

func wantsCurrentRead(ctx context.Context) bool {
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		return false
	}
	sess, ok := sessionFrom(sqlCtx)
	if !ok {
		return false
	}
	return sess.currentRead()
}

func sessionIsolation(ctx *sql.Context, s *Session) string {
	if ctx == nil || s == nil {
		return isoRepeatableRead
	}
	val, err := s.GetSessionVariable(ctx, "transaction_isolation")
	if err != nil {
		return isoRepeatableRead
	}
	text, ok := val.(string)
	if !ok || text == "" {
		return isoRepeatableRead
	}
	return strings.ToUpper(text)
}

func sessionReadOnly(ctx *sql.Context, s *Session) bool {
	if ctx == nil || s == nil {
		return false
	}
	for _, name := range []string{"transaction_read_only", "tx_read_only"} {
		val, err := s.GetSessionVariable(ctx, name)
		if err != nil {
			continue
		}
		if truthy(val) {
			return true
		}
	}
	return false
}

func truthy(val interface{}) bool {
	switch v := val.(type) {
	case bool:
		return v
	case int8:
		return v != 0
	case int64:
		return v != 0
	case int:
		return v != 0
	case uint64:
		return v != 0
	default:
		return false
	}
}

func lockWait(ctx *sql.Context) time.Duration {
	sec := int64(50)
	if ctx != nil && ctx.Session != nil {
		val, err := ctx.Session.GetSessionVariable(ctx, "innodb_lock_wait_timeout")
		if err == nil {
			switch n := val.(type) {
			case int64:
				sec = n
			case int32:
				sec = int64(n)
			case int:
				sec = int64(n)
			case int8:
				sec = int64(n)
			case uint64:
				sec = int64(n)
			}
		}
	}
	if sec < 1 {
		sec = 1
	}
	return time.Duration(sec) * time.Second
}
