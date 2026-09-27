package persist

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

var (
	bucketDatabases  = []byte("databases")
	bucketTables     = []byte("tables")
	bucketRows       = []byte("rows")
	keyName          = []byte("name")
	keySchema        = []byte("schema")
	keyComment       = []byte("comment")
	keyCollation     = []byte("collation")
	keyViews         = []byte("views")
	keyIndexes       = []byte("indexes")
	keyForeignKeys   = []byte("foreignKeys")
	keyTriggers      = []byte("triggers")
	keyProcedures    = []byte("procedures")
	keyEvents        = []byte("events")
	keyAutoInc       = []byte("autoinc")
	keyChecks        = []byte("checks")
	keyTargetRows    = []byte("targetRowSize")
	keyFormat        = []byte("format")
	keyRowCount      = []byte("rowCount")
	keyDataBytes     = []byte("dataBytes")
	bucketIndex      = []byte("index")
	keySourceGTID    = []byte("sourceGtid")
	keyRaftApplied   = []byte("raftApplied")
	keyRetrievedGTID = []byte("retrievedGtid")
	keyReplicaSource = []byte("replicaSource")
)

// formatCurrent is sortable keys plus binary rows. A missing key is the
// previous format: decimal key parts wrapped as bucket entries, and JSON rows.
// formatCurrent is collation-ordered string keys. format 1 stored raw bytes.
const formatCurrent uint16 = 2

// Store is a go-mysql-server database provider backed by one Badger directory.
type Store struct {
	db         *badger.DB
	path       string
	mu         sync.Mutex
	dbMu       sync.RWMutex
	syncWrites bool
	cluster    *cluster
	bin        *binlog
	raftDir    string
	// groupID is the range group this directory belongs to. The primary
	// group uses the shared server UUID. A split group sets its own.
	groupID  string
	repl     *replicaState
	replOnce sync.Once
	// privMu guards privDB and privSkip. Persist sets privSkip before it waits
	// for Raft, so the apply path can see that this process already updated
	// the in-memory accounts and must not take the MySQLDb editor lock.
	privMu   sync.Mutex
	privDB   *mysql_db.MySQLDb
	privSkip int
	lockOnce sync.Once
	rowLock  *lockTable
	gcStop   chan struct{}
	gcDone   chan struct{}
	// fsmApplied is the newest Raft index the FSM has finished. It lags
	// Raft's AppliedIndex, which moves when a batch is queued.
	fsmApplied uint64
	// autoMu guards autoRanges and autoEpoch. A refill holds it across the
	// commit so two sessions cannot reserve the same span.
	autoMu     sync.Mutex
	autoRanges map[tableRef]autoRange
	autoEpoch  uint64
}

var _ sql.DatabaseProvider = (*Store)(nil)
var _ sql.MutableDatabaseProvider = (*Store)(nil)

// OpenOptions controls how a store is opened.
type OpenOptions struct {
	// BulkLoad turns fsync off for the life of the store. Normal commits keep
	// SyncWrites on, matching InnoDB with flush at commit.
	BulkLoad bool
	// NoSync turns fsync off. OpenCluster sets it: the Raft log is the commit
	// record. Badger is fsynced on snapshot and shutdown.
	NoSync bool
}

// Open opens or creates the Badger directory at path.
// GMS_DATA is the usual way to choose the path; the server default is data/gms.
// Commits fsync. Use OpenWithOptions for a bulk load.
func Open(path string) (*Store, error) {
	return OpenWithOptions(path, OpenOptions{})
}

// OpenWithOptions opens or creates the Badger directory at path.
func OpenWithOptions(path string, opts OpenOptions) (*Store, error) {
	if err := recoverRestoreDirs(path); err != nil {
		return nil, err
	}
	syncWrites := !opts.BulkLoad && !opts.NoSync
	db, err := openBadger(path, syncWrites)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, path: path, syncWrites: syncWrites}
	s.startValueLogGC()
	return s, nil
}

// badgerDB returns the open database. A snapshot restore swaps it under dbMu.
func (s *Store) badgerDB() *badger.DB {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db
}

// Close releases the file lock. A cluster node leaves the Raft group first.
func (s *Store) Close() error {
	s.stopValueLogGC()
	s.stopReplica()
	if s.cluster != nil {
		if err := s.cluster.shutdown(); err != nil {
			return err
		}
	}
	if s.bin != nil {
		s.bin.close()
	}
	if err := s.syncData(); err != nil {
		return err
	}
	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	return s.db.Close()
}

// syncData fsyncs Badger when commits themselves do not. A single-node store
// already fsyncs each commit.
func (s *Store) syncData() error {
	if s.syncWrites {
		return nil
	}
	db := s.badgerDB()
	if db == nil {
		return nil
	}
	return db.Sync()
}

// FSMApplied is the newest Raft index written into Badger. Raft's AppliedIndex
// moves earlier, when a batch is only queued, so readers use this one.
func (s *Store) FSMApplied() uint64 {
	return atomic.LoadUint64(&s.fsmApplied)
}

// noteFSMApplied records that the FSM finished applying index.
func (s *Store) noteFSMApplied(index uint64) {
	for {
		cur := atomic.LoadUint64(&s.fsmApplied)
		if index <= cur || atomic.CompareAndSwapUint64(&s.fsmApplied, cur, index) {
			return
		}
	}
}

// readRaftApplied reads the applied index stored with the last Badger commit.
func (s *Store) readRaftApplied() uint64 {
	db := s.badgerDB()
	if db == nil {
		return 0
	}
	var idx uint64
	_ = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(entryKey(nil, keyRaftApplied, kindValue))
		if err != nil {
			return nil
		}
		return item.Value(func(val []byte) error {
			if len(val) == 8 {
				idx = binary.BigEndian.Uint64(val)
			}
			return nil
		})
	})
	return idx
}

// Path returns the Badger directory path.
func (s *Store) Path() string {
	return s.path
}

// Database implements sql.DatabaseProvider.
func (s *Store) Database(ctx *sql.Context, name string) (sql.Database, error) {
	db, ok, err := s.loadDatabase(name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(name)
	}
	return db, nil
}

// HasDatabase implements sql.DatabaseProvider.
func (s *Store) HasDatabase(ctx *sql.Context, name string) bool {
	_, ok, err := s.loadDatabase(name)
	return err == nil && ok
}

// AllDatabases implements sql.DatabaseProvider.
func (s *Store) AllDatabases(ctx *sql.Context) []sql.Database {
	var names []string
	_ = s.view(func(tx *kvTx) error {
		root := tx.Bucket(bucketDatabases)
		if root == nil {
			return nil
		}
		return root.ForEach(func(k, v []byte) error {
			if v != nil {
				return nil
			}
			bucket := root.Bucket(k)
			if bucket == nil {
				return nil
			}
			names = append(names, string(bucket.Get(keyName)))
			return nil
		})
	})
	sort.Strings(names)
	dbs := make([]sql.Database, len(names))
	for i, name := range names {
		dbs[i] = &Database{store: s, name: name}
	}
	return dbs
}

// CreateDatabase implements sql.MutableDatabaseProvider.
func (s *Store) CreateDatabase(ctx *sql.Context, name string) error {
	if name == "" {
		return fmt.Errorf("persist: database name is empty")
	}
	return s.updateQuery(ctx, func(tx *kvTx) error {
		root, err := tx.CreateBucketIfNotExists(bucketDatabases)
		if err != nil {
			return err
		}
		key := bucketKey(name)
		if root.Bucket(key) != nil {
			return sql.ErrDatabaseExists.New(name)
		}
		bucket, err := root.CreateBucket(key)
		if err != nil {
			return err
		}
		if err := bucket.Put(keyName, []byte(name)); err != nil {
			return err
		}
		var coll [2]byte
		binary.BigEndian.PutUint16(coll[:], uint16(sql.Collation_Default))
		if err := bucket.Put(keyCollation, coll[:]); err != nil {
			return err
		}
		_, err = bucket.CreateBucket(bucketTables)
		return err
	})
}

// DropDatabase implements sql.MutableDatabaseProvider.
func (s *Store) DropDatabase(ctx *sql.Context, name string) error {
	err := s.updateQuery(ctx, func(tx *kvTx) error {
		root := tx.Bucket(bucketDatabases)
		if root == nil || root.Bucket(bucketKey(name)) == nil {
			return sql.ErrDatabaseNotFound.New(name)
		}
		return root.DeleteBucket(bucketKey(name))
	})
	if err != nil {
		return err
	}
	s.forgetAutoDatabase(name)
	return nil
}

func (s *Store) loadDatabase(name string) (*Database, bool, error) {
	var stored string
	var found bool
	err := s.view(func(tx *kvTx) error {
		bucket := databaseBucket(tx, name)
		if bucket == nil {
			return nil
		}
		found = true
		stored = string(bucket.Get(keyName))
		return nil
	})
	if err != nil || !found {
		return nil, false, err
	}
	return &Database{store: s, name: stored}, true, nil
}

// Database is one persisted MySQL database.
type Database struct {
	store *Store
	name  string
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)

// Name implements sql.Nameable.
func (d *Database) Name() string { return d.name }

// GetTableInsensitive implements sql.Database.
func (d *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	meta, name, ok, err := d.store.loadTable(d.name, tblName)
	if err != nil || !ok {
		return nil, false, err
	}
	return &Table{store: d.store, dbName: d.name, name: name, meta: meta}, true, nil
}

// GetTableNames implements sql.Database.
func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	var names []string
	err := d.store.view(func(tx *kvTx) error {
		tables := tablesBucket(tx, d.name)
		if tables == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		return tables.ForEach(func(k, v []byte) error {
			if v != nil {
				return nil
			}
			bucket := tables.Bucket(k)
			if bucket == nil {
				return nil
			}
			names = append(names, string(bucket.Get(keyName)))
			return nil
		})
	})
	sort.Strings(names)
	return names, err
}

// CreateTable implements sql.TableCreator.
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	if name == "" {
		return fmt.Errorf("persist: table name is empty")
	}
	if collation == sql.Collation_Unspecified {
		collation = sql.Collation_Default
	}
	raw, err := encodeSchema(ctx, schema, collation, comment)
	if err != nil {
		return err
	}
	return d.store.updateQuery(ctx, func(tx *kvTx) error {
		tables := tablesBucket(tx, d.name)
		if tables == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		if tables.Bucket(bucketKey(name)) != nil {
			return sql.ErrTableAlreadyExists.New(name)
		}
		bucket, err := tables.CreateBucket(bucketKey(name))
		if err != nil {
			return err
		}
		if err := bucket.Put(keyName, []byte(name)); err != nil {
			return err
		}
		if err := bucket.Put(keySchema, raw); err != nil {
			return err
		}
		if comment != "" {
			if err := bucket.Put(keyComment, []byte(comment)); err != nil {
				return err
			}
		}
		var coll [2]byte
		binary.BigEndian.PutUint16(coll[:], uint16(collation))
		if err := bucket.Put(keyCollation, coll[:]); err != nil {
			return err
		}
		if err := putFormat(bucket); err != nil {
			return err
		}
		if err := putUint64(bucket, keyRowCount, 0); err != nil {
			return err
		}
		if err := putUint64(bucket, keyDataBytes, 0); err != nil {
			return err
		}
		_, err = bucket.CreateBucket(bucketRows)
		return err
	})
}

// RenameTable implements sql.TableRenamer.
func (d *Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	if newName == "" {
		return fmt.Errorf("persist: table name is empty")
	}
	err := d.store.update(func(tx *kvTx) error {
		tables := tablesBucket(tx, d.name)
		if tables == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		src := tables.Bucket(bucketKey(oldName))
		if src == nil {
			return sql.ErrTableNotFound.New(oldName)
		}
		if strings.EqualFold(oldName, newName) {
			return src.Put(keyName, []byte(newName))
		}
		if tables.Bucket(bucketKey(newName)) != nil {
			return sql.ErrTableAlreadyExists.New(newName)
		}
		dst, err := tables.CreateBucket(bucketKey(newName))
		if err != nil {
			return err
		}
		if err := copyBucket(src, dst); err != nil {
			return err
		}
		if err := dst.SetSequence(src.Sequence()); err != nil {
			return err
		}
		if err := dst.Put(keyName, []byte(newName)); err != nil {
			return err
		}
		return tables.DeleteBucket(bucketKey(oldName))
	})
	if err != nil {
		return err
	}
	d.store.forgetAutoIncrement(tableRef{db: strings.ToLower(d.name), name: strings.ToLower(oldName)})
	if sess, ok := sessionFrom(ctx); ok {
		sess.rename(tableRef{db: strings.ToLower(d.name), name: strings.ToLower(oldName)}, tableRef{db: strings.ToLower(d.name), name: strings.ToLower(newName)})
	}
	return nil
}

func copyBucket(src, dst *kvBucket) error {
	if err := src.forEachRaw(func(k, v []byte) error {
		return dst.PutRaw(k, v)
	}); err != nil {
		return err
	}
	return src.ForEach(func(k, v []byte) error {
		key := append([]byte(nil), k...)
		if v != nil {
			return dst.Put(key, append([]byte(nil), v...))
		}
		nested := src.Bucket(k)
		child, err := dst.CreateBucket(key)
		if err != nil {
			return err
		}
		if err := child.SetSequence(nested.Sequence()); err != nil {
			return err
		}
		return copyBucket(nested, child)
	})
}

// DropTable implements sql.TableDropper.
func (d *Database) DropTable(ctx *sql.Context, name string) error {
	err := d.store.updateQuery(ctx, func(tx *kvTx) error {
		tables := tablesBucket(tx, d.name)
		if tables == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		if tables.Bucket(bucketKey(name)) == nil {
			return sql.ErrTableNotFound.New(name)
		}
		return tables.DeleteBucket(bucketKey(name))
	})
	if err != nil {
		return err
	}
	d.store.forgetAutoIncrement(tableRef{db: strings.ToLower(d.name), name: strings.ToLower(name)})
	return nil
}

func (s *Store) loadTable(dbName, tableName string) (tableMeta, string, bool, error) {
	meta, name, format, ok, err := s.readTable(dbName, tableName)
	if err != nil || !ok {
		return tableMeta{}, "", false, err
	}
	if format < formatCurrent {
		if err := s.migrateTable(dbName, tableName); err != nil {
			return tableMeta{}, "", false, err
		}
		meta, name, _, ok, err = s.readTable(dbName, tableName)
	}
	return meta, name, ok, err
}

func (s *Store) readTable(dbName, tableName string) (tableMeta, string, uint16, bool, error) {
	var raw []byte
	var name string
	var targetRowSize uint64
	var format uint16
	err := s.view(func(tx *kvTx) error {
		tables := tablesBucket(tx, dbName)
		if tables == nil {
			return sql.ErrDatabaseNotFound.New(dbName)
		}
		bucket := tables.Bucket(bucketKey(tableName))
		if bucket == nil {
			return nil
		}
		raw = append([]byte(nil), bucket.Get(keySchema)...)
		name = string(bucket.Get(keyName))
		format = tableFormat(bucket)
		if size := bucket.Get(keyTargetRows); len(size) == 8 {
			targetRowSize = binary.BigEndian.Uint64(size)
		}
		return nil
	})
	if err != nil || raw == nil {
		return tableMeta{}, "", 0, false, err
	}
	meta, err := decodeSchema(raw, dbName, name)
	if err != nil {
		return tableMeta{}, "", 0, false, err
	}
	meta.targetRowSize = targetRowSize
	return meta, name, format, true, nil
}

type storedRow struct {
	key []byte
	row sql.Row
	// raw is the on-disk image. It is nil when the row comes from the session buffer.
	raw []byte
}

func (s *Store) getRow(ctx context.Context, t *Table, key []byte) (sql.Row, bool, error) {
	row, _, ok, err := s.getRowImage(ctx, t, key, wantsCurrentRead(ctx))
	return row, ok, err
}

func (s *Store) getRowImage(ctx context.Context, t *Table, key []byte, current bool) (sql.Row, []byte, bool, error) {
	var raw []byte
	err := s.rowView(ctx, current, func(tx *kvTx) error {
		rows := rowsBucket(tx, t.dbName, t.name)
		if rows == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		raw = append([]byte(nil), rows.GetRaw(key)...)
		return nil
	})
	if err != nil || len(raw) == 0 {
		return nil, nil, false, err
	}
	row, err := decodeRow(ctx, t.meta.schema, raw)
	if err != nil {
		return nil, nil, false, err
	}
	return row, raw, true, nil
}

func (s *Store) indexGet(ctx context.Context, t *Table, indexName string, key []byte) ([]byte, error) {
	var val []byte
	err := s.rowView(ctx, wantsCurrentRead(ctx), func(tx *kvTx) error {
		data := indexData(tx, t.dbName, t.name, indexName)
		if data == nil {
			return nil
		}
		val = append([]byte(nil), data.GetRaw(key)...)
		return nil
	})
	if err != nil || len(val) == 0 {
		return nil, err
	}
	return val, nil
}

func (s *Store) apply(t *Table, edits []edit, statement, gtid string) error {
	return s.applyAll(map[tableRef][]edit{t.ref(): edits}, nil, statement, gtid)
}

func (s *Store) applyAll(pending map[tableRef][]edit, reads []rowImage, statement, gtid string) error {
	if len(pending) == 0 && len(reads) == 0 {
		if gtid == "" {
			return nil
		}
		return s.commitGTID("", gtid, func(tx *kvTx) error { return nil })
	}
	return s.commitGTID(statement, gtid, func(tx *kvTx) error {
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

func checkReads(tx *kvTx, reads []rowImage) error {
	for _, rd := range reads {
		rows := rowsBucket(tx, rd.ref.db, rd.ref.name)
		var current []byte
		if rows != nil {
			current = rows.GetRaw(rd.key)
		}
		if !bytesEqual(current, rd.raw) {
			return sql.ErrLockDeadlock.New("row changed")
		}
	}
	return nil
}

func changedImage(rows *kvBucket, key, expected []byte) error {
	if expected == nil {
		return nil
	}
	if bytesEqual(rows.GetRaw(key), expected) {
		return nil
	}
	return sql.ErrLockDeadlock.New("row changed")
}

func applyEdits(tx *kvTx, ref tableRef, edits []edit) error {
	bucket := tableBucket(tx, ref.db, ref.name)
	if bucket == nil {
		return sql.ErrTableNotFound.New(ref.name)
	}
	rows := bucket.Bucket(bucketRows)
	if rows == nil {
		return sql.ErrTableNotFound.New(ref.name)
	}
	schemaRaw := append([]byte(nil), bucket.Get(keySchema)...)
	meta, err := decodeSchema(schemaRaw, ref.db, ref.name)
	if err != nil {
		return err
	}
	edits, err = resolveProvisional(rows, edits)
	if err != nil {
		return err
	}
	indexes, err := indexesIn(bucket)
	if err != nil {
		return err
	}
	count, nbytes, err := loadCounts(bucket, rows)
	if err != nil {
		return err
	}
	ctx := context.Background()
	for _, ed := range edits {
		switch ed.op {
		case opDelete:
			if err := changedImage(rows, ed.key, ed.expected); err != nil {
				return err
			}
			old := rows.GetRaw(ed.key)
			if len(old) == 0 {
				continue
			}
			oldRow, err := decodeRow(ctx, meta.schema, old)
			if err != nil {
				return err
			}
			if err := deleteIndexEntries(ctx, bucket, meta.schema, indexes, oldRow, ed.key); err != nil {
				return err
			}
			if err := rows.DeleteRaw(ed.key); err != nil {
				return err
			}
			tx.noteRow(rowChange{
				Database: ref.db,
				Table:    ref.name,
				Schema:   schemaRaw,
				Op:       int(opDelete),
				Before:   old,
			})
			count--
			nbytes -= uint64(len(old))
		case opInsert:
			if rows.GetRaw(ed.key) != nil {
				return sql.ErrPrimaryKeyViolation.New()
			}
			if err := checkUniqueWrite(ctx, bucket, meta.schema, indexes, ed.row, ed.key, nil); err != nil {
				return err
			}
			if err := rows.PutRaw(ed.key, ed.raw); err != nil {
				return err
			}
			tx.noteRow(rowChange{
				Database: ref.db,
				Table:    ref.name,
				Schema:   schemaRaw,
				Op:       int(opInsert),
				After:    ed.raw,
			})
			if err := putIndexEntries(ctx, bucket, meta.schema, indexes, ed.row, ed.key); err != nil {
				return err
			}
			count++
			nbytes += uint64(len(ed.raw))
		case opUpdate:
			if err := changedImage(rows, ed.oldKey, ed.expected); err != nil {
				return err
			}
			old := rows.GetRaw(ed.oldKey)
			var oldRow sql.Row
			if len(old) > 0 {
				oldRow, err = decodeRow(ctx, meta.schema, old)
				if err != nil {
					return err
				}
				if err := deleteIndexEntries(ctx, bucket, meta.schema, indexes, oldRow, ed.oldKey); err != nil {
					return err
				}
				if !bytesEqual(ed.oldKey, ed.key) {
					if err := rows.DeleteRaw(ed.oldKey); err != nil {
						return err
					}
				}
				count--
				nbytes -= uint64(len(old))
			}
			if !bytesEqual(ed.oldKey, ed.key) && rows.GetRaw(ed.key) != nil {
				return sql.ErrPrimaryKeyViolation.New()
			}
			if err := checkUniqueWrite(ctx, bucket, meta.schema, indexes, ed.row, ed.key, ed.oldKey); err != nil {
				return err
			}
			if err := rows.PutRaw(ed.key, ed.raw); err != nil {
				return err
			}
			tx.noteRow(rowChange{
				Database: ref.db,
				Table:    ref.name,
				Schema:   schemaRaw,
				Op:       int(opUpdate),
				Before:   old,
				After:    ed.raw,
			})
			if err := putIndexEntries(ctx, bucket, meta.schema, indexes, ed.row, ed.key); err != nil {
				return err
			}
			count++
			nbytes += uint64(len(ed.raw))
		default:
			return fmt.Errorf("persist: unknown edit %d", ed.op)
		}
	}
	if err := putUint64(bucket, keyRowCount, count); err != nil {
		return err
	}
	return putUint64(bucket, keyDataBytes, nbytes)
}

func (s *Store) truncate(t *Table) (int, error) {
	var n int
	err := s.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		rows := bucket.Bucket(bucketRows)
		if rows == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		count, _, err := loadCounts(bucket, rows)
		if err != nil {
			return err
		}
		n = int(count)
		if err := clearIndexData(bucket); err != nil {
			return err
		}
		if err := bucket.DeleteBucket(bucketRows); err != nil {
			return err
		}
		if _, err := bucket.CreateBucket(bucketRows); err != nil {
			return err
		}
		if err := putUint64(bucket, keyRowCount, 0); err != nil {
			return err
		}
		return putUint64(bucket, keyDataBytes, 0)
	})
	return n, err
}

func (s *Store) migrateTable(dbName, tableName string) error {
	return s.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, dbName, tableName)
		if bucket == nil {
			return sql.ErrTableNotFound.New(tableName)
		}
		if tableFormat(bucket) >= formatCurrent {
			return nil
		}
		schemaRaw := bucket.Get(keySchema)
		name := string(bucket.Get(keyName))
		meta, err := decodeSchema(schemaRaw, dbName, name)
		if err != nil {
			return err
		}
		rows := bucket.Bucket(bucketRows)
		ctx := context.Background()
		stored, err := readStoredRows(ctx, rows, meta.schema)
		if err != nil {
			return err
		}
		var maxSeq uint64
		if rows != nil && rows.Sequence() > maxSeq {
			maxSeq = rows.Sequence()
		}
		rewritten := make([]storedRow, 0, len(stored))
		for _, row := range stored {
			key := row.key
			if len(meta.pk) == 0 {
				if len(key) == 8 {
					seq := binary.BigEndian.Uint64(key)
					if seq > maxSeq {
						maxSeq = seq
					}
				}
			} else {
				key, err = primaryKey(ctx, meta.schema, meta.pk, row.row)
				if err != nil {
					return err
				}
			}
			rewritten = append(rewritten, storedRow{key: key, row: row.row})
		}
		// Re-encode while writing so the byte length matches the stored value.
		if err := clearIndexData(bucket); err != nil {
			return err
		}
		if rows != nil {
			if err := bucket.DeleteBucket(bucketRows); err != nil {
				return err
			}
		}
		fresh, err := bucket.CreateBucket(bucketRows)
		if err != nil {
			return err
		}
		if err := fresh.SetSequence(maxSeq); err != nil {
			return err
		}
		indexes, err := indexesIn(bucket)
		if err != nil {
			return err
		}
		var nbytes uint64
		for _, row := range rewritten {
			raw, err := encodeRow(ctx, meta.schema, row.row)
			if err != nil {
				return err
			}
			if err := fresh.PutRaw(row.key, raw); err != nil {
				return err
			}
			if err := putIndexEntries(ctx, bucket, meta.schema, indexes, row.row, row.key); err != nil {
				return err
			}
			nbytes += uint64(len(raw))
		}
		if err := putUint64(bucket, keyRowCount, uint64(len(rewritten))); err != nil {
			return err
		}
		if err := putUint64(bucket, keyDataBytes, nbytes); err != nil {
			return err
		}
		return putFormat(bucket)
	})
}

func bucketKey(name string) []byte {
	return []byte(strings.ToLower(name))
}

func databaseBucket(tx *kvTx, name string) *kvBucket {
	root := tx.Bucket(bucketDatabases)
	if root == nil {
		return nil
	}
	return root.Bucket(bucketKey(name))
}

func tablesBucket(tx *kvTx, dbName string) *kvBucket {
	db := databaseBucket(tx, dbName)
	if db == nil {
		return nil
	}
	return db.Bucket(bucketTables)
}

func tableBucket(tx *kvTx, dbName, tableName string) *kvBucket {
	tables := tablesBucket(tx, dbName)
	if tables == nil {
		return nil
	}
	return tables.Bucket(bucketKey(tableName))
}

func rowsBucket(tx *kvTx, dbName, tableName string) *kvBucket {
	table := tableBucket(tx, dbName, tableName)
	if table == nil {
		return nil
	}
	return table.Bucket(bucketRows)
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
