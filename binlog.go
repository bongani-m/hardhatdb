package persist

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
)

var binlogMagic = []byte{0xfe, 0x62, 0x69, 0x6e}

// defaultBinlogMax is MySQL's default max_binlog_size.
const defaultBinlogMax = 1 << 30

// binlog is the MySQL row-binlog export of committed Raft batches. It is
// written on every node from the same log entry, so a new leader can stream
// it. It is not the consensus log. Files roll to binlog.NNNNNN.
type binlog struct {
	mu          chan struct{}
	dir         string
	file        *os.File
	path        string
	name        string
	files       []string
	format      mysql.BinlogFormat
	sid         mysql.SID
	executed    mysql.Mysql56GTIDSet
	position    uint32
	gtidsInFile int
	maxBytes    uint64
	notify      chan struct{}
	replicas    []registeredReplica
}

type registeredReplica struct {
	host string
	port uint16
}

func openBinlog(dir, serverUUID string, maxBytes uint64) (*binlog, error) {
	sid, err := mysql.ParseSID(serverUUID)
	if err != nil {
		return nil, fmt.Errorf("persist: binlog server uuid: %w", err)
	}
	if maxBytes == 0 {
		maxBytes = defaultBinlogMax
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	names, err := binlogNames(dir)
	if err != nil {
		return nil, err
	}
	b := &binlog{
		mu:       make(chan struct{}, 1),
		dir:      dir,
		format:   mysql.NewMySQL56BinlogFormat(),
		sid:      sid,
		executed: mysql.Mysql56GTIDSet{},
		notify:   make(chan struct{}, 1),
		maxBytes: maxBytes,
		files:    names,
	}
	b.mu <- struct{}{}
	if len(names) == 0 {
		if err := b.writeNewFileLocked("binlog.000001", mysql.Mysql56GTIDSet{}); err != nil {
			return nil, err
		}
		b.files = []string{b.name}
		if err := b.writeIndexLocked(); err != nil {
			b.file.Close()
			return nil, err
		}
		return b, nil
	}
	if err := b.loadExecutedLocked(); err != nil {
		return nil, err
	}
	last := names[len(names)-1]
	path := filepath.Join(dir, last)
	file, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, err
	}
	b.file = file
	b.path = path
	b.name = last
	b.position = uint32(info.Size())
	events, format, err := readBinlogFile(path)
	if err != nil {
		file.Close()
		return nil, err
	}
	if format.FormatVersion != 0 {
		b.format = format
	}
	for _, ev := range events {
		if ev.IsGTID() {
			b.gtidsInFile++
		}
	}
	if err := b.writeIndexLocked(); err != nil {
		file.Close()
		return nil, err
	}
	return b, nil
}

func binlogSequence(name string) (int, bool) {
	const prefix = "binlog."
	if !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+6 {
		return 0, false
	}
	n, err := strconv.Atoi(name[len(prefix):])
	if err != nil || n < 1 {
		return 0, false
	}
	return n, true
}

func binlogNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if _, ok := binlogSequence(entry.Name()); ok {
			names = append(names, entry.Name())
		}
	}
	sort.Slice(names, func(i, j int) bool {
		ai, _ := binlogSequence(names[i])
		aj, _ := binlogSequence(names[j])
		return ai < aj
	})
	return names, nil
}

func nextBinlogName(name string) (string, error) {
	n, ok := binlogSequence(name)
	if !ok {
		return "", fmt.Errorf("persist: binlog name %q", name)
	}
	if n >= 999999 {
		return "", fmt.Errorf("persist: binlog sequence exhausted")
	}
	return fmt.Sprintf("binlog.%06d", n+1), nil
}

func (b *binlog) lock() {
	<-b.mu
}

func (b *binlog) unlock() {
	b.mu <- struct{}{}
}

func (b *binlog) close() {
	b.lock()
	defer b.unlock()
	if b.file != nil {
		b.file.Close()
		b.file = nil
	}
}

func (b *binlog) writeBootstrapLocked(prev mysql.Mysql56GTIDSet) error {
	meta := mysql.BinlogEventMetadata{ServerID: 1}
	if err := b.writeEventLocked(mysql.NewFormatDescriptionEvent(b.format, meta)); err != nil {
		return err
	}
	if prev == nil {
		prev = mysql.Mysql56GTIDSet{}
	}
	return b.writeEventLocked(mysql.NewPreviousGtidsEvent(b.format, meta, prev))
}

func (b *binlog) writeNewFileLocked(name string, prev mysql.Mysql56GTIDSet) error {
	path := filepath.Join(b.dir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if b.file != nil {
		_ = b.file.Close()
	}
	b.file = file
	b.path = path
	b.name = name
	b.position = 0
	b.gtidsInFile = 0
	if _, err := file.Write(binlogMagic); err != nil {
		return err
	}
	b.position = uint32(len(binlogMagic))
	return b.writeBootstrapLocked(prev)
}

func (b *binlog) writeIndexLocked() error {
	var buf strings.Builder
	for _, name := range b.files {
		buf.WriteString(name)
		buf.WriteByte('\n')
	}
	return os.WriteFile(filepath.Join(b.dir, "binlog.index"), []byte(buf.String()), 0o644)
}

func (b *binlog) loadExecutedLocked() error {
	b.executed = mysql.Mysql56GTIDSet{}
	for _, name := range b.files {
		events, format, err := readBinlogFile(filepath.Join(b.dir, name))
		if err != nil {
			return err
		}
		if format.FormatVersion != 0 {
			b.format = format
		}
		for _, ev := range events {
			if !ev.IsGTID() {
				continue
			}
			gtid, _, err := ev.GTID(b.format)
			if err != nil {
				return err
			}
			b.executed = b.executed.AddGTID(gtid).(mysql.Mysql56GTIDSet)
		}
	}
	return nil
}

func (b *binlog) maybeRotateLocked() error {
	if b.gtidsInFile == 0 || b.maxBytes == 0 || uint64(b.position) < b.maxBytes {
		return nil
	}
	return b.rotateLocked()
}

func (b *binlog) rotate() error {
	b.lock()
	defer b.unlock()
	return b.rotateLocked()
}

func (b *binlog) rotateLocked() error {
	next, err := nextBinlogName(b.name)
	if err != nil {
		return err
	}
	meta := mysql.BinlogEventMetadata{ServerID: 1}
	if err := b.writeEventLocked(mysql.NewRotateEvent(b.format, meta, 4, next)); err != nil {
		return err
	}
	prev := b.executed
	if err := b.writeNewFileLocked(next, prev); err != nil {
		return err
	}
	b.files = append(b.files, next)
	if err := b.writeIndexLocked(); err != nil {
		return err
	}
	b.signal()
	return nil
}

func (b *binlog) writeEventLocked(ev mysql.BinlogEvent) error {
	raw := ev.Bytes()
	next := b.position + uint32(len(raw))
	if len(raw) >= 17 {
		binary.LittleEndian.PutUint32(raw[13:17], next)
		mysql.UpdateChecksum(b.format, ev)
	}
	if _, err := b.file.Write(ev.Bytes()); err != nil {
		return err
	}
	b.position = next
	return nil
}

func (b *binlog) signal() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

func (s *Store) appendBinlog(index uint64, batch replBatch) error {
	if s.bin == nil {
		return nil
	}
	return s.bin.append(index, batch)
}

func (b *binlog) append(index uint64, batch replBatch) error {
	gtid := mysql.Mysql56GTID{Server: b.sid, Sequence: int64(index)}
	b.lock()
	if b.executed.ContainsGTID(gtid) {
		b.unlock()
		return nil
	}
	b.unlock()
	events, err := b.build(index, batch)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	b.lock()
	defer b.unlock()
	if b.executed.ContainsGTID(gtid) {
		return nil
	}
	for _, ev := range events {
		if err := b.writeEventLocked(ev); err != nil {
			return err
		}
		if ev.IsGTID() {
			gtid, _, err := ev.GTID(b.format)
			if err != nil {
				return err
			}
			b.executed = b.executed.AddGTID(gtid).(mysql.Mysql56GTIDSet)
			b.gtidsInFile++
		}
	}
	if err := b.maybeRotateLocked(); err != nil {
		return err
	}
	b.signal()
	return nil
}

func (b *binlog) build(index uint64, batch replBatch) ([]mysql.BinlogEvent, error) {
	rows := batchRowChanges(batch)
	if len(rows) == 0 && batch.Statement == "" {
		return nil, nil
	}
	meta := mysql.BinlogEventMetadata{ServerID: 1, Timestamp: batch.Unix}
	gtid := mysql.Mysql56GTID{Server: b.sid, Sequence: int64(index)}
	events := []mysql.BinlogEvent{
		mysql.NewMySQLGTIDEvent(b.format, meta, gtid, len(rows) > 0),
	}
	if len(rows) == 0 {
		events = append(events, mysql.NewQueryEvent(b.format, meta, mysql.Query{SQL: batch.Statement}))
		return events, nil
	}
	events = append(events, mysql.NewQueryEvent(b.format, meta, mysql.Query{
		Database: rows[0].Database,
		SQL:      "BEGIN",
	}))
	var tableID uint64
	schemas := map[string][]byte{}
	for i := 0; i < len(rows); {
		filled, err := fillRowSchema(schemas, rows[i])
		if err != nil {
			return nil, err
		}
		group := []rowChange{filled}
		j := i + 1
		for j < len(rows) {
			next, err := fillRowSchema(schemas, rows[j])
			if err != nil {
				return nil, err
			}
			if next.Database != filled.Database || next.Table != filled.Table || next.Op != filled.Op {
				break
			}
			group = append(group, next)
			j++
		}
		tableID++
		rowEvents, err := b.rowGroupEvents(tableID, group, meta)
		if err != nil {
			return nil, err
		}
		events = append(events, rowEvents...)
		i = j
	}
	events = append(events, mysql.NewXIDEvent(b.format, meta))
	return events, nil
}

// fillRowSchema keeps the first schema stored for a table in this batch and
// copies it onto later rows that omit it. Older log entries carry the schema
// on every row.
func fillRowSchema(schemas map[string][]byte, change rowChange) (rowChange, error) {
	key := change.Database + "\x00" + change.Table
	if len(change.Schema) > 0 {
		schemas[key] = change.Schema
		return change, nil
	}
	schema, ok := schemas[key]
	if !ok {
		return change, fmt.Errorf("persist: missing schema for %s.%s", change.Database, change.Table)
	}
	change.Schema = schema
	return change, nil
}

// rowGroupEvents emits one table map and one rows event for consecutive
// edits of the same table and operation.
func (b *binlog) rowGroupEvents(tableID uint64, group []rowChange, meta mysql.BinlogEventMetadata) ([]mysql.BinlogEvent, error) {
	head := group[0]
	tableMeta, err := decodeSchema(head.Schema, head.Database, head.Table)
	if err != nil {
		return nil, err
	}
	tableMap, err := tableMapFor(head.Database, head.Table, tableMeta.schema)
	if err != nil {
		return nil, err
	}
	mapEvent, err := mysql.NewTableMapEvent(b.format, meta, tableID, tableMap)
	if err != nil {
		return nil, err
	}
	n := len(tableMeta.schema)
	switch head.Op {
	case int(opInsert):
		rows := mysql.Rows{DataColumns: presentColumns(n)}
		for _, change := range group {
			data, nulls, err := encodeBinlogRow(tableMeta.schema, change.After)
			if err != nil {
				return nil, err
			}
			rows.Rows = append(rows.Rows, mysql.Row{NullColumns: nulls, Data: data})
		}
		return []mysql.BinlogEvent{mapEvent, mysql.NewWriteRowsEvent(b.format, meta, tableID, rows)}, nil
	case int(opDelete):
		rows := mysql.Rows{IdentifyColumns: presentColumns(n)}
		for _, change := range group {
			data, nulls, err := encodeBinlogRow(tableMeta.schema, change.Before)
			if err != nil {
				return nil, err
			}
			rows.Rows = append(rows.Rows, mysql.Row{NullIdentifyColumns: nulls, Identify: data})
		}
		return []mysql.BinlogEvent{mapEvent, mysql.NewDeleteRowsEvent(b.format, meta, tableID, rows)}, nil
	case int(opUpdate):
		rows := mysql.Rows{
			IdentifyColumns: presentColumns(n),
			DataColumns:     presentColumns(n),
		}
		for _, change := range group {
			before, beforeNulls, err := encodeBinlogRow(tableMeta.schema, change.Before)
			if err != nil {
				return nil, err
			}
			after, afterNulls, err := encodeBinlogRow(tableMeta.schema, change.After)
			if err != nil {
				return nil, err
			}
			rows.Rows = append(rows.Rows, mysql.Row{
				NullIdentifyColumns: beforeNulls,
				Identify:            before,
				NullColumns:         afterNulls,
				Data:                after,
			})
		}
		return []mysql.BinlogEvent{mapEvent, mysql.NewUpdateRowsEvent(b.format, meta, tableID, rows)}, nil
	default:
		return nil, fmt.Errorf("persist: binlog row op %d", head.Op)
	}
}

func presentColumns(n int) mysql.Bitmap {
	bits := mysql.NewServerBitmap(n)
	for i := 0; i < n; i++ {
		bits.Set(i, true)
	}
	return bits
}

func tableMapFor(database, table string, schema sql.Schema) (*mysql.TableMap, error) {
	types := make([]byte, len(schema))
	metadata := make([]uint16, len(schema))
	nulls := mysql.NewServerBitmap(len(schema))
	for i, col := range schema {
		typ, meta := columnBinlogMeta(col)
		types[i] = typ
		metadata[i] = meta
		if col.Nullable {
			nulls.Set(i, true)
		}
	}
	return &mysql.TableMap{
		Database:  database,
		Name:      table,
		Types:     types,
		CanBeNull: nulls,
		Metadata:  metadata,
	}, nil
}

func columnBinlogMeta(col *sql.Column) (byte, uint16) {
	switch col.Type.Type() {
	case query.Type_INT8, query.Type_UINT8:
		return mysql.TypeTiny, 0
	case query.Type_INT16, query.Type_UINT16:
		return mysql.TypeShort, 0
	case query.Type_INT24, query.Type_UINT24:
		return mysql.TypeInt24, 0
	case query.Type_INT32, query.Type_UINT32:
		return mysql.TypeLong, 0
	case query.Type_INT64, query.Type_UINT64:
		return mysql.TypeLongLong, 0
	case query.Type_FLOAT32:
		return mysql.TypeFloat, 4
	case query.Type_FLOAT64:
		return mysql.TypeDouble, 8
	case query.Type_VARCHAR, query.Type_VARBINARY, query.Type_CHAR, query.Type_BINARY, query.Type_TEXT, query.Type_BLOB:
		max := uint16(255)
		if st, ok := col.Type.(sql.StringType); ok && st.MaxByteLength() > 0 && st.MaxByteLength() <= 65535 {
			max = uint16(st.MaxByteLength())
		}
		if col.Type.Type() == query.Type_TEXT || col.Type.Type() == query.Type_BLOB || max > 255 {
			n := uint16(1)
			if max > 255 {
				n = 2
			}
			if max > 65535 {
				n = 4
			}
			return mysql.TypeBlob, n
		}
		return mysql.TypeVarchar, max
	default:
		return mysql.TypeBlob, 2
	}
}

func encodeBinlogRow(schema sql.Schema, raw []byte) ([]byte, mysql.Bitmap, error) {
	nulls := mysql.NewServerBitmap(len(schema))
	if len(raw) == 0 {
		for i := range schema {
			nulls.Set(i, true)
		}
		return nil, nulls, nil
	}
	row, err := decodeRow(context.Background(), schema, raw)
	if err != nil {
		return nil, mysql.Bitmap{}, err
	}
	var data []byte
	for i, col := range schema {
		if i >= len(row) || row[i] == nil {
			nulls.Set(i, true)
			continue
		}
		encoded, err := encodeBinlogValue(col, row[i])
		if err != nil {
			return nil, mysql.Bitmap{}, err
		}
		data = append(data, encoded...)
	}
	return data, nulls, nil
}

func encodeBinlogValue(col *sql.Column, value interface{}) ([]byte, error) {
	converted, _, err := col.Type.Convert(context.Background(), value)
	if err != nil {
		converted = value
	}
	if converted == nil {
		return nil, nil
	}
	switch col.Type.Type() {
	case query.Type_INT8, query.Type_UINT8:
		n, ok := asUint64(converted)
		if !ok {
			break
		}
		return []byte{byte(n)}, nil
	case query.Type_INT16, query.Type_UINT16:
		n, ok := asUint64(converted)
		if !ok {
			break
		}
		var buf [2]byte
		binary.LittleEndian.PutUint16(buf[:], uint16(n))
		return buf[:], nil
	case query.Type_INT24, query.Type_UINT24:
		n, ok := asUint64(converted)
		if !ok {
			break
		}
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(n))
		return buf[:3], nil
	case query.Type_INT32, query.Type_UINT32:
		n, ok := asUint64(converted)
		if !ok {
			break
		}
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(n))
		return buf[:], nil
	case query.Type_INT64, query.Type_UINT64:
		n, ok := asUint64(converted)
		if !ok {
			break
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], n)
		return buf[:], nil
	case query.Type_FLOAT32:
		f, ok := converted.(float32)
		if !ok {
			break
		}
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], math.Float32bits(f))
		return buf[:], nil
	case query.Type_FLOAT64:
		f, ok := converted.(float64)
		if !ok {
			break
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f))
		return buf[:], nil
	}
	text := fmt.Sprint(converted)
	if s, ok := converted.(string); ok {
		text = s
	} else if b, ok := converted.([]byte); ok {
		text = string(b)
	}
	return encodeLenPrefixed([]byte(text), columnLengthBytes(col)), nil
}

func columnLengthBytes(col *sql.Column) int {
	_, meta := columnBinlogMeta(col)
	switch col.Type.Type() {
	case query.Type_VARCHAR, query.Type_VARBINARY:
		if meta > 255 {
			return 2
		}
		return 1
	case query.Type_TEXT, query.Type_BLOB:
		if meta == 0 {
			return 1
		}
		return int(meta)
	default:
		typ, _ := columnBinlogMeta(col)
		if typ == mysql.TypeBlob {
			if meta == 0 {
				return 2
			}
			return int(meta)
		}
		if typ == mysql.TypeVarchar && meta > 255 {
			return 2
		}
		return 1
	}
}

func encodeLenPrefixed(b []byte, n int) []byte {
	if n < 1 {
		n = 1
	}
	buf := make([]byte, n+len(b))
	switch n {
	case 1:
		buf[0] = byte(len(b))
	case 2:
		binary.LittleEndian.PutUint16(buf, uint16(len(b)))
	case 3:
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], uint32(len(b)))
		copy(buf, tmp[:3])
	default:
		binary.LittleEndian.PutUint32(buf, uint32(len(b)))
	}
	copy(buf[n:], b)
	return buf
}

func asUint64(v interface{}) (uint64, bool) {
	switch n := v.(type) {
	case int:
		return uint64(n), true
	case int8:
		return uint64(n), true
	case int16:
		return uint64(n), true
	case int32:
		return uint64(n), true
	case int64:
		return uint64(n), true
	case uint:
		return uint64(n), true
	case uint8:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint64:
		return n, true
	default:
		return 0, false
	}
}

func (b *binlog) read() ([]mysql.BinlogEvent, mysql.BinlogFormat, error) {
	b.lock()
	names := append([]string(nil), b.files...)
	format := b.format
	b.unlock()
	var all []mysql.BinlogEvent
	for _, name := range names {
		events, fileFormat, err := readBinlogFile(filepath.Join(b.dir, name))
		if err != nil {
			return nil, mysql.BinlogFormat{}, err
		}
		if fileFormat.FormatVersion != 0 {
			format = fileFormat
		}
		all = append(all, events...)
	}
	return all, format, nil
}

func readBinlogFile(path string) ([]mysql.BinlogEvent, mysql.BinlogFormat, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, mysql.BinlogFormat{}, err
	}
	return parseBinlog(raw)
}

// eventsFor is the binlog a replica with executed would be sent, across files.
func (b *binlog) eventsFor(executed mysql.GTIDSet) ([]mysql.BinlogEvent, error) {
	b.lock()
	names := append([]string(nil), b.files...)
	dir := b.dir
	b.unlock()
	var out []mysql.BinlogEvent
	skip := false
	for _, name := range names {
		events, format, err := readBinlogFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		for _, ev := range events {
			if ev.IsGTID() {
				gtid, _, err := ev.GTID(format)
				if err != nil {
					return nil, err
				}
				skip = executed != nil && executed.ContainsGTID(gtid)
			}
			if skip && !ev.IsFormatDescription() && !ev.IsPreviousGTIDs() && !ev.IsRotate() {
				continue
			}
			out = append(out, ev)
		}
	}
	return out, nil
}

func (b *binlog) fileNames() []string {
	b.lock()
	defer b.unlock()
	return append([]string(nil), b.files...)
}

func parseBinlog(raw []byte) ([]mysql.BinlogEvent, mysql.BinlogFormat, error) {
	if len(raw) < len(binlogMagic) {
		return nil, mysql.BinlogFormat{}, io.ErrUnexpectedEOF
	}
	rest := raw[len(binlogMagic):]
	var format mysql.BinlogFormat
	var events []mysql.BinlogEvent
	for len(rest) > 0 {
		if len(rest) < 19 {
			return events, format, fmt.Errorf("persist: truncated binlog")
		}
		n := int(binary.LittleEndian.Uint32(rest[9:13]))
		if n < 19 || n > len(rest) {
			return events, format, fmt.Errorf("persist: bad binlog event length %d", n)
		}
		ev := mysql.NewMysql56BinlogEvent(rest[:n])
		if !ev.IsValid() {
			return events, format, fmt.Errorf("persist: invalid binlog event")
		}
		if ev.IsFormatDescription() {
			parsed, err := ev.Format()
			if err != nil {
				return nil, mysql.BinlogFormat{}, err
			}
			format = parsed
		}
		events = append(events, ev)
		rest = rest[n:]
	}
	return events, format, nil
}

// ReadBinlog parses the on-disk binlog for this node.
func (s *Store) ReadBinlog() ([]mysql.BinlogEvent, mysql.BinlogFormat, error) {
	if s.bin == nil {
		return nil, mysql.BinlogFormat{}, fmt.Errorf("persist: binlog is not enabled")
	}
	return s.bin.read()
}

var _ binlogreplication.BinlogPrimaryController = (*Store)(nil)

func (s *Store) binlogOrErr() (*binlog, error) {
	if s.bin == nil {
		return nil, fmt.Errorf("persist: binlog replication requires cluster mode")
	}
	return s.bin, nil
}

// RegisterReplica implements binlogreplication.BinlogPrimaryController.
func (s *Store) RegisterReplica(_ *sql.Context, _ *mysql.Conn, host string, port uint16) error {
	b, err := s.binlogOrErr()
	if err != nil {
		return err
	}
	b.lock()
	defer b.unlock()
	b.replicas = append(b.replicas, registeredReplica{host: host, port: port})
	return nil
}

// BinlogDumpGtid streams row events starting after the replica's executed set.
// It returns when the connection write fails. Closed files are read from disk.
func (s *Store) BinlogDumpGtid(ctx *sql.Context, conn *mysql.Conn, executed mysql.GTIDSet) error {
	b, err := s.binlogOrErr()
	if err != nil {
		return mysql.NewSQLError(mysql.ERMasterFatalReadingBinlog, "HY000", "%v", err)
	}
	fileIdx := 0
	sent := 0
	skip := false
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		names := b.fileNames()
		if fileIdx >= len(names) {
			if err := waitBinlog(ctx, b); err != nil {
				return err
			}
			continue
		}
		events, format, err := readBinlogFile(filepath.Join(b.dir, names[fileIdx]))
		if err != nil {
			return err
		}
		if sent > len(events) {
			sent = len(events)
		}
		for _, ev := range events[sent:] {
			if ev.IsGTID() {
				gtid, _, gerr := ev.GTID(format)
				if gerr != nil {
					return gerr
				}
				skip = executed != nil && executed.ContainsGTID(gtid)
			}
			if skip && !ev.IsFormatDescription() && !ev.IsPreviousGTIDs() && !ev.IsRotate() {
				sent++
				continue
			}
			if err := conn.WriteBinlogEvent(ev, false); err != nil {
				return err
			}
			sent++
		}
		names = b.fileNames()
		if fileIdx < len(names)-1 && sent >= len(events) {
			fileIdx++
			sent = 0
			continue
		}
		if err := waitBinlog(ctx, b); err != nil {
			return err
		}
	}
}

func waitBinlog(ctx *sql.Context, b *binlog) error {
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()
	if ctx == nil {
		select {
		case <-b.notify:
		case <-timer.C:
		}
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.notify:
		return nil
	case <-timer.C:
		return nil
	}
}

// ListReplicas implements binlogreplication.BinlogPrimaryController.
func (s *Store) ListReplicas(*sql.Context) error {
	return nil
}

// ListBinaryLogs implements binlogreplication.BinlogPrimaryController.
func (s *Store) ListBinaryLogs(*sql.Context) ([]binlogreplication.BinaryLogFileMetadata, error) {
	b, err := s.binlogOrErr()
	if err != nil {
		return nil, nil
	}
	names := b.fileNames()
	out := make([]binlogreplication.BinaryLogFileMetadata, 0, len(names))
	for _, name := range names {
		info, err := os.Stat(filepath.Join(b.dir, name))
		if err != nil {
			return nil, err
		}
		out = append(out, binlogreplication.BinaryLogFileMetadata{
			Name: name,
			Size: uint64(info.Size()),
		})
	}
	return out, nil
}

// GetBinaryLogStatus implements binlogreplication.BinlogPrimaryController.
func (s *Store) GetBinaryLogStatus(*sql.Context) ([]binlogreplication.BinaryLogStatus, error) {
	b, err := s.binlogOrErr()
	if err != nil {
		return nil, nil
	}
	b.lock()
	defer b.unlock()
	return []binlogreplication.BinaryLogStatus{{
		File:          b.name,
		Position:      uint(b.position),
		ExecutedGtids: b.executed.String(),
	}}, nil
}

// RotateBinaryLog rolls the active binlog. In a cluster the roll is a Raft
// entry, so every node opens the next file at the same point.
func (s *Store) RotateBinaryLog(*sql.Context) error {
	if s.bin == nil {
		return fmt.Errorf("persist: binlog is not enabled")
	}
	if s.cluster == nil {
		return s.bin.rotate()
	}
	return s.commitGTID("", "", func(tx *kvTx) error {
		tx.rotate = true
		return nil
	})
}

func (s *Store) rotateBinlog() error {
	if s.bin == nil {
		return fmt.Errorf("persist: binlog is not enabled")
	}
	return s.bin.rotate()
}
