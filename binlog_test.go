package persist

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestBinlogRotateAndResume(t *testing.T) {
	dir := t.TempDir()
	b, err := openBinlog(dir, testServerUUID, 1)
	require.NoError(t, err)
	require.NoError(t, b.append(1, replBatch{Statement: "create table t (id int)", Unix: 1}))
	require.NoError(t, b.append(2, replBatch{Statement: "insert into t values (1)", Unix: 2}))
	names := b.fileNames()
	require.GreaterOrEqual(t, len(names), 2)

	var seqs []int64
	for _, name := range names {
		events, format, err := readBinlogFile(filepath.Join(dir, name))
		require.NoError(t, err)
		for _, ev := range events {
			if !ev.IsGTID() {
				continue
			}
			gtid, _, err := ev.GTID(format)
			require.NoError(t, err)
			seqs = append(seqs, gtid.(mysql.Mysql56GTID).Sequence)
		}
	}
	require.Equal(t, []int64{1, 2}, seqs)

	set := mysql.Mysql56GTIDSet{}
	set = set.AddGTID(mysql.Mysql56GTID{Server: b.sid, Sequence: 1}).(mysql.Mysql56GTIDSet)
	got, err := b.eventsFor(set)
	require.NoError(t, err)
	var sawRotate, saw2 bool
	for _, ev := range got {
		if ev.IsRotate() {
			sawRotate = true
		}
		if !ev.IsGTID() {
			continue
		}
		gtid, _, err := ev.GTID(b.format)
		require.NoError(t, err)
		seq := gtid.(mysql.Mysql56GTID).Sequence
		require.NotEqual(t, int64(1), seq)
		if seq == 2 {
			saw2 = true
		}
	}
	require.True(t, sawRotate)
	require.True(t, saw2)
	require.True(t, b.executed.ContainsGTID(mysql.Mysql56GTID{Server: b.sid, Sequence: 2}))
	last := names[len(names)-1]
	b.close()

	reopened, err := openBinlog(dir, testServerUUID, 1)
	require.NoError(t, err)
	defer reopened.close()
	require.Equal(t, last, reopened.name)
	require.True(t, reopened.executed.ContainsGTID(mysql.Mysql56GTID{Server: b.sid, Sequence: 1}))
	require.True(t, reopened.executed.ContainsGTID(mysql.Mysql56GTID{Server: b.sid, Sequence: 2}))
}

func TestBinlogAppendSkipsDuplicateGTID(t *testing.T) {
	dir := t.TempDir()
	b, err := openBinlog(dir, testServerUUID, 0)
	require.NoError(t, err)
	defer b.close()
	batch := replBatch{Statement: "create table t (id int)", Unix: 1}
	require.NoError(t, b.append(1, batch))
	require.NoError(t, b.append(1, batch))

	events, format, err := readBinlogFile(b.path)
	require.NoError(t, err)
	var seqs []int64
	for _, ev := range events {
		if !ev.IsGTID() {
			continue
		}
		gtid, _, err := ev.GTID(format)
		require.NoError(t, err)
		seqs = append(seqs, gtid.(mysql.Mysql56GTID).Sequence)
	}
	require.Equal(t, []int64{1}, seqs)
}

func TestBinlogSchemaOncePerTable(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	sch := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, PrimaryKey: true, Source: "t"},
	})
	rawSchema, err := encodeSchema(ctx, sch, sql.Collation_Default, "")
	require.NoError(t, err)
	first, err := encodeRow(ctx, sch.Schema, sql.NewRow(int64(1)))
	require.NoError(t, err)
	second, err := encodeRow(ctx, sch.Schema, sql.NewRow(int64(2)))
	require.NoError(t, err)

	full := replBatch{Unix: 1, Rows: []rowChange{
		{Database: "db", Table: "t", Schema: rawSchema, Op: int(opInsert), After: first},
		{Database: "db", Table: "t", Schema: append([]byte(nil), rawSchema...), Op: int(opInsert), After: second},
	}}
	compact := replBatch{Unix: 1, Rows: []rowChange{
		{Database: "db", Table: "t", Schema: rawSchema, Op: int(opInsert), After: first},
		{Database: "db", Table: "t", Op: int(opInsert), After: second},
	}}

	tagged := replBatch{Unix: 1, Ops: []kvOp{
		{Row: true, Database: "db", Table: "t", Schema: rawSchema, Op: int(opInsert), Value: first},
		{Row: true, Database: "db", Table: "t", Op: int(opInsert), Value: second},
	}}

	b, err := openBinlog(t.TempDir(), testServerUUID, 0)
	require.NoError(t, err)
	defer b.close()
	evFull, err := b.build(1, full)
	require.NoError(t, err)
	evCompact, err := b.build(1, compact)
	require.NoError(t, err)
	evTagged, err := b.build(1, tagged)
	require.NoError(t, err)
	require.Equal(t, len(evFull), len(evCompact))
	for i := range evFull {
		require.Equal(t, evFull[i].Bytes(), evCompact[i].Bytes())
		require.Equal(t, evFull[i].Bytes(), evTagged[i].Bytes())
	}
	requireGroupedWrite(t, b, evCompact, 1, 2)
}

func TestBinlogGroupsOneMapPerTable(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	sch := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, PrimaryKey: true, Source: "t"},
	})
	rawSchema, err := encodeSchema(ctx, sch, sql.Collation_Default, "")
	require.NoError(t, err)
	first, err := encodeRow(ctx, sch.Schema, sql.NewRow(int64(1)))
	require.NoError(t, err)
	second, err := encodeRow(ctx, sch.Schema, sql.NewRow(int64(2)))
	require.NoError(t, err)

	batch := replBatch{Unix: 1, Rows: []rowChange{
		{Database: "db", Table: "t", Schema: rawSchema, Op: int(opInsert), After: first},
		{Database: "db", Table: "u", Schema: rawSchema, Op: int(opInsert), After: second},
		{Database: "db", Table: "u", Op: int(opInsert), After: first},
	}}
	b, err := openBinlog(t.TempDir(), testServerUUID, 0)
	require.NoError(t, err)
	defer b.close()
	events, err := b.build(1, batch)
	require.NoError(t, err)
	requireGroupedWrite(t, b, events, 2, 3)
}

func requireGroupedWrite(t *testing.T, b *binlog, events []mysql.BinlogEvent, maps, rows int) {
	t.Helper()
	var mapCount, writeCount, rowCount int
	var tm *mysql.TableMap
	for _, ev := range events {
		if b.format.ChecksumAlgorithm != 0 {
			stripped, _, err := ev.StripChecksum(b.format)
			require.NoError(t, err)
			ev = stripped
		}
		if ev.IsTableMap() {
			mapCount++
			parsed, err := ev.TableMap(b.format)
			require.NoError(t, err)
			tm = parsed
		}
		if ev.IsWriteRows() {
			writeCount++
			require.NotNil(t, tm)
			parsed, err := ev.Rows(b.format, tm)
			require.NoError(t, err)
			rowCount += len(parsed.Rows)
		}
	}
	require.Equal(t, maps, mapCount)
	require.Equal(t, maps, writeCount)
	require.Equal(t, rows, rowCount)
}
