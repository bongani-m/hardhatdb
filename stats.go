package persist

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
)

var bucketStats = []byte("stats")

const histogramBuckets = 16

type savedStat struct {
	Rows     uint64     `json:"rows"`
	Distinct uint64     `json:"distinct"`
	Nulls    uint64     `json:"nulls"`
	Avg      uint64     `json:"avg"`
	Columns  []string   `json:"columns"`
	Bounds   [][]string `json:"bounds"`
	Counts   []uint64   `json:"counts"`
}

var _ sql.StatsProvider = (*Store)(nil)

func statKey(db, table, index string) []byte {
	return []byte(db + "\x00" + table + "\x00" + index)
}

func (s *Store) GetTableStats(ctx *sql.Context, sch, db string, table sql.Table) ([]sql.Statistic, error) {
	t, ok := table.(*Table)
	if !ok || t == nil {
		return nil, nil
	}
	indexes, err := t.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}
	var out []sql.Statistic
	for _, idx := range indexes {
		stat, ok := s.GetStats(ctx, sql.NewStatQualifier(t.dbName, sch, t.name, idx.ID()), nil)
		if ok {
			out = append(out, stat)
		}
	}
	return out, nil
}

func (s *Store) AnalyzeTable(ctx *sql.Context, table sql.Table, db string) error {
	t, ok := table.(*Table)
	if !ok || t == nil {
		return nil
	}
	indexes, err := t.GetIndexes(ctx)
	if err != nil {
		return err
	}
	rows, err := t.visibleRows(ctx)
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		pi, ok := idx.(*Index)
		if !ok {
			continue
		}
		saved := analyzeIndex(ctx, t.meta.schema, pi.columns, rows)
		if err := s.putStat(t.dbName, t.name, pi.name, saved); err != nil {
			return err
		}
	}
	return nil
}

func analyzeIndex(ctx *sql.Context, schema sql.Schema, columns []string, rows []sql.Row) savedStat {
	ordinals := make([]int, len(columns))
	for i, name := range columns {
		ordinals[i] = columnOrdinal(schema, name)
	}
	distinct := map[string]struct{}{}
	var nulls uint64
	var bounds [][]string
	var counts []uint64
	step := 1
	if len(rows) > histogramBuckets {
		step = len(rows) / histogramBuckets
	}
	var inBucket uint64
	for i, row := range rows {
		vals := make([]string, len(ordinals))
		anyNull := false
		for j, ord := range ordinals {
			if ord < 0 || ord >= len(row) || row[ord] == nil {
				anyNull = true
				vals[j] = ""
				continue
			}
			vals[j] = fmt.Sprint(row[ord])
		}
		if anyNull {
			nulls++
		}
		distinct[fmt.Sprint(vals)] = struct{}{}
		inBucket++
		if (i+1)%step == 0 || i == len(rows)-1 {
			bounds = append(bounds, vals)
			counts = append(counts, inBucket)
			inBucket = 0
		}
	}
	var avg uint64
	if len(rows) > 0 {
		var nbytes uint64
		for _, row := range rows {
			for _, val := range row {
				nbytes += uint64(len(fmt.Sprint(val)))
			}
		}
		avg = nbytes / uint64(len(rows))
	}
	_ = ctx
	return savedStat{
		Rows:     uint64(len(rows)),
		Distinct: uint64(len(distinct)),
		Nulls:    nulls,
		Avg:      avg,
		Columns:  append([]string(nil), columns...),
		Bounds:   bounds,
		Counts:   counts,
	}
}

func (s *Store) SetStats(ctx *sql.Context, stat sql.Statistic) error {
	qual := stat.Qualifier()
	saved := savedStat{
		Rows:     stat.RowCount(),
		Distinct: stat.DistinctCount(),
		Nulls:    stat.NullCount(),
		Avg:      stat.AvgSize(),
		Columns:  append([]string(nil), stat.Columns()...),
	}
	for _, bucket := range stat.Histogram() {
		var bound []string
		for _, val := range bucket.UpperBound() {
			bound = append(bound, fmt.Sprint(val))
		}
		saved.Bounds = append(saved.Bounds, bound)
		saved.Counts = append(saved.Counts, bucket.RowCount())
	}
	return s.putStat(qual.Database, qual.Table(), qual.Index(), saved)
}

func (s *Store) GetStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) (sql.Statistic, bool) {
	saved, ok := s.readStat(qual.Database, qual.Table(), qual.Index())
	if !ok {
		return nil, false
	}
	if len(cols) > 0 && !sameColumns(saved.Columns, cols) {
		return nil, false
	}
	var hist []sql.HistogramBucket
	for i, bound := range saved.Bounds {
		var count uint64
		if i < len(saved.Counts) {
			count = saved.Counts[i]
		}
		row := make(sql.Row, len(bound))
		for j, val := range bound {
			row[j] = val
		}
		hist = append(hist, stats.NewHistogramBucket(count, 0, 0, count, row, nil, nil))
	}
	var types []sql.Type
	return stats.NewStatistic(saved.Rows, saved.Distinct, saved.Nulls, saved.Avg, time.Now(), qual, saved.Columns, types, hist, sql.IndexClassDefault, nil), true
}

func (s *Store) DropStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) error {
	return s.deleteStat(qual.Database, qual.Table(), qual.Index())
}

func (s *Store) DropDbStats(ctx *sql.Context, sch, db string, flush bool) error {
	return s.update(func(tx *kvTx) error {
		root := tx.Bucket(bucketStats)
		if root == nil {
			return nil
		}
		var keys [][]byte
		err := root.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			if len(k) > len(db) && string(k[:len(db)]) == db && k[len(db)] == 0 {
				keys = append(keys, append([]byte(nil), k...))
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := root.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) RowCount(ctx *sql.Context, sch, db string, table sql.Table) (uint64, error) {
	if counter, ok := table.(sql.StatisticsTable); ok {
		n, _, err := counter.RowCount(ctx)
		return n, err
	}
	return 0, nil
}

func (s *Store) DataLength(ctx *sql.Context, sch, db string, table sql.Table) (uint64, error) {
	if counter, ok := table.(sql.StatisticsTable); ok {
		return counter.DataLength(ctx)
	}
	return 0, nil
}

func (s *Store) putStat(db, table, index string, saved savedStat) error {
	raw, err := json.Marshal(saved)
	if err != nil {
		return err
	}
	return s.update(func(tx *kvTx) error {
		root, err := tx.CreateBucketIfNotExists(bucketStats)
		if err != nil {
			return err
		}
		return root.Put(statKey(db, table, index), raw)
	})
}

func (s *Store) readStat(db, table, index string) (savedStat, bool) {
	var saved savedStat
	var ok bool
	_ = s.view(func(tx *kvTx) error {
		root := tx.Bucket(bucketStats)
		if root == nil {
			return nil
		}
		raw := root.Get(statKey(db, table, index))
		if len(raw) == 0 {
			return nil
		}
		if err := json.Unmarshal(raw, &saved); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return saved, ok
}

func (s *Store) deleteStat(db, table, index string) error {
	return s.update(func(tx *kvTx) error {
		root := tx.Bucket(bucketStats)
		if root == nil {
			return nil
		}
		return root.Delete(statKey(db, table, index))
	})
}

func sameColumns(a, b []string) bool {
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
