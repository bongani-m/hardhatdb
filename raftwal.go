package persist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	walCompactBytes = 1 << 20
	walMaxRecord    = 256 << 20
)

// raftWAL is an append-only Raft log. StoreLogs fsyncs the group once.
// The stable store stays in bbolt; this file holds only log entries.
type raftWAL struct {
	mu      sync.Mutex
	path    string
	meta    string
	file    *os.File
	size    int64
	offsets map[uint64]int64
	first   uint64
	last    uint64
	// floor is the first index still live. Records below it are ignored
	// so a prefix delete does not have to rewrite the file.
	floor uint64
}

func openRaftWAL(dir string, bolt *raftboltdb.BoltStore) (*raftWAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w, err := openRaftWALFile(dir)
	if err != nil {
		return nil, err
	}
	if w.last != 0 || bolt == nil {
		return w, nil
	}
	last, err := bolt.LastIndex()
	if err != nil {
		w.Close()
		return nil, err
	}
	if last == 0 {
		return w, nil
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	if err := os.Remove(filepath.Join(dir, "raft.wal")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.Remove(filepath.Join(dir, "raft.wal.meta")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := importBoltLogs(dir, bolt); err != nil {
		return nil, err
	}
	return openRaftWALFile(dir)
}

func openRaftWALFile(dir string) (*raftWAL, error) {
	path := filepath.Join(dir, "raft.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	w := &raftWAL{
		path:    path,
		meta:    filepath.Join(dir, "raft.wal.meta"),
		file:    f,
		offsets: map[uint64]int64{},
	}
	if err := w.load(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

func importBoltLogs(dir string, bolt *raftboltdb.BoltStore) error {
	w, err := openRaftWALFile(dir)
	if err != nil {
		return err
	}
	failed := true
	defer func() {
		w.Close()
		if failed {
			_ = os.Remove(filepath.Join(dir, "raft.wal"))
			_ = os.Remove(filepath.Join(dir, "raft.wal.meta"))
		}
	}()
	first, err := bolt.FirstIndex()
	if err != nil {
		return err
	}
	last, err := bolt.LastIndex()
	if err != nil {
		return err
	}
	var batch []*raft.Log
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := w.StoreLogs(batch)
		batch = nil
		return err
	}
	for i := first; i <= last && last > 0; i++ {
		lg := new(raft.Log)
		err := bolt.GetLog(i, lg)
		if errors.Is(err, raft.ErrLogNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		batch = append(batch, lg)
		if len(batch) == 64 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	failed = false
	return nil
}

func (w *raftWAL) load() error {
	floor, err := readWALMeta(w.meta)
	if err != nil {
		return err
	}
	w.floor = floor
	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	w.size = info.Size()
	var off int64
	for off < w.size {
		lg, next, err := w.readAt(off)
		if err != nil {
			if err := w.file.Truncate(off); err != nil {
				return err
			}
			if err := w.file.Sync(); err != nil {
				return err
			}
			w.size = off
			break
		}
		if lg.Index > 0 && (w.floor == 0 || lg.Index >= w.floor) {
			w.offsets[lg.Index] = off
		}
		off = next
	}
	if _, err := w.file.Seek(w.size, io.SeekStart); err != nil {
		return err
	}
	w.recompute()
	return nil
}

func (w *raftWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *raftWAL) FirstIndex() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.offsets) == 0 {
		return 0, nil
	}
	return w.first, nil
}

func (w *raftWAL) LastIndex() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last, nil
}

func (w *raftWAL) GetLog(index uint64, lg *raft.Log) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	off, ok := w.offsets[index]
	if !ok {
		return raft.ErrLogNotFound
	}
	got, _, err := w.readAt(off)
	if err != nil {
		return err
	}
	*lg = *got
	return nil
}

func (w *raftWAL) StoreLog(lg *raft.Log) error {
	return w.StoreLogs([]*raft.Log{lg})
}

func (w *raftWAL) StoreLogs(logs []*raft.Log) error {
	if len(logs) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	old := w.size
	written := make([]struct {
		index uint64
		off   int64
	}, 0, len(logs))
	for _, lg := range logs {
		raw, err := encodeWALRecord(lg)
		if err != nil {
			return w.rewind(old, err)
		}
		if _, err := w.file.Write(raw); err != nil {
			return w.rewind(old, err)
		}
		written = append(written, struct {
			index uint64
			off   int64
		}{lg.Index, w.size})
		w.size += int64(len(raw))
	}
	if err := w.file.Sync(); err != nil {
		return w.rewind(old, err)
	}
	for _, rec := range written {
		w.offsets[rec.index] = rec.off
	}
	w.recompute()
	return nil
}

func (w *raftWAL) rewind(size int64, cause error) error {
	if err := w.file.Truncate(size); err != nil {
		return fmt.Errorf("%w (truncate: %v)", cause, err)
	}
	if _, err := w.file.Seek(size, io.SeekStart); err != nil {
		return fmt.Errorf("%w (seek: %v)", cause, err)
	}
	w.size = size
	return cause
}

// DeleteRange drops every log in [min, max]. A prefix is forgotten by
// raising the floor. Any other range is rewritten out of the file.
func (w *raftWAL) DeleteRange(min, max uint64) error {
	if max < min {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.offsets) == 0 {
		return nil
	}
	keep := make(map[uint64]int64, len(w.offsets))
	deleted := false
	var deletedOff int64 = -1
	var liveOff int64 = -1
	for idx, off := range w.offsets {
		if idx >= min && idx <= max {
			deleted = true
			if off > deletedOff {
				deletedOff = off
			}
			continue
		}
		keep[idx] = off
		if liveOff < 0 || off < liveOff {
			liveOff = off
		}
	}
	if !deleted {
		return nil
	}
	prefix := liveOff < 0 || deletedOff < liveOff
	if prefix {
		floor := max + 1
		if err := writeWALMeta(w.meta, floor); err != nil {
			return err
		}
		w.floor = floor
		w.offsets = keep
		w.recompute()
		if liveOff < 0 || liveOff > walCompactBytes {
			return w.compact(keep)
		}
		return nil
	}
	if err := w.compact(keep); err != nil {
		return err
	}
	w.offsets = keep
	w.recompute()
	return writeWALMeta(w.meta, w.first)
}

func (w *raftWAL) compact(keep map[uint64]int64) error {
	idxs := make([]uint64, 0, len(keep))
	for idx := range keep {
		idxs = append(idxs, idx)
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i] < idxs[j] })
	tmp := w.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	next := map[uint64]int64{}
	var pos int64
	for _, idx := range idxs {
		lg, _, err := w.readAt(keep[idx])
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		raw, err := encodeWALRecord(lg)
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		if _, err := f.Write(raw); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		next[idx] = pos
		pos += int64(len(raw))
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := w.file.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}
	reopened, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := reopened.Seek(pos, io.SeekStart); err != nil {
		reopened.Close()
		return err
	}
	w.file = reopened
	w.size = pos
	for idx, off := range next {
		keep[idx] = off
	}
	return nil
}

func (w *raftWAL) recompute() {
	w.first = 0
	w.last = 0
	for idx := range w.offsets {
		if w.first == 0 || idx < w.first {
			w.first = idx
		}
		if idx > w.last {
			w.last = idx
		}
	}
}

func (w *raftWAL) readAt(off int64) (*raft.Log, int64, error) {
	var lenBuf [4]byte
	if _, err := readFullAt(w.file, lenBuf[:], off); err != nil {
		return nil, 0, err
	}
	bodyLen := binary.LittleEndian.Uint32(lenBuf[:])
	if bodyLen == 0 || bodyLen > walMaxRecord {
		return nil, 0, io.ErrUnexpectedEOF
	}
	body := make([]byte, bodyLen)
	if _, err := readFullAt(w.file, body, off+4); err != nil {
		return nil, 0, err
	}
	var crcBuf [4]byte
	if _, err := readFullAt(w.file, crcBuf[:], off+4+int64(bodyLen)); err != nil {
		return nil, 0, err
	}
	if binary.LittleEndian.Uint32(crcBuf[:]) != crc32.ChecksumIEEE(body) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	lg, err := decodeWALRecord(body)
	if err != nil {
		return nil, 0, err
	}
	return lg, off + 4 + int64(bodyLen) + 4, nil
}

func readFullAt(r io.ReaderAt, buf []byte, off int64) (int, error) {
	n, err := r.ReadAt(buf, off)
	if n == len(buf) {
		return n, nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func encodeWALRecord(lg *raft.Log) ([]byte, error) {
	var body bytes.Buffer
	if err := binary.Write(&body, binary.LittleEndian, lg.Index); err != nil {
		return nil, err
	}
	if err := binary.Write(&body, binary.LittleEndian, lg.Term); err != nil {
		return nil, err
	}
	if err := body.WriteByte(byte(lg.Type)); err != nil {
		return nil, err
	}
	if err := binary.Write(&body, binary.LittleEndian, lg.AppendedAt.UnixNano()); err != nil {
		return nil, err
	}
	if err := writeWALBytes(&body, lg.Data); err != nil {
		return nil, err
	}
	if err := writeWALBytes(&body, lg.Extensions); err != nil {
		return nil, err
	}
	raw := body.Bytes()
	var out bytes.Buffer
	if err := binary.Write(&out, binary.LittleEndian, uint32(len(raw))); err != nil {
		return nil, err
	}
	if _, err := out.Write(raw); err != nil {
		return nil, err
	}
	if err := binary.Write(&out, binary.LittleEndian, crc32.ChecksumIEEE(raw)); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func writeWALBytes(w io.Writer, b []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func decodeWALRecord(body []byte) (*raft.Log, error) {
	r := bytes.NewReader(body)
	var lg raft.Log
	if err := binary.Read(r, binary.LittleEndian, &lg.Index); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &lg.Term); err != nil {
		return nil, err
	}
	var typ byte
	if err := binary.Read(r, binary.LittleEndian, &typ); err != nil {
		return nil, err
	}
	lg.Type = raft.LogType(typ)
	var nano int64
	if err := binary.Read(r, binary.LittleEndian, &nano); err != nil {
		return nil, err
	}
	if nano != 0 {
		lg.AppendedAt = time.Unix(0, nano)
	}
	data, err := readWALBytes(r)
	if err != nil {
		return nil, err
	}
	ext, err := readWALBytes(r)
	if err != nil {
		return nil, err
	}
	lg.Data = data
	lg.Extensions = ext
	return &lg, nil
}

func readWALBytes(r io.Reader) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	if n > walMaxRecord {
		return nil, io.ErrUnexpectedEOF
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func readWALMeta(path string) (uint64, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("persist: raft wal meta %s is %d bytes", path, len(raw))
	}
	return binary.LittleEndian.Uint64(raw), nil
}

func writeWALMeta(path string, floor uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], floor)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf[:]); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}
