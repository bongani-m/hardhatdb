package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
)

// batchMagic is the first byte of a compact Raft entry. Gob streams from this
// process do not start with it; DecodeBatch falls back to gob when it is absent.
const batchMagic byte = 0xA5

const batchVersion byte = 1

func init() {
	gob.RegisterName("github.com/bongani-m/hardhatdb.kvOp", KVOp{})
	gob.RegisterName("github.com/bongani-m/hardhatdb.rowChange", RowChange{})
	gob.RegisterName("github.com/bongani-m/hardhatdb.replBatch", ReplBatch{})
}

// Peer is one voter in a Raft group.
type Peer struct {
	ID      string
	Address string
}

// RaftStatus is one process's view of a Raft group. A standalone store reports
// role "standalone", an empty suffrage, and zero indexes.
type RaftStatus struct {
	Role     string
	Leader   string
	Commit   uint64
	Applied  uint64
	Lag      uint64
	Suffrage string
}

// KVOp is one recorded Badger write. Delete is set instead of an empty value
// so a stored empty value stays distinct from a removal.
type KVOp struct {
	Key      []byte
	Value    []byte
	Delete   bool
	Row      bool
	Database string
	Table    string
	Schema   []byte
	Op       int
	Before   []byte
}

// RowChange is the before/after image of one row edit.
type RowChange struct {
	Database string
	Table    string
	Schema   []byte
	Op       int
	Before   []byte
	After    []byte
}

// ReplBatch is one Raft log entry.
type ReplBatch struct {
	Ops       []KVOp
	Rows      []RowChange
	Statement string
	Unix      uint32
	ID        uint64
	Rotate    bool
	Phase     byte
	PrepareID string
	CommitNo  uint64
}

const (
	PhaseApply   byte = 0
	PhasePrepare byte = 1
	PhaseCommit  byte = 2
	PhaseAbort   byte = 3
)

// Marked is how a Raft batch should be applied. The zero value is a normal commit.
type Marked struct {
	Phase     byte
	PrepareID string
	CommitNo  uint64
}

// EncodeBatch serializes a Raft log entry in the compact form.
func EncodeBatch(batch ReplBatch) ([]byte, error) {
	buf := make([]byte, 0, 64+len(batch.Statement))
	buf = append(buf, batchMagic, batchVersion)
	var err error
	buf, err = appendOps(buf, batch.Ops)
	if err != nil {
		return nil, err
	}
	buf, err = appendRows(buf, batch.Rows)
	if err != nil {
		return nil, err
	}
	buf = appendString(buf, batch.Statement)
	buf = binary.AppendUvarint(buf, uint64(batch.Unix))
	buf = binary.AppendUvarint(buf, batch.ID)
	if batch.Rotate {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = append(buf, batch.Phase)
	buf = appendString(buf, batch.PrepareID)
	buf = binary.AppendUvarint(buf, batch.CommitNo)
	return buf, nil
}

// DecodeBatch reads a Raft log entry. Compact entries start with batchMagic.
// Older entries are gob.
func DecodeBatch(raw []byte) (ReplBatch, error) {
	if len(raw) >= 2 && raw[0] == batchMagic {
		return decodeCompact(raw)
	}
	var batch ReplBatch
	err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&batch)
	return batch, err
}

func decodeCompact(raw []byte) (ReplBatch, error) {
	if raw[1] != batchVersion {
		return ReplBatch{}, fmt.Errorf("hardhatdb: batch version %d", raw[1])
	}
	r := bytes.NewReader(raw[2:])
	var batch ReplBatch
	var err error
	batch.Ops, err = readOps(r)
	if err != nil {
		return ReplBatch{}, err
	}
	batch.Rows, err = readRows(r)
	if err != nil {
		return ReplBatch{}, err
	}
	batch.Statement, err = readString(r)
	if err != nil {
		return ReplBatch{}, err
	}
	unix, err := binary.ReadUvarint(r)
	if err != nil {
		return ReplBatch{}, err
	}
	batch.Unix = uint32(unix)
	batch.ID, err = binary.ReadUvarint(r)
	if err != nil {
		return ReplBatch{}, err
	}
	rot, err := r.ReadByte()
	if err != nil {
		return ReplBatch{}, err
	}
	batch.Rotate = rot != 0
	batch.Phase, err = r.ReadByte()
	if err != nil {
		return ReplBatch{}, err
	}
	batch.PrepareID, err = readString(r)
	if err != nil {
		return ReplBatch{}, err
	}
	batch.CommitNo, err = binary.ReadUvarint(r)
	if err != nil {
		return ReplBatch{}, err
	}
	return batch, nil
}

func appendOps(dst []byte, ops []KVOp) ([]byte, error) {
	dst = binary.AppendUvarint(dst, uint64(len(ops)))
	for _, op := range ops {
		dst = appendBytes(dst, op.Key)
		dst = appendBytes(dst, op.Value)
		if op.Delete {
			dst = append(dst, 1)
		} else {
			dst = append(dst, 0)
		}
		if op.Row {
			dst = append(dst, 1)
		} else {
			dst = append(dst, 0)
		}
		dst = appendString(dst, op.Database)
		dst = appendString(dst, op.Table)
		dst = appendBytes(dst, op.Schema)
		dst = binary.AppendUvarint(dst, uint64(op.Op))
		dst = appendBytes(dst, op.Before)
	}
	return dst, nil
}

func readOps(r *bytes.Reader) ([]KVOp, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	ops := make([]KVOp, 0, n)
	for i := uint64(0); i < n; i++ {
		var op KVOp
		op.Key, err = readBytes(r)
		if err != nil {
			return nil, err
		}
		op.Value, err = readBytes(r)
		if err != nil {
			return nil, err
		}
		flag, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		op.Delete = flag != 0
		flag, err = r.ReadByte()
		if err != nil {
			return nil, err
		}
		op.Row = flag != 0
		op.Database, err = readString(r)
		if err != nil {
			return nil, err
		}
		op.Table, err = readString(r)
		if err != nil {
			return nil, err
		}
		op.Schema, err = readBytes(r)
		if err != nil {
			return nil, err
		}
		opn, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		op.Op = int(opn)
		op.Before, err = readBytes(r)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func appendRows(dst []byte, rows []RowChange) ([]byte, error) {
	dst = binary.AppendUvarint(dst, uint64(len(rows)))
	for _, row := range rows {
		dst = appendString(dst, row.Database)
		dst = appendString(dst, row.Table)
		dst = appendBytes(dst, row.Schema)
		dst = binary.AppendUvarint(dst, uint64(row.Op))
		dst = appendBytes(dst, row.Before)
		dst = appendBytes(dst, row.After)
	}
	return dst, nil
}

func readRows(r *bytes.Reader) ([]RowChange, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	rows := make([]RowChange, 0, n)
	for i := uint64(0); i < n; i++ {
		var row RowChange
		row.Database, err = readString(r)
		if err != nil {
			return nil, err
		}
		row.Table, err = readString(r)
		if err != nil {
			return nil, err
		}
		row.Schema, err = readBytes(r)
		if err != nil {
			return nil, err
		}
		opn, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		row.Op = int(opn)
		row.Before, err = readBytes(r)
		if err != nil {
			return nil, err
		}
		row.After, err = readBytes(r)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func readBytes(r *bytes.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func readString(r *bytes.Reader) (string, error) {
	b, err := readBytes(r)
	return string(b), err
}
