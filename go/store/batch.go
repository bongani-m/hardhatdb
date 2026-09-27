package store

import (
	"bytes"
	"encoding/gob"
)

func init() {
	// Raft logs and forward messages written before the package split use the
	// original type names.
	gob.RegisterName("github.com/bongani-m/persist.kvOp", KVOp{})
	gob.RegisterName("github.com/bongani-m/persist.rowChange", RowChange{})
	gob.RegisterName("github.com/bongani-m/persist.replBatch", ReplBatch{})
}

// Peer is one voter in a Raft group.
type Peer struct {
	ID      string
	Address string
}

// RaftStatus is one process's view of a Raft group. A standalone store reports
// role "standalone" and zero indexes.
type RaftStatus struct {
	Role    string
	Leader  string
	Commit  uint64
	Applied uint64
	Lag     uint64
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

// EncodeBatch serializes a Raft log entry.
func EncodeBatch(batch ReplBatch) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(batch); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeBatch reads a Raft log entry.
func DecodeBatch(raw []byte) (ReplBatch, error) {
	var batch ReplBatch
	err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&batch)
	return batch, err
}
