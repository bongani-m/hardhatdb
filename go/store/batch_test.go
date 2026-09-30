package store

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func TestCompactBatchRoundTrip(t *testing.T) {
	in := ReplBatch{
		Ops: []KVOp{{
			Key:      []byte("k"),
			Value:    []byte("v"),
			Row:      true,
			Database: "db",
			Table:    "t",
			Schema:   []byte("sch"),
			Op:       2,
			Before:   []byte("old"),
		}},
		Statement: "insert",
		Unix:      42,
		ID:        7,
		Rotate:    true,
		Phase:     PhaseCommit,
		PrepareID: "p",
		CommitNo:  9,
	}
	raw, err := EncodeBatch(in)
	if err != nil {
		t.Fatal(err)
	}
	if raw[0] != batchMagic {
		t.Fatalf("magic %x", raw[0])
	}
	out, err := DecodeBatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	if out.Statement != in.Statement || out.ID != in.ID || !out.Rotate || out.Phase != in.Phase || out.PrepareID != in.PrepareID || out.CommitNo != in.CommitNo {
		t.Fatalf("header %+v", out)
	}
	if len(out.Ops) != 1 || string(out.Ops[0].Key) != "k" || string(out.Ops[0].Before) != "old" || !out.Ops[0].Row {
		t.Fatalf("ops %+v", out.Ops)
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(in); err != nil {
		t.Fatal(err)
	}
	gobRaw := buf.Bytes()
	if len(gobRaw) == 0 || gobRaw[0] == batchMagic {
		t.Fatalf("gob encoding collides with compact magic: %x", gobRaw)
	}
	decoded, err := DecodeBatch(gobRaw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Statement != in.Statement || decoded.ID != in.ID {
		t.Fatalf("gob decode %+v", decoded)
	}
	if len(gobRaw) <= len(raw) {
		t.Fatalf("compact %d is not smaller than gob %d", len(raw), len(gobRaw))
	}
}
