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

func TestChunkFieldsRoundTrip(t *testing.T) {
	in := ReplBatch{
		Ops:        []KVOp{{Key: []byte("k"), Value: []byte("v")}},
		Statement:  "commit",
		Unix:       7,
		ID:         3,
		Phase:      PhaseCommit,
		PrepareID:  "txn",
		CommitNo:   4,
		Chunk:      2,
		ChunkCount: 5,
	}
	raw, err := EncodeBatch(in)
	if err != nil {
		t.Fatal(err)
	}
	if raw[1] != batchVersion2 {
		t.Fatalf("version %d", raw[1])
	}
	out, err := DecodeBatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	if out.Chunk != 2 || out.ChunkCount != 5 || out.Phase != PhaseCommit || out.PrepareID != "txn" {
		t.Fatalf("header %+v", out)
	}
	if len(out.Ops) != 1 || string(out.Ops[0].Value) != "v" {
		t.Fatalf("ops %+v", out.Ops)
	}
}

func TestChunkOpsStayWithinLimit(t *testing.T) {
	var ops []KVOp
	for i := 0; i < 50; i++ {
		ops = append(ops, KVOp{
			Key:   []byte("key-" + string(rune('a'+i%26)) + string(rune('0'+i/26))),
			Value: bytes.Repeat([]byte("x"), 40),
			Row:   true,
		})
	}
	const limit = 300
	parts := ChunkOps(ops, limit, "txn")
	if len(parts) < 2 {
		t.Fatalf("parts %d", len(parts))
	}
	var n int
	for i, part := range parts {
		if len(part) == 0 {
			t.Fatalf("empty part %d", i)
		}
		n += len(part)
		raw, err := EncodeBatch(ReplBatch{
			Ops:       part,
			Phase:     PhaseStage,
			PrepareID: "txn",
			Chunk:     uint64(i),
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(part) > 1 && len(raw) > limit {
			t.Fatalf("part %d encoded %d", i, len(raw))
		}
		if StagedLen(part, "txn") < len(raw) {
			t.Fatalf("estimate %d < encoded %d", StagedLen(part, "txn"), len(raw))
		}
	}
	if n != len(ops) {
		t.Fatalf("kept %d of %d", n, len(ops))
	}
}
