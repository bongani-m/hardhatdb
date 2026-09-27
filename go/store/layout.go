package store

import (
	"bytes"
	"encoding/binary"
)

// RawMark prefixes row and index keys stored in key order. Ten 0xFF bytes
// overflow a uvarint, so these keys are not bucket entries, and a constant
// prefix keeps their relative order equal to the memcomparable key.
var RawMark = bytes.Repeat([]byte{0xFF}, binary.MaxVarintLen64)

const (
	// KindValue marks a bucket entry that stores a value.
	KindValue byte = 0
	// KindBucket marks a bucket entry that is a child bucket.
	KindBucket byte = 1
)

// EntryKey encodes one path component as uvarint(len) || name || kind.
// A bucket's children are stored under its sentinel key, so a prefix scan of
// that sentinel yields the bucket and everything inside it.
func EntryKey(prefix, name []byte, kind byte) []byte {
	var n [binary.MaxVarintLen64]byte
	nn := binary.PutUvarint(n[:], uint64(len(name)))
	key := make([]byte, 0, len(prefix)+nn+len(name)+1)
	key = append(key, prefix...)
	key = append(key, n[:nn]...)
	key = append(key, name...)
	key = append(key, kind)
	return key
}

// ParseComponent splits one path component off rest.
func ParseComponent(rest []byte) (name []byte, kind byte, n int, ok bool) {
	length, k := binary.Uvarint(rest)
	if k <= 0 || k >= len(rest) {
		return nil, 0, 0, false
	}
	if length > uint64(len(rest)-k-1) {
		return nil, 0, 0, false
	}
	nameEnd := k + int(length)
	kind = rest[nameEnd]
	if kind != KindValue && kind != KindBucket {
		return nil, 0, 0, false
	}
	return rest[k:nameEnd], kind, nameEnd + 1, true
}
