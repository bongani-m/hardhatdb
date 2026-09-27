package persist

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	// RangeActive is a range that accepts reads and writes.
	RangeActive = "active"
	// RangeCopying is a range whose right half is being copied to a new group.
	// Writes that touch it fail until the split publishes both halves.
	RangeCopying = "copying"
)

// keyRanges is the Badger key of the range list. It lives in the meta store.
var keyRanges = []byte("ranges")

// RangePeer is one member of the Raft group that stores a key range.
type RangePeer struct {
	ID      string `json:"id"`
	Raft    string `json:"raft"`
	Forward string `json:"forward"`
}

// KeyRange is one half-open span of memcomparable primary keys, [Start, End).
// An empty Start or End is unbounded. Group is the Raft group that stores it.
type KeyRange struct {
	ID    string      `json:"id"`
	DB    string      `json:"db"`
	Table string      `json:"table"`
	Start []byte      `json:"start,omitempty"`
	End   []byte      `json:"end,omitempty"`
	Group string      `json:"group"`
	Peers []RangePeer `json:"peers"`
	State string      `json:"state"`
	// SplitKey, RightID, RightGroup, and RightPeers are set while State is
	// RangeCopying so a restarted leader can finish the same split.
	SplitKey   []byte      `json:"splitKey,omitempty"`
	RightID    string      `json:"rightId,omitempty"`
	RightGroup string      `json:"rightGroup,omitempty"`
	RightPeers []RangePeer `json:"rightPeers,omitempty"`
}

// TableKey is the lowercase database and table.
func (r KeyRange) TableKey() string {
	return strings.ToLower(r.DB) + "." + strings.ToLower(r.Table)
}

// Holds reports that key is inside this span. An empty bound is open.
func (r KeyRange) Holds(key []byte) bool {
	if len(r.Start) > 0 && bytes.Compare(key, r.Start) < 0 {
		return false
	}
	if len(r.End) > 0 && bytes.Compare(key, r.End) >= 0 {
		return false
	}
	return true
}

// OverlapsInclusive reports that this span shares a key with the inclusive
// integer span [lo, hi]. A missing side is unbounded.
func (r KeyRange) OverlapsInclusive(lo, hi []byte, hasLo, hasHi bool) bool {
	if hasLo && len(r.End) > 0 && bytes.Compare(r.End, lo) <= 0 {
		return false
	}
	if hasHi && len(r.Start) > 0 && bytes.Compare(r.Start, hi) > 0 {
		return false
	}
	return true
}

// EncodeIntKey is the memcomparable form of a signed integer primary key.
func EncodeIntKey(id int64) []byte {
	return encodeSigned(id)
}

// DecodeIntKey reads a key produced by EncodeIntKey.
func DecodeIntKey(b []byte) (int64, bool) {
	if len(b) != 9 || b[0] != 0x01 {
		return 0, false
	}
	u := binary.BigEndian.Uint64(b[1:])
	return int64(u ^ (1 << 63)), true
}

// KeySuccessor is the smallest bound strictly above key. It is the exclusive
// end of a span that still contains key, so a split point stays on the left.
func KeySuccessor(key []byte) []byte {
	out := make([]byte, len(key)+1)
	copy(out, key)
	return out
}

// RangesFor returns the ranges stored for one table, in list order.
func RangesFor(list []KeyRange, db, table string) []KeyRange {
	key := strings.ToLower(db) + "." + strings.ToLower(table)
	var out []KeyRange
	for _, r := range list {
		if r.TableKey() == key {
			out = append(out, r)
		}
	}
	return out
}

// RangeHolding returns the range of table that contains key.
func RangeHolding(list []KeyRange, db, table string, key []byte) (KeyRange, bool) {
	for _, r := range RangesFor(list, db, table) {
		if r.Holds(key) {
			return r, true
		}
	}
	return KeyRange{}, false
}

// PutRange inserts or replaces one range and replicates the list.
func (s *Store) PutRange(r KeyRange) error {
	r = normalizeRange(r)
	if r.ID == "" || r.DB == "" || r.Table == "" || r.Group == "" {
		return fmt.Errorf("persist: range needs an id, database, table, and group")
	}
	return s.UpdateRanges(func(list []KeyRange) ([]KeyRange, error) {
		for i := range list {
			if list[i].ID == r.ID {
				list[i] = r
				return list, nil
			}
		}
		return append(list, r), nil
	})
}

// Ranges reads the range list from this store.
func (s *Store) Ranges() ([]KeyRange, error) {
	var list []KeyRange
	err := s.view(func(tx *kvTx) error {
		var err error
		list, err = readRanges(tx)
		return err
	})
	return list, err
}

const (
	// RangeOpMark records a split that is still copying.
	RangeOpMark = "mark"
	// RangeOpPublish replaces one range with its left and right halves.
	RangeOpPublish = "publish"
	// RangeOpAddPeer adds a voter to every range stored by a group.
	RangeOpAddPeer = "add-peer"
	// RangeOpDelPeer removes a voter from every range stored by a group.
	RangeOpDelPeer = "del-peer"
)

// RangeOp is one catalog change. The meta leader applies it inside UpdateRanges.
type RangeOp struct {
	Kind     string    `json:"kind"`
	ID       string    `json:"id,omitempty"`
	SplitKey []byte    `json:"splitKey,omitempty"`
	Bound    []byte    `json:"bound,omitempty"`
	Right    KeyRange  `json:"right,omitempty"`
	Group    string    `json:"group,omitempty"`
	Peer     RangePeer `json:"peer,omitempty"`
}

// RangeCatalog is the meta group as seen by a split. A follower forwards
// ApplyRangeOp to the meta leader.
type RangeCatalog interface {
	Ranges() ([]KeyRange, error)
	ApplyRangeOp(RangeOp) error
}

// ApplyRangeOp replicates one range-catalog change.
func (s *Store) ApplyRangeOp(op RangeOp) error {
	return s.UpdateRanges(func(list []KeyRange) ([]KeyRange, error) {
		return applyRangeOp(list, op)
	})
}

func applyRangeOp(cur []KeyRange, op RangeOp) ([]KeyRange, error) {
	switch op.Kind {
	case RangeOpMark:
		for i := range cur {
			if cur[i].ID != op.ID {
				continue
			}
			if cur[i].State == RangeCopying && len(cur[i].SplitKey) > 0 {
				return cur, nil
			}
			cur[i].State = RangeCopying
			cur[i].SplitKey = append([]byte(nil), op.SplitKey...)
			cur[i].RightID = op.Right.ID
			cur[i].RightGroup = op.Right.Group
			cur[i].RightPeers = append([]RangePeer(nil), op.Right.Peers...)
			return cur, nil
		}
		return nil, fmt.Errorf("persist: range %s disappeared", op.ID)
	case RangeOpPublish:
		var next []KeyRange
		placed := false
		for _, curRange := range cur {
			if curRange.ID != op.ID && curRange.ID != op.Right.ID {
				next = append(next, curRange)
				continue
			}
			if curRange.ID == op.ID {
				left := curRange
				left.End = append([]byte(nil), op.Bound...)
				left.State = RangeActive
				left.SplitKey = nil
				left.RightID = ""
				left.RightGroup = ""
				left.RightPeers = nil
				next = append(next, left)
				placed = true
			}
		}
		if !placed {
			return nil, fmt.Errorf("persist: range %s disappeared", op.ID)
		}
		right := op.Right
		right.State = RangeActive
		right.Start = append([]byte(nil), op.Right.Start...)
		right.End = append([]byte(nil), op.Right.End...)
		return append(next, normalizeRange(right)), nil
	case RangeOpAddPeer:
		for i := range cur {
			if cur[i].Group != op.Group {
				continue
			}
			found := false
			for _, p := range cur[i].Peers {
				if p.ID == op.Peer.ID {
					found = true
					break
				}
			}
			if !found {
				cur[i].Peers = append(cur[i].Peers, op.Peer)
			}
		}
		return cur, nil
	case RangeOpDelPeer:
		for i := range cur {
			if cur[i].Group != op.Group {
				continue
			}
			var kept []RangePeer
			for _, p := range cur[i].Peers {
				if p.ID != op.Peer.ID {
					kept = append(kept, p)
				}
			}
			cur[i].Peers = kept
		}
		return cur, nil
	default:
		return nil, fmt.Errorf("persist: unknown range op %s", op.Kind)
	}
}

// UpdateRanges replaces the range list in one replicated write.
func (s *Store) UpdateRanges(fn func([]KeyRange) ([]KeyRange, error)) error {
	return s.update(func(tx *kvTx) error {
		list, err := readRanges(tx)
		if err != nil {
			return err
		}
		list, err = fn(list)
		if err != nil {
			return err
		}
		if list == nil {
			list = []KeyRange{}
		}
		raw, err := json.Marshal(list)
		if err != nil {
			return err
		}
		return tx.root().Put(keyRanges, raw)
	})
}

func normalizeRange(r KeyRange) KeyRange {
	r.DB = strings.ToLower(r.DB)
	r.Table = strings.ToLower(r.Table)
	if r.State == "" {
		r.State = RangeActive
	}
	return r
}

func readRanges(tx *kvTx) ([]KeyRange, error) {
	raw := tx.root().Get(keyRanges)
	if len(raw) == 0 {
		return nil, nil
	}
	var list []KeyRange
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	return list, nil
}
