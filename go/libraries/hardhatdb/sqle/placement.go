package sqle

import (
	"encoding/json"
	"fmt"
	"strings"
)

// keyPlacements is the Badger key of the shard placement list. It lives in
// the meta store, so every node that replicates meta can route.
var keyPlacements = []byte("placements")

// TablePlacement names the column that places rows of one table, and the
// unique columns checked on every range. The spans themselves live in the
// range list.
type TablePlacement struct {
	DB     string   `json:"db"`
	Table  string   `json:"table"`
	Column string   `json:"column"`
	Check  []string `json:"check,omitempty"`
}

// Key is the lowercase database and table.
func (p TablePlacement) Key() string {
	return strings.ToLower(p.DB) + "." + strings.ToLower(p.Table)
}

// SavePlacement upserts one table's placement and replicates it.
func (s *Store) SavePlacement(p TablePlacement) error {
	if p.DB == "" || p.Table == "" || p.Column == "" {
		return fmt.Errorf("hardhatdb: placement needs a database, table, and column")
	}
	p.DB = strings.ToLower(p.DB)
	p.Table = strings.ToLower(p.Table)
	p.Column = strings.ToLower(p.Column)
	for i, col := range p.Check {
		p.Check[i] = strings.ToLower(col)
	}
	return s.update(func(tx *kvTx) error {
		list, err := readPlacements(tx)
		if err != nil {
			return err
		}
		replaced := false
		for i := range list {
			if list[i].Key() == p.Key() {
				list[i] = p
				replaced = true
				break
			}
		}
		if !replaced {
			list = append(list, p)
		}
		raw, err := json.Marshal(list)
		if err != nil {
			return err
		}
		return tx.root().Put(keyPlacements, raw)
	})
}

// Placements reads the placement list from this store.
func (s *Store) Placements() ([]TablePlacement, error) {
	var list []TablePlacement
	err := s.view(func(tx *kvTx) error {
		var err error
		list, err = readPlacements(tx)
		return err
	})
	return list, err
}

func readPlacements(tx *kvTx) ([]TablePlacement, error) {
	raw := tx.root().Get(keyPlacements)
	if len(raw) == 0 {
		return nil, nil
	}
	var list []TablePlacement
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	return list, nil
}
