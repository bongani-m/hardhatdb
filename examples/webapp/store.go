// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

const peopleTable = "mytable"

var (
	// ErrNotFound is returned when no row matches the id.
	ErrNotFound = errors.New("not found")
	// ErrConflict is returned when a row with the same primary key already exists.
	ErrConflict = errors.New("conflict")
)

// Person is one row of the example server's mytable.
type Person struct {
	ID           int64
	Name         string
	Email        string
	PhoneNumbers []string
	CreatedAt    time.Time
}

// Filter selects rows. Empty fields are ignored and the rest are combined with AND.
type Filter struct {
	Name          string
	Email         string
	Phone         string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
}

// ListResult is one page of people plus the unpaged match count.
type ListResult struct {
	People []Person
	Total  int
}

// Store is the persistence used by the HTTP API.
type Store interface {
	List(ctx context.Context, f Filter, page, size int) (ListResult, error)
	Get(ctx context.Context, id int64) (Person, error)
	Insert(ctx context.Context, p Person) (Person, error)
	Update(ctx context.Context, p Person) error
	Delete(ctx context.Context, id int64) error
}

type mysqlStore struct {
	nodes []*sql.DB
	next  atomic.Uint64
}

// NewMySQLStore talks to every address. Reads and writes round-robin across
// the nodes. A broken connection tries the next node.
func NewMySQLStore(nodes ...*sql.DB) Store {
	return &mysqlStore{nodes: nodes}
}

// call runs fn on the next node. A broken connection tries each other node once.
func (s *mysqlStore) call(fn func(*sql.DB) error) error {
	n := len(s.nodes)
	start := int(s.next.Add(1) - 1)
	var last error
	for i := 0; i < n; i++ {
		err := fn(s.nodes[(start+i)%n])
		if err == nil || !connErr(err) {
			return err
		}
		last = err
	}
	return last
}

func (s *mysqlStore) List(ctx context.Context, f Filter, page, size int) (ListResult, error) {
	var out ListResult
	err := s.call(func(db *sql.DB) error {
		where, args := f.where()
		var total int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+peopleTable+where, args...).Scan(&total); err != nil {
			return err
		}

		offset := (page - 1) * size
		listArgs := append(append([]any{}, args...), size, offset)
		rows, err := db.QueryContext(ctx,
			"SELECT id, name, email, phone_numbers, created_at FROM "+peopleTable+where+" ORDER BY name, email LIMIT ? OFFSET ?",
			listArgs...,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		people := make([]Person, 0)
		for rows.Next() {
			p, err := scanPerson(rows)
			if err != nil {
				return err
			}
			people = append(people, p)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		out = ListResult{People: people, Total: total}
		return nil
	})
	return out, err
}

func (s *mysqlStore) Get(ctx context.Context, id int64) (Person, error) {
	var out Person
	err := s.call(func(db *sql.DB) error {
		row := db.QueryRowContext(ctx,
			"SELECT id, name, email, phone_numbers, created_at FROM "+peopleTable+" WHERE id = ?",
			id,
		)
		p, err := scanPerson(row)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		out = p
		return nil
	})
	return out, err
}

func (s *mysqlStore) Insert(ctx context.Context, p Person) (Person, error) {
	phones, err := json.Marshal(normalizePhones(p.PhoneNumbers))
	if err != nil {
		return Person{}, err
	}
	var out Person
	err = s.call(func(db *sql.DB) error {
		res, err := db.ExecContext(ctx,
			"INSERT INTO "+peopleTable+" (name, email, phone_numbers, created_at) VALUES (?, ?, ?, ?)",
			p.Name, p.Email, phones, p.CreatedAt.UTC(),
		)
		if err != nil {
			return mapSQL(err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		out = p
		out.ID = id
		out.PhoneNumbers = normalizePhones(p.PhoneNumbers)
		return nil
	})
	return out, err
}

func (s *mysqlStore) Update(ctx context.Context, p Person) error {
	phones, err := json.Marshal(normalizePhones(p.PhoneNumbers))
	if err != nil {
		return err
	}
	return s.call(func(db *sql.DB) error {
		res, err := db.ExecContext(ctx,
			"UPDATE "+peopleTable+" SET name = ?, email = ?, phone_numbers = ?, created_at = ? WHERE id = ?",
			p.Name, p.Email, phones, p.CreatedAt.UTC(), p.ID,
		)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

func (s *mysqlStore) Delete(ctx context.Context, id int64) error {
	return s.call(func(db *sql.DB) error {
		res, err := db.ExecContext(ctx,
			"DELETE FROM "+peopleTable+" WHERE id = ?",
			id,
		)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// connErr reports a failure to reach the node. A SQL result is not a broken connection.
func connErr(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, sql.ErrConnDone) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 2002, 2003, 2006, 2013:
			return true
		}
	}
	return false
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanPerson(row rowScanner) (Person, error) {
	var p Person
	var phones []byte
	if err := row.Scan(&p.ID, &p.Name, &p.Email, &phones, &p.CreatedAt); err != nil {
		return Person{}, err
	}
	p.CreatedAt = p.CreatedAt.UTC()
	if len(phones) == 0 {
		p.PhoneNumbers = []string{}
		return p, nil
	}
	if err := json.Unmarshal(phones, &p.PhoneNumbers); err != nil {
		return Person{}, err
	}
	p.PhoneNumbers = normalizePhones(p.PhoneNumbers)
	return p, nil
}

func (f Filter) where() (string, []any) {
	var conds []string
	var args []any
	if f.Name != "" {
		conds = append(conds, "name LIKE ?")
		args = append(args, likeContains(f.Name))
	}
	if f.Email != "" {
		conds = append(conds, "email LIKE ?")
		args = append(args, likeContains(f.Email))
	}
	if f.Phone != "" {
		conds = append(conds, "CAST(phone_numbers AS CHAR) LIKE ?")
		args = append(args, likeContains(f.Phone))
	}
	if f.CreatedAfter != nil {
		conds = append(conds, "created_at >= ?")
		args = append(args, f.CreatedAfter.UTC())
	}
	if f.CreatedBefore != nil {
		conds = append(conds, "created_at <= ?")
		args = append(args, f.CreatedBefore.UTC())
	}
	if len(conds) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

// likeContains matches a substring. MySQL's default LIKE escape is backslash.
func likeContains(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('%')
	for _, r := range s {
		switch r {
		case '\\', '%', '_':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('%')
	return b.String()
}

func normalizePhones(phones []string) []string {
	if phones == nil {
		return []string{}
	}
	return phones
}

func mapSQL(err error) error {
	if err == nil {
		return nil
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == 1062 {
		return ErrConflict
	}
	return err
}
