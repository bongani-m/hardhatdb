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
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestListPagination(t *testing.T) {
	rec := perform(t, NewHandler(newSeedStore()), http.MethodGet, "/people", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != jsonContentType {
		t.Fatalf("content type %q", ct)
	}
	body := decode[listDoc](t, rec)
	if body.Page != 1 || body.Size != 2 || body.Total != 4 {
		t.Fatalf("page meta %+v", body)
	}
	if len(body.People) != 2 {
		t.Fatalf("got %d people", len(body.People))
	}
	if body.People[0].Name != "Jane Deo" || body.People[1].Email != "jane@doe.com" {
		t.Fatalf("order %+v", body.People)
	}
	if body.People[0].CreatedAt != "2022-11-01T12:00:00.000001Z" {
		t.Fatalf("created_at %s", body.People[0].CreatedAt)
	}
	if body.People[0].ID != idJaneDeo {
		t.Fatalf("id %d", body.People[0].ID)
	}

	rec = perform(t, NewHandler(newSeedStore()), http.MethodGet, "/people?page=2&size=2", "")
	body = decode[listDoc](t, rec)
	if body.People[0].ID != idJohnDoe || body.People[1].ID != idJohnAlt {
		t.Fatalf("page 2 %+v", body.People)
	}
}

func TestListFilters(t *testing.T) {
	store := newSeedStore()
	target := "/people?name=Jane&email=doe.com&phone=555&created_after=2022-01-01T00:00:00Z&created_before=2023-01-01%2000:00:00&size=1"
	rec := perform(t, NewHandler(store), http.MethodGet, target, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	store.mu.Lock()
	got := store.last
	store.mu.Unlock()
	if got.Name != "Jane" || got.Email != "doe.com" || got.Phone != "555" {
		t.Fatalf("filter %+v", got)
	}
	if got.CreatedAfter == nil || !got.CreatedAfter.Equal(time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("created_after %#v", got.CreatedAfter)
	}
	if got.CreatedBefore == nil || !got.CreatedBefore.Equal(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("created_before %#v", got.CreatedBefore)
	}

	body := decode[listDoc](t, rec)
	if body.Total != 0 || len(body.People) != 0 {
		t.Fatalf("filtered page total=%d people=%+v", body.Total, body.People)
	}

	rec = perform(t, NewHandler(newSeedStore()), http.MethodGet, "/people?name=Jane&size=1", "")
	body = decode[listDoc](t, rec)
	if body.Total != 2 || body.People[0].Email != "janedeo@gmail.com" {
		t.Fatalf("name filter %+v", body)
	}
}

func TestCreateGetUpdateDelete(t *testing.T) {
	store := newSeedStore()
	h := NewHandler(store)

	rec := perform(t, h, http.MethodPost, "/people", `{
		"name": "Ada Lovelace",
		"email": "ada@example.com",
		"phone_numbers": ["111-222-333"],
		"created_at": "2024-05-06T07:08:09.000010Z"
	}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status %d body %s", rec.Code, rec.Body)
	}
	created := decode[personDoc](t, rec)
	if created.ID != 5 {
		t.Fatalf("id %d", created.ID)
	}
	if rec.Header().Get("Location") != personPath(created.ID) {
		t.Fatalf("location %q id %d", rec.Header().Get("Location"), created.ID)
	}

	rec = perform(t, h, http.MethodGet, personPath(created.ID), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get status %d", rec.Code)
	}
	got := decode[personDoc](t, rec)
	if len(got.PhoneNumbers) != 1 || got.PhoneNumbers[0] != "111-222-333" {
		t.Fatalf("phones %#v", got.PhoneNumbers)
	}

	rec = perform(t, h, http.MethodPut, personPath(created.ID), `{
		"name": "Ada Lovelace",
		"email": "ada@example.com",
		"phone_numbers": ["999"],
		"created_at": "2024-01-02 03:04:05"
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("update status %d body %s", rec.Code, rec.Body)
	}
	updated := decode[personDoc](t, rec)
	if updated.ID != created.ID || updated.PhoneNumbers[0] != "999" || updated.CreatedAt != "2024-01-02T03:04:05.000000Z" {
		t.Fatalf("updated %+v", updated)
	}

	rec = perform(t, h, http.MethodDelete, personPath(created.ID), "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("delete body %q", rec.Body)
	}

	rec = perform(t, h, http.MethodGet, personPath(created.ID), "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing status %d", rec.Code)
	}
	missing := decode[errorDoc](t, rec)
	if missing.Error != "not found" {
		t.Fatalf("404 body %+v", missing)
	}
}

func TestBadInput(t *testing.T) {
	h := NewHandler(newSeedStore())

	rec := perform(t, h, http.MethodGet, "/people?size=0", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("size status %d", rec.Code)
	}
	rec = perform(t, h, http.MethodGet, "/people?created_after=yesterday", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("time status %d", rec.Code)
	}
	rec = perform(t, h, http.MethodPost, "/people", `{"email":"a@b.c"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("create status %d", rec.Code)
	}
	rec = perform(t, h, http.MethodGet, "/people/nope", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("id status %d body %s", rec.Code, rec.Body)
	}
	rec = perform(t, h, http.MethodPut, personPath(idJaneDoe), `{"phone_numbers":[],"created_at":"2024-01-02T03:04:05Z"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("put status %d body %s", rec.Code, rec.Body)
	}
	rec = perform(t, h, http.MethodGet, personPath(idMissing), "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing status %d", rec.Code)
	}
}

func TestNodesRoundRobin(t *testing.T) {
	first := unusedDB(t)
	second := unusedDB(t)
	store := NewMySQLStore(first, second).(*mysqlStore)
	var got []*sql.DB
	for i := 0; i < 3; i++ {
		err := store.call(func(db *sql.DB) error {
			got = append(got, db)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if got[0] != first || got[1] != second || got[2] != first {
		t.Fatal("statements did not alternate across nodes")
	}
}

func TestCallUsesNextNodeAfterBrokenConnection(t *testing.T) {
	down := unusedDB(t)
	up := unusedDB(t)
	store := NewMySQLStore(down, up).(*mysqlStore)
	var seen []*sql.DB
	err := store.call(func(db *sql.DB) error {
		seen = append(seen, db)
		if db == down {
			return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0] != down || seen[1] != up {
		t.Fatalf("tried %#v", seen)
	}
}

func TestCallDoesNotRetrySQLError(t *testing.T) {
	first := unusedDB(t)
	second := unusedDB(t)
	store := NewMySQLStore(first, second).(*mysqlStore)
	n := 0
	err := store.call(func(db *sql.DB) error {
		n++
		return &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	})
	if n != 1 {
		t.Fatalf("tries %d", n)
	}
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1062 {
		t.Fatal(err)
	}
}

func TestMySQLAddrs(t *testing.T) {
	t.Setenv("MYSQL_ADDRS", "")
	t.Setenv("MYSQL_HOST", "db.internal")
	t.Setenv("MYSQL_PORT", "3306")
	got := mysqlAddrs()
	if len(got) != 1 || got[0] != "db.internal:3306" {
		t.Fatalf("default %#v", got)
	}
	t.Setenv("MYSQL_ADDRS", " 127.0.0.1:3306, ,127.0.0.1:3307 ")
	got = mysqlAddrs()
	if len(got) != 2 || got[0] != "127.0.0.1:3306" || got[1] != "127.0.0.1:3307" {
		t.Fatalf("list %#v", got)
	}
}

func TestSplitAddrs(t *testing.T) {
	if splitAddrs("") != nil {
		t.Fatal("empty list")
	}
	got := splitAddrs(" 127.0.0.1:3307, ,127.0.0.1:3308 ")
	if len(got) != 2 || got[0] != "127.0.0.1:3307" || got[1] != "127.0.0.1:3308" {
		t.Fatalf("addrs %#v", got)
	}
}

func unusedDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:1)/mydb")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestLikePatternEscapesWildcards(t *testing.T) {
	if got := likeContains(`100%_a\b`); got != `%100\%\_a\\b%` {
		t.Fatalf("like pattern %q", got)
	}
}

func perform(t *testing.T, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rdr)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func personPath(id int64) string {
	return "/people/" + strconv.FormatInt(id, 10)
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body)
	}
	return v
}

type memoryStore struct {
	mu     sync.Mutex
	people []Person
	last   Filter
}

const (
	idJaneDeo int64 = 1
	idJaneDoe int64 = 2
	idJohnDoe int64 = 3
	idJohnAlt int64 = 4
	idMissing int64 = 999
)

func newSeedStore() *memoryStore {
	created := time.Unix(0, 1667304000000001000).UTC()
	return &memoryStore{people: []Person{
		{ID: idJaneDeo, Name: "Jane Deo", Email: "janedeo@gmail.com", PhoneNumbers: []string{"556-565-566", "777-777-777"}, CreatedAt: created},
		{ID: idJaneDoe, Name: "Jane Doe", Email: "jane@doe.com", PhoneNumbers: []string{}, CreatedAt: created},
		{ID: idJohnDoe, Name: "John Doe", Email: "john@doe.com", PhoneNumbers: []string{"555-555-555"}, CreatedAt: created},
		{ID: idJohnAlt, Name: "John Doe", Email: "johnalt@doe.com", PhoneNumbers: []string{}, CreatedAt: created},
	}}
}

func (m *memoryStore) List(_ context.Context, f Filter, page, size int) (ListResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.last = f
	matched := make([]Person, 0, len(m.people))
	for _, p := range m.people {
		if !matchPerson(p, f) {
			continue
		}
		cp := p
		cp.PhoneNumbers = append([]string(nil), p.PhoneNumbers...)
		matched = append(matched, cp)
	}
	sortPeople(matched)
	total := len(matched)
	start := (page - 1) * size
	if start > total {
		start = total
	}
	end := start + size
	if end > total {
		end = total
	}
	return ListResult{People: append([]Person(nil), matched[start:end]...), Total: total}, nil
}

func (m *memoryStore) Get(_ context.Context, id int64) (Person, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.people {
		if p.ID == id {
			p.PhoneNumbers = append([]string(nil), p.PhoneNumbers...)
			return p, nil
		}
	}
	return Person{}, ErrNotFound
}

func (m *memoryStore) Insert(_ context.Context, p Person) (Person, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p.ID = 1
	for _, existing := range m.people {
		if existing.ID >= p.ID {
			p.ID = existing.ID + 1
		}
	}
	p.PhoneNumbers = append([]string(nil), normalizePhones(p.PhoneNumbers)...)
	m.people = append(m.people, p)
	return p, nil
}

func (m *memoryStore) Update(_ context.Context, p Person) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.people {
		if existing.ID == p.ID {
			p.PhoneNumbers = append([]string(nil), p.PhoneNumbers...)
			m.people[i] = p
			return nil
		}
	}
	return ErrNotFound
}

func (m *memoryStore) Delete(_ context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.people {
		if existing.ID == id {
			m.people = append(m.people[:i], m.people[i+1:]...)
			return nil
		}
	}
	return ErrNotFound
}

func matchPerson(p Person, f Filter) bool {
	if f.Name != "" && !strings.Contains(p.Name, f.Name) {
		return false
	}
	if f.Email != "" && !strings.Contains(p.Email, f.Email) {
		return false
	}
	if f.Phone != "" && !strings.Contains(strings.Join(p.PhoneNumbers, " "), f.Phone) {
		return false
	}
	if f.CreatedAfter != nil && p.CreatedAt.Before(*f.CreatedAfter) {
		return false
	}
	if f.CreatedBefore != nil && p.CreatedAt.After(*f.CreatedBefore) {
		return false
	}
	return true
}

func sortPeople(people []Person) {
	for i := 1; i < len(people); i++ {
		j := i
		for j > 0 && (people[j].Name < people[j-1].Name || (people[j].Name == people[j-1].Name && people[j].Email < people[j-1].Email)) {
			people[j], people[j-1] = people[j-1], people[j]
			j--
		}
	}
}
