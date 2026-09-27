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

func TestHomePage(t *testing.T) {
	rec := perform(t, NewHandler(newSeedStore()), http.MethodGet, "/", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Ada Lovelace") || !strings.Contains(body, "leader") {
		t.Fatalf("page %s", body)
	}
	if !strings.Contains(body, `value="n2:3306"`) {
		t.Fatalf("write targets %s", body)
	}
}

func TestCreateFormShowsOtherNodeMissing(t *testing.T) {
	body := "name=Lin&email=lin@example.com&status=1&tags=demo&note=hello&node=n2:3306"
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	NewHandler(newSeedStore()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	page := rec.Body.String()
	if !strings.Contains(page, "Wrote on n2:3306.") || !strings.Contains(page, "Same connection: Lin.") {
		t.Fatalf("write result %s", page)
	}
	if !strings.Contains(page, "Other node n3:3306: missing.") {
		t.Fatalf("other node %s", page)
	}
}

func TestListPagination(t *testing.T) {
	rec := perform(t, NewHandler(newSeedStore()), http.MethodGet, "/accounts?size=2", "")
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
	if len(body.Accounts) != 2 {
		t.Fatalf("got %d accounts", len(body.Accounts))
	}
	if body.Accounts[0].Name != "Ada Lovelace" || body.Accounts[1].Email != "alan@example.com" {
		t.Fatalf("order %+v", body.Accounts)
	}
	if body.Accounts[0].CreatedAt != "2022-11-01T12:00:00.000001Z" {
		t.Fatalf("created_at %s", body.Accounts[0].CreatedAt)
	}
	if len(body.Accounts[0].Notes) != 2 || body.Accounts[0].Notes[0].Body != "Wrote the first program" {
		t.Fatalf("notes %+v", body.Accounts[0].Notes)
	}

	rec = perform(t, NewHandler(newSeedStore()), http.MethodGet, "/accounts?page=2&size=2", "")
	body = decode[listDoc](t, rec)
	if body.Accounts[0].Email != "grace@example.com" || body.Accounts[1].Email != "katherine@example.com" {
		t.Fatalf("page 2 %+v", body.Accounts)
	}
}

func TestListFilters(t *testing.T) {
	store := newSeedStore()
	target := "/accounts?name=Ada&email=example.com&tag=demo&status=1&created_after=2022-01-01T00:00:00Z&created_before=2023-01-01%2000:00:00&size=1"
	rec := perform(t, NewHandler(store), http.MethodGet, target, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	store.mu.Lock()
	got := store.last
	store.mu.Unlock()
	if got.Name != "Ada" || got.Email != "example.com" || got.Tag != "demo" || got.Status == nil || *got.Status != 1 {
		t.Fatalf("filter %+v", got)
	}
	if got.CreatedAfter == nil || !got.CreatedAfter.Equal(time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("created_after %#v", got.CreatedAfter)
	}
	if got.CreatedBefore == nil || !got.CreatedBefore.Equal(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("created_before %#v", got.CreatedBefore)
	}

	body := decode[listDoc](t, rec)
	if body.Total != 1 || body.Accounts[0].Email != "ada@example.com" {
		t.Fatalf("filtered page %+v", body)
	}
}

func TestCreateGetUpdateDelete(t *testing.T) {
	store := newSeedStore()
	h := NewHandler(store)

	rec := perform(t, h, http.MethodPost, "/accounts", `{
		"name": "Lin",
		"email": "lin@example.com",
		"status": 1,
		"tags": ["demo"],
		"note": "hello",
		"node": "n2:3306",
		"created_at": "2024-05-06T07:08:09.000010Z"
	}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status %d body %s", rec.Code, rec.Body)
	}
	created := decode[insertDoc](t, rec)
	if created.Account.ID != 5 || created.WroteOn != "n2:3306" {
		t.Fatalf("created %+v", created)
	}
	if created.ReadBack == nil || created.ReadBack.Name != "Lin" || len(created.ReadBack.Notes) != 1 {
		t.Fatalf("read back %+v", created.ReadBack)
	}
	if created.OtherNode.Addr != "n3:3306" || created.OtherNode.Account != nil {
		t.Fatalf("other %+v", created.OtherNode)
	}
	if rec.Header().Get("Location") != accountPath(created.Account.ID) {
		t.Fatalf("location %q", rec.Header().Get("Location"))
	}

	rec = perform(t, h, http.MethodGet, accountPath(created.Account.ID), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get status %d", rec.Code)
	}
	got := decode[accountDoc](t, rec)
	if len(got.Notes) != 1 || got.Notes[0].Body != "hello" {
		t.Fatalf("notes %#v", got.Notes)
	}

	rec = perform(t, h, http.MethodPut, accountPath(created.Account.ID), `{
		"name": "Lin",
		"email": "lin@example.com",
		"status": 0,
		"tags": ["later"],
		"created_at": "2024-01-02 03:04:05"
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("update status %d body %s", rec.Code, rec.Body)
	}
	updated := decode[accountDoc](t, rec)
	if updated.Status != 0 || updated.Tags[0] != "later" || updated.CreatedAt != "2024-01-02T03:04:05.000000Z" {
		t.Fatalf("updated %+v", updated)
	}

	rec = perform(t, h, http.MethodDelete, accountPath(created.Account.ID), "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("delete body %q", rec.Body)
	}
	rec = perform(t, h, http.MethodGet, accountPath(created.Account.ID), "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing status %d", rec.Code)
	}
}

func TestBadInput(t *testing.T) {
	h := NewHandler(newSeedStore())

	rec := perform(t, h, http.MethodGet, "/accounts?size=0", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("size status %d", rec.Code)
	}
	rec = perform(t, h, http.MethodGet, "/accounts?created_after=yesterday", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("time status %d", rec.Code)
	}
	rec = perform(t, h, http.MethodPost, "/accounts", `{"email":"a@b.c"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("create status %d", rec.Code)
	}
	rec = perform(t, h, http.MethodGet, "/accounts/nope", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("id status %d body %s", rec.Code, rec.Body)
	}
	rec = perform(t, h, http.MethodPut, accountPath(idAda), `{"tags":[],"created_at":"2024-01-02T03:04:05Z"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("put status %d body %s", rec.Code, rec.Body)
	}
	rec = perform(t, h, http.MethodGet, accountPath(idMissing), "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing status %d", rec.Code)
	}
}

func TestConflict(t *testing.T) {
	rec := perform(t, NewHandler(newSeedStore()), http.MethodPost, "/accounts", `{
		"name": "Ada Lovelace",
		"email": "ada@example.com",
		"note": "again"
	}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	body := decode[errorDoc](t, rec)
	if body.Error != "account already exists" {
		t.Fatalf("body %+v", body)
	}
}

func TestCluster(t *testing.T) {
	rec := perform(t, NewHandler(newSeedStore()), http.MethodGet, "/cluster", "")
	body := decode[clusterDoc](t, rec)
	if len(body.Nodes) != 3 || body.Nodes[0].Role != "leader" || body.Nodes[1].Role != "follower" {
		t.Fatalf("nodes %+v", body.Nodes)
	}
	if body.Nodes[0].Lag != "0" || body.Nodes[2].Suffrage != "voter" {
		t.Fatalf("status %+v", body.Nodes)
	}
}

func TestWithAddrPrefersNode(t *testing.T) {
	first := unusedDB(t)
	second := unusedDB(t)
	store := NewMySQLStore([]string{"n1:3306", "n2:3306"}, []*sql.DB{first, second}).(*mysqlStore)
	var got *sql.DB
	addr, err := store.withAddr("n2:3306", func(db *sql.DB, addr string) error {
		got = db
		if addr != "n2:3306" {
			t.Fatalf("addr %s", addr)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if addr != "n2:3306" || got != second {
		t.Fatalf("addr %s db %v", addr, got)
	}
}

func TestWithAddrUsesNextNodeAfterBrokenConnection(t *testing.T) {
	down := unusedDB(t)
	up := unusedDB(t)
	store := NewMySQLStore([]string{"down:3306", "up:3306"}, []*sql.DB{down, up}).(*mysqlStore)
	var seen []string
	addr, err := store.withAddr("", func(db *sql.DB, addr string) error {
		seen = append(seen, addr)
		if db == down {
			return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if addr != "up:3306" || len(seen) != 2 || seen[0] != "down:3306" || seen[1] != "up:3306" {
		t.Fatalf("tried %#v addr %s", seen, addr)
	}
}

func TestWithAddrDoesNotRetrySQLError(t *testing.T) {
	first := unusedDB(t)
	second := unusedDB(t)
	store := NewMySQLStore([]string{"a:3306", "b:3306"}, []*sql.DB{first, second}).(*mysqlStore)
	n := 0
	_, err := store.withAddr("", func(db *sql.DB, addr string) error {
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

func TestStatusRecordsDialError(t *testing.T) {
	store := NewMySQLStore([]string{"127.0.0.1:1"}, []*sql.DB{unusedDB(t)}).(*mysqlStore)
	nodes := store.Status(context.Background())
	if len(nodes) != 1 || nodes[0].Err == "" || nodes[0].Role != "" {
		t.Fatalf("status %+v", nodes)
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

func accountPath(id int64) string {
	return "/accounts/" + strconv.FormatInt(id, 10)
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
	mu       sync.Mutex
	accounts []Account
	last     Filter
	addrs    []string
}

const (
	idAda     int64 = 1
	idMissing int64 = 999
)

func newSeedStore() *memoryStore {
	created := time.Unix(0, 1667304000000001000).UTC()
	note := func(id, accountID int64, body string) Note {
		return Note{ID: id, AccountID: accountID, Body: body, CreatedAt: created}
	}
	return &memoryStore{
		addrs: []string{"n1:3306", "n2:3306", "n3:3306"},
		accounts: []Account{
			{ID: 1, Name: "Ada Lovelace", Email: "ada@example.com", Status: 1, Tags: []string{"demo"}, CreatedAt: created, Notes: []Note{note(1, 1, "Wrote the first program"), note(2, 1, "Cluster demo")}},
			{ID: 2, Name: "Grace Hopper", Email: "grace@example.com", Status: 1, Tags: []string{"demo", "compiler"}, CreatedAt: created, Notes: []Note{note(3, 2, "A compiler is a program"), note(4, 2, "Second note")}},
			{ID: 3, Name: "Alan Turing", Email: "alan@example.com", Status: 0, Tags: []string{}, CreatedAt: created, Notes: []Note{note(5, 3, "Can machines think"), note(6, 3, "Second note")}},
			{ID: 4, Name: "Katherine Johnson", Email: "katherine@example.com", Status: 1, Tags: []string{"orbit"}, CreatedAt: created, Notes: []Note{note(7, 4, "Calculated the trajectory"), note(8, 4, "Second note")}},
		},
	}
}

func (m *memoryStore) Addrs() []string {
	return append([]string(nil), m.addrs...)
}

func (m *memoryStore) Status(context.Context) []NodeStatus {
	out := make([]NodeStatus, len(m.addrs))
	for i, addr := range m.addrs {
		role := "follower"
		if i == 0 {
			role = "leader"
		}
		out[i] = NodeStatus{
			Addr:         addr,
			Role:         role,
			Leader:       "10.116.0.2:7001",
			CommitIndex:  "3",
			AppliedIndex: "3",
			Lag:          "0",
			Suffrage:     "voter",
		}
	}
	return out
}

func (m *memoryStore) List(_ context.Context, f Filter, page, size int, _ string) (ListResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.last = f
	matched := make([]Account, 0, len(m.accounts))
	for _, account := range m.accounts {
		if !matchAccount(account, f) {
			continue
		}
		matched = append(matched, copyAccount(account))
	}
	sortAccounts(matched)
	total := len(matched)
	start := (page - 1) * size
	if start > total {
		start = total
	}
	end := start + size
	if end > total {
		end = total
	}
	return ListResult{Accounts: append([]Account(nil), matched[start:end]...), Total: total}, nil
}

func (m *memoryStore) Get(_ context.Context, id int64, _ string) (Account, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, account := range m.accounts {
		if account.ID == id {
			return copyAccount(account), nil
		}
	}
	return Account{}, ErrNotFound
}

func (m *memoryStore) Insert(_ context.Context, in InsertInput) (InsertResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.accounts {
		if existing.Email == in.Email {
			return InsertResult{}, ErrConflict
		}
	}
	account := Account{
		ID:        1,
		Name:      in.Name,
		Email:     in.Email,
		Status:    in.Status,
		Tags:      append([]string(nil), normalizeTags(in.Tags)...),
		CreatedAt: in.CreatedAt.UTC(),
	}
	for _, existing := range m.accounts {
		if existing.ID >= account.ID {
			account.ID = existing.ID + 1
		}
	}
	account.Notes = []Note{{ID: account.ID, AccountID: account.ID, Body: in.Note, CreatedAt: account.CreatedAt}}
	m.accounts = append(m.accounts, copyAccount(account))
	wrote := m.addrs[0]
	for _, addr := range m.addrs {
		if addr == in.Node {
			wrote = addr
			break
		}
	}
	other := ""
	for i, addr := range m.addrs {
		if addr == wrote && len(m.addrs) > 1 {
			other = m.addrs[(i+1)%len(m.addrs)]
		}
	}
	read := copyAccount(account)
	return InsertResult{Account: account, WroteOn: wrote, ReadBack: &read, OtherAddr: other}, nil
}

func (m *memoryStore) Update(_ context.Context, a Account, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.accounts {
		if existing.ID == a.ID {
			a.Notes = append([]Note(nil), existing.Notes...)
			a.Tags = append([]string(nil), a.Tags...)
			m.accounts[i] = a
			return nil
		}
	}
	return ErrNotFound
}

func (m *memoryStore) Delete(_ context.Context, id int64, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.accounts {
		if existing.ID == id {
			m.accounts = append(m.accounts[:i], m.accounts[i+1:]...)
			return nil
		}
	}
	return ErrNotFound
}

func copyAccount(account Account) Account {
	account.Tags = append([]string(nil), account.Tags...)
	account.Notes = append([]Note(nil), account.Notes...)
	return account
}

func matchAccount(account Account, f Filter) bool {
	if f.Name != "" && !strings.Contains(account.Name, f.Name) {
		return false
	}
	if f.Email != "" && !strings.Contains(account.Email, f.Email) {
		return false
	}
	if f.Tag != "" && !strings.Contains(strings.Join(account.Tags, " "), f.Tag) {
		return false
	}
	if f.Status != nil && account.Status != *f.Status {
		return false
	}
	if f.CreatedAfter != nil && account.CreatedAt.Before(*f.CreatedAfter) {
		return false
	}
	if f.CreatedBefore != nil && account.CreatedAt.After(*f.CreatedBefore) {
		return false
	}
	return true
}

func sortAccounts(accounts []Account) {
	for i := 1; i < len(accounts); i++ {
		j := i
		for j > 0 && (accounts[j].Name < accounts[j-1].Name || (accounts[j].Name == accounts[j-1].Name && accounts[j].Email < accounts[j-1].Email)) {
			accounts[j], accounts[j-1] = accounts[j-1], accounts[j]
			j--
		}
	}
}
