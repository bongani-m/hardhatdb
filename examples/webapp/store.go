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
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	accountsTable = "accounts"
	notesTable    = "notes"
)

var (
	// ErrNotFound is returned when no row matches the id.
	ErrNotFound = errors.New("not found")
	// ErrConflict is returned when a row with the same email already exists.
	ErrConflict = errors.New("conflict")
)

// Account is one row of mydb.accounts, with its notes.
type Account struct {
	ID        int64
	Name      string
	Email     string
	Status    int
	Tags      []string
	CreatedAt time.Time
	Notes     []Note
}

// Note is one row of mydb.notes.
type Note struct {
	ID        int64
	AccountID int64
	Body      string
	CreatedAt time.Time
}

// Filter selects accounts. Empty fields are ignored and the rest are combined with AND.
type Filter struct {
	Name          string
	Email         string
	Tag           string
	Status        *int
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
}

// ListResult is one page of accounts plus the unpaged match count.
type ListResult struct {
	Accounts []Account
	Total    int
}

// InsertInput is a new account and the note written in the same transaction.
type InsertInput struct {
	Name      string
	Email     string
	Status    int
	Tags      []string
	Note      string
	CreatedAt time.Time
	Node      string
	// ID is the shard column. Zero leaves it to AUTO_INCREMENT. A sharded
	// insert has to send it so the row can be placed on one range.
	ID int64
}

// InsertResult is the committed account, the read on that connection, and a read on another node.
type InsertResult struct {
	Account   Account
	WroteOn   string
	ReadBack  *Account
	OtherAddr string
	Other     *Account
}

// NodeStatus is one SHOW RAFT STATUS row, or the error from dialing that address.
type NodeStatus struct {
	Addr         string `json:"addr"`
	Role         string `json:"role,omitempty"`
	Leader       string `json:"leader,omitempty"`
	CommitIndex  string `json:"commit_index,omitempty"`
	AppliedIndex string `json:"applied_index,omitempty"`
	Lag          string `json:"lag,omitempty"`
	Suffrage     string `json:"suffrage,omitempty"`
	Err          string `json:"error,omitempty"`
}

// Store is the persistence used by the HTTP API.
type Store interface {
	Addrs() []string
	Status(ctx context.Context) []NodeStatus
	List(ctx context.Context, f Filter, page, size int, node string) (ListResult, error)
	Get(ctx context.Context, id int64, node string) (Account, error)
	Insert(ctx context.Context, in InsertInput) (InsertResult, error)
	Update(ctx context.Context, a Account, node string) error
	Delete(ctx context.Context, id int64, node string) error
}

type mysqlStore struct {
	addrs []string
	nodes []*sql.DB
}

// NewMySQLStore talks to every address. One request stays on one node.
// A broken connection tries the next address.
func NewMySQLStore(addrs []string, nodes []*sql.DB) Store {
	return &mysqlStore{
		addrs: append([]string(nil), addrs...),
		nodes: nodes,
	}
}

func (s *mysqlStore) Addrs() []string {
	return append([]string(nil), s.addrs...)
}

func (s *mysqlStore) Status(ctx context.Context) []NodeStatus {
	out := make([]NodeStatus, len(s.nodes))
	for i, db := range s.nodes {
		out[i].Addr = s.addrs[i]
		var role, leader, commit, applied, lag, suffrage string
		err := db.QueryRowContext(ctx, "SHOW RAFT STATUS").Scan(&role, &leader, &commit, &applied, &lag, &suffrage)
		if err != nil {
			out[i].Err = err.Error()
			continue
		}
		out[i].Role = role
		out[i].Leader = leader
		out[i].CommitIndex = commit
		out[i].AppliedIndex = applied
		out[i].Lag = lag
		out[i].Suffrage = suffrage
	}
	return out
}

// withAddr runs fn on the preferred node. A broken connection tries each other node once.
func (s *mysqlStore) withAddr(prefer string, fn func(*sql.DB, string) error) (string, error) {
	start := 0
	for i, addr := range s.addrs {
		if prefer != "" && addr == prefer {
			start = i
			break
		}
	}
	var last error
	for i := 0; i < len(s.nodes); i++ {
		idx := (start + i) % len(s.nodes)
		err := fn(s.nodes[idx], s.addrs[idx])
		if err == nil {
			return s.addrs[idx], nil
		}
		if !connErr(err) {
			return "", err
		}
		last = err
	}
	return "", last
}

func (s *mysqlStore) otherAddr(addr string) string {
	if len(s.addrs) < 2 {
		return ""
	}
	for i, candidate := range s.addrs {
		if candidate == addr {
			return s.addrs[(i+1)%len(s.addrs)]
		}
	}
	return s.addrs[0]
}

func (s *mysqlStore) dbFor(addr string) *sql.DB {
	for i, candidate := range s.addrs {
		if candidate == addr {
			return s.nodes[i]
		}
	}
	return nil
}

func (s *mysqlStore) List(ctx context.Context, f Filter, page, size int, node string) (ListResult, error) {
	var out ListResult
	_, err := s.withAddr(node, func(db *sql.DB, _ string) error {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		where, args := f.where()
		countRows, err := conn.QueryContext(ctx, "SELECT COUNT(*) FROM "+accountsTable+where, args...)
		if err != nil {
			return err
		}
		var total int
		for countRows.Next() {
			var n int
			if err := countRows.Scan(&n); err != nil {
				countRows.Close()
				return err
			}
			total += n
		}
		if err := countRows.Close(); err != nil {
			return err
		}
		if err := countRows.Err(); err != nil {
			return err
		}
		offset := (page - 1) * size
		listArgs := append(append([]any{}, args...), size, offset)
		rows, err := conn.QueryContext(ctx,
			"SELECT id, name, email, status, tags, created_at FROM "+accountsTable+where+" ORDER BY name, email LIMIT ? OFFSET ?",
			listArgs...,
		)
		if err != nil {
			return err
		}
		accounts, err := scanAccounts(rows)
		rows.Close()
		if err != nil {
			return err
		}
		if err := attachNotes(ctx, conn, accounts); err != nil {
			return err
		}
		out = ListResult{Accounts: accounts, Total: total}
		return nil
	})
	return out, err
}

func (s *mysqlStore) Get(ctx context.Context, id int64, node string) (Account, error) {
	var out Account
	_, err := s.withAddr(node, func(db *sql.DB, _ string) error {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		account, found, err := fetchAccount(ctx, conn, id)
		if err != nil {
			return err
		}
		if !found {
			return ErrNotFound
		}
		out = account
		return nil
	})
	return out, err
}

func (s *mysqlStore) Insert(ctx context.Context, in InsertInput) (InsertResult, error) {
	tags, err := json.Marshal(normalizeTags(in.Tags))
	if err != nil {
		return InsertResult{}, err
	}
	var result InsertResult
	addr, err := s.withAddr(in.Node, func(db *sql.DB, addr string) error {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		query := "INSERT INTO " + accountsTable + " (name, email, status, tags, created_at) VALUES (?, ?, ?, ?, ?)"
		args := []any{in.Name, in.Email, in.Status, tags, in.CreatedAt.UTC()}
		if in.ID > 0 {
			query = "INSERT INTO " + accountsTable + " (id, name, email, status, tags, created_at) VALUES (?, ?, ?, ?, ?, ?)"
			args = []any{in.ID, in.Name, in.Email, in.Status, tags, in.CreatedAt.UTC()}
		}
		res, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			_ = tx.Rollback()
			return mapSQL(err)
		}
		id := in.ID
		if id == 0 {
			id, err = res.LastInsertId()
		}
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO "+notesTable+" (account_id, body, created_at) VALUES (?, ?, ?)",
			id, in.Note, in.CreatedAt.UTC(),
		); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		result.WroteOn = addr
		account, found, err := fetchAccount(ctx, conn, id)
		if err != nil {
			if connErr(err) {
				result.Account.ID = id
				return nil
			}
			return err
		}
		result.Account = account
		if found {
			copied := account
			result.ReadBack = &copied
		}
		return nil
	})
	if err != nil {
		return InsertResult{}, err
	}
	if result.WroteOn == "" {
		result.WroteOn = addr
	}
	other := s.otherAddr(result.WroteOn)
	result.OtherAddr = other
	if other == "" {
		return result, nil
	}
	otherDB := s.dbFor(other)
	conn, err := otherDB.Conn(ctx)
	if err != nil {
		return result, nil
	}
	defer conn.Close()
	account, found, err := fetchAccount(ctx, conn, result.Account.ID)
	if err != nil {
		if connErr(err) {
			return result, nil
		}
		return InsertResult{}, err
	}
	if found {
		result.Other = &account
	}
	return result, nil
}

func (s *mysqlStore) Update(ctx context.Context, a Account, node string) error {
	tags, err := json.Marshal(normalizeTags(a.Tags))
	if err != nil {
		return err
	}
	_, err = s.withAddr(node, func(db *sql.DB, _ string) error {
		res, err := db.ExecContext(ctx,
			"UPDATE "+accountsTable+" SET name = ?, email = ?, status = ?, tags = ?, created_at = ? WHERE id = ?",
			a.Name, a.Email, a.Status, tags, a.CreatedAt.UTC(), a.ID,
		)
		if err != nil {
			return mapSQL(err)
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
	return err
}

func (s *mysqlStore) Delete(ctx context.Context, id int64, node string) error {
	_, err := s.withAddr(node, func(db *sql.DB, _ string) error {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+notesTable+" WHERE account_id = ?", id); err != nil {
			_ = tx.Rollback()
			return err
		}
		res, err := tx.ExecContext(ctx, "DELETE FROM "+accountsTable+" WHERE id = ?", id)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if n == 0 {
			_ = tx.Rollback()
			return ErrNotFound
		}
		return tx.Commit()
	})
	return err
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func fetchAccount(ctx context.Context, q queryer, id int64) (Account, bool, error) {
	row := q.QueryRowContext(ctx,
		"SELECT id, name, email, status, tags, created_at FROM "+accountsTable+" WHERE id = ?",
		id,
	)
	account, err := scanAccount(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Account{}, false, nil
	}
	if err != nil {
		return Account{}, false, err
	}
	accounts := []Account{account}
	if err := attachNotes(ctx, q, accounts); err != nil {
		return Account{}, false, err
	}
	return accounts[0], true, nil
}

func attachNotes(ctx context.Context, q queryer, accounts []Account) error {
	if len(accounts) == 0 {
		return nil
	}
	ids := make([]any, len(accounts))
	placeholders := make([]string, len(accounts))
	byID := make(map[int64]int, len(accounts))
	for i, account := range accounts {
		ids[i] = account.ID
		placeholders[i] = "?"
		byID[account.ID] = i
		accounts[i].Notes = []Note{}
	}
	rows, err := q.QueryContext(ctx,
		"SELECT id, account_id, body, created_at FROM "+notesTable+" WHERE account_id IN ("+strings.Join(placeholders, ",")+") ORDER BY id",
		ids...,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var note Note
		if err := rows.Scan(&note.ID, &note.AccountID, &note.Body, &note.CreatedAt); err != nil {
			return err
		}
		note.CreatedAt = note.CreatedAt.UTC()
		if i, ok := byID[note.AccountID]; ok {
			accounts[i].Notes = append(accounts[i].Notes, note)
		}
	}
	return rows.Err()
}

func scanAccounts(rows *sql.Rows) ([]Account, error) {
	accounts := make([]Account, 0)
	for rows.Next() {
		account, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return accounts, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanAccount(row rowScanner) (Account, error) {
	var account Account
	var tags []byte
	if err := row.Scan(&account.ID, &account.Name, &account.Email, &account.Status, &tags, &account.CreatedAt); err != nil {
		return Account{}, err
	}
	account.CreatedAt = account.CreatedAt.UTC()
	account.Tags = decodeTags(tags)
	account.Notes = []Note{}
	return account, nil
}

func decodeTags(raw []byte) []string {
	if len(raw) == 0 {
		return []string{}
	}
	var tags []string
	if err := json.Unmarshal(raw, &tags); err != nil {
		return []string{}
	}
	return normalizeTags(tags)
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
	if f.Tag != "" {
		conds = append(conds, "CAST(tags AS CHAR) LIKE ?")
		args = append(args, likeContains(f.Tag))
	}
	if f.Status != nil {
		conds = append(conds, "status = ?")
		args = append(args, *f.Status)
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

func normalizeTags(tags []string) []string {
	if tags == nil {
		return []string{}
	}
	return tags
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
