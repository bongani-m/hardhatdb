package sqlserver

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/cluster"
	hardhatdb "github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/sqle"
	"github.com/dolthub/go-mysql-server/server"
)

const (
	adminStatus      = "status"
	adminAdd         = "add"
	adminAddNonvoter = "add-nonvoter"
	adminRemove      = "remove"
)

type adminCmd struct {
	kind string
	id   string
	addr string
}

// parseAdmin recognizes SHOW RAFT STATUS, RAFT ADD VOTER, RAFT ADD NONVOTER,
// and RAFT REMOVE SERVER. The SQL parser does not know these statements.
// Other statements return before the tokenizer runs.
func parseAdmin(query string) (adminCmd, bool) {
	q := strings.TrimSpace(query)
	if strings.HasSuffix(q, ";") {
		q = strings.TrimSpace(strings.TrimSuffix(q, ";"))
	}
	if !adminPrefix(q) {
		return adminCmd{}, false
	}
	if strings.EqualFold(q, "SHOW RAFT STATUS") {
		return adminCmd{kind: adminStatus}, true
	}
	fields, err := splitAdmin(q)
	if err != nil || len(fields) == 0 {
		return adminCmd{}, false
	}
	if len(fields) == 5 && strings.EqualFold(fields[0], "RAFT") && strings.EqualFold(fields[1], "ADD") && strings.EqualFold(fields[2], "VOTER") {
		if fields[3] == "" || fields[4] == "" {
			return adminCmd{}, false
		}
		return adminCmd{kind: adminAdd, id: fields[3], addr: fields[4]}, true
	}
	if len(fields) == 5 && strings.EqualFold(fields[0], "RAFT") && strings.EqualFold(fields[1], "ADD") && strings.EqualFold(fields[2], "NONVOTER") {
		if fields[3] == "" || fields[4] == "" {
			return adminCmd{}, false
		}
		return adminCmd{kind: adminAddNonvoter, id: fields[3], addr: fields[4]}, true
	}
	if len(fields) == 4 && strings.EqualFold(fields[0], "RAFT") && strings.EqualFold(fields[1], "REMOVE") && strings.EqualFold(fields[2], "SERVER") {
		if fields[3] == "" {
			return adminCmd{}, false
		}
		return adminCmd{kind: adminRemove, id: fields[3]}, true
	}
	return adminCmd{}, false
}

// adminPrefix reports whether q can be a Raft admin statement.
func adminPrefix(q string) bool {
	if hasWordPrefix(q, "RAFT") {
		return true
	}
	return hasWordPrefix(q, "SHOW RAFT")
}

func hasWordPrefix(q, prefix string) bool {
	if len(q) < len(prefix) || !strings.EqualFold(q[:len(prefix)], prefix) {
		return false
	}
	if len(q) == len(prefix) {
		return true
	}
	switch q[len(prefix)] {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

func splitAdmin(q string) ([]string, error) {
	var out []string
	var b strings.Builder
	var quote byte
	for i := 0; i < len(q); i++ {
		c := q[i]
		if quote != 0 {
			if c == quote {
				quote = 0
				continue
			}
			b.WriteByte(c)
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			continue
		}
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			if b.Len() > 0 {
				out = append(out, b.String())
				b.Reset()
			}
			continue
		}
		b.WriteByte(c)
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	if b.Len() > 0 {
		out = append(out, b.String())
	}
	return out, nil
}

// adminHandler serves Raft admin statements on a standalone process.
// Ordinary statements go to the engine. A replicating node uses forwardHandler.
type adminHandler struct {
	*server.Handler
	store *hardhatdb.Store
}

func newAdminHandler(inner *server.Handler, store *hardhatdb.Store) mysql.Handler {
	return &adminHandler{Handler: inner, store: store}
}

func (h *adminHandler) ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error {
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	if cmd, ok := parseAdmin(query); ok {
		return runAdminLocal(h.store, cmd, callback)
	}
	return h.Handler.ComQuery(ctx, c, query, callback)
}

func (h *adminHandler) ComMultiQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) (string, error) {
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	first, rest := splitFirst(query)
	if cmd, ok := parseAdmin(first); ok {
		if err := runAdminLocal(h.store, cmd, callback); err != nil {
			return "", err
		}
		return rest, nil
	}
	return h.Handler.ComMultiQuery(ctx, c, query, callback)
}

func (h *adminHandler) ComStmtExecute(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	ctx, cancel := withQueryTimeout(ctx)
	defer cancel()
	if cmd, ok := parseAdmin(prepare.PrepareStmt); ok {
		return runAdminLocal(h.store, cmd, func(res *sqltypes.Result, _ bool) error {
			return callback(res)
		})
	}
	return h.Handler.ComStmtExecute(ctx, c, prepare, callback)
}

// runAdminLocal runs a Raft admin statement on this process.
func runAdminLocal(store *hardhatdb.Store, cmd adminCmd, callback mysql.ResultSpoolFn) error {
	if cmd.kind != adminStatus && !store.Replicating() {
		return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "hardhatdb: store is not replicating")
	}
	reply, err := runAdmin(store, cmd)
	if err != nil {
		return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", err.Error())
	}
	return spoolReply(reply, callback)
}

func runAdmin(store *hardhatdb.Store, cmd adminCmd) (cluster.ForwardReply, error) {
	switch cmd.kind {
	case adminStatus:
		return statusReply(store), nil
	case adminAdd:
		if err := store.AddVoter(cmd.id, cmd.addr); err != nil {
			return cluster.ForwardReply{}, err
		}
		return cluster.ForwardReply{Info: "voter added"}, nil
	case adminAddNonvoter:
		if err := store.AddNonvoter(cmd.id, cmd.addr); err != nil {
			return cluster.ForwardReply{}, err
		}
		return cluster.ForwardReply{Info: "nonvoter added"}, nil
	case adminRemove:
		if err := store.RemoveServer(cmd.id); err != nil {
			return cluster.ForwardReply{}, err
		}
		return cluster.ForwardReply{Info: "server removed"}, nil
	default:
		return cluster.ForwardReply{}, fmt.Errorf("hardhatdb: unknown statement")
	}
}

func statusReply(store *hardhatdb.Store) cluster.ForwardReply {
	st := store.Status()
	names := []string{"role", "leader", "commit_index", "applied_index", "lag", "suffrage"}
	vals := []string{
		st.Role,
		st.Leader,
		strconv.FormatUint(st.Commit, 10),
		strconv.FormatUint(st.Applied, 10),
		strconv.FormatUint(st.Lag, 10),
		st.Suffrage,
	}
	fields := make([]cluster.ForwardField, len(names))
	cells := make([]cluster.ForwardCell, len(names))
	for i, name := range names {
		fields[i] = cluster.ForwardField{
			Name:         name,
			Type:         int32(querypb.Type_VARCHAR),
			Charset:      45,
			ColumnLength: 256,
		}
		cells[i] = cluster.ForwardCell{Type: int32(querypb.Type_VARCHAR), Raw: []byte(vals[i])}
	}
	return cluster.ForwardReply{Fields: fields, Rows: [][]cluster.ForwardCell{cells}}
}
