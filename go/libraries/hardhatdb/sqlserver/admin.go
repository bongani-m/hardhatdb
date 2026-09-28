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
	adminStatus          = "status"
	adminAdd             = "add"
	adminAddNonvoter     = "add-nonvoter"
	adminRemove          = "remove"
	adminBackup          = "backup"
	adminRestore         = "restore-binlog"
	adminMetaStatus      = "meta-status"
	adminMetaBackup      = "meta-backup"
	adminMetaRestore     = "meta-restore"
	adminMetaAdd         = "meta-add"
	adminMetaAddNonvoter = "meta-add-nonvoter"
)

type adminCmd struct {
	kind  string
	id    string
	addr  string
	path  string
	after uint64
	// meta is true when the statement applies to the meta catalog, not the data group.
	meta bool
}

// parseAdmin recognizes SHOW RAFT STATUS, RAFT ADD VOTER, RAFT ADD NONVOTER,
// RAFT REMOVE SERVER, BACKUP TO, RESTORE BINLOG FROM, and the meta-catalog
// forms of those statements. The SQL parser does not know them. Other
// statements return before the tokenizer runs.
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
	if strings.EqualFold(q, "SHOW META STATUS") {
		return adminCmd{kind: adminMetaStatus, meta: true}, true
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
	if len(fields) == 4 && strings.EqualFold(fields[0], "BACKUP") && strings.EqualFold(fields[1], "META") && strings.EqualFold(fields[2], "TO") && fields[3] != "" {
		return adminCmd{kind: adminMetaBackup, path: fields[3], meta: true}, true
	}
	if len(fields) == 3 && strings.EqualFold(fields[0], "BACKUP") && strings.EqualFold(fields[1], "TO") && fields[2] != "" {
		return adminCmd{kind: adminBackup, path: fields[2]}, true
	}
	if len(fields) == 7 && strings.EqualFold(fields[0], "RESTORE") && strings.EqualFold(fields[1], "META") && strings.EqualFold(fields[2], "BINLOG") && strings.EqualFold(fields[3], "FROM") && strings.EqualFold(fields[5], "AFTER") && fields[4] != "" {
		after, err := strconv.ParseUint(fields[6], 10, 64)
		if err != nil {
			return adminCmd{}, false
		}
		return adminCmd{kind: adminMetaRestore, path: fields[4], after: after, meta: true}, true
	}
	if len(fields) == 6 && strings.EqualFold(fields[0], "RESTORE") && strings.EqualFold(fields[1], "BINLOG") && strings.EqualFold(fields[2], "FROM") && strings.EqualFold(fields[4], "AFTER") && fields[3] != "" {
		after, err := strconv.ParseUint(fields[5], 10, 64)
		if err != nil {
			return adminCmd{}, false
		}
		return adminCmd{kind: adminRestore, path: fields[3], after: after}, true
	}
	if len(fields) == 5 && strings.EqualFold(fields[0], "META") && strings.EqualFold(fields[1], "ADD") && (strings.EqualFold(fields[2], "VOTER") || strings.EqualFold(fields[2], "NONVOTER")) {
		if fields[3] == "" || fields[4] == "" {
			return adminCmd{}, false
		}
		kind := adminMetaAdd
		if strings.EqualFold(fields[2], "NONVOTER") {
			kind = adminMetaAddNonvoter
		}
		return adminCmd{kind: kind, id: fields[3], addr: fields[4], meta: true}, true
	}
	return adminCmd{}, false
}

// adminPrefix reports whether q can be a Raft admin statement.
func adminPrefix(q string) bool {
	if hasWordPrefix(q, "RAFT") || hasWordPrefix(q, "BACKUP") || hasWordPrefix(q, "RESTORE") || hasWordPrefix(q, "META") {
		return true
	}
	return hasWordPrefix(q, "SHOW RAFT") || hasWordPrefix(q, "SHOW META")
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

// runAdminLocal runs a data-group admin statement on this process.
// Meta statements are refused here; the shard handler runs those on the catalog.
func runAdminLocal(store *hardhatdb.Store, cmd adminCmd, callback mysql.ResultSpoolFn) error {
	if cmd.meta {
		return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "hardhatdb: meta catalog is not configured")
	}
	return finishAdmin(store, cmd, callback)
}

// finishAdmin runs cmd on store. The caller chooses the data group or the meta catalog.
func finishAdmin(store *hardhatdb.Store, cmd adminCmd, callback mysql.ResultSpoolFn) error {
	if cmd.kind != adminStatus && cmd.kind != adminMetaStatus && cmd.kind != adminBackup && cmd.kind != adminMetaBackup && !store.Replicating() {
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
	case adminStatus, adminMetaStatus:
		return statusReply(store), nil
	case adminAdd, adminMetaAdd:
		if err := store.AddVoter(cmd.id, cmd.addr); err != nil {
			return cluster.ForwardReply{}, err
		}
		return cluster.ForwardReply{Info: "voter added"}, nil
	case adminAddNonvoter, adminMetaAddNonvoter:
		if err := store.AddNonvoter(cmd.id, cmd.addr); err != nil {
			return cluster.ForwardReply{}, err
		}
		return cluster.ForwardReply{Info: "nonvoter added"}, nil
	case adminRemove:
		if err := store.RemoveServer(cmd.id); err != nil {
			return cluster.ForwardReply{}, err
		}
		return cluster.ForwardReply{Info: "server removed"}, nil
	case adminBackup, adminMetaBackup:
		index, err := store.BackupTo(cmd.path)
		if err != nil {
			return cluster.ForwardReply{}, err
		}
		return indexReply(index), nil
	case adminRestore, adminMetaRestore:
		if !store.IsLeader() {
			return cluster.ForwardReply{}, fmt.Errorf("hardhatdb: not the leader")
		}
		if err := store.ReplayBinlogAfter(cmd.path, cmd.after); err != nil {
			return cluster.ForwardReply{}, err
		}
		return cluster.ForwardReply{Info: "binlog restored"}, nil
	default:
		return cluster.ForwardReply{}, fmt.Errorf("hardhatdb: unknown statement")
	}
}

func indexReply(index uint64) cluster.ForwardReply {
	fields := []cluster.ForwardField{{
		Name:         "index",
		Type:         int32(querypb.Type_VARCHAR),
		Charset:      45,
		ColumnLength: 32,
	}}
	cells := []cluster.ForwardCell{{
		Type: int32(querypb.Type_VARCHAR),
		Raw:  []byte(strconv.FormatUint(index, 10)),
	}}
	return cluster.ForwardReply{Fields: fields, Rows: [][]cluster.ForwardCell{cells}}
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
