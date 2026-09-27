package persist

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

func TestPrivilegesSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gms")
	store, err := Open(path)
	require.NoError(t, err)

	ctx := sql.NewContext(context.Background())
	db := mysql_db.CreateEmptyMySQLDb()
	db.SetPersister(store.AttachPrivileges(db))
	writeSuperUser(t, ctx, db, "root", "%", "s3cret")
	require.NoError(t, store.Close())

	store, err = Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	db = mysql_db.CreateEmptyMySQLDb()
	db.SetPersister(store.AttachPrivileges(db))
	found, err := store.LoadPrivileges(ctx)
	require.NoError(t, err)
	require.True(t, found)
	requireUser(t, db, "root")
}

func TestPrivilegesReplicate(t *testing.T) {
	cluster := newMemCluster(t, 2)
	leader := cluster.open(t, 0, true, nil)
	require.NoError(t, leader.WaitReady(10*time.Second))
	follower := cluster.open(t, 1, false, nil)
	require.NoError(t, leader.AddVoter("node-1", string(cluster.addrs[1])))
	waitCaughtUp(t, leader, follower)

	ctx := sql.NewContext(context.Background())
	leaderDB := mysql_db.CreateEmptyMySQLDb()
	followerDB := mysql_db.CreateEmptyMySQLDb()
	leaderDB.SetPersister(leader.AttachPrivileges(leaderDB))
	followerDB.SetPersister(follower.AttachPrivileges(followerDB))
	followerDB.SetEnabled(true)
	requireUser(t, followerDB, "")

	writeSuperUser(t, ctx, leaderDB, "root", "%", "s3cret")
	waitCaughtUp(t, leader, follower)
	requireUser(t, followerDB, "root")
}

func writeSuperUser(t *testing.T, ctx *sql.Context, db *mysql_db.MySQLDb, user, host, password string) {
	t.Helper()
	ed := db.Editor()
	defer ed.Close()
	db.AddSuperUser(ed, user, host, password)
	require.NoError(t, db.Persist(ctx, ed))
}

func requireUser(t *testing.T, db *mysql_db.MySQLDb, want string) {
	t.Helper()
	rd := db.Reader()
	defer rd.Close()
	user := db.GetUser(rd, "root", "10.1.1.1", false)
	if want == "" {
		require.Nil(t, user)
		return
	}
	require.NotNil(t, user)
	require.Equal(t, want, user.User)
	require.Equal(t, "caching_sha2_password", user.Plugin)
	require.NotEmpty(t, user.AuthString)
}
