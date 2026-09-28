package sqlserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseAdmin(t *testing.T) {
	status, ok := parseAdmin("  show raft status ; ")
	require.True(t, ok)
	require.Equal(t, adminStatus, status.kind)

	add, ok := parseAdmin(`RAFT ADD VOTER 'n4' '10.1.0.5:7001'`)
	require.True(t, ok)
	require.Equal(t, adminCmd{kind: adminAdd, id: "n4", addr: "10.1.0.5:7001"}, add)

	nonvoter, ok := parseAdmin(`RAFT ADD NONVOTER 'n4' '10.1.0.5:7001'`)
	require.True(t, ok)
	require.Equal(t, adminCmd{kind: adminAddNonvoter, id: "n4", addr: "10.1.0.5:7001"}, nonvoter)

	remove, ok := parseAdmin(`raft remove server "n4"`)
	require.True(t, ok)
	require.Equal(t, adminCmd{kind: adminRemove, id: "n4"}, remove)

	_, ok = parseAdmin("SELECT 1")
	require.False(t, ok)
	_, ok = parseAdmin("SHOW TABLES")
	require.False(t, ok)
	_, ok = parseAdmin("RAFT ADD VOTER 'only-one'")
	require.False(t, ok)
	_, ok = parseAdmin("RAFT ADD NONVOTER 'only-one'")
	require.False(t, ok)

	backup, ok := parseAdmin(`BACKUP TO '/var/backups/1'`)
	require.True(t, ok)
	require.Equal(t, adminCmd{kind: adminBackup, path: "/var/backups/1"}, backup)

	replay, ok := parseAdmin(`RESTORE BINLOG FROM '/var/backups/1/binlog' AFTER 12`)
	require.True(t, ok)
	require.Equal(t, adminCmd{kind: adminRestore, path: "/var/backups/1/binlog", after: 12}, replay)

	_, ok = parseAdmin("BACKUP TO")
	require.False(t, ok)
	_, ok = parseAdmin("RESTORE BINLOG FROM '/tmp/binlog' AFTER nope")
	require.False(t, ok)

	metaStatus, ok := parseAdmin(" show meta status ")
	require.True(t, ok)
	require.Equal(t, adminCmd{kind: adminMetaStatus, meta: true}, metaStatus)

	metaBackup, ok := parseAdmin(`BACKUP META TO '/var/backups/meta'`)
	require.True(t, ok)
	require.Equal(t, adminCmd{kind: adminMetaBackup, path: "/var/backups/meta", meta: true}, metaBackup)

	metaReplay, ok := parseAdmin(`RESTORE META BINLOG FROM '/var/backups/meta/binlog' AFTER 9`)
	require.True(t, ok)
	require.Equal(t, adminCmd{kind: adminMetaRestore, path: "/var/backups/meta/binlog", after: 9, meta: true}, metaReplay)

	metaAdd, ok := parseAdmin(`META ADD VOTER 'g2' '10.1.0.6:7001'`)
	require.True(t, ok)
	require.Equal(t, adminCmd{kind: adminMetaAdd, id: "g2", addr: "10.1.0.6:7001", meta: true}, metaAdd)

	metaNonvoter, ok := parseAdmin(`META ADD NONVOTER 'g4' '10.1.0.8:7001'`)
	require.True(t, ok)
	require.Equal(t, adminCmd{kind: adminMetaAddNonvoter, id: "g4", addr: "10.1.0.8:7001", meta: true}, metaNonvoter)

	_, ok = parseAdmin("BACKUP META TO")
	require.False(t, ok)
	_, ok = parseAdmin("META ADD VOTER 'only-one'")
	require.False(t, ok)
	_, ok = parseAdmin("RESTORE META BINLOG FROM '/tmp/binlog' AFTER nope")
	require.False(t, ok)
}

func TestLoadLimitsDefaults(t *testing.T) {
	t.Setenv("HARDHATDB_MAX_CONNECTIONS", "")
	t.Setenv("HARDHATDB_NET_READ_TIMEOUT", "")
	t.Setenv("HARDHATDB_NET_WRITE_TIMEOUT", "")
	t.Setenv("HARDHATDB_MAX_EXECUTION_TIME", "")
	t.Setenv("HARDHATDB_SHUTDOWN_TIMEOUT", "")
	got, err := loadLimits()
	require.NoError(t, err)
	require.Equal(t, uint64(151), got.maxConns)
	require.Equal(t, time.Duration(0), got.read)
	require.Equal(t, time.Duration(0), got.write)
	require.Equal(t, time.Duration(0), got.exec)
	require.Equal(t, 15*time.Second, got.shutdown)
}

func TestLoadLimitsOverrides(t *testing.T) {
	t.Setenv("HARDHATDB_MAX_CONNECTIONS", "32")
	t.Setenv("HARDHATDB_NET_READ_TIMEOUT", "5s")
	t.Setenv("HARDHATDB_NET_WRITE_TIMEOUT", "9")
	t.Setenv("HARDHATDB_MAX_EXECUTION_TIME", "250")
	t.Setenv("HARDHATDB_SHUTDOWN_TIMEOUT", "2s")
	got, err := loadLimits()
	require.NoError(t, err)
	require.Equal(t, uint64(32), got.maxConns)
	require.Equal(t, 5*time.Second, got.read)
	require.Equal(t, 9*time.Second, got.write)
	require.Equal(t, 250*time.Millisecond, got.exec)
	require.Equal(t, 2*time.Second, got.shutdown)

	t.Setenv("HARDHATDB_MAX_CONNECTIONS", "0")
	_, err = loadLimits()
	require.Error(t, err)
}

func TestSeedExample(t *testing.T) {
	t.Setenv("HARDHATDB_SEED_EXAMPLE", "")
	require.False(t, seedExample())
	t.Setenv("HARDHATDB_SEED_EXAMPLE", "1")
	require.True(t, seedExample())
	t.Setenv("HARDHATDB_SEED_EXAMPLE", "true")
	require.True(t, seedExample())
}
