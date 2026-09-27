package main

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

	remove, ok := parseAdmin(`raft remove server "n4"`)
	require.True(t, ok)
	require.Equal(t, adminCmd{kind: adminRemove, id: "n4"}, remove)

	_, ok = parseAdmin("SELECT 1")
	require.False(t, ok)
	_, ok = parseAdmin("SHOW TABLES")
	require.False(t, ok)
	_, ok = parseAdmin("RAFT ADD VOTER 'only-one'")
	require.False(t, ok)
}

func TestLoadLimitsDefaults(t *testing.T) {
	t.Setenv("GMS_MAX_CONNECTIONS", "")
	t.Setenv("GMS_NET_READ_TIMEOUT", "")
	t.Setenv("GMS_NET_WRITE_TIMEOUT", "")
	t.Setenv("GMS_MAX_EXECUTION_TIME", "")
	t.Setenv("GMS_SHUTDOWN_TIMEOUT", "")
	got, err := loadLimits()
	require.NoError(t, err)
	require.Equal(t, uint64(151), got.maxConns)
	require.Equal(t, time.Duration(0), got.read)
	require.Equal(t, time.Duration(0), got.write)
	require.Equal(t, time.Duration(0), got.exec)
	require.Equal(t, 15*time.Second, got.shutdown)
}

func TestLoadLimitsOverrides(t *testing.T) {
	t.Setenv("GMS_MAX_CONNECTIONS", "32")
	t.Setenv("GMS_NET_READ_TIMEOUT", "5s")
	t.Setenv("GMS_NET_WRITE_TIMEOUT", "9")
	t.Setenv("GMS_MAX_EXECUTION_TIME", "250")
	t.Setenv("GMS_SHUTDOWN_TIMEOUT", "2s")
	got, err := loadLimits()
	require.NoError(t, err)
	require.Equal(t, uint64(32), got.maxConns)
	require.Equal(t, 5*time.Second, got.read)
	require.Equal(t, 9*time.Second, got.write)
	require.Equal(t, 250*time.Millisecond, got.exec)
	require.Equal(t, 2*time.Second, got.shutdown)

	t.Setenv("GMS_MAX_CONNECTIONS", "0")
	_, err = loadLimits()
	require.Error(t, err)
}

func TestSeedExample(t *testing.T) {
	t.Setenv("GMS_SEED_EXAMPLE", "")
	require.False(t, seedExample())
	t.Setenv("GMS_SEED_EXAMPLE", "1")
	require.True(t, seedExample())
	t.Setenv("GMS_SEED_EXAMPLE", "true")
	require.True(t, seedExample())
}
