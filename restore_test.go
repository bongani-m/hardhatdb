package persist

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoverRestoreFinishesSwap(t *testing.T) {
	dir := t.TempDir()
	live := filepath.Join(dir, "gms")
	require.NoError(t, os.Mkdir(live+".old", 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(live+".old", "keep"), []byte("old"), 0o644))
	require.NoError(t, os.Mkdir(live+".restore", 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(live+".restore", "keep"), []byte("new"), 0o644))

	require.NoError(t, recoverRestoreDirs(live))
	got, err := os.ReadFile(filepath.Join(live, "keep"))
	require.NoError(t, err)
	require.Equal(t, "new", string(got))
	_, err = os.Stat(live + ".old")
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(live + ".restore")
	require.True(t, os.IsNotExist(err))
}

func TestRecoverRestoreKeepsLive(t *testing.T) {
	dir := t.TempDir()
	live := filepath.Join(dir, "gms")
	require.NoError(t, os.Mkdir(live, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(live, "keep"), []byte("live"), 0o644))
	require.NoError(t, os.Mkdir(live+".restore", 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(live+".restore", "keep"), []byte("partial"), 0o644))

	require.NoError(t, recoverRestoreDirs(live))
	got, err := os.ReadFile(filepath.Join(live, "keep"))
	require.NoError(t, err)
	require.Equal(t, "live", string(got))
	_, err = os.Stat(live + ".restore")
	require.True(t, os.IsNotExist(err))
}
