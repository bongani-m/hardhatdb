package sqle

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

// TestMain pins this process to UTC. Engine-suite expectations such as
// UnixTimeInLocal are computed while packages initialize, which is before
// TestMain, so the process is started again with TZ=UTC first.
func TestMain(m *testing.M) {
	if os.Getenv("HARDHATDB_TEST_UTC") != "1" {
		cmd := exec.Command(os.Args[0], os.Args[1:]...)
		cmd.Env = append(os.Environ(), "TZ=UTC", "HARDHATDB_TEST_UTC=1")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err == nil {
			os.Exit(0)
		}
		if exit, ok := err.(*exec.ExitError); ok {
			os.Exit(exit.ExitCode())
		}
		os.Exit(1)
	}
	time.Local = time.UTC
	os.Exit(m.Run())
}
