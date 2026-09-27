package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// serverLimits is the listener and shutdown configuration read from the
// environment once, at process start. SET GLOBAL max_connections does not
// resize the listener.
type serverLimits struct {
	maxConns uint64
	read     time.Duration
	write    time.Duration
	exec     time.Duration
	shutdown time.Duration
}

func loadLimits() (serverLimits, error) {
	maxConns, err := parseUintEnv("GMS_MAX_CONNECTIONS", 151, 1, 100000)
	if err != nil {
		return serverLimits{}, err
	}
	// Unset means no socket deadline. A deadline re-armed on every read and
	// write costs a syscall per packet.
	read, err := parseOptionalSecondsEnv("GMS_NET_READ_TIMEOUT")
	if err != nil {
		return serverLimits{}, err
	}
	write, err := parseOptionalSecondsEnv("GMS_NET_WRITE_TIMEOUT")
	if err != nil {
		return serverLimits{}, err
	}
	exec, err := parseExecTimeout()
	if err != nil {
		return serverLimits{}, err
	}
	shutdown, err := parseSecondsEnv("GMS_SHUTDOWN_TIMEOUT", 15*time.Second)
	if err != nil {
		return serverLimits{}, err
	}
	return serverLimits{
		maxConns: maxConns,
		read:     read,
		write:    write,
		exec:     exec,
		shutdown: shutdown,
	}, nil
}

func parseUintEnv(key string, fallback, min, max uint64) (uint64, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s: %q", key, raw)
	}
	return n, nil
}

// parseOptionalSecondsEnv is parseSecondsEnv with no default. Empty and 0
// both mean the timeout is off.
func parseOptionalSecondsEnv(key string) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" || raw == "0" {
		return 0, nil
	}
	return parseSecondsEnv(key, 0)
}

// parseSecondsEnv accepts a bare number of seconds or a Go duration.
func parseSecondsEnv(key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	if n, err := strconv.Atoi(raw); err == nil {
		if n < 1 {
			return 0, fmt.Errorf("%s: %q", key, raw)
		}
		return time.Duration(n) * time.Second, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s: %q", key, raw)
	}
	return d, nil
}

// parseExecTimeout reads GMS_MAX_EXECUTION_TIME as milliseconds. 0 disables it.
func parseExecTimeout() (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv("GMS_MAX_EXECUTION_TIME"))
	if raw == "" || raw == "0" {
		return 0, nil
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("GMS_MAX_EXECUTION_TIME: %q", raw)
	}
	return time.Duration(n) * time.Millisecond, nil
}

func seedExample() bool {
	v := strings.TrimSpace(os.Getenv("GMS_SEED_EXAMPLE"))
	return v == "1" || strings.EqualFold(v, "true")
}
