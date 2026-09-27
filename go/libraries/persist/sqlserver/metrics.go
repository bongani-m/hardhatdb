package sqlserver

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"

	persist "github.com/bongani-m/persist/go/libraries/persist/sqle"
	"github.com/dolthub/go-mysql-server/server"
)

// countingMetric is a server.Counter that ignores labels and keeps one total.
type countingMetric struct {
	mu sync.Mutex
	n  float64
}

func (c *countingMetric) With(...string) server.Counter { return c }

func (c *countingMetric) Add(delta float64) {
	c.mu.Lock()
	c.n += delta
	c.mu.Unlock()
}

func (c *countingMetric) total() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// startMetrics serves /healthz, /readyz, and /metrics on addr.
func startMetrics(addr string, store *persist.Store, queries, queryErrs *countingMetric) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if store.Ready() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		http.Error(w, "not ready\n", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		st := store.Status()
		fmt.Fprintf(w, "gms_queries_total %g\n", queries.total())
		fmt.Fprintf(w, "gms_query_errors_total %g\n", queryErrs.total())
		fmt.Fprintf(w, "gms_raft_commit_index %d\n", st.Commit)
		fmt.Fprintf(w, "gms_raft_applied_index %d\n", st.Applied)
		fmt.Fprintf(w, "gms_raft_lag %d\n", st.Lag)
		fmt.Fprintf(w, "gms_raft_role{role=%q} 1\n", st.Role)
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Handler: mux}
	go func() {
		err := srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics: %v", err)
		}
	}()
	return srv, nil
}
