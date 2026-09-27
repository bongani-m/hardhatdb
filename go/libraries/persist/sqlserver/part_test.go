package sqlserver

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bongani-m/persist/go/libraries/persist/cluster"
)

func TestScatterParallelSkipsLocal(t *testing.T) {
	localAddr := "10.0.0.1:7102"
	addrs := []string{localAddr, "10.0.0.2:7102", "10.0.0.3:7102"}

	var mu sync.Mutex
	var localSeen []string
	dialed := map[string]int{}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	go func() {
		defer close(release)
		for i := 0; i < 2; i++ {
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Error("shards did not run together")
				return
			}
		}
	}()

	merged, err := scatterCalls(addrs, localAddr,
		func(addr string) (cluster.ForwardReply, error) {
			mu.Lock()
			localSeen = append(localSeen, addr)
			mu.Unlock()
			return cluster.ForwardReply{Rows: rowsOf("local")}, nil
		},
		func(addr string) (cluster.ForwardReply, error) {
			mu.Lock()
			dialed[addr]++
			mu.Unlock()
			entered <- struct{}{}
			<-release
			return cluster.ForwardReply{Rows: rowsOf(addr)}, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{localAddr}, localSeen)
	require.Equal(t, map[string]int{"10.0.0.2:7102": 1, "10.0.0.3:7102": 1}, dialed)
	require.Equal(t, [][]cluster.ForwardCell{
		{{Raw: []byte("local")}},
		{{Raw: []byte("10.0.0.2:7102")}},
		{{Raw: []byte("10.0.0.3:7102")}},
	}, merged.Rows)
}

func rowsOf(text string) [][]cluster.ForwardCell {
	return [][]cluster.ForwardCell{{{Raw: []byte(text)}}}
}
