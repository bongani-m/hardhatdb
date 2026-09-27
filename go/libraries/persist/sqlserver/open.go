package sqlserver

import (
	"github.com/bongani-m/persist/go/libraries/persist/cluster"
	persist "github.com/bongani-m/persist/go/libraries/persist/sqle"
)

// openCluster opens Badger and starts the Raft group. sqlserver is the only
// package that wires the two.
func openCluster(path string, opts cluster.ClusterOptions) (*persist.Store, error) {
	st, err := persist.OpenWithOptions(path, persist.OpenOptions{NoSync: true})
	if err != nil {
		return nil, err
	}
	g, err := cluster.Start(st, opts)
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	st.Attach(g)
	return st, nil
}
