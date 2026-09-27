package sqlserver

import (
	"github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/cluster"
	hardhatdb "github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/sqle"
)

// openCluster opens Badger and starts the Raft group. sqlserver is the only
// package that wires the two.
func openCluster(path string, opts cluster.ClusterOptions) (*hardhatdb.Store, error) {
	st, err := hardhatdb.OpenWithOptions(path, hardhatdb.OpenOptions{NoSync: true})
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
