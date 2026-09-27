package sqle

import "github.com/bongani-m/persist/go/libraries/persist/cluster"

// ClusterOptions is the Raft configuration tests pass to OpenCluster.
type ClusterOptions = cluster.ClusterOptions

// OpenCluster opens Badger and starts Raft the way sqlserver does.
func OpenCluster(path string, opts ClusterOptions) (*Store, error) {
	st, err := OpenWithOptions(path, OpenOptions{NoSync: true})
	if err != nil {
		return nil, err
	}
	g, err := cluster.Start(st, opts)
	if err != nil {
		st.Close()
		return nil, err
	}
	st.Attach(g)
	return st, nil
}

func clusterOf(s *Store) *cluster.Group {
	return s.group.(*cluster.Group)
}

func (s *Store) caughtUpTo(index uint64) bool {
	if s.group == nil {
		return true
	}
	return clusterOf(s).CaughtUp(index)
}
