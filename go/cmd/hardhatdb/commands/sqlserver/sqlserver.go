package sqlserver

import server "github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/sqlserver"

// Exec runs the MySQL server. Configuration comes from HARDHATDB_* environment variables.
func Exec(args []string) int {
	server.Serve()
	return 0
}
