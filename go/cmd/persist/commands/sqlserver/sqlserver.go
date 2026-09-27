package sqlserver

import server "github.com/bongani-m/persist/go/libraries/persist/sqlserver"

// Exec runs the MySQL server. Configuration comes from GMS_* environment variables.
func Exec(args []string) int {
	server.Serve()
	return 0
}
