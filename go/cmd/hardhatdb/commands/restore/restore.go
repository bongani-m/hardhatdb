package restore

import (
	"fmt"
	"os"

	"github.com/bongani-m/hardhatdb/go/store"
)

// Exec loads a backup into an empty data directory. The server for that
// directory is stopped.
func Exec(args []string) int {
	from, data, err := parseArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hardhatdb: %s\n%s", err, usage)
		return 1
	}
	if from == "" && data == "" {
		fmt.Fprint(os.Stdout, usage)
		return 0
	}
	if err := store.RestoreBackup(from, data); err != nil {
		fmt.Fprintf(os.Stderr, "hardhatdb: %s\n", err)
		return 1
	}
	return 0
}

const usage = "usage: hardhatdb restore --from <backup-dir> --data <data-dir>\n"

func parseArgs(args []string) (from, data string, err error) {
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--from":
			i++
			if i >= len(args) {
				return "", "", fmt.Errorf("restore: --from needs a directory")
			}
			from = args[i]
		case "--data":
			i++
			if i >= len(args) {
				return "", "", fmt.Errorf("restore: --data needs a directory")
			}
			data = args[i]
		case "-h", "--help":
			return "", "", nil
		default:
			return "", "", fmt.Errorf("restore: unknown argument %q", args[i])
		}
	}
	if from == "" || data == "" {
		return "", "", fmt.Errorf("restore: --from and --data are required")
	}
	return from, data, nil
}
