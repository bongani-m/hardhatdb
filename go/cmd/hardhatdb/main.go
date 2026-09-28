package main

import (
	"fmt"
	"os"

	"github.com/bongani-m/hardhatdb/go/cmd/hardhatdb/commands/restore"
	"github.com/bongani-m/hardhatdb/go/cmd/hardhatdb/commands/sqlserver"
	"github.com/bongani-m/hardhatdb/go/cmd/hardhatdb/version"
)

type command struct {
	Name string
	Desc string
	Exec func(args []string) int
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	commands := []command{
		{Name: "sql-server", Desc: "start the MySQL server", Exec: sqlserver.Exec},
		{Name: "restore", Desc: "load a backup into an empty data directory", Exec: restore.Exec},
		{Name: "version", Desc: "print the HardhatDB version", Exec: execVersion},
	}
	if len(args) == 0 {
		printCommands(commands)
		return 1
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		printCommands(commands)
		return 0
	}
	for _, cmd := range commands {
		if cmd.Name == args[0] {
			return cmd.Exec(args[1:])
		}
	}
	fmt.Fprintf(os.Stderr, "hardhatdb: unknown command %q\n\n", args[0])
	printCommands(commands)
	return 1
}

func execVersion(args []string) int {
	fmt.Println(version.Version)
	return 0
}

func printCommands(commands []command) {
	fmt.Println("hardhatdb commands:")
	for _, cmd := range commands {
		fmt.Printf("  %-12s %s\n", cmd.Name, cmd.Desc)
	}
}
