// Command kvctl is a tiny CLI wrapper around client.Client, mostly
// useful for the docker-compose demo: `kvctl put foo bar`, kill the
// leader's container, then `kvctl get foo` again to watch a new
// leader answer without you changing the command.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/Atharva9890/raft-kv-store/client"
)

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(1)
	}

	addrs := strings.Split(envOr("KVCTL_ADDRS", "localhost:9001,localhost:9002,localhost:9003,localhost:9004,localhost:9005"), ",")
	c := client.New(addrs)

	switch os.Args[1] {
	case "get":
		if len(os.Args) != 3 {
			usage()
			os.Exit(1)
		}
		value, found, err := c.Get(os.Args[2])
		if err != nil {
			fatal(err)
		}
		if !found {
			fmt.Println("(not found)")
			return
		}
		fmt.Println(value)

	case "put":
		if len(os.Args) != 4 {
			usage()
			os.Exit(1)
		}
		if err := c.Put(os.Args[2], os.Args[3]); err != nil {
			fatal(err)
		}
		fmt.Println("OK")

	case "delete":
		if len(os.Args) != 3 {
			usage()
			os.Exit(1)
		}
		if err := c.Delete(os.Args[2]); err != nil {
			fatal(err)
		}
		fmt.Println("OK")

	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kvctl get <key> | put <key> <value> | delete <key>")
	fmt.Fprintln(os.Stderr, "cluster addresses come from KVCTL_ADDRS (comma-separated host:port), default is the 5-node docker-compose cluster")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
