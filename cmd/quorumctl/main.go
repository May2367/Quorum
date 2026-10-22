package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"quorum/client"
)

func main() {
	endpoints := flag.String("endpoints", "127.0.0.1:9090", "comma-separated node addresses")
	clientID := flag.String("client-id", "quorumctl", "client ID for write deduplication")
	sequence := flag.Uint64("seq", 1, "client sequence number for write deduplication")
	flag.Parse()

	c := client.New(client.Config{Endpoints: splitEndpoints(*endpoints)})
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	ctx := context.Background()
	switch args[0] {
	case "endpoints":
		fmt.Println(strings.Join(c.Endpoints(), ","))
	case "get":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		value, revision, err := c.Get(ctx, args[1])
		fatalIf(err)
		fmt.Printf("%s revision=%d\n", value, revision)
	case "put":
		if len(args) != 3 {
			usage()
			os.Exit(2)
		}
		revision, err := c.Put(ctx, args[1], args[2], *clientID, *sequence)
		fatalIf(err)
		fmt.Printf("revision=%d\n", revision)
	case "delete":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		revision, err := c.Delete(ctx, args[1], *clientID, *sequence)
		fatalIf(err)
		fmt.Printf("revision=%d\n", revision)
	case "cas":
		if len(args) != 4 {
			usage()
			os.Exit(2)
		}
		result, err := c.CompareAndSwap(ctx, args[1], args[2], args[3], *clientID, *sequence)
		fatalIf(err)
		fmt.Printf("swapped=%s current=%s revision=%d\n", strconv.FormatBool(result.Swapped), result.Current, result.Revision)
	default:
		usage()
		os.Exit(2)
	}
}

func splitEndpoints(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage:\n")
	fmt.Fprintf(os.Stderr, "  quorumctl [flags] endpoints\n")
	fmt.Fprintf(os.Stderr, "  quorumctl [flags] get <key>\n")
	fmt.Fprintf(os.Stderr, "  quorumctl [flags] put <key> <value>\n")
	fmt.Fprintf(os.Stderr, "  quorumctl [flags] delete <key>\n")
	fmt.Fprintf(os.Stderr, "  quorumctl [flags] cas <key> <expected> <value>\n")
}
