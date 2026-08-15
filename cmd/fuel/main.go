package main

import (
	"fmt"
	"os"
)

// version 由编译期 -ldflags 注入，默认 dev。
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "mount":
		if err := runMount(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "fuel mount: %v\n", err)
			os.Exit(1)
		}
	case "version":
		fmt.Printf("fuel %s\n", version)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `fuel - POSIX cache filesystem for object storage

Usage:
  fuel mount --config <config.yaml> [--mount-point <path>]
  fuel version
`)
}
