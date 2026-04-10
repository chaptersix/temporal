package main

import (
	"fmt"
	"os"

	"go.temporal.io/server/tools/schedutil"
)

func main() {
	if err := schedutil.Run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
