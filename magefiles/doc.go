// Package main contains mage targets that replace the project Makefile.
//
// Usage:
//
//	go run mage.go <namespace>:<target>
//	go run mage.go -l              # list all targets
//
// Examples:
//
//	go run mage.go build:server
//	go run mage.go lint:code
//	go run mage.go test:unit
package main

import (
	"os"

	// mg is used by mage for namespace resolution.
	_ "github.com/magefile/mage/mg"
)

// Default target when none is specified.
var Default = Build.Server

// Aliases provides short names for commonly used targets.
var Aliases = map[string]interface{}{
	"install": Build.Bins,
	"lint":    Lint.All,
	"fmt":     Fmt.All,
	"test":    Test.All,
	"proto":   Proto.All,
	"clean":   Clean.All,
	"start":   Server.Sqlite,
}

func init() {
	// Use verbose output by default.
	os.Setenv("MAGEFILE_VERBOSE", "true")
}
