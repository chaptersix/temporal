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

// mg is used by mage for namespace resolution.
import _ "github.com/magefile/mage/mg"

// Default target when none is specified.
var Default = Build.Server

// Aliases provides short names for commonly used targets.
var Aliases = map[string]any{
	"install": Build.Bins,
	"lint":    Lint.All,
	"fmt":     Fmt.All,
	"test":    Test.All,
	"proto":   Proto.All,
	"clean":   Clean.All,
	"start":   Server.Sqlite,
}
