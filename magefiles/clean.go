package main

import (
	"os"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Clean contains targets for removing build artifacts.
type Clean mg.Namespace

// Bins deletes compiled binaries.
func (Clean) Bins() error {
	color("Delete old binaries...")
	bins := []string{
		"temporal-server",
		"temporal-server-debug",
		"temporal-cassandra-tool",
		"tdbg",
		"temporal-sql-tool",
		"temporal-elasticsearch-tool",
	}
	for _, b := range bins {
		os.Remove(b)
	}
	return nil
}

// TestOutput deletes test output and clears test cache.
func (Clean) TestOutput() error {
	color("Delete test output...")
	os.RemoveAll(testOutputRoot)
	return sh.RunV("go", "clean", "-testcache")
}

// Tools deletes installed development tool binaries.
func (Clean) Tools() error {
	color("Delete tools...")
	os.RemoveAll(localBin)
	return nil
}

// All deletes all build artifacts.
func (c Clean) All() {
	mg.Deps(c.Bins, c.TestOutput, c.Tools)
}
