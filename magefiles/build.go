package main

import (
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Build contains targets for building binaries.
type Build mg.Namespace

// Server builds the temporal-server binary.
func (Build) Server() error {
	return goBuild("temporal-server", "./cmd/server")
}

// ServerDebug builds the temporal-server-debug binary with debug tags.
func (Build) ServerDebug() error {
	color("Build temporal-server-debug with CGO_ENABLED=%s for %s/%s...", cgoEnabled, goos, goarch)
	env := map[string]string{"CGO_ENABLED": cgoEnabled}
	return sh.RunWithV(env, "go", "build", buildTagFlag()+",TEMPORAL_DEBUG", "-o", "temporal-server-debug", "./cmd/server")
}

// Tdbg builds the tdbg binary.
func (Build) Tdbg() error {
	return goBuild("tdbg", "./cmd/tools/tdbg")
}

// CassandraTool builds the temporal-cassandra-tool binary.
func (Build) CassandraTool() error {
	return goBuild("temporal-cassandra-tool", "./cmd/tools/cassandra")
}

// SqlTool builds the temporal-sql-tool binary.
func (Build) SqlTool() error {
	return goBuild("temporal-sql-tool", "./cmd/tools/sql")
}

// EsTool builds the temporal-elasticsearch-tool binary.
func (Build) EsTool() error {
	return goBuild("temporal-elasticsearch-tool", "./cmd/tools/elasticsearch")
}

// Bins builds all binaries.
func (b Build) Bins() {
	mg.Deps(b.Server, b.Tdbg, b.CassandraTool, b.SqlTool, b.EsTool)
}

// Tests compiles all tests without running them.
func (Build) Tests() error {
	color("Build tests...")
	env := map[string]string{"CGO_ENABLED": cgoEnabled}
	dirs, err := findTestDirs()
	if err != nil {
		return err
	}
	args := []string{"test", testTagFlag(), `-exec=true`, "-count=0"}
	args = append(args, dirs...)
	return sh.RunWithV(env, "go", args...)
}
