package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/magefile/mage/sh"
)

const localBin = ".bin"

// Heavy tools installed from devtools/ module into .bin/.
// These share a dependency graph via devtools/go.mod.
var heavyTools = map[string]string{
	"golangci-lint": "github.com/golangci/golangci-lint/v2/cmd/golangci-lint",
	"gci":           "github.com/daixiang0/gci",
	"gotestsum":     "gotest.tools/gotestsum",
	"api-linter":    "github.com/googleapis/api-linter/cmd/api-linter",
	"actionlint":    "github.com/rhysd/actionlint/cmd/actionlint",
	"workflowcheck": "go.temporal.io/sdk/contrib/tools/workflowcheck",
	"yamlfmt":       "github.com/google/yamlfmt/cmd/yamlfmt",
	"gomajor":       "github.com/icholy/gomajor",
	"errortype":     "fillmore-labs.com/errortype",
}

// Standalone tools installed directly with @version.
// These have transitive dependency conflicts with other tools and
// cannot share the devtools module's dependency graph.
var standaloneTools = map[string]string{
	"buf": "github.com/bufbuild/buf/cmd/buf@v1.6.0",
}

// goTool runs `go tool <name> <args...>` (lightweight tools in go.mod tool section).
func goTool(name string, args ...string) error {
	allArgs := append([]string{"tool", name}, args...)
	return sh.RunV("go", allArgs...)
}

// goToolPath returns the filesystem path of a tool binary via `go tool -n <name>`.
func goToolPath(name string) (string, error) {
	out, err := sh.Output("go", "tool", "-n", name)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// ensureBin ensures the .bin/ directory exists.
func ensureBin() error {
	return os.MkdirAll(localBin, 0o755)
}

// devtoolPath returns the path to a heavy tool binary, installing it if needed.
func devtoolPath(name string) (string, error) {
	binPath := filepath.Join(localBin, name)
	if _, err := os.Stat(binPath); err == nil {
		return binPath, nil
	}
	if err := ensureBin(); err != nil {
		return "", err
	}
	absbin, err := filepath.Abs(localBin)
	if err != nil {
		return "", err
	}
	env := map[string]string{"GOBIN": absbin}

	// Standalone tools: install directly with @version to avoid dep conflicts.
	if pkgVersion, ok := standaloneTools[name]; ok {
		color("Installing %s...", name)
		if err := sh.RunWithV(env, "go", "install", pkgVersion); err != nil {
			return "", fmt.Errorf("installing %s: %w", name, err)
		}
		return binPath, nil
	}

	// Devtools module tools: install via shared devtools/go.mod.
	pkg, ok := heavyTools[name]
	if !ok {
		return "", fmt.Errorf("unknown tool: %s", name)
	}
	color("Installing %s...", name)
	if err := sh.RunWithV(env, "go", "install", "-C", "devtools", pkg); err != nil {
		return "", fmt.Errorf("installing %s: %w", name, err)
	}
	return binPath, nil
}

// devtool runs a heavy tool from .bin/, installing it if needed.
func devtool(name string, args ...string) error {
	bin, err := devtoolPath(name)
	if err != nil {
		return err
	}
	return sh.RunV(bin, args...)
}
