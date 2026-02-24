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
// Map from binary name to Go package path.
var heavyTools = map[string]string{
	"golangci-lint": "github.com/golangci/golangci-lint/v2/cmd/golangci-lint",
	"gci":           "github.com/daixiang0/gci",
	"gotestsum":     "gotest.tools/gotestsum",
	"api-linter":    "github.com/googleapis/api-linter/cmd/api-linter",
	"buf":           "github.com/bufbuild/buf/cmd/buf",
	"actionlint":    "github.com/rhysd/actionlint/cmd/actionlint",
	"workflowcheck": "go.temporal.io/sdk/contrib/tools/workflowcheck",
	"yamlfmt":       "github.com/google/yamlfmt/cmd/yamlfmt",
	"gomajor":       "github.com/icholy/gomajor",
	"errortype":     "fillmore-labs.com/errortype",
}

// goTool runs `go tool <name> <args...>` (lightweight tools in go.mod tool section).
func goTool(name string, args ...string) error {
	allArgs := append([]string{"tool", name}, args...)
	return sh.RunV("go", allArgs...)
}

// goToolOutput runs `go tool <name> <args...>` and returns stdout.
func goToolOutput(name string, args ...string) (string, error) {
	allArgs := append([]string{"tool", name}, args...)
	return sh.Output("go", allArgs...)
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
	pkg, ok := heavyTools[name]
	if !ok {
		return "", fmt.Errorf("unknown heavy tool: %s", name)
	}
	if err := ensureBin(); err != nil {
		return "", err
	}
	color("Installing %s...", name)
	absbin, err := filepath.Abs(localBin)
	if err != nil {
		return "", err
	}
	env := map[string]string{"GOBIN": absbin}
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

// goAPIVersion returns the version of go.temporal.io/api from go.mod.
func goAPIVersion() (string, error) {
	out, err := sh.Output("go", "list", "-m", "-f", "{{.Version}}", "go.temporal.io/api")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}
