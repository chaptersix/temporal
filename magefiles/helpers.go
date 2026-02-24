package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/magefile/mage/sh"
)

// envOrDefault returns the environment variable value or the default.
func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// goEnv returns the output of `go env <key>`.
func goEnv(key string) string {
	out, err := sh.Output("go", "env", key)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// isTruthy returns true for values like 1, on, y, yes, t, true.
func isTruthy(val string) bool {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "1", "on", "y", "yes", "t", "true":
		return true
	}
	return false
}

// rootDir returns the git repository root.
func rootDir() string {
	out, err := sh.Output("git", "rev-parse", "--show-toplevel")
	if err != nil {
		// Fallback to working directory.
		wd, _ := os.Getwd()
		return wd
	}
	return strings.TrimSpace(out)
}

// color prints a colored header message.
func color(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("\033[1;36m%s\033[0m\n", msg)
}

// yellow prints a yellow warning message.
func yellow(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("\033[1;33m%s\033[0m\n", msg)
}

// red prints a red error message.
func red(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("\033[1;31m%s\033[0m\n", msg)
}

// goBuild executes `go build` with standard flags.
func goBuild(output, pkg string) error {
	color("Build %s with CGO_ENABLED=%s for %s/%s...", output, cgoEnabled, goos, goarch)
	env := map[string]string{"CGO_ENABLED": cgoEnabled}
	return sh.RunWithV(env, "go", "build", buildTagFlag(), "-o", output, pkg)
}

// findGoFiles returns all .go files in the repository.
func findGoFiles() ([]string, error) {
	var files []string
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".go") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// findTestDirs returns directories containing _test.go files.
func findTestDirs() ([]string, error) {
	seen := make(map[string]bool)
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, "_test.go") {
			dir := "./" + filepath.Dir(path)
			seen[dir] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	dirs := make([]string, 0, len(seen))
	for d := range seen {
		dirs = append(dirs, d)
	}
	return dirs, nil
}

// unitTestDirs returns test directories excluding functional, integration, and temporaltest dirs.
func unitTestDirs() ([]string, error) {
	if custom := os.Getenv("UNIT_TEST_DIRS"); custom != "" {
		return strings.Fields(custom), nil
	}
	all, err := findTestDirs()
	if err != nil {
		return nil, err
	}
	excluded := []string{
		functionalTestRoot,
		functionalTestXDCRoot,
		functionalTestNDCRoot,
		dbIntegrationTestRoot,
		dbToolIntegrationTestRoot,
		"./temporaltest",
	}
	var dirs []string
	for _, d := range all {
		skip := false
		for _, ex := range excluded {
			if d == ex || strings.HasPrefix(d, ex+"/") {
				skip = true
				break
			}
		}
		if !skip {
			dirs = append(dirs, d)
		}
	}
	return dirs, nil
}

// findProtoFiles returns all .proto files under proto/internal.
// Paths are prefixed with "./" to match `find ./proto/internal` behavior,
// which the api-linter requires for correct -I path resolution.
func findProtoFiles() ([]string, error) {
	root := "./" + filepath.Join(protoRoot, "internal")
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".proto") {
			files = append(files, "./"+path)
		}
		return nil
	})
	return files, err
}

// findChasmProtoFiles returns all .proto files under chasm/lib.
func findChasmProtoFiles() ([]string, error) {
	var files []string
	err := filepath.Walk("chasm/lib", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".proto") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// protoDirs returns sorted unique directories containing proto files.
func protoDirs() ([]string, error) {
	files, err := findProtoFiles()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	for _, f := range files {
		seen[filepath.Dir(f)+"/"] = true
	}
	dirs := make([]string, 0, len(seen))
	for d := range seen {
		dirs = append(dirs, d)
	}
	return dirs, nil
}

// findScripts returns all .sh files in the repository.
func findScripts() ([]string, error) {
	var scripts []string
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".sh") {
			scripts = append(scripts, path)
		}
		return nil
	})
	return scripts, err
}

// coverpkgFlag returns the -coverpkg flag value listing all packages.
func coverpkgFlag() (string, error) {
	out, err := sh.Output("go", "list", "./...")
	if err != nil {
		return "", err
	}
	pkgs := strings.ReplaceAll(strings.TrimSpace(out), "\n", ",")
	return "-coverpkg=" + pkgs, nil
}

// newCoverProfile returns a unique coverage profile path.
func newCoverProfile() string {
	out, _ := sh.Output("xxd", "-p", "-l", "16", "/dev/urandom")
	return fmt.Sprintf("%s/coverage.%s.out", testOutputRoot, strings.TrimSpace(out))
}

// newReport returns a unique junit report path.
func newReport() string {
	out, _ := sh.Output("xxd", "-p", "-l", "16", "/dev/urandom")
	return fmt.Sprintf("%s/junit.%s.xml", testOutputRoot, strings.TrimSpace(out))
}

// findSystemWorkflowDirs returns subdirectories of the system workflows root.
func findSystemWorkflowDirs() ([]string, error) {
	entries, err := os.ReadDir(systemWorkflowsRoot)
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, filepath.Join(systemWorkflowsRoot, e.Name()))
		}
	}
	return dirs, nil
}
