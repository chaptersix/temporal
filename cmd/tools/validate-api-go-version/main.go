package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

const defaultGoModPath = "go.mod"

// knownModules defines the set of dependencies to validate.
// Add new entries here to extend validation to additional modules.
var knownModules = []moduleSpec{
	{modulePath: "go.temporal.io/api"},
	{modulePath: "go.temporal.io/sdk"},
}

type moduleSpec struct {
	modulePath string
}

type options struct {
	baseBranch string
	goModPath  string
}

func main() {
	baseBranch := flag.String("base-branch", "", "PR base branch (e.g. main, release/v1.31)")
	goModPath := flag.String("go-mod", defaultGoModPath, "Path to go.mod")
	flag.Parse()

	opts := options{
		baseBranch: strings.TrimSpace(*baseBranch),
		goModPath:  strings.TrimSpace(*goModPath),
	}

	if err := run(context.Background(), os.Stdout, opts); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer, opts options) error {
	if opts.baseBranch == "" {
		return errors.New("base branch is required; pass --base-branch")
	}

	goModPath := opts.goModPath
	if goModPath == "" {
		goModPath = defaultGoModPath
	}

	goModData, err := os.ReadFile(goModPath)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", goModPath, err)
	}

	modFile, err := modfile.Parse(goModPath, goModData, nil)
	if err != nil {
		return fmt.Errorf("failed to parse %s: %w", goModPath, err)
	}

	versions := make(map[string]string, len(modFile.Require))
	for _, req := range modFile.Require {
		versions[req.Mod.Path] = req.Mod.Version
	}

	if isReleaseLikeBranch(opts.baseBranch) {
		return validateReleaseBranch(ctx, out, versions, opts.baseBranch, knownModules, verifyTaggedRelease)
	}

	if opts.baseBranch == "main" {
		return validateMainBranch(ctx, out, versions, knownModules, verifyTaggedRelease, resolveModuleOrigin)
	}

	_, _ = fmt.Fprintf(out, "No dependency policy for base branch %q; skipping validation\n", opts.baseBranch)
	return nil
}

func isReleaseLikeBranch(baseBranch string) bool {
	return strings.HasPrefix(baseBranch, "release/") || strings.HasPrefix(baseBranch, "cloud/")
}

func validateReleaseBranch(
	ctx context.Context,
	out io.Writer,
	versions map[string]string,
	baseBranch string,
	modules []moduleSpec,
	isTaggedRelease func(context.Context, string, string) (bool, error),
) error {
	_, _ = fmt.Fprintf(out, "Base branch %q requires tagged dependency releases\n", baseBranch)

	var failures []string
	for _, mod := range modules {
		version, ok := versions[mod.modulePath]
		if !ok {
			failures = append(failures, fmt.Sprintf("%s: dependency not found in go.mod", mod.modulePath))
			continue
		}

		if !semver.IsValid(version) || semver.Prerelease(version) != "" {
			failures = append(failures, fmt.Sprintf("%s: version %q must be a stable semantic tag (vX.Y.Z)", mod.modulePath, version))
			continue
		}

		taggedRelease, err := isTaggedRelease(ctx, mod.modulePath, version)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: failed to verify tag %q: %v", mod.modulePath, version, err))
			continue
		}
		if !taggedRelease {
			failures = append(failures, fmt.Sprintf("%s: version %q is not a published tag", mod.modulePath, version))
			continue
		}

		_, _ = fmt.Fprintf(out, "  - %s@%s (ok)\n", mod.modulePath, version)
	}

	if len(failures) > 0 {
		return fmt.Errorf("release dependency validation failed:\n  - %s", strings.Join(failures, "\n  - "))
	}

	_, _ = fmt.Fprintln(out, "All required dependencies use tagged releases")
	return nil
}

func validateMainBranch(
	ctx context.Context,
	out io.Writer,
	versions map[string]string,
	modules []moduleSpec,
	isTaggedRelease func(context.Context, string, string) (bool, error),
	resolveOrigin func(context.Context, string, string) (*moduleOrigin, error),
) error {
	var failures []string
	for _, mod := range modules {
		if failure := validateMainModule(ctx, out, versions, mod, isTaggedRelease, resolveOrigin); failure != "" {
			failures = append(failures, failure)
		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("main branch dependency validation failed:\n  - %s", strings.Join(failures, "\n  - "))
	}

	return nil
}

func validateMainModule(
	ctx context.Context,
	out io.Writer,
	versions map[string]string,
	mod moduleSpec,
	isTaggedRelease func(context.Context, string, string) (bool, error),
	resolveOrigin func(context.Context, string, string) (*moduleOrigin, error),
) string {
	version, ok := versions[mod.modulePath]
	if !ok {
		return fmt.Sprintf("%s: dependency not found in go.mod", mod.modulePath)
	}

	_, _ = fmt.Fprintf(out, "Found %s version: %s\n", mod.modulePath, version)

	if !module.IsPseudoVersion(version) {
		taggedRelease, err := isTaggedRelease(ctx, mod.modulePath, version)
		if err != nil {
			return fmt.Sprintf("%s@%s: failed to verify tagged release: %v", mod.modulePath, version, err)
		}
		if !taggedRelease {
			return fmt.Sprintf("%s@%s: not a tagged release; use a published tag or a pseudo-version from the module repository default branch", mod.modulePath, version)
		}
		_, _ = fmt.Fprintf(out, "  - %s@%s is a tagged release (ok)\n", mod.modulePath, version)
		return ""
	}

	origin, err := resolveOrigin(ctx, mod.modulePath, version)
	if err != nil {
		return fmt.Sprintf("%s@%s: failed to resolve module origin: %v", mod.modulePath, version, err)
	}

	if !origin.onDefault {
		return fmt.Sprintf("%s@%s: commit %s is not on the default branch (%s) of %s",
			mod.modulePath, version, origin.hash, origin.defaultBranch, origin.url)
	}

	_, _ = fmt.Fprintf(out, "  - %s@%s is on %s (ok)\n", mod.modulePath, version, origin.defaultBranch)
	return ""
}

type moduleOrigin struct {
	hash          string // full commit hash
	url           string // VCS repository URL
	defaultBranch string // e.g. "master"
	onDefault     bool   // whether the commit is on the default branch
}

// resolveModuleOrigin uses go mod download with GOPROXY=direct to clone the
// VCS repository, then checks if the pseudo-version commit is on the default branch.
func resolveModuleOrigin(ctx context.Context, modulePath string, version string) (*moduleOrigin, error) {
	if !module.IsPseudoVersion(version) {
		return nil, fmt.Errorf("version %q is not a pseudo-version", version)
	}

	// Use a temporary module cache so go mod download clones the VCS repo fresh.
	tmpCache, err := os.MkdirTemp("", "validate-api-go-version-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp cache dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpCache) }()

	cmd := exec.CommandContext(ctx, "go", "mod", "download", "-json", modulePath+"@"+version)
	cmd.Env = append(os.Environ(), "GOPROXY=direct", "GOMODCACHE="+tmpCache, "GONOSUMCHECK=*")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go mod download failed: %w", err)
	}

	var payload struct {
		Origin struct {
			URL  string `json:"URL"`
			Hash string `json:"Hash"`
		} `json:"Origin"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		return nil, fmt.Errorf("failed to decode go mod download output: %w", err)
	}
	if payload.Origin.URL == "" {
		return nil, fmt.Errorf("go mod download did not return an origin URL for %s@%s", modulePath, version)
	}
	if payload.Origin.Hash == "" {
		return nil, fmt.Errorf("go mod download did not return an origin hash for %s@%s", modulePath, version)
	}

	defaultBranch, err := gitDefaultBranch(ctx, payload.Origin.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to determine default branch for %s: %w", payload.Origin.URL, err)
	}

	// The VCS cache from go mod download is a bare git repo. Find it and
	// use git branch --contains to check if the commit is on the default branch.
	vcsDir, err := findVCSCache(tmpCache)
	if err != nil {
		return nil, fmt.Errorf("failed to find VCS cache: %w", err)
	}

	onDefault, err := gitCommitOnBranch(ctx, vcsDir, payload.Origin.Hash, defaultBranch)
	if err != nil {
		return nil, fmt.Errorf("failed to check branch containment: %w", err)
	}

	return &moduleOrigin{
		hash:          payload.Origin.Hash,
		url:           payload.Origin.URL,
		defaultBranch: defaultBranch,
		onDefault:     onDefault,
	}, nil
}

// findVCSCache finds the bare git repo directory inside the go module VCS cache.
func findVCSCache(modCache string) (string, error) {
	vcsRoot := filepath.Join(modCache, "cache", "vcs")
	entries, err := os.ReadDir(vcsRoot)
	if err != nil {
		return "", fmt.Errorf("failed to read VCS cache dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(vcsRoot, entry.Name()), nil
		}
	}
	return "", errors.New("no VCS cache directory found")
}

// gitCommitOnBranch checks if a commit is reachable from a branch in a bare git repo.
func gitCommitOnBranch(ctx context.Context, repoDir string, commitHash string, branch string) (bool, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", repoDir, "branch", "--contains", commitHash)
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("git branch --contains failed: %w", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		name := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "* "))
		if name == branch {
			return true, nil
		}
	}
	return false, nil
}

// gitDefaultBranch uses git ls-remote to determine the default branch of a
// remote repository.
func gitDefaultBranch(ctx context.Context, repoURL string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "ls-remote", "--symref", repoURL, "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git ls-remote failed for %s: %w", repoURL, err)
	}

	// Parse output like: "ref: refs/heads/master\tHEAD"
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "ref: ") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				return strings.TrimPrefix(parts[1], "refs/heads/"), nil
			}
		}
	}

	return "", fmt.Errorf("could not determine default branch from git ls-remote output for %s", repoURL)
}

func verifyTaggedRelease(ctx context.Context, modulePath string, version string) (bool, error) {
	cmd := exec.CommandContext(ctx, "go", "list", "-m", "-json", modulePath+"@"+version)
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return false, nil
		}
		return false, fmt.Errorf("failed to query module version with go tooling: %w", err)
	}

	var payload struct {
		Version   string   `json:"Version"`
		Retracted []string `json:"Retracted"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		return false, fmt.Errorf("failed to decode go list output: %w", err)
	}

	if payload.Version == "" {
		return false, nil
	}

	for _, retracted := range payload.Retracted {
		if strings.Contains(retracted, payload.Version) {
			return false, nil
		}
	}

	return payload.Version == version, nil
}
