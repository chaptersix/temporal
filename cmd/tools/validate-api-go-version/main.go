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

	expectedRef := "refs/heads/" + origin.defaultBranch
	if origin.ref != expectedRef {
		return fmt.Sprintf("%s@%s: pseudo-version resolved to branch %q but must be on the default branch %q; use a tagged release or a pseudo-version from %s",
			mod.modulePath, version, origin.ref, expectedRef, origin.defaultBranch)
	}

	_, _ = fmt.Fprintf(out, "  - %s@%s is on %s (ok)\n", mod.modulePath, version, origin.defaultBranch)
	return ""
}

type moduleOrigin struct {
	ref           string // e.g. "refs/heads/master"
	defaultBranch string // e.g. "master"
}

// resolveModuleOrigin uses go mod download with GOPROXY=direct to get the
// Origin.Ref and Origin.URL from the VCS repository directly, then uses
// git ls-remote to determine the default branch.
func resolveModuleOrigin(ctx context.Context, modulePath string, version string) (*moduleOrigin, error) {
	if !module.IsPseudoVersion(version) {
		return nil, fmt.Errorf("version %q is not a pseudo-version", version)
	}

	// Use a temporary module cache so Go fetches directly from VCS.
	// The Origin.Ref field is only populated on a fresh git fetch;
	// a warm cache omits it.
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
			URL string `json:"URL"`
			Ref string `json:"Ref"`
		} `json:"Origin"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		return nil, fmt.Errorf("failed to decode go mod download output: %w", err)
	}
	if payload.Origin.Ref == "" {
		return nil, fmt.Errorf("go mod download did not return an origin ref for %s@%s", modulePath, version)
	}
	if payload.Origin.URL == "" {
		return nil, fmt.Errorf("go mod download did not return an origin URL for %s@%s", modulePath, version)
	}

	defaultBranch, err := gitDefaultBranch(ctx, payload.Origin.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to determine default branch for %s: %w", payload.Origin.URL, err)
	}

	return &moduleOrigin{
		ref:           payload.Origin.Ref,
		defaultBranch: defaultBranch,
	}, nil
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
