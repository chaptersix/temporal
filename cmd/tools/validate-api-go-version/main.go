package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

const defaultGoModPath = "go.mod"

type moduleSpec struct {
	modulePath    string
	repoURL       string
	defaultBranch string
}

var knownModules = []moduleSpec{
	{
		modulePath:    "go.temporal.io/api",
		repoURL:       "https://github.com/temporalio/api-go.git",
		defaultBranch: "master",
	},
	{
		modulePath:    "go.temporal.io/sdk",
		repoURL:       "https://github.com/temporalio/sdk-go.git",
		defaultBranch: "master",
	},
}

const defaultBranch = "main"

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

	if err := run(context.Background(), opts); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, opts options) error {
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

	if strings.HasPrefix(opts.baseBranch, "release/") ||
		strings.HasPrefix(opts.baseBranch, "cloud/") {
		return validateReleaseBranch(ctx, modFile)
	}

	if opts.baseBranch == defaultBranch {
		return validateMainBranch(ctx, modFile)
	}

	fmt.Printf("No dependency policy for base branch %q; skipping validation\n", opts.baseBranch)
	return nil
}

func validateReleaseBranch(
	ctx context.Context,
	modFile *modfile.File,
) error {
	var failures []string
	for _, mod := range knownModules {
		modVersion, ok := findRequiredModuleVersion(modFile, mod.modulePath)
		if !ok {
			failures = append(failures, fmt.Sprintf("%s: dependency not found in go.mod", mod.modulePath))
			continue
		}

		if !semver.IsValid(modVersion.Version) || module.IsPseudoVersion(modVersion.Version) {
			failures = append(failures, fmt.Sprintf("%s: version %q must be a valid semantic tag and not a pseudo-version", mod.modulePath, modVersion.Version))
			continue
		}

		taggedRelease, err := verifyVersion(ctx, modVersion)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: failed to verify tag %q: %v", mod.modulePath, modVersion.Version, err))
			continue
		}
		if !taggedRelease {
			failures = append(failures, fmt.Sprintf("%s: version %q is not a published tag", mod.modulePath, modVersion.Version))
			continue
		}

		fmt.Printf("  - %s@%s (ok)\n", mod.modulePath, modVersion.Version)
	}

	if len(failures) > 0 {
		return fmt.Errorf("release dependency validation failed:\n  - %s", strings.Join(failures, "\n  - "))
	}

	fmt.Println("All required dependencies use tagged releases")
	return nil
}

func validateMainBranch(
	ctx context.Context,
	modFile *modfile.File,
) error {
	var failures []string
	for _, mod := range knownModules {
		if failure := validateMainModule(ctx, modFile, mod); failure != "" {
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
	modFile *modfile.File,
	mod moduleSpec,
) string {
	modVersion, ok := findRequiredModuleVersion(modFile, mod.modulePath)
	if !ok {
		return fmt.Sprintf("%s: dependency not found in go.mod", mod.modulePath)
	}
	version := modVersion.Version

	fmt.Printf("Found %s version: %s\n", mod.modulePath, version)

	if !module.IsPseudoVersion(version) {
		taggedRelease, err := verifyVersion(ctx, modVersion)
		if err != nil {
			return fmt.Sprintf("%s@%s: failed to verify tagged release: %v", mod.modulePath, version, err)
		}
		if !taggedRelease {
			return fmt.Sprintf("%s@%s: not a tagged release; use a published tag or a pseudo-version from the module repository default branch", mod.modulePath, version)
		}
		fmt.Printf("  - %s@%s is a tagged release (ok)\n", mod.modulePath, version)
		return ""
	}

	shortHash, err := module.PseudoVersionRev(version)
	if err != nil {
		return fmt.Sprintf("%s@%s: failed to parse pseudo-version revision: %v", mod.modulePath, version, err)
	}

	onDefault, err := resolveModuleOriginForSpec(ctx, mod, shortHash)
	if err != nil {
		return fmt.Sprintf("%s@%s: failed to resolve module origin: %v", mod.modulePath, version, err)
	}

	if !onDefault {
		return fmt.Sprintf("%s@%s: commit %s is not on the default branch (%s) of %s",
			mod.modulePath, version, shortHash, mod.defaultBranch, mod.repoURL)
	}

	fmt.Printf("  - %s@%s is on %s (ok)\n", mod.modulePath, version, mod.defaultBranch)
	return ""
}

func findRequiredModuleVersion(modFile *modfile.File, modulePath string) (module.Version, bool) {
	for _, req := range modFile.Require {
		if req.Mod.Path == modulePath {
			return req.Mod, true
		}
	}
	return module.Version{}, false
}

func resolveModuleOriginForSpec(ctx context.Context, mod moduleSpec, shortHash string) (bool, error) {
	tmpRepo, err := os.MkdirTemp("", "validate-api-go-version-*")
	if err != nil {
		return false, fmt.Errorf("failed to create temp repo dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpRepo) }()

	cmd := exec.CommandContext(ctx, "git", "clone", "--bare", "--filter=blob:none", "--single-branch", "--branch", mod.defaultBranch, mod.repoURL, tmpRepo)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("git clone failed: %w: %s", err, strings.TrimSpace(string(out)))
	}

	onDefault, err := gitCommitOnBranch(ctx, tmpRepo, shortHash, mod.defaultBranch)
	if err != nil {
		return false, fmt.Errorf("failed to check branch containment: %w", err)
	}

	return onDefault, nil
}

// gitCommitOnBranch checks if a commit is reachable from a branch in a bare git repo.
func gitCommitOnBranch(ctx context.Context, repoDir string, commitHash string, branch string) (bool, error) {
	checkCommit := exec.CommandContext(ctx, "git", "-C", repoDir, "cat-file", "-e", commitHash+"^{commit}")
	if err := checkCommit.Run(); err != nil {
		if _, ok := errors.AsType[*exec.ExitError](err); ok {
			return false, nil
		}
		return false, fmt.Errorf("git cat-file failed: %w", err)
	}

	cmd := exec.CommandContext(ctx, "git", "-C", repoDir, "merge-base", "--is-ancestor", commitHash, "refs/heads/"+branch)
	err := cmd.Run()
	if err == nil {
		return true, nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}

	return false, fmt.Errorf("git merge-base --is-ancestor failed: %w", err)
}

func verifyVersion(ctx context.Context, v module.Version) (bool, error) {
	cmd := exec.CommandContext(ctx, "go", "list", "-m", "-json", v.String())
	out, err := cmd.Output()
	if err != nil {
		if _, ok := errors.AsType[*exec.ExitError](err); ok {
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

	return payload.Version == v.Version, nil
}
