package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Proto contains targets for protobuf compilation and linting.
type Proto mg.Namespace

// ApiBinpb generates the proto dependencies image.
// Runs getproto in a loop until it resolves all imports.
func (Proto) ApiBinpb() error {
	color("Generating proto dependencies image...")
	for {
		out, err := sh.Output("go", "run", "./cmd/tools/getproto", "--out", apiBinpb)
		if strings.TrimSpace(out) != "<rerun>" {
			return err
		}
	}
}

// InternalBinpb generates the internal proto image.
func (p Proto) InternalBinpb() error {
	mg.Deps(p.ApiBinpb)
	color("Generate proto image...")
	protoFiles, err := findProtoFiles()
	if err != nil {
		return err
	}
	args := []string{
		"--descriptor_set_in=" + apiBinpb,
		"-I=" + protoRoot + "/internal",
	}
	args = append(args, protoFiles...)
	args = append(args, "-o", internalBinpb)
	return sh.RunV("protoc", args...)
}

// ChasmBinpb generates the CHASM proto image.
func (p Proto) ChasmBinpb() error {
	mg.Deps(p.ApiBinpb, p.InternalBinpb)
	color("Generate CHASM proto image...")
	chasmFiles, err := findChasmProtoFiles()
	if err != nil {
		return err
	}
	args := []string{
		"--descriptor_set_in=" + apiBinpb + ":" + internalBinpb,
		"-I=.",
	}
	args = append(args, chasmFiles...)
	args = append(args, "-o", chasmBinpb)
	return sh.RunV("protoc", args...)
}

// Protoc runs the proto code generator.
func (p Proto) Protoc() error {
	mg.Deps(p.ApiBinpb)
	color("Run protoc code generation...")

	root := rootDir()

	protogenPath, err := goToolPath("protogen")
	if err != nil {
		return fmt.Errorf("resolving protogen: %w", err)
	}
	goimportsPath, err := goToolPath("goimports")
	if err != nil {
		return fmt.Errorf("resolving goimports: %w", err)
	}
	mockgenPath, err := goToolPath("mockgen")
	if err != nil {
		return fmt.Errorf("resolving mockgen: %w", err)
	}
	protocGenGoPath, err := goToolPath("protoc-gen-go")
	if err != nil {
		return fmt.Errorf("resolving protoc-gen-go: %w", err)
	}
	protocGenGoGrpcPath, err := goToolPath("protoc-gen-go-grpc")
	if err != nil {
		return fmt.Errorf("resolving protoc-gen-go-grpc: %w", err)
	}
	protocGenGoHelpersPath, err := goToolPath("protoc-gen-go-helpers")
	if err != nil {
		return fmt.Errorf("resolving protoc-gen-go-helpers: %w", err)
	}

	// Build protoc-gen-go-chasm locally.
	chasmBin := filepath.Join(root, "protoc-gen-go-chasm")
	if err := sh.RunV("go", "build", "-o", chasmBin, "./cmd/tools/protoc-gen-go-chasm"); err != nil {
		return err
	}

	dirs, err := protoDirs()
	if err != nil {
		return err
	}

	args := []string{"run", "./cmd/tools/protogen",
		"-root=" + root,
		"-proto-out=" + protoOut,
		"-proto-root=" + protoRoot,
		"-api-binpb=" + apiBinpb,
		"-protogen-bin=" + protogenPath,
		"-goimports-bin=" + goimportsPath,
		"-mockgen-bin=" + mockgenPath,
		"-protoc-gen-go-chasm-bin=" + chasmBin,
		"-protoc-gen-go-bin=" + protocGenGoPath,
		"-protoc-gen-go-grpc-bin=" + protocGenGoGrpcPath,
		"-protoc-gen-go-helpers-bin=" + protocGenGoHelpersPath,
	}
	args = append(args, dirs...)
	return sh.RunV("go", args...)
}

// Codegen generates service clients and interceptors.
func (Proto) Codegen() error {
	color("Generate service clients...")
	if err := sh.RunV("go", "generate", "-run", "genrpcwrappers", "./client/..."); err != nil {
		return err
	}
	color("Generate server interceptors...")
	if err := sh.RunV("go", "generate", "./common/rpc/interceptor/logtags/..."); err != nil {
		return err
	}
	color("Generate search attributes helpers...")
	return sh.RunV("go", "generate", "-run", "gensearchattributehelpers", "./common/searchattribute/...")
}

// BufBreaking runs buf breaking change detection.
//
// Compares current proto images against the PR merge base to detect
// backward-incompatible protobuf changes. In CI (PR_BASE_COMMIT set),
// it clones the repo to a temp dir, checks out the base, builds its
// proto images, and runs buf breaking against the current images.
func (p Proto) BufBreaking() error {
	mg.Deps(p.ApiBinpb, p.InternalBinpb, p.ChasmBinpb)
	color("Run buf breaking proto changes check...")

	// Check for uncommitted changes.
	porcelain, err := sh.Output("git", "status", "--porcelain", "--untracked-files=no")
	if err != nil {
		return err
	}
	if porcelain != "" {
		red("Commit all local changes before running buf-breaking")
		_ = sh.RunV("git", "status")
		// Exit cleanly: in CI, ensure-no-changes will catch this later.
		return nil
	}

	bufPath, err := devtoolPath("buf")
	if err != nil {
		return err
	}

	commit, err := sh.Output("git", "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	commit = strings.TrimSpace(commit)

	prBaseCommit := envOrDefault("PR_BASE_COMMIT", "")
	if prBaseCommit == "" {
		base, err := sh.Output("git", "merge-base", "HEAD", "main")
		if err != nil {
			return fmt.Errorf("finding merge base: %w", err)
		}
		prBaseCommit = strings.TrimSpace(base)
	}

	if prBaseCommit == commit {
		color("HEAD is the merge base, nothing to compare")
		return nil
	}

	// Fetch enough history for the merge base in shallow clones (CI).
	color("Fetching more commits from %s...", prBaseCommit)
	_ = sh.RunV("git", "fetch", "--no-tags", "--no-recurse-submodules", "--depth=100", "origin", prBaseCommit)

	// Clone the repo to a temp directory.
	tmp, err := os.MkdirTemp("", "temporal-buf-breaking.*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	color("Cloning repo to temp dir...")
	if err := sh.RunV("git", "clone", ".", tmp); err != nil {
		return err
	}

	// Find the merge base in the clone.
	if err := sh.RunV("git", "-C", tmp, "fetch", "origin", prBaseCommit); err != nil {
		return err
	}
	base, err := sh.Output("git", "-C", tmp, "merge-base", "HEAD", prBaseCommit)
	if err != nil {
		yellow("Can't find merge base for breaking check, skipping")
		return nil
	}
	base = strings.TrimSpace(base)

	return bufBreakingCheck(bufPath, tmp, base, "merge base")
}

// bufBreakingCheck checks proto images against a specific commit.
func bufBreakingCheck(bufPath, tmpRepo, commit, name string) error {
	color("Breaking check against %s:", name)
	if err := sh.RunV("git", "-C", tmpRepo, "checkout", "--detach", commit); err != nil {
		return err
	}

	// Check if the base commit's Makefile supports the targets we need.
	makefile, err := os.ReadFile(filepath.Join(tmpRepo, "Makefile"))
	if err != nil {
		return fmt.Errorf("reading base Makefile: %w", err)
	}
	makeContent := string(makefile)

	if strings.Contains(makeContent, "INTERNAL_BINPB") {
		if err := sh.RunV("make", "-C", tmpRepo, internalBinpb); err != nil {
			return err
		}
		if err := sh.RunV(bufPath, "breaking", internalBinpb,
			"--against", filepath.Join(tmpRepo, internalBinpb),
			"--config", "proto/internal/buf.yaml"); err != nil {
			return err
		}
	} else {
		yellow("%s commit is too old to support breaking check for internal protos", name)
	}

	if strings.Contains(makeContent, "CHASM_BINPB") {
		if err := sh.RunV("make", "-C", tmpRepo, chasmBinpb); err != nil {
			return err
		}
		if err := sh.RunV(bufPath, "breaking", chasmBinpb,
			"--against", filepath.Join(tmpRepo, chasmBinpb),
			"--config", "chasm/lib/buf.yaml"); err != nil {
			return err
		}
	} else {
		yellow("%s commit is too old to support breaking check for chasm protos", name)
	}

	return nil
}

// All runs the full proto pipeline: lint, compile, and codegen.
func (p Proto) All() error {
	// Lint first, then protoc, then codegen (serial dependencies).
	l := Lint{}
	mg.SerialDeps(l.Protos, l.Api, p.Protoc, p.Codegen)
	return nil
}

// UpdateGoAPI updates go.temporal.io/api to latest master.
func (Proto) UpdateGoAPI() error {
	color("Update go.temporal.io/api@master...")
	return sh.RunV("go", "get", "-u", "go.temporal.io/api@master")
}
