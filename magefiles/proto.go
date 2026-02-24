package main

import (
	"fmt"
	"path/filepath"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Proto contains targets for protobuf compilation and linting.
type Proto mg.Namespace

// ApiBinpb generates the proto dependencies image.
func (Proto) ApiBinpb() error {
	color("Generating proto dependencies image...")
	return sh.RunV("./cmd/tools/getproto/run.sh", "--out", apiBinpb)
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
func (p Proto) BufBreaking() error {
	mg.Deps(p.ApiBinpb, p.InternalBinpb, p.ChasmBinpb)
	color("Run buf breaking proto changes check...")
	bufPath, err := devtoolPath("buf")
	if err != nil {
		return err
	}
	env := map[string]string{
		"BUF":            bufPath,
		"API_BINPB":      apiBinpb,
		"INTERNAL_BINPB": internalBinpb,
		"CHASM_BINPB":    chasmBinpb,
		"MAIN_BRANCH":    "main",
	}
	return sh.RunWithV(env, "./develop/buf-breaking.sh")
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
