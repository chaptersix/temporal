package main

import (
	"fmt"
	"os/exec"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Lint contains targets for running linters.
type Lint mg.Namespace

// Code runs golangci-lint and errortype vet.
func (Lint) Code() error {
	color("Linting code...")
	if err := devtool("golangci-lint",
		"run",
		"--verbose",
		"--build-tags="+allTestTags(),
		"--timeout=10m",
		"--fix="+golangciLintFix,
		"--new-from-rev="+golangciLintBaseRev,
		"--config=.github/.golangci.yml",
	); err != nil {
		return err
	}
	errortypePath, err := devtoolPath("errortype")
	if err != nil {
		return fmt.Errorf("resolving errortype path: %w", err)
	}
	return sh.RunV("go", "vet",
		"-tags="+allTestTags(),
		"-vettool="+errortypePath,
		"-style-check=false",
		"./...",
	)
}

// Actions runs actionlint on GitHub Actions workflows.
func (Lint) Actions() error {
	color("Linting GitHub actions...")
	return devtool("actionlint")
}

// Api runs the API proto linter.
//
//nolint:revive,staticcheck // mage derives CLI target name from method name
func (l Lint) Api() error {
	color("Linting proto API...")
	mg.Deps(Proto.ApiBinpb)
	protoFiles, err := findProtoFiles()
	if err != nil {
		return err
	}
	args := []string{
		"--set-exit-status",
		"-I=" + protoRoot + "/internal",
		"--descriptor-set-in", apiBinpb,
		"--config=" + protoRoot + "/api-linter.yaml",
	}
	args = append(args, protoFiles...)
	// Capture output and only show on failure (matches old silent_exec behavior).
	bin, err := devtoolPath("api-linter")
	if err != nil {
		return err
	}
	cmd := exec.Command(bin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Print(string(out))
		return err
	}
	return nil
}

// Protos runs buf lint on proto definitions.
func (l Lint) Protos() error {
	color("Linting proto definitions...")
	mg.Deps(Proto.InternalBinpb, Proto.ChasmBinpb)
	if err := devtool("buf", "lint", internalBinpb); err != nil {
		return err
	}
	return devtool("buf", "lint", "--config", "chasm/lib/buf.yaml", chasmBinpb)
}

// Yaml checks YAML formatting.
func (Lint) Yaml() error {
	color("Checking YAML formatting...")
	return devtool("yamlfmt", "-conf", ".github/.yamlfmt", "-lint", ".")
}

// ShellCheck runs shellcheck on all shell scripts.
func (Lint) ShellCheck() error {
	color("Run shellcheck for script files...")
	scripts, err := findScripts()
	if err != nil {
		return err
	}
	args := append([]string{}, scripts...)
	return sh.RunV("shellcheck", args...)
}

// WorkflowCheck runs workflowcheck on system workflows.
func (Lint) WorkflowCheck() error {
	color("Run workflowcheck for system workflows...")
	dirs, err := findSystemWorkflowDirs()
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		fmt.Println("Running workflowcheck on", dir)
		// Prefix with ./ so Go tooling treats it as a local package.
		pkg := "./" + dir
		if err := devtool("workflowcheck", pkg); err != nil {
			return err
		}
	}
	return nil
}

// All runs all linters.
func (l Lint) All() {
	mg.Deps(l.Code, l.Actions, l.Api, l.Protos, l.Yaml)
}
