package main

import (
	"fmt"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Generate contains targets for code generation and module maintenance.
type Generate mg.Namespace

// GoGenerate runs go generate on all packages.
func (Generate) GoGenerate() error {
	color("Process go:generate directives...")
	return sh.RunV("go", "generate", "./...")
}

// ModTidy runs go mod tidy.
func (Generate) ModTidy() error {
	color("go mod tidy...")
	return sh.RunV("go", "mod", "tidy")
}

// EnsureNoChanges checks that there are no uncommitted changes.
func (Generate) EnsureNoChanges() error {
	color("Check for local changes...")
	color("========================================================================")
	if err := sh.RunV("git", "status", "--porcelain"); err != nil {
		return err
	}
	out, err := sh.Output("git", "status", "--porcelain")
	if err != nil {
		return err
	}
	if out != "" {
		color("========================================================================")
		red("Above files are not regenerated properly. Regenerate them and try again.")
		return fmt.Errorf("uncommitted changes detected")
	}
	return nil
}

// UpdateDeps updates all dependencies to their latest minor versions.
func (Generate) UpdateDeps() error {
	color("Update dependencies (minor versions only) ...")
	if err := sh.RunV("go", "get", "-u", "-t", "./..."); err != nil {
		return err
	}
	return sh.RunV("go", "mod", "tidy")
}

// UpdateDepsMajor updates dependencies to their latest major versions.
func (Generate) UpdateDepsMajor() error {
	color("Major version upgrades available:")
	if err := devtool("gomajor", "list", "-major"); err != nil {
		return err
	}
	fmt.Println()
	color("Update dependencies (major versions only) ...")
	if err := devtool("gomajor", "get", "-major", "all"); err != nil {
		return err
	}
	return sh.RunV("go", "mod", "tidy")
}

// UpdateDashboards updates the Grafana dashboards submodule.
func (Generate) UpdateDashboards() error {
	color("Update dashboards submodule from remote...")
	return sh.RunV("git", "submodule", "update", "--force", "--init", "--remote",
		"develop/docker-compose/grafana/provisioning/temporalio-dashboards")
}
