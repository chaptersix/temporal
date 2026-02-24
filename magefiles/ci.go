package main

import (
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// CI contains targets for continuous integration.
type CI mg.Namespace

// BuildMisc runs the ci-build-misc composite target.
func (CI) BuildMisc() error {
	// Print Go version first.
	if err := sh.RunV("go", "version"); err != nil {
		return err
	}

	// Run steps serially as in the original Makefile.
	p := Proto{}
	g := Generate{}
	l := Lint{}
	f := Fmt{}

	// proto
	if err := p.All(); err != nil {
		return err
	}

	// go-generate
	if err := g.GoGenerate(); err != nil {
		return err
	}

	// buf-breaking
	if err := p.BufBreaking(); err != nil {
		return err
	}

	// shell-check
	if err := l.ShellCheck(); err != nil {
		return err
	}

	// goimports
	if err := f.Goimports(); err != nil {
		return err
	}

	// gomodtidy
	if err := g.ModTidy(); err != nil {
		return err
	}

	// ensure-no-changes
	return g.EnsureNoChanges()
}
