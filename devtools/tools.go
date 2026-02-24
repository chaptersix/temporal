//go:build tools

// Package devtools tracks heavy development tool dependencies.
// This file ensures the imports are not removed by go mod tidy.
package devtools

import (
	_ "fillmore-labs.com/errortype"
	_ "github.com/bufbuild/buf/cmd/buf"
	_ "github.com/daixiang0/gci"
	_ "github.com/golangci/golangci-lint/v2/cmd/golangci-lint"
	_ "github.com/google/yamlfmt/cmd/yamlfmt"
	_ "github.com/googleapis/api-linter/cmd/api-linter"
	_ "github.com/icholy/gomajor"
	_ "github.com/rhysd/actionlint/cmd/actionlint"
	_ "go.temporal.io/sdk/contrib/tools/workflowcheck"
	_ "gotest.tools/gotestsum"
)
