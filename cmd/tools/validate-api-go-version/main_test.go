package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testAPIModule = moduleSpec{modulePath: "go.temporal.io/api"}
	testSDKModule = moduleSpec{modulePath: "go.temporal.io/sdk"}
)

func TestValidateReleaseBranchSuccess(t *testing.T) {
	t.Parallel()

	versions := map[string]string{
		"go.temporal.io/api": "v1.62.3",
		"go.temporal.io/sdk": "v1.41.1",
	}

	var out bytes.Buffer
	err := validateReleaseBranch(
		context.Background(),
		&out,
		versions,
		"release/v1.31",
		[]moduleSpec{testAPIModule, testSDKModule},
		func(context.Context, string, string) (bool, error) {
			return true, nil
		},
	)
	require.NoError(t, err)
}

func TestValidateReleaseBranchFailureMessage(t *testing.T) {
	t.Parallel()

	versions := map[string]string{
		"go.temporal.io/api": "v1.62.3-0.20260327234204-dbc016f3811d",
		"go.temporal.io/sdk": "v1.41.1",
	}

	var out bytes.Buffer
	err := validateReleaseBranch(
		context.Background(),
		&out,
		versions,
		"release/v1.31",
		[]moduleSpec{testAPIModule},
		func(context.Context, string, string) (bool, error) {
			return false, nil
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "release dependency validation failed")
	require.Contains(t, err.Error(), "must be a stable semantic tag")
}

func TestValidateMainBranchTaggedRelease(t *testing.T) {
	t.Parallel()

	versions := map[string]string{
		"go.temporal.io/api": "v1.62.3",
		"go.temporal.io/sdk": "v1.41.1",
	}

	validatedModules := make([]string, 0, 2)
	var out bytes.Buffer
	err := validateMainBranch(
		context.Background(),
		&out,
		versions,
		[]moduleSpec{testAPIModule, testSDKModule},
		func(_ context.Context, modulePath string, _ string) (bool, error) {
			validatedModules = append(validatedModules, modulePath)
			return true, nil
		},
		func(context.Context, string, string) (*moduleOrigin, error) { return nil, nil },
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"go.temporal.io/api", "go.temporal.io/sdk"}, validatedModules)
}

func TestValidateMainBranchPseudoVersionOnDefault(t *testing.T) {
	t.Parallel()

	versions := map[string]string{
		"go.temporal.io/api": "v1.62.3-0.20260327234204-dbc016f3811d",
		"go.temporal.io/sdk": "v1.41.1",
	}

	var out bytes.Buffer
	err := validateMainBranch(
		context.Background(),
		&out,
		versions,
		[]moduleSpec{testAPIModule},
		func(context.Context, string, string) (bool, error) { return false, nil },
		func(context.Context, string, string) (*moduleOrigin, error) {
			return &moduleOrigin{ref: "refs/heads/master", defaultBranch: "master"}, nil
		},
	)
	require.NoError(t, err)
	require.Contains(t, out.String(), "is on master (ok)")
}

func TestValidateMainBranchPseudoVersionNotOnDefault(t *testing.T) {
	t.Parallel()

	versions := map[string]string{
		"go.temporal.io/api": "v1.62.3-0.20260327234204-dbc016f3811d",
	}

	var out bytes.Buffer
	err := validateMainBranch(
		context.Background(),
		&out,
		versions,
		[]moduleSpec{testAPIModule},
		func(context.Context, string, string) (bool, error) { return false, nil },
		func(context.Context, string, string) (*moduleOrigin, error) {
			return &moduleOrigin{ref: "refs/heads/serverless", defaultBranch: "master"}, nil
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "main branch dependency validation failed")
	require.Contains(t, err.Error(), "must be on the default branch")
	require.Contains(t, err.Error(), "refs/heads/serverless")
}

func TestValidateMainBranchSDKPseudoVersionOnDefault(t *testing.T) {
	t.Parallel()

	versions := map[string]string{
		"go.temporal.io/api": "v1.62.3",
		"go.temporal.io/sdk": "v1.41.1-0.20260401120000-abcdef123456",
	}

	var out bytes.Buffer
	err := validateMainBranch(
		context.Background(),
		&out,
		versions,
		[]moduleSpec{testAPIModule, testSDKModule},
		func(_ context.Context, modulePath string, _ string) (bool, error) {
			return modulePath == "go.temporal.io/api", nil
		},
		func(context.Context, string, string) (*moduleOrigin, error) {
			return &moduleOrigin{ref: "refs/heads/master", defaultBranch: "master"}, nil
		},
	)
	require.NoError(t, err)
}

func TestRunSkipsUnconfiguredBranches(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	goModPath := filepath.Join(tempDir, "go.mod")
	err := os.WriteFile(goModPath, []byte("module go.temporal.io/server\n"), 0644)
	require.NoError(t, err)

	var out bytes.Buffer
	err = run(context.Background(), &out, options{baseBranch: "feature/foo", goModPath: goModPath})
	require.NoError(t, err)
	require.Contains(t, out.String(), "skipping validation")
}

func TestGitDefaultBranch(t *testing.T) {
	t.Parallel()

	branch, err := gitDefaultBranch(context.Background(), "https://github.com/temporalio/api-go")
	require.NoError(t, err)
	require.Equal(t, "master", branch)
}
