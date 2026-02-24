package main

import (
	"os"
	"strings"
)

// Build configuration.
var (
	goos       = envOrDefault("GOOS", goEnv("GOOS"))
	goarch     = envOrDefault("GOARCH", goEnv("GOARCH"))
	cgoEnabled = envOrDefault("CGO_ENABLED", "0")
)

// Build tags.
const baseBuildTag = "disable_grpc_modules"

func allBuildTags() string {
	extra := os.Getenv("BUILD_TAG")
	if extra != "" {
		return baseBuildTag + "," + extra
	}
	return baseBuildTag
}

func allTestTags() string {
	tags := allBuildTags() + ",test_dep"
	extra := os.Getenv("TEST_TAG")
	if extra != "" {
		tags += "," + extra
	}
	return tags
}

func buildTagFlag() string {
	return "-tags=" + allBuildTags()
}

func testTagFlag() string {
	return "-tags=" + allTestTags()
}

// Test configuration.
var (
	testTimeout = envOrDefault("TEST_TIMEOUT", "35m")
	maxAttempts = envOrDefault("MAX_TEST_ATTEMPTS", "3")
	testRace    = envOrDefault("TEST_RACE_FLAG", "on")
	testShuffle = envOrDefault("TEST_SHUFFLE_FLAG", "on")
)

// Persistence configuration.
var (
	persistenceType   = envOrDefault("PERSISTENCE_TYPE", "nosql")
	persistenceDriver = envOrDefault("PERSISTENCE_DRIVER", "cassandra")
	temporalDB        = envOrDefault("TEMPORAL_DB", "temporal")
	visibilityDB      = envOrDefault("VISIBILITY_DB", "temporal_visibility")
)

// Database configuration.
var (
	sqlUser     = envOrDefault("SQL_USER", "temporal")
	sqlPassword = envOrDefault("SQL_PASSWORD", "temporal")
)

// Lint configuration.
var (
	golangciLintBaseRev = envOrDefault("GOLANGCI_LINT_BASE_REV", "main")
	golangciLintFix     = envOrDefault("GOLANGCI_LINT_FIX", "true")
)

// Fmt configuration.
var (
	gofixFlags         = envOrDefault("GOFIX_FLAGS", "-any -rangeint")
	gofixMaxIterations = envOrDefault("GOFIX_MAX_ITERATIONS", "5")
)

// Paths.
const (
	protoRoot     = "proto"
	protoOut      = "api"
	apiBinpb      = protoRoot + "/api.binpb"
	internalBinpb = protoRoot + "/image.bin"
	chasmBinpb    = protoRoot + "/chasm.bin"
)

// Test directories.
const (
	functionalTestRoot        = "./tests"
	functionalTestXDCRoot     = "./tests/xdc"
	functionalTestNDCRoot     = "./tests/ndc"
	dbIntegrationTestRoot     = "./common/persistence/tests"
	dbToolIntegrationTestRoot = "./tools/tests"
	testOutputRoot            = "./.testoutput"
	systemWorkflowsRoot       = "./service/worker"
)

var integrationTestDirs = []string{
	dbIntegrationTestRoot,
	dbToolIntegrationTestRoot,
	"./temporaltest",
}

// testCgoEnabled returns the CGO_ENABLED value for tests.
// Race detection requires cgo, so force it on when race is enabled.
func testCgoEnabled() string {
	if isTruthy(testRace) {
		return "1"
	}
	return cgoEnabled
}

// compiledTestArgs returns the common test flags.
func compiledTestArgs() []string {
	args := []string{
		"-timeout=" + testTimeout,
	}
	if isTruthy(testRace) {
		args = append(args, "-race")
	}
	if isTruthy(testShuffle) {
		args = append(args, "-shuffle=on")
	}
	if p := os.Getenv("TEST_PARALLEL_FLAGS"); p != "" {
		args = append(args, strings.Fields(p)...)
	}
	if a := os.Getenv("TEST_ARGS"); a != "" {
		args = append(args, strings.Fields(a)...)
	}
	args = append(args, testTagFlag())
	return args
}

