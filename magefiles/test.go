package main

import (
	"os"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Test contains targets for running tests.
type Test mg.Namespace

// Unit runs unit tests.
func (Test) Unit() error {
	mg.Deps(Clean.TestOutput)
	color("Run unit tests...")
	dirs, err := unitTestDirs()
	if err != nil {
		return err
	}
	env := map[string]string{"CGO_ENABLED": cgoEnabled}
	args := append([]string{"test"}, dirs...)
	args = append(args, compiledTestArgs()...)
	return sh.RunWithV(env, "go", args...)
}

// Integration runs integration tests.
func (Test) Integration() error {
	mg.Deps(Clean.TestOutput)
	color("Run integration tests...")
	env := map[string]string{"CGO_ENABLED": cgoEnabled}
	args := append([]string{"test"}, integrationTestDirs...)
	args = append(args, compiledTestArgs()...)
	return sh.RunWithV(env, "go", args...)
}

// Functional runs functional tests.
func (Test) Functional() error {
	mg.Deps(Clean.TestOutput)
	color("Run functional tests...")
	env := map[string]string{"CGO_ENABLED": cgoEnabled}
	tArgs := compiledTestArgs()
	persistenceArgs := []string{
		"-persistenceType=" + persistenceType,
		"-persistenceDriver=" + persistenceDriver,
	}

	for _, root := range []string{functionalTestRoot, functionalTestNDCRoot, functionalTestXDCRoot} {
		args := append([]string{"test", root}, tArgs...)
		args = append(args, persistenceArgs...)
		if err := sh.RunWithV(env, "go", args...); err != nil {
			return err
		}
	}
	return nil
}

// FunctionalFaultInjection runs functional tests with fault injection.
func (Test) FunctionalFaultInjection() error {
	mg.Deps(Clean.TestOutput)
	color("Run integration tests with fault injection...")
	env := map[string]string{"CGO_ENABLED": cgoEnabled}
	tArgs := compiledTestArgs()
	persistenceArgs := []string{
		"-enableFaultInjection=true",
		"-persistenceType=" + persistenceType,
		"-persistenceDriver=" + persistenceDriver,
	}

	for _, root := range []string{functionalTestRoot, functionalTestNDCRoot, functionalTestXDCRoot} {
		args := append([]string{"test", root}, tArgs...)
		args = append(args, persistenceArgs...)
		if err := sh.RunWithV(env, "go", args...); err != nil {
			return err
		}
	}
	return nil
}

// UnitCoverage runs unit tests with coverage and retries.
func (t Test) UnitCoverage() error {
	mg.Deps(Clean.TestOutput)
	if err := os.MkdirAll(testOutputRoot, 0o755); err != nil {
		return err
	}
	color("Run unit tests with coverage...")
	dirs, err := unitTestDirs()
	if err != nil {
		return err
	}
	goTestsumPath, err := devtoolPath("gotestsum")
	if err != nil {
		return err
	}
	args := []string{"run", "./cmd/tools/test-runner", "test",
		"--gotestsum-path=" + goTestsumPath,
		"--max-attempts=" + maxAttempts,
		"--junitfile=" + newReport(),
		"--",
	}
	args = append(args, compiledTestArgs()...)
	args = append(args, "-coverprofile="+newCoverProfile())
	args = append(args, dirs...)
	return sh.RunV("go", args...)
}

// IntegrationCoverage runs integration tests with coverage and retries.
func (t Test) IntegrationCoverage() error {
	mg.Deps(Clean.TestOutput)
	if err := os.MkdirAll(testOutputRoot, 0o755); err != nil {
		return err
	}
	color("Run integration tests with coverage...")
	goTestsumPath, err := devtoolPath("gotestsum")
	if err != nil {
		return err
	}
	args := []string{"run", "./cmd/tools/test-runner", "test",
		"--gotestsum-path=" + goTestsumPath,
		"--max-attempts=" + maxAttempts,
		"--junitfile=" + newReport(),
		"--",
	}
	args = append(args, compiledTestArgs()...)
	args = append(args, "-coverprofile="+newCoverProfile())
	args = append(args, integrationTestDirs...)
	return sh.RunV("go", args...)
}

// FunctionalCoverage runs functional tests with coverage and retries.
func (t Test) FunctionalCoverage() error {
	mg.Deps(Clean.TestOutput)
	if err := os.MkdirAll(testOutputRoot, 0o755); err != nil {
		return err
	}
	color("Run functional tests with coverage with %s driver...", persistenceDriver)
	goTestsumPath, err := devtoolPath("gotestsum")
	if err != nil {
		return err
	}
	coverpkg, err := coverpkgFlag()
	if err != nil {
		return err
	}
	args := []string{"run", "./cmd/tools/test-runner", "test",
		"--gotestsum-path=" + goTestsumPath,
		"--max-attempts=" + maxAttempts,
		"--junitfile=" + newReport(),
		"--",
	}
	args = append(args, compiledTestArgs()...)
	args = append(args, "-coverprofile="+newCoverProfile(), coverpkg, functionalTestRoot)
	args = append(args, "-args",
		"-persistenceType="+persistenceType,
		"-persistenceDriver="+persistenceDriver,
	)
	return sh.RunV("go", args...)
}

// FunctionalXDCCoverage runs functional XDC tests with coverage and retries.
func (t Test) FunctionalXDCCoverage() error {
	mg.Deps(Clean.TestOutput)
	if err := os.MkdirAll(testOutputRoot, 0o755); err != nil {
		return err
	}
	color("Run functional test for cross DC with coverage with %s driver...", persistenceDriver)
	goTestsumPath, err := devtoolPath("gotestsum")
	if err != nil {
		return err
	}
	coverpkg, err := coverpkgFlag()
	if err != nil {
		return err
	}
	args := []string{"run", "./cmd/tools/test-runner", "test",
		"--gotestsum-path=" + goTestsumPath,
		"--max-attempts=" + maxAttempts,
		"--junitfile=" + newReport(),
		"--",
	}
	args = append(args, compiledTestArgs()...)
	args = append(args, "-coverprofile="+newCoverProfile(), coverpkg, functionalTestXDCRoot)
	args = append(args, "-args",
		"-persistenceType="+persistenceType,
		"-persistenceDriver="+persistenceDriver,
	)
	return sh.RunV("go", args...)
}

// FunctionalNDCCoverage runs functional NDC tests with coverage and retries.
func (t Test) FunctionalNDCCoverage() error {
	mg.Deps(Clean.TestOutput)
	if err := os.MkdirAll(testOutputRoot, 0o755); err != nil {
		return err
	}
	color("Run functional test for NDC with coverage with %s driver...", persistenceDriver)
	goTestsumPath, err := devtoolPath("gotestsum")
	if err != nil {
		return err
	}
	coverpkg, err := coverpkgFlag()
	if err != nil {
		return err
	}
	args := []string{"run", "./cmd/tools/test-runner", "test",
		"--gotestsum-path=" + goTestsumPath,
		"--max-attempts=" + maxAttempts,
		"--junitfile=" + newReport(),
		"--",
	}
	args = append(args, compiledTestArgs()...)
	args = append(args, "-coverprofile="+newCoverProfile(), coverpkg, functionalTestNDCRoot)
	args = append(args, "-args",
		"-persistenceType="+persistenceType,
		"-persistenceDriver="+persistenceDriver,
	)
	return sh.RunV("go", args...)
}

// PreBuildFunctionalCoverage pre-compiles functional tests with coverage.
func (Test) PreBuildFunctionalCoverage() error {
	if err := os.MkdirAll(testOutputRoot, 0o755); err != nil {
		return err
	}
	coverpkg, err := coverpkgFlag()
	if err != nil {
		return err
	}
	testArgs := os.Getenv("TEST_ARGS")
	args := []string{"test", "-c", "-cover", "-o", "/dev/null", functionalTestRoot}
	if testArgs != "" {
		args = append(args, strings.Fields(testArgs)...)
	}
	args = append(args, testTagFlag(), coverpkg)
	return sh.RunV("go", args...)
}

// ReportCrash generates a test crash junit report.
func (Test) ReportCrash() error {
	if err := os.MkdirAll(testOutputRoot, 0o755); err != nil {
		return err
	}
	color("Generate test crash junit report...")
	crashReportName := os.Getenv("CRASH_REPORT_NAME")
	return sh.RunV("go", "run", "./cmd/tools/test-runner", "report-crash",
		"--gotestsum=report-crash",
		"--junitfile="+testOutputRoot+"/junit.crash.xml",
		"--crashreportname="+crashReportName,
	)
}

// All runs unit, integration, and functional tests.
func (t Test) All() {
	mg.SerialDeps(t.Unit, t.Integration, t.Functional)
}
