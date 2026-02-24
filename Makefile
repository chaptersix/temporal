############################# Transitional Makefile #############################
# This Makefile delegates all targets to mage. It exists for backward
# compatibility with CI and developer muscle memory. Once CI workflows are
# updated to call `go run mage.go` directly, this file can be removed.
#
# Usage is unchanged: make <target>
# Under the hood:    go run mage.go <namespace>:<mageTarget>
#################################################################################

# Run mage with the host OS/arch so `go run mage.go` produces a native binary,
# even when cross-compiling (e.g. GOOS=windows make bins).
MAGE := GOOS= GOARCH= go run mage.go

# Forward environment variables that mage targets read.
export CGO_ENABLED ?= 0
export GOOS ?= $(shell go env GOOS)
export GOARCH ?= $(shell go env GOARCH)
export PERSISTENCE_TYPE
export PERSISTENCE_DRIVER
export TEMPORAL_DB
export VISIBILITY_DB
export SQL_USER
export SQL_PASSWORD
export BUILD_TAG
export TEST_TAG
export TEST_TIMEOUT
export MAX_TEST_ATTEMPTS
export TEST_RACE_FLAG
export TEST_SHUFFLE_FLAG
export TEST_PARALLEL_FLAGS
export TEST_ARGS
export UNIT_TEST_DIRS
export GOLANGCI_LINT_BASE_REV
export GOLANGCI_LINT_FIX
export GOFIX_FLAGS
export GOFIX_MAX_ITERATIONS
export CRASH_REPORT_NAME
export OTEL

.PHONY: install bins all ci-build-misc clean proto protoc

############################# Main targets #############################

install: bins

bins:
	$(MAGE) build:bins

all:
	$(MAGE) clean:all
	$(MAGE) proto:all
	$(MAGE) build:bins
	$(MAGE) lint:all
	$(MAGE) lint:shellCheck
	$(MAGE) test:all

ci-build-misc:
	$(MAGE) ci:buildMisc

clean:
	$(MAGE) clean:all

clean-bins:
	$(MAGE) clean:bins

clean-test-output:
	$(MAGE) clean:testOutput

############################# Proto #############################

proto:
	$(MAGE) proto:all

protoc:
	$(MAGE) proto:protoc

proto-codegen:
	$(MAGE) proto:codegen

proto/api.binpb:
	$(MAGE) proto:apiBinpb

proto/image.bin:
	$(MAGE) proto:internalBinpb

proto/chasm.bin:
	$(MAGE) proto:chasmBinpb

buf-breaking:
	$(MAGE) proto:bufBreaking

update-go-api:
	$(MAGE) proto:updateGoAPI

############################# Binaries #############################

temporal-server:
	$(MAGE) build:server

temporal-server-debug:
	$(MAGE) build:serverDebug

tdbg:
	$(MAGE) build:tdbg

temporal-cassandra-tool:
	$(MAGE) build:cassandraTool

temporal-sql-tool:
	$(MAGE) build:sqlTool

temporal-elasticsearch-tool:
	$(MAGE) build:esTool

build-tests:
	$(MAGE) build:tests

############################# Lint / Fmt #############################

lint:
	$(MAGE) lint:all

lint-code:
	$(MAGE) lint:code

lint-actions:
	$(MAGE) lint:actions

lint-api:
	$(MAGE) lint:api

lint-protos:
	$(MAGE) lint:protos

lint-yaml:
	$(MAGE) lint:yaml

shell-check:
	$(MAGE) lint:shellCheck

workflowcheck:
	$(MAGE) lint:workflowCheck

check: lint shell-check

fmt:
	$(MAGE) fmt:all

fmt-gofix:
	$(MAGE) fmt:goFix

fmt-imports:
	$(MAGE) fmt:imports

fmt-yaml:
	$(MAGE) fmt:yaml

goimports:
	$(MAGE) fmt:goimports

############################# Tests #############################

unit-test:
	$(MAGE) test:unit

integration-test:
	$(MAGE) test:integration

functional-test:
	$(MAGE) test:functional

functional-with-fault-injection-test:
	$(MAGE) test:functionalFaultInjection

test: unit-test integration-test functional-test

unit-test-coverage:
	$(MAGE) test:unitCoverage

integration-test-coverage:
	$(MAGE) test:integrationCoverage

functional-test-coverage:
	$(MAGE) test:functionalCoverage

functional-test-xdc-coverage:
	$(MAGE) test:functionalXDCCoverage

functional-test-ndc-coverage:
	$(MAGE) test:functionalNDCCoverage

pre-build-functional-test-coverage:
	$(MAGE) test:preBuildFunctionalCoverage

report-test-crash:
	$(MAGE) test:reportCrash

############################# Schema #############################

install-schema-cass-es:
	$(MAGE) schema:cassEs

install-schema-mysql: install-schema-mysql8

install-schema-mysql8:
	$(MAGE) schema:mysql8

install-schema-postgresql: install-schema-postgresql12

install-schema-postgresql12:
	$(MAGE) schema:postgresql12

install-schema-es:
	$(MAGE) schema:es

install-schema-es-secondary:
	$(MAGE) schema:esSecondary

install-schema-xdc:
	$(MAGE) schema:xdc

############################# Server #############################

start: start-sqlite

start-sqlite:
	$(MAGE) server:sqlite

start-sqlite-file:
	$(MAGE) server:sqliteFile

start-cass-es:
	$(MAGE) server:cassEs

start-cass-es-dual:
	$(MAGE) server:cassEsDual

start-cass-es-custom:
	$(MAGE) server:cassEsCustom

start-es-fi:
	$(MAGE) server:esFi

start-mysql: start-mysql8

start-mysql8:
	$(MAGE) server:mysql8

start-mysql-es:
	$(MAGE) server:mysqlEs

start-postgres: start-postgres12

start-postgres12:
	$(MAGE) server:postgres12

start-xdc-cluster-a:
	$(MAGE) server:xdcClusterA

start-xdc-cluster-b:
	$(MAGE) server:xdcClusterB

start-xdc-cluster-c:
	$(MAGE) server:xdcClusterC

############################# Docker #############################

start-dependencies:
	$(MAGE) docker:startDeps

stop-dependencies:
	$(MAGE) docker:stopDeps

start-dependencies-dual:
	$(MAGE) docker:startDepsDual

stop-dependencies-dual:
	$(MAGE) docker:stopDepsDual

start-dependencies-cdc:
	$(MAGE) docker:startDepsCDC

stop-dependencies-cdc:
	$(MAGE) docker:stopDepsCDC

############################# Auxiliary #############################

go-generate:
	$(MAGE) generate:goGenerate

gomodtidy:
	$(MAGE) generate:modTidy

ensure-no-changes:
	$(MAGE) generate:ensureNoChanges

update-dependencies:
	$(MAGE) generate:updateDeps

update-dependencies-major:
	$(MAGE) generate:updateDepsMajor

update-dashboards:
	$(MAGE) generate:updateDashboards

print-go-version:
	@go version

clean-tools:
	$(MAGE) clean:tools

parallelize-tests:
	$(MAGE) fmt:parallelizeTests
