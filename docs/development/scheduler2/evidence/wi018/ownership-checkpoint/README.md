# Ownership checkpoint verification

Source review and tests accept this implementation increment only. Whole bounded Scheduler runtime remains incomplete.

Commands (CGO_ENABLED=0 GOMAXPROCS=4):
- go test -tags test_dep -p 2 ./chasm/lib/scheduler2/... -count=1 (composed log)
- go test -tags test_dep,integration -p 2 -parallel 4 ./tests -run '^TestSchedule2' -count=1 -timeout=10m
- make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false

Independent effect test was added after the public run; its directed run and final lint cover that test-only addition. Named timezone source is a separate uncommitted capability with an empty-GOROOT correction pending and is excluded from this source manifest.
