# Plan 3: Operational Red Team

## Prerequisite

Plan 2 must provide a reviewed workload corpus containing:

- Low-cost valid schedules.
- Typical generated schedules.
- Exact budget-transition schedules.
- Maximum matching-work schedules.
- Maximum validation-work schedules.
- Query-local empty schedules.
- Invalid structural and unsatisfiable schedules.
- Timezone and DST schedules.

## Objective

Evaluate cancellation, deadlines, concurrency, memory, repeated abuse, and error/retry
behavior under realistic and adversarial workloads. This remains test-only analysis and
does not alter production request handling.

## Cancellable Analysis API

Extend the copied validator and calculator with context-aware entry points:

```go
func ValidateSchedule(ctx context.Context, spec *schedulepb.ScheduleSpec, options ValidationOptions) (ValidationResult, error)

func ComputeMatchingTimes(ctx context.Context, spec *schedulepb.ScheduleSpec, start, end time.Time, seed string, options ComputeOptions) (ComputeResult, error)
```

Check context at deterministic work-tick boundaries. Record cancellation-check count in
the work breakdown.

Required outcomes:

- `context.Canceled` remains distinguishable from budget exhaustion.
- `context.DeadlineExceeded` remains distinguishable from budget exhaustion.
- Cancellation never returns a response marked complete.
- Work stops within a bounded number of ticks after cancellation becomes observable.
- No goroutine or timer survives the test.

## Concurrency Matrix

Use the Plan 2 corpus across:

```text
1 worker
2 workers
10 workers
20 workers
100 workers (opt-in stress only)
```

The ten-worker tier is the required tenfold-load comparison against the single-worker
baseline.

For each tier record:

- Throughput.
- P50, P90, P99, and maximum latency.
- CPU utilization when available.
- Allocated bytes and peak resident memory when available.
- Completed, budget-exhausted, cancelled, deadline, and invalid counts.
- Goroutine count before and after.
- Work-count determinism relative to single-worker execution.

Run homogeneous workloads and mixed workloads so cheap requests cannot hide starvation
caused by expensive requests.

## Cancellation Campaigns

Cancel at deterministic points:

- Before validation.
- During component satisfiability.
- During effective-set proof.
- Before first matching result.
- After one result.
- During exclusion retries.
- One tick before the configured work limit.
- Simultaneously across all concurrent workers.

Use a test hook keyed to work count rather than sleeps. Wall-clock deadline tests are
separate and tolerate scheduling variance.

Properties:

- Earlier deterministic cancellation never consumes more work than later cancellation.
- Cancellation at work `N` stops within the documented polling interval.
- Raising work budget does not defeat cancellation.
- Cancellation does not change validation or result prefixes produced before it.

## Deadline Campaigns

Test deadlines at:

```text
already expired
1 millisecond
10 milliseconds
100 milliseconds
1 second
long enough to complete
```

Run each against low, transition, and maximum-work corpus cases. Record whether budget
or deadline wins when both are near the boundary; outcome ordering must follow a
documented check order and remain deterministic for deterministic cancellation hooks.

## Repeated-Abuse Campaign

Simulate repeated identical expensive queries:

- Sequential requests against one spec.
- Ten concurrent callers against one spec.
- Many callers across different expensive specs.
- Alternating cheap and expensive requests.
- Repeated invalid specs expensive to validate.

Measure whether caches improve cost, leak memory, or introduce nondeterministic results.
The copied calculator currently builds a fresh `SpecBuilder`; add explicit cached and
uncached modes rather than changing behavior invisibly.

## Memory And Allocation Analysis

Benchmark:

- Maximum result slice allocation.
- Day-bitset validation buffers.
- Many calendar/range components.
- Large timezone data.
- Rapidly cancelled requests.
- Repeated validation with and without cache.

Required checks:

- Allocation remains bounded by documented spec/result limits.
- Buffers are released after cancellation.
- No retained candidate or diagnostic contains unbounded input data.
- Partial results do not retain large backing arrays after error handling.
- Repeated campaigns reach a stable memory plateau.

Use `go test -benchmem`, heap profiles for opt-in stress runs, and leak checks already
available in the repository where appropriate.

## Race And Shared-State Testing

Run the analysis package with `-race` over:

- Shared `SpecBuilder` timezone cache.
- Cached compiled specs.
- Concurrent validation of the same spec.
- Concurrent matching with different jitter seeds.
- Cancellation while cache lookup is active.

Properties:

- Shared caches do not alter work results other than separately reported cache work.
- Identical inputs remain deterministic under concurrency.
- Different seeds affect only jittered times.
- No caller mutates the input protobuf.

## Error And Retry Red Team

Build a small client-policy simulator for:

- Invalid structural spec.
- Unsatisfiable spec.
- Validation indeterminate.
- Matching budget exhausted.
- Cancellation.
- Deadline exceeded.

Evaluate retry strategies:

- Immediate identical retry.
- Retry with a smaller query window.
- Retry with fewer requested results.
- Retry with a larger analysis budget.
- No retry.

Record which errors are safely actionable. Repeating the identical request after a
deterministic budget failure must be identified as non-progressing unless configuration
changes.

## Malformed And Hostile Input Fuzzing

Add dedicated fuzz targets for:

- Raw schedule protobuf unmarshalling followed by validation.
- Malformed timezone data.
- Extreme but protobuf-valid timestamps and durations.
- Large repeated fields within accepted size limits.
- Unicode cron strings and comments.
- Cancellation racing with parsing and validation.

All fuzz targets enforce outer timeouts and allocation limits. Panics, hangs, and
unbounded allocations are failures even for invalid input.

## Commands

Representative commands:

```bash
go test -tags test_dep ./service/worker/scheduler/propertytest -run 'TestOperational' -count=1
go test -race -tags test_dep ./service/worker/scheduler/propertytest -run 'TestOperationalConcurrency'
go test -tags test_dep ./service/worker/scheduler/propertytest -run '^$' -bench 'BenchmarkOperational' -benchmem
go test -tags test_dep ./service/worker/scheduler/propertytest \
  -run='^$' -fuzz='^FuzzOperational' -fuzztime=30s -parallel=1
```

High-concurrency and profile-producing commands remain opt-in.

## Deliverables

- Context-aware copied validator and calculator.
- Deterministic cancellation hooks.
- Concurrency and deadline experiment runner.
- Allocation benchmarks and selected profiles.
- Race-test results.
- Error/retry behavior matrix.
- Operational findings appended to `FINDINGS.md`.
- Guard evidence updated with tenfold-load results.

## Acceptance Criteria

- Cancellation and deadlines stop work without returning complete results.
- Cancellation response latency is bounded in deterministic work ticks.
- Single-worker and ten-worker comparisons exist for every corpus class.
- No races or goroutine leaks are detected.
- Memory remains bounded and reaches a stable plateau under repeated campaigns.
- Invalid and deterministic budget failures do not invite non-progressing identical
  retries.
- Every operational failure is reproducible from a retained corpus case and recorded
  configuration.
- No production code or production guard is changed.

## Failure Modes And Interpretation

- Wall-clock latency is environment-sensitive; always pair it with deterministic work.
- Race mode changes timing and must not be used for latency calibration.
- A test-only context polling interval is evidence, not a production choice.
- Cache improvements can hide worst-case cold behavior, so always report cold and warm
  runs separately.
- Tenfold load that scales poorly may indicate CPU saturation rather than an algorithmic
  defect; correlate latency with work, CPU, and allocations before concluding.
