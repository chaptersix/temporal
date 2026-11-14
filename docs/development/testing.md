# Testing

This document describes the project's testing setup, utilities and best practices.

## Setup

### Build tags
- `test_dep`: This Go build tag enables the test hooks implementation. Only very few tests require it; they will fail if not enabled.
- `TEMPORAL_DEBUG`: Extends functional test timeouts to allow sufficient time for debugging sessions.

### Environment variables
- `CGO_ENABLED`: Set to `0` to disable CGO, which can significantly speed up compilation time.
- `TEMPORAL_TEST_LOG_FORMAT`: Controls the console output format for test logs. Available options: `json` or `console` (default)
- `TEMPORAL_TEST_LOG_LEVEL`: Sets the minimum verbosity level written to console output. Available levels: `debug` (default), `info`, `warn`, `error`, `fatal`
- `TEMPORAL_TEST_LOG_FILE`: Path to a file that receives a separate copy of test logs. When unset (default), file logging is disabled. Useful in CI to route debug-level logs to a downloadable artifact without flooding the job log.
- `TEMPORAL_TEST_LOG_FILE_FORMAT`: Output format for the log file. Available options: `json` (default) or `console`
- `TEMPORAL_TEST_LOG_FILE_LEVEL`: Minimum verbosity level written to the log file. Available levels: `debug` (default), `info`, `warn`, `error`, `fatal`
- `TEMPORAL_TEST_OTEL_OUTPUT`: Enables OpenTelemetry (OTEL) trace output for failed tests to the provided file path.
- `TEMPORAL_TEST_SHARED_CLUSTERS`: Number of shared clusters in the pool. Each can be used by multiple tests simultaneously.
- `TEMPORAL_TEST_DEDICATED_CLUSTERS`: Number of dedicated clusters in the pool. Each can be used by one test only at a time.
- `TEMPORAL_TEST_TIMEOUT`: Sets the duration timeout per test (e.g., `90s` for 90 seconds). This can be overridden per-test using `testcore.WithTimeout()`.
- `TEMPORAL_TEST_DATA_ENCODING`: Controls the encoding used for persistence DataBlobs. Available options: `proto3` (default) or `json`.

### Debugging via IDE

#### GoLand

For general instructions, see [GoLand Debugging](https://www.jetbrains.com/help/go/debugging-code.html).
To pass in the required build tags, add them to the "Go tool arguments" field in the Run/Debug configuration:

```
-tags disable_grpc_modules,test_dep
```

## Best Practices

### Use `require` instead of `assert`

Always use `require.X` (and `protorequire.X`) instead of `assert.X` (and `protoassert.X`).
`assert` records a failure but lets the test continue, which often leads to confusing
cascading errors.

### Polling with await.Require

For polling/retry loops in tests, use `await.Require` (or `await.Requiref`)
from `common/testing/await` instead of testify's `EventuallyWithT`.

Use `t.Context()` inside the callback for a context derived from the parent
context and canceled when the parent context is canceled or the await timeout
expires.

```go
await.Require(ctx, t, func(t *await.T) {
    resp, err := client.GetStatus(t.Context())
    require.NoError(t, err)
    require.Equal(t, "ready", resp.Status)
}, 5*time.Second, 200*time.Millisecond)
```

Use `RequireTrue` instead of testify's `Eventually` for simple local bool-returning predicates.

```go
await.RequireTrue(t, func() bool {
    return cache.Ready()
}, 5*time.Second, 200*time.Millisecond)
```

`RequireTrue` is the wrong tool when dealing with errors or assertions; use `Require` instead.

### Blocking on channels

Use `await.Rcv` and `await.Snd` for blocking channel operations.
They fail the test if its context ends or the channel closes.

```go
headers := await.Rcv(t, requestHeaders)
await.Snd(t, responses, response)
```

Keep an explicit `select` when testing non-blocking behavior or waiting on multiple channels.

### Parallelization

All tests (and subtests!) should use `t.Parallel()` to be run concurrently;
unless there is a reason not to.

`make parallelize-tests` can be used to automatically add `t.Parallel()`.
Use `//parallelize:ignore` to opt your test out of it.

## Property tests

Property tests assert invariants against randomly-generated inputs instead of hand-written cases.
They use [rapid](https://pkg.go.dev/pgregory.net/rapid), which generates the inputs and, on
failure, shrinks them to a minimal counterexample.

They run in their own tier, separate from unit tests, because they need a different runner
configuration; see [Why retries are disabled](#why-retries-are-disabled).

The tier is every package directory whose name ends in `proptest`; see
[Adding a property test](#adding-a-property-test).

### Running locally

```bash
make property-test                     # rapid's default: 100 generated cases per property
make property-test RAPID_CHECKS=50000  # soak before pushing a change to a covered package
```

Or with plain `go test`. Note that `-rapid.*` are test *binary* flags, so the package has to come
before them — otherwise `go test` stops parsing and fails with `[setup failed]`:

```bash
go test -tags test_dep ./chasm/lib/scheduler/schedulerproptest/ -run TestSchedulerProperties -rapid.checks=1000
```

### Configuration

`RAPID_CHECKS` sets the number of generated cases per property, and defaults to `100` — cheap
enough to run on every PR. Every rapid flag also has an environment variable form
(`-rapid.checks` / `RAPID_CHECKS`, and so on); see rapid's documentation for the full set.

Seeds are **not** pinned in CI. Retries are disabled instead, which keeps a counterexample from
being discarded while still letting coverage accumulate across commits.

### In CI

| Job | Workflow | Gates merges |
| --- | --- | --- |
| `Property test` | `run-tests.yml`, every PR | intended |
| `Property test soak` | `property-test-soak.yml`, nightly 0600 UTC | no |

Both run a single attempt. The soak also accepts a manual `workflow_dispatch` with a `rapid_checks`
input.

`Property test` only blocks a merge once it is listed as a required status check in the repository's
branch protection settings; the workflow alone does not make it one.

The soak is deliberately never a required check. A counterexample it finds is a real bug, but it is
found on an input that no particular PR introduced, so it must not block unrelated merges.

### When a counterexample is found

rapid prints the minimized counterexample along with a line naming both ways to reproduce it — the
*fail file* it just wrote, and the seed:

```
To reproduce, specify -run="<TestName>" -rapid.failfile="<path>" (or -rapid.seed=<seed>)
```

Either form works. The fail file path is relative to the package directory:

```bash
go test -tags test_dep ./chasm/lib/scheduler/schedulerproptest/ -run TestSchedulerProperties \
  -rapid.failfile="testdata/rapid/TestSchedulerProperties/TestSchedulerProperties-<timestamp>-<pid>.fail"
```

In CI the fail file is uploaded as a job artifact — `property-test-counterexamples`, or
`property-test-soak-counterexamples` for the nightly. Download it to reproduce locally.

### Pinning a counterexample

**Pin the counterexample as a hand-written deterministic test, in the same change as the fix.**
Translate the shrunk inputs into an explicit test — `TestLedger_DeferredStartsStrandedByPausedCompletion`
in [schedulertest](../../chasm/lib/scheduler/schedulertest) is an example — and put it next to the
other unit tests for the package, not in the property tier.

**Do not commit the fail file.** `testdata/rapid/` is gitignored, because a fail file is not a
durable regression test: it replays a raw sequence of draws, so adding, removing or reordering a
draw in the generator makes the stored sequence decode to different values. It then stops
reproducing the bug **silently** — rapid reports no error, the replay simply passes, and the
protection is gone without anything failing to tell you. A hand-written test says what it means and
breaks loudly when behaviour changes.

Use the fail file for what it is good at — reproducing the failure *now*, while you write the
real test.

### Deferring a fix

If the bug cannot be fixed immediately, the thing to avoid is any marker that passes silently once
the bug is gone. A `t.Skip` and a filter in the invariant hook both do, which is how this
repository ended up carrying a skip *and* an invariant exemption for a stranding bug that had
already been fixed on main seven hours earlier — the skip's own comment asked for its deletion, and
nothing deleted it because nothing broke when the bug disappeared.

So, in order of preference:

- Fix it. A shrunk counterexample is usually small, and the fix often is too.
- Open an issue, and paste in both the seed and the fail file's contents — a few lines of hex that
  fully describe the case. Do not rely on the CI artifact; those expire.
- If the property has to keep running while the bug is open, constrain the **generator** rather
  than filtering violations in the hook. A generator constraint is visible in the inputs and
  self-documenting; a violation filter silently swallows every other bug of the same shape, not
  just the one it names.
- If you must leave a marker, make it fail when the bug is fixed — for example a deterministic test
  that asserts the current wrong outcome behind a `// BUG(#NNNNN):` comment, so whoever fixes the
  bug is forced to update it. Put the issue number in any `t.Skip` message so it is at least
  greppable.

### Why retries are disabled

`property-test-coverage` passes `--max-attempts=1` to the test runner, unlike the unit and
functional tiers, which retry up to `MAX_TEST_ATTEMPTS` times. **Do not re-enable retries for this
tier.**

The test runner exits with the exit code of its *last* attempt, and each retry re-runs the test in a
fresh process, where rapid draws a new random seed. A retried property test therefore checks a
different set of inputs than the one that failed: the counterexample would be replaced by a
likely-passing run, and the job would go green having found a real bug.

### Adding a property test

**Put property tests in their own package directory, named with a `proptest` suffix.** The tier is
selected by directory name — `PROPERTY_TEST_DIRS` in the [Makefile](../../Makefile) matches any
directory ending in `proptest` — so creating one enrolls it automatically and excludes it from the
unit tier. Nothing needs registering. Also add it to the `property` category in
[find_altered_tests.go](../../cmd/tools/test/find_altered_tests.go), which is a separate list.

The dedicated directory is not stylistic. The tier subtracts whole directories from
`UNIT_TEST_DIRS`, so a property test placed inside the package it exercises forces a choice between
pulling that package's entire unit test suite into the no-retry tier, or leaving the property test
in the retrying tier where its counterexamples are discarded.

Because a sibling directory can only reach exported API, the package under test needs an exported
harness — see [schedulertest](../../chasm/lib/scheduler/schedulertest), which is a non-test package
precisely so property tests can import its driver.

## Test helpers

Test helpers can be found in the [common/testing](../../common/testing) package.

### parallelsuite package

Use `parallelsuite.Suite` to ensure your test suite is fast and safe: it runs all test methods and sub-tests in parallel by default;
and provides assertion helpers and safety mechanisms.

It replaces all use of `testify`'s `Suite`.

#### Context shorthand

```go
ctx := s.Context()
```

`s.Context()` returns the subtest-scoped context - equivalent to `testcontext.New(s.T())`.

#### Await shorthand

```go
s.Await(func(s *MySuite) {
    resp, err := client.GetStatus(s.Context())
    s.NoError(err)
    s.Equal("ready", resp.Status)
}, 5*time.Second, 200*time.Millisecond)
```

Inside an `s.Await` callback, `s.Context()` is capped to that await's timeout.

### testvars package

Instead of creating identifiers like task queue name, namespace or worker identity by hand,
use the `testvars` package.

Example:

```go
func TestFoo(t *testing.T) {
    tv := testvars.New(t)

    req := &workflowservice.SignalWithStartWorkflowExecutionRequest{
        RequestId:    tv.Any().String(),
        Namespace:    tv.NamespaceName().String(),
        WorkflowId:   tv.WorkflowID(),
        WorkflowType: tv.WorkflowType(),
        TaskQueue:    tv.TaskQueue(),
        SignalName:   tv.SignalName(),
    }
}
```
Later you can assert on the generated values. `testvars` guarantees to provide the same value every time you call the same method. 

```go
require.Equal(t, tv.WorkflowID(), startedWorkflow.WorkflowId)
```

If you need more than one value for the same entity in one test, you can use `WithEntityNumber()` method to
get a new instance of `testvars` with a different value.

```go
func TestFoo(t *testing.T) {

    tv := testvars.New(t)
    tv1 := tv.WithUpdateIDNumber(1)
    tv2 := tv.WithUpdateIDNumber(2)

    req1 := &workflowservice.UpdateWorkflowExecutionRequest{
        Namespace:         tv1.NamespaceName().String(),
        WorkflowExecution: tv1.WorkflowExecution(),
        Request: &updatepb.Request{
            Meta: &updatepb.Meta{UpdateId: tv1.UpdateID()},
            Input: &updatepb.Input{
                Name: tv1.HandlerName(),
                Args: payloads.EncodeString("args-value-of-" + tv1.UpdateID()),
            },
        },
    }

	req2 := &workflowservice.UpdateWorkflowExecutionRequest{
        Namespace:         tv2.NamespaceName().String(),
        WorkflowExecution: tv2.WorkflowExecution(),
        Request: &updatepb.Request{
            Meta: &updatepb.Meta{UpdateId: tv2.UpdateID()},
            Input: &updatepb.Input{
                Name: tv2.HandlerName(),
                Args: payloads.EncodeString("args-value-of-" + tv2.UpdateID()),
            },
        },
    }
}
```

If you don't care about specific value, you can use `Any()` method to generate a random value.
It indicates that value doesn't matter for this test and will never be asserted on (but required for API, for example).

### testcontext package

There's no need to create your own `context.Context` via `context.WithTimeout`; use `testcontext.New(t)` instead. It returns a test-scoped `context.Context`, memoized per `*testing.T` and canceled on test end or timeout.

### taskpoller package

For end-to-end testing, consider using `taskpoller.TaskPoller` to handle workflow tasks. This is
useful when you need full control over the worker behavior in a way that the SDK cannot provide;
or if there's no SDK support for that API available yet.

You'll find a fully initialized task poller in any functional test suite, look for `s.TaskPoller`.

_NOTE: The previous `testcore.TaskPoller` has been deprecated and should not be used in new code._

### gRPC fault injection

The `testcore` package injects faults into gRPC calls by intercepting requests and responses.
The fault function determines which RPCs trigger a fault and returns the error to inject.
It returns a cleanup function that can be used to remove the fault again.

**Example:**

```go
env.InjectRequestFault(
    func(req any) error {
        if _, ok := req.(*matchingservice.AddWorkflowTaskRequest); ok {
            return serviceerror.NewNotFound("injected fault")
        }
        return nil
    })
```

`InjectRequestFault` runs _before_ the handler, so an injected error prevents the operation from
executing. `InjectResponseFault` runs _after_ the handler, so an injected error can model an
operation that executed but whose response was lost. The test fails if the fault never triggers.
Only unary RPCs are intercepted; streaming RPCs are unaffected.

The `env` methods scope faults to the test's namespace.

### HTTP fault injection

`InjectHTTPRequestFault` and `InjectHTTPResponseFault` inject faults into HTTP clients that install
the test fault generator. Callback delivery and Nexus operation clients support these faults. The
faults are scoped to the test namespace and are unavailable in builds without `test_dep`.

Return an `httpfaults.Outcome` with one action:

- `Response` returns a synthetic HTTP response.
- `Error` returns a transport error.

```go
env.InjectHTTPRequestFault(func(ctx context.Context, req *http.Request) *httpfaults.Outcome {
    if req.URL.Path != "/callback" {
        return nil
    }
    return &httpfaults.Outcome{
        Response: httpfaults.NewResponse(http.StatusServiceUnavailable, "injected"),
    }
})
```

### Persistence fault injection

Use `testcore.InjectPersistenceFault` to return custom errors from persistence calls. The fault is
active before the test cluster starts. You can filter it by store and method.

```go
option := testcore.InjectPersistenceFault(
    func(config.FaultInjectionTarget) error {
        return context.DeadlineExceeded
    },
    testcore.WithStore(config.ShardStoreName),
    testcore.WithMethod("UpdateShard"),
)
env := testcore.NewEnv(t, option)
```

The test fails if no matching call reaches the faulty path. This ensures that if we end up refactoring
the code (i.e., "UpdateShard" doesn't exist anymore), and the path isn't reachable, that the test
that injects fault there will intentionally fail.

### Persistence order enforcement

Use `testcore.NewPersistenceCallGate` when a test must control call order. It returns a
`*testcore.PersistenceCallGate` and a `TestOption` that installs the gate. Matching calls block at
the gate. Call `WaitForPersistenceCall` before you check other state. Call `Release` on the gate to
continue the persistence operation. Call `ReleaseWithError` to return an error from the blocked
operation. 

```go
gate, option := testcore.NewPersistenceCallGate(
    testcore.WithStore(config.ShardStoreName),
    testcore.WithMethod("UpdateShard"),
)
env := testcore.NewEnv(t, option)

// Wait until one UpdateShard call blocks at the gate.
testcore.WaitForPersistenceCall(t, gate, 10*time.Second)

// Check state, do work, make assertions, etc... while the call is blocked.

// Later, release the gate manually.
gate.Release() // Or gate.ReleaseWithError(err) to fail the blocked call.
```

The test fails if no matching call reaches the gate.

### testhooks package

The `testhooks` package injects test-specific behavior into production code paths that are otherwise
difficult to test. This is a **last resort** - prefer mocking and dependency injection when possible.

**Example:**

The UpdateWithStart API has a race window between releasing a lock and starting a workflow where
another request could create the same workflow first. The `UpdateWithStartInBetweenLockAndStart`
hook lets tests inject a callback at this exact point, making it possible to reliably test
conflict handling.

_NOTE: Tests using testhooks must be run with `-tags=test_dep`._

### softassert package

`softassert.That` is a "soft" assertion that logs an error if the given condition is false.

It is useful to highlight invariant violations in production code.
It is *not* a substitute for regular error handling, validation, or control flow.

In functional tests, a failed soft assertion will not stop the test execution immediately, but it
will ultimately fail the test.

### protorequire package

Use `protorequire.ProtoEqual` to compare proto messages with proto semantics.
Prefer a single `ProtoEqual` call over asserting fields one-by-one, since it catches unexpected field changes and keeps the expected value next to the assertion.

To ignore specific fields on the top-level message (e.g. non-deterministic timestamps), pass `protorequire.IgnoreFields`:

```go
protorequire.ProtoEqual(t, expected, actual,
    protorequire.IgnoreFields(
        "execution_duration",
        "schedule_time",
    ),
)
```

### historyrequire package

`historyrequire` has assertions to verify workflow event histories.

Use `EqualHistoryEvents` to assert the full event sequence:

```go
events := env.GetHistory(env.Namespace().String(), workflowExecution)
s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled {"Attempt": 1}
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`, events)
```

Optional inline JSON (e.g. `{"Attempt": 1}`) can be used to assert on specific attributes.

Use `ContainsHistoryEvents` when you only care about a particular segment:

```go
s.ContainsHistoryEvents(`
  4 WorkflowTaskFailed {"Identity": "worker-1"}
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, events)
```

Use `RequireHistoryEvent` when you only care about a single event type:

```go
completed := s.RequireHistoryEvent(events, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED)
require.Equal(t, "expected-result", completed.GetWorkflowExecutionCompletedEventAttributes().Result)
```

Or use `RequireNoHistoryEvent` when you expect no event of a given type to be present.

### Test Cluster

Use `testcore.NewEnv(t)` to create a test environment with access to a Temporal cluster for end-to-end testing.

```go
func (s* TestMyFeatureSuite) func TestXYZ(t *testing.T) {
    s := testcore.NewEnv(t)
    // ...
}
```

Note that each test has its own namespace (`s.Namespace()`) for isolation.

## OpenTelemetry (OTEL)

To debug your test by analysing observability traces, set the following environment variables:

```bash
export OTEL_BSP_SCHEDULE_DELAY=100
export OTEL_EXPORTER_OTLP_TRACES_INSECURE=true
export OTEL_TRACES_EXPORTER=otlp
export TEMPORAL_OTEL_DEBUG=true
```

And have an OTEL collector running, such as Grafana Tempo (`make start-dependencies`).

See [tracing.md](../../docs/development/tracing.md) for more details.

## Code coverage

You'll find the code coverage reporting in Codecov: https://app.codecov.io/gh/temporalio/temporal.

Consider installing the [Codecov Browser Extension](https://docs.codecov.com/docs/the-codecov-browser-extension)
to see code coverage directly in GitHub PRs.
