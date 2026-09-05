# Experimental extensible CHASM scheduler

The experiment gives one CHASM scheduler core two action implementations:
scheduled workflow starts and standalone activity starts. The core owns
occurrence generation, buffering, catch-up, capacity, start request retries,
durable progress, and retention. An action implementation owns validation,
target ID generation, request construction, dispatch, completion handling, and
the overlap policies it supports.

The persisted vocabulary is action, execution, target ID, and task queue. A
workflow action may retain the legacy workflow projections for compatibility;
an activity action does not write activity identity or status into those
workflow fields. Results and running execution information use the generic
execution representation when present.

## Local checkout

Keep the integration work file and generated server configuration outside the
repositories. Set these paths to the local checkouts used for the experiment:

```bash
SERVER_CHECKOUT=/home/alex/Work/tries/2026-09-04-cd/temporal
API_GO_CHECKOUT=/tmp/temporal-saa-deps.2Fy5wa/api-go
SDK_CHECKOUT=/tmp/temporal-saa-deps.2Fy5wa/sdk-go
SDK_TEST_CHECKOUT=$SDK_CHECKOUT/test
```

The API/proto source checkout supplies the generated Go API; it is not a Go module and
must not be added to `go.work`. Create the four-module work file with the
generated API, SDK root, SDK test module, and server checkout:

```bash
cat > /tmp/extensible-go.work <<EOF
go 1.26.4

use (
 $SERVER_CHECKOUT
 $API_GO_CHECKOUT
 $SDK_CHECKOUT
 $SDK_TEST_CHECKOUT
)
EOF
```

The linked dependency revisions are:

| Checkout | Revision | Branch |
| --- | --- | --- |
| API | `181d052558a750032a1b95de96961ba9987e00fe` | `experiment/saa-scheduler-api` |
| Generated API | `2938c7453bd7ea9add85dd8a5294d0c1ba464d3c` | `experiment/saa-scheduler-generated` |
| SDK | `a6bed7c8ed3c4b90c467f003571c98b44ea8785b` | `experiment/saa-scheduler-sdk` |

The generated API checkout is the `api-go` module at the generated revision;
the SDK test module is included separately so SDK integration tests resolve
against the same local API and server pins.

From the server checkout:

```bash
cd /home/alex/Work/tries/2026-09-04-cd/temporal
GOWORK=/tmp/extensible-go.work go test -tags test_dep ./chasm/lib/scheduler ./chasm/chasmtest
GOWORK=/tmp/extensible-go.work make lint-code-fast
```

Resolve the local proto dependency first, then regenerate with the pinned API
version:

```bash
cd /home/alex/Work/tries/2026-09-04-cd/temporal
GOWORK=/tmp/extensible-go.work ./cmd/tools/getproto/run.sh --out proto/api.binpb
GOWORK=/tmp/extensible-go.work make proto GO_API_VER=v1.63.5
GOWORK=/tmp/extensible-go.work make temporal-server
```

Create the local server and dynamic configuration from the repository
defaults, then append the experiment's overrides:

```bash
cp "$SERVER_CHECKOUT/config/development-sqlite.yaml" /tmp/extensible-server.yaml
sed -i 's#config/dynamicconfig/development-sql.yaml#/tmp/extensible-dynamicconfig.yaml#' /tmp/extensible-server.yaml
cp "$SERVER_CHECKOUT/config/dynamicconfig/development-sql.yaml" /tmp/extensible-dynamicconfig.yaml
cat >> /tmp/extensible-dynamicconfig.yaml <<'EOF'

activity.enableStandalone:
  - value: true
activity.enableCallbacks:
  - value: true
activity.startDelayEnabled:
  - value: true
history.enableCHASMSchedulerCreation:
  - value: true
history.chasmSchedulerCreationRolloutPercent:
  - value: 100
history.enableCHASMSchedulerRouting:
  - value: true
history.enableCHASMSchedulerSentinels:
  - value: true
history.enableCHASMSchedulerMigration:
  - value: false
EOF
```

Verify the work file and dependency resolution with:

```bash
GOWORK=/tmp/extensible-go.work go env GOWORK
GOWORK=/tmp/extensible-go.work go list -m all
```

## Enabling CHASM schedules

The existing namespace dynamic settings control the CHASM backend:

| Setting | Local experimental value | Purpose |
| --- | --- | --- |
| `history.enableCHASMSchedulerCreation` | `true` | Create new schedules in CHASM. |
| `history.chasmSchedulerCreationRolloutPercent` | `100` | Include every namespace in creation rollout. |
| `history.enableCHASMSchedulerRouting` | `true` | Route schedule RPCs to CHASM first. |
| `history.enableCHASMSchedulerSentinels` | `true` | Reserve schedule ID collision sentinels. |
| `history.enableCHASMSchedulerMigration` | `false` | Keep V1 migration disabled for this experiment. |

Set these through `/tmp/extensible-dynamicconfig.yaml`, which is referenced by
`/tmp/extensible-server.yaml`.
Standalone activities also require these activity settings:

| Setting | Local experimental value | Purpose |
| --- | --- | --- |
| `activity.enableStandalone` | `true` | Enable standalone activity APIs. |
| `activity.enableCallbacks` | `true` | Allow scheduler completion callbacks (the default is `false`). |
| `activity.startDelayEnabled` | `true` | Allow non-zero activity start delays. |

`activity.enableStandalone` and `activity.startDelayEnabled` default to
`true`; the local override for `activity.enableCallbacks` is required by this
experiment.
The activity implementation is CHASM-only in this experiment:
requests routed to the old scheduler backend are rejected, and activity
schedules cannot be migrated to that backend or converted to V1.

## Action and policy selection

Workflow schedules retain their existing overlap policies and default behavior.
Standalone activity schedules must provide an explicit policy on create and
update; `UNSPECIFIED` is rejected. A trigger or backfill override inherits the
schedule policy when omitted. An override takes precedence over the schedule
policy, and an implementation default is used only when the action declares
one. Conflicting, unknown, or unsupported selectors are rejected.

The standalone activity implementation supports:

```text
SKIP, BUFFER_ONE, BUFFER_ALL, ALLOW_ALL, TERMINATE_OTHER,
temporal.buffer_latest
```

`CANCEL_OTHER` is rejected for activities at every configuration and override
entry point. Workflow schedules reject `temporal.buffer_latest`.

`temporal.buffer_latest` keeps the newest pending occurrence while an active
execution or another selected non-overlapping start blocks it. Newest means
the greatest scheduled occurrence time, with buffer insertion order breaking a
tie. Replacing a waiting occurrence records an overlap skip and does not spend
action capacity. Started executions and waiting occurrences using another
policy are never replaced.

The runnable example uses `client.ScheduleActivityAction` and
`ScheduleOptions.CustomOverlapPolicy: "temporal.buffer_latest"`. Builtin
policies use the existing `Overlap` enum field; leave it unspecified when
selecting a custom policy.

The SDK owns activity arguments, timeouts, retry policy, headers, context
propagation, search attributes, metadata, priority, and start delay. Scheduler
request IDs, callbacks, and target ID policy remain scheduler-owned.

## Execution and completion

An accepted activity occupies its overlap slot while queued, delayed, running,
or retrying according to the selected policy. Activity retries remain in the
activity subsystem; scheduler retries cover only failed start requests.
Termination uses the recorded activity ID and run ID and follows the shared
termination ordering and action budget. Completion callbacks are durable and
native activity terminal statuses are retained in generic recent-action and
last-execution results.

Workflow completion history continues to drive workflow last-success and
continued-failure behavior. Activity actions do not receive implicit previous
results through activity arguments, and list or visibility responses do not
contain result payloads.

## Review boundaries

The published draft layers are [source API](https://github.com/chaptersix/temporal-api/pull/4),
[generated API-Go](https://github.com/chaptersix/api-go/pull/1),
[action contracts](https://github.com/chaptersix/temporal/pull/59),
[standalone activities](https://github.com/chaptersix/temporal/pull/60),
[visibility](https://github.com/chaptersix/temporal/pull/61),
[SDK](https://github.com/chaptersix/temporal-sdk-go/pull/1), and
[integration coverage/example](https://github.com/chaptersix/temporal/pull/62).
The server review base is `95d50ed2a8b406ed6ef7e13d558dbf544dd550d8`; the
validated implementation is `f7f30fc4be01f16ac838f6106f47484643eb4a40`.

This is an experimental linked stack. Keep the local work file and dependency
pins reproducible, preserve published branches, and organize review layers
around generic APIs/results, action and policy contracts, activity execution,
visibility and projections, SDK support, and integration coverage. No layer is
merged by this document.

## Running the local demo

Run the example from
the server checkout with:

```bash
cd /home/alex/Work/tries/2026-09-04-cd/temporal
GOWORK=/tmp/extensible-go.work go run ./docs/examples/chasm-scheduler
```

Start the server separately with the prepared local configuration:

```bash
cd /home/alex/Work/tries/2026-09-04-cd/temporal
GOTOOLCHAIN=go1.26.7 GOWORK=/tmp/extensible-go.work ./temporal-server --config-file /tmp/extensible-server.yaml start
```

The example pauses and deletes its schedules and terminates tracked running
executions before exiting. Stop the local server with `Ctrl-C`; the configured
SQLite stores use in-memory mode, so stopping the process removes that demo
state.

## Validation

Validated locally with Go 1.26.7 and the four-module workspace:

```bash
go test -tags test_dep ./chasm/chasmtest ./chasm/lib/scheduler/... ./chasm/lib/activity ./docs/examples/chasm-scheduler
go test -tags test_dep ./tests -run '^TestScheduleActivity(BufferAll|TerminateOther)$' -count=1
go test -tags test_dep ./tests -run '^TestScheduleCHASM$/(TestBasic|TestOverlap|TestScheduledWorkflowContinueAsNewCompletion|PauseOnFailure_|PausedBehavior)' -count=1
go test -tags test_dep ./service/frontend -run 'TestWorkflowHandlerSuite/(TestActivitySchedulePolicyAndRoutingValidation|TestCreateSchedule|TestUpdateSchedule)' -count=1
make lint-code-fast
```

The canonical SDK build runner passed `check`, the schedule conversion unit
tests with race detection, and
`integration-test -run 'TestIntegrationSuite/TestScheduleStandaloneActivity$'`
against the modified local server. The broad SDK check used an isolated local
workspace to resolve the existing gRPC OpenTelemetry module split in contributed
modules. The four-module server workspace and all committed dependency files
remain unchanged.

The runnable example completed with workflow and activity results,
`temporal.buffer_latest` overlap skips, native activity termination, and schedule
cleanup. Public API `buf lint` and server `make proto GO_API_VER=v1.63.5` passed.

Planning uses detached snapshots and bounded buffers. Execution reconciliation
matches stable occurrence identity and retry state; a selected start reserves
its overlap slot while the RPC response is outstanding. Failed starts release
waiting work, and duplicate acknowledgements do not spend capacity again.
