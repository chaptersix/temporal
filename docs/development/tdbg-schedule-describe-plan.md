# TDBG Schedule Describe Plan

## Purpose

Add a schedule-focused debugging command that renders fully decoded scheduler internals as JSON or as a standalone HTML report.

The command is intended for engineers debugging scheduler behavior. Public schedule identity and API-level description fields provide context, but the primary information is the persisted CHASM component state, logical tasks, Invoker buffer, Backfillers, and component event logs.

The implementation and layout will be developed iteratively. A local fixture harness will make it inexpensive to rebuild the server and `tdbg`, recreate an interesting schedule, regenerate both output formats, and review the HTML after each change.

## Reference fixture

The initial fixture was produced with `temporal-server` and `tdbg` built from this checkout. It contains:

- A paused CHASM schedule with a one-second interval and `BufferAll` overlap policy.
- A workflow action targeting an intentionally unserved task queue.
- An active one-year Backfiller at attempt 2.
- 460 fully decoded `BufferedStart` entries: one started entry and 459 deferred entries.
- Scheduler, Generator, Invoker, and Backfiller event logs.
- Pending Generator, Backfiller, and visibility logical tasks.

Ignored local reference artifacts:

- `.tmp/tdbg-chasm-internals-main-dump.txt`
- `.tmp/tdbg-chasm-main-tree.json`

These artifacts are useful during design, but automated tests must use small checked-in fixtures or construct protobufs directly rather than depending on `.tmp`.

## User interface

### Command

```bash
tdbg schedule describe \
  --namespace default \
  --schedule-id example \
  --format json

tdbg schedule describe \
  --namespace default \
  --schedule-id example \
  --format html \
  --output-filename .tmp/tdbg-schedule-describe/report.html
```

Proposed behavior:

- `--format` accepts `json` and `html` and defaults to `json`.
- `--output-filename` is optional. Without it, the selected format is written to stdout.
- JSON and HTML are projections of the same decoded report model.
- The existing `tdbg execution describe` command remains unchanged.
- The first implementation specializes in CHASM schedules. Workflow-backed schedules should either receive a clearly identified reduced view or return a precise unsupported error until a useful V1 internal model is designed. It should not invent Generator, Invoker, or Backfiller parity for V1.

### HTML layout

The layout follows the UI mock-up, with the Invoker buffer promoted to the main debugging surface.

#### Compact header

Show only high-signal context:

- Created and updated time.
- Paused and closed state.
- Schedule specification summary.
- Overlap policy.
- Search attributes.
- Timezone selector.

Namespace, schedule ID, run ID, shard, history host, conflict token, and branch information belong in a collapsed execution-details section.

#### Center: Invoker buffer

The buffer is the largest panel on the page.

Its summary bar shows:

- Total entries.
- New/unprocessed entries.
- Deferred entries.
- Started/running entries.
- Retrying entries.
- Recently completed entries retained in the buffer.
- Pending cancellation and termination counts.

The table includes:

- Nominal, actual, and desired time when present.
- Manual/backfill indicator.
- Overlap policy.
- Attempt value.
- Request ID.
- Workflow ID and run ID.
- Start time and completion information.
- Any other fully decoded `BufferedStart` fields present in the protobuf.

Every row has an expandable detail view containing its complete decoded value. The HTML may initially collapse long IDs and payloads visually, but it must not discard them from the document or report model.

The table supports client-side searching and filtering by attempt/state, manual status, Backfiller/request ID, workflow ID, and time. The maximum scheduler buffer is currently 1,000 entries, so an ordinary table with lightweight filtering should be sufficient; virtualization is unnecessary unless measurements show otherwise.

#### Component internals

Display component cards adjacent to or above the buffer:

- **Scheduler:** complete decoded scheduler state, migration state, idle-close time, counters, last-completion result, versioned transitions, and owned tasks.
- **Generator:** last-processed high-water mark, future action times, decoded state, and owned tasks.
- **Invoker:** last-processed time, cancel/terminate queues, decoded fields not represented by the buffer table, and owned tasks.
- **Backfillers:** one card per Backfiller with request type, range, Backfiller ID, last-processed time, attempt, decoded request, progress, and owned tasks.

Each card presents important fields first and includes a complete decoded-data disclosure. There is no separate “raw nodes” panel: node metadata, decoded component data, and tasks stay associated with their owning component.

Collection and data nodes such as `Backfillers`, `LastCompletionResult`, `Visibility`, and `Visibility$Memo` receive appropriately typed views rather than being forced into component cards.

#### Logical tasks

Show pure and side-effect tasks in a compact table grouped by owner path:

- Owner component.
- Pure or side-effect kind.
- Task FQN and type ID.
- Destination.
- Scheduled time.
- Physical task status.
- Versioned transition and offset when available.
- Complete decoded task data.

Unknown task types retain their raw `DataBlob` and display the decode error on that task.

#### Unified event timeline

Merge events from Scheduler, Generator, Invoker, and every Backfiller EventLog into one stable chronological stream.

Each row includes:

- Timestamp in the selected timezone.
- Duration since the previous event in the merged stream.
- Source component and node path.
- Message.
- Stable source/index ordering for events with identical timestamps.

The timeline supports source filters. The original per-component order must be retained in JSON so equal-timestamp events are deterministic.

The current fixture demonstrates the intended use: a Backfiller enqueues 450 starts, the Invoker readies one and defers 449, starts one workflow, then the Backfiller retries and enqueues another ten. Today that sequence is split across multiple EventLog nodes.

#### Supporting panels

- Action definition and fully decoded input payloads.
- Memo and search attributes.
- Visibility state and task.
- CHASM node metadata and versioned transitions.
- Cache/database origin and branch information without printing duplicate mutable states.

## Report model

Use an explicit renderer-independent Go model instead of passing persistence protobufs directly to templates.

```text
ScheduleDescribeReport
├── Summary
├── ExecutionDetails
├── Schedule
│   ├── Spec
│   ├── Policies
│   ├── State
│   └── Action
├── Components
│   ├── Scheduler
│   ├── Generator
│   ├── Invoker
│   │   └── BufferedStarts[]
│   └── Backfillers[]
├── DataNodes[]
├── LogicalTasks[]
├── EventTimeline[]
├── Visibility
└── DecodeErrors[]
```

`DecodeErrors` only records actual decoding failures with their node/task paths. It is not a collection of inferred scheduler warnings.

The model should retain:

- The fully decoded protobuf data for every known node and task.
- Node metadata, type ID/FQN, node kind, and versioned transitions.
- Raw blobs only when a type is unknown or decoding fails.
- Complete buffered starts without truncation.
- Original EventLog source path and event index.

Prefer typed scheduler fields for rendering and `json.RawMessage` for complete decoded payloads that do not need additional interpretation. Lists used by the HTML must have deterministic ordering; do not expose persistence maps directly.

## Data collection and decoding

1. Resolve namespace and schedule ID.
2. Request the CHASM scheduler mutable state using `chasm.SchedulerArchetypeID` and the schedule ID as the execution business ID.
3. Use database mutable state as the authoritative persisted snapshot.
4. Record whether cache mutable state is present, but do not duplicate it in the normal report. A later iteration may add a focused cache/database comparison if it proves useful.
5. Build the CHASM registry and decode every node and logical task.
6. Classify nodes using both component FQN and path:
   - Root scheduler: empty path.
   - Generator: `Generator`.
   - Invoker: `Invoker`.
   - Backfillers: `scheduler.backfiller` components below the Backfillers collection.
   - Event logs: `scheduler.eventlog`, associated with their parent path.
   - Visibility and data nodes by FQN/path and node kind.
7. Flatten EventLog entries into the merged timeline while retaining their source.
8. Decode payloads using the existing payload decoder where possible so action input, memo, and search attributes are readable rather than base64-only.
9. Extract branch, shard, and history-host details once.

Errors from RPCs, output files, JSON encoding, templates, and writers are returned. A single unknown component or task should not prevent rendering the remaining report; that item retains its raw blob and local decode error.

## Proposed code organization

- `tools/tdbg/tdbg_commands.go`: register `schedule describe` and flags.
- `tools/tdbg/flags.go`: add the format flag constant.
- `tools/tdbg/schedule_describe.go`: command orchestration and RPC collection.
- `tools/tdbg/schedule_describe_model.go`: report types and CHASM node classification.
- `tools/tdbg/schedule_describe_json.go`: deterministic JSON rendering.
- `tools/tdbg/schedule_describe_html.go`: template execution and embedded assets.
- `tools/tdbg/schedule_describe.html.tmpl`: standalone HTML structure, CSS, and small client-side controls.
- `tools/tdbg/schedule_describe_test.go`: command/model tests.
- `tools/tdbg/schedule_describe_html_test.go`: template, escaping, and structural tests.
- `tools/tdbg/testdata/schedule-describe/`: small golden inputs and expected outputs where direct assertions are insufficient.

Use Go’s standard `html/template`, `encoding/json`, and `embed` support. Do not add a third-party UI dependency.

## Iterative fixture and visual-review scripts

Add a development harness under `develop/tdbg-schedule-describe/`. Runtime files go under the ignored `.tmp/tdbg-schedule-describe/` directory.

### `start.sh`

- Resolve the repository root without depending on the caller’s working directory.
- Build `temporal-server` and `tdbg` from the checkout.
- Generate an isolated file-backed SQLite configuration under `.tmp`.
- Use dedicated ports so the harness does not collide with a normal local Temporal instance.
- Generate dynamic config enabling CHASM, CHASM scheduler creation, 100% creation rollout, and CHASM-first schedule routing.
- Start the server in the background with logs redirected to `.tmp/tdbg-schedule-describe/server.log`.
- Store its PID in `.tmp/tdbg-schedule-describe/server.pid`.
- Poll the frontend health endpoint/RPC with a bounded timeout instead of sleeping for a fixed duration.
- Create the `default` namespace if it does not already exist.
- Be idempotent: if the recorded process is healthy, report its address and exit successfully.

### `stop.sh`

- Read only the harness PID file.
- Verify that the PID belongs to this repository’s `temporal-server` command before signaling it.
- Send `TERM`, wait for bounded graceful shutdown, and report failure rather than killing unrelated processes.
- Remove a stale PID file when the process no longer exists.
- Preserve SQLite data and generated reports.

### `reset.sh`

- Call `stop.sh`.
- Remove only the explicitly resolved harness database and generated fixture/output files beneath `.tmp/tdbg-schedule-describe/`.
- Never recursively delete a variable or broad directory.
- Restart the server through `start.sh` unless passed a documented `--no-start` option.

### `seed.sh`

- Require a healthy harness server.
- Delete/recreate only the known fixture schedule ID, or reset the harness database when deterministic IDs are required.
- Create the paused one-second `BufferAll` schedule with representative nested action input and memo.
- Submit a large historical backfill against an unserved task queue.
- Poll `tdbg execution describe` until all of these are true:
  - A Backfiller component exists.
  - Its attempt is greater than zero.
  - The Invoker buffer contains both a started and deferred entry.
  - Scheduler, Generator, Invoker, and Backfiller EventLog nodes exist.
- Fail with paths to the server log and last dump when the state is not reached within the timeout.

### `render.sh`

- Build `tdbg` if necessary.
- Run the new command in both formats against the seeded fixture.
- Write timestamp-free stable paths:
  - `.tmp/tdbg-schedule-describe/latest.json`
  - `.tmp/tdbg-schedule-describe/latest.html`
- Validate JSON with `jq` when available.
- Perform inexpensive HTML checks through a Go test or repository-provided validator rather than introducing an external dependency.
- Print clickable absolute paths to both files.
- Do not automatically open a browser; print an optional explicit command so visual review remains user-controlled.

### `refresh.sh`

This is the normal implementation/feedback loop:

1. Ensure the server is running.
2. Rebuild `tdbg`.
3. Reuse the existing seeded state when it is still suitable.
4. Reseed when passed `--reseed` or when the Backfiller/buffer fixture is no longer present.
5. Regenerate JSON and HTML through `render.sh`.
6. Print report paths, fixture summary, server status, and log path.

Example workflow:

```bash
develop/tdbg-schedule-describe/start.sh
develop/tdbg-schedule-describe/seed.sh
develop/tdbg-schedule-describe/render.sh

# After an implementation or layout change:
develop/tdbg-schedule-describe/refresh.sh

# Recreate the runtime state from scratch:
develop/tdbg-schedule-describe/refresh.sh --reseed

develop/tdbg-schedule-describe/stop.sh
```

Shared behavior such as paths, ports, PID validation, and health polling belongs in `develop/tdbg-schedule-describe/lib.sh`. Scripts should use `set -euo pipefail`, quote paths, and emit concise actionable errors.

## Visual feedback loop

Each iteration should be small enough to review directly:

1. Run focused Go tests.
2. Run `refresh.sh` to regenerate the current fixture reports.
3. Inspect `latest.html` at desktop and narrower widths.
4. Compare `latest.json` with the underlying decoded CHASM tree to ensure no state was dropped.
5. Record feedback about grouping, density, labels, default expansion, filters, and timestamp presentation.
6. Change the model or renderer, rerun `refresh.sh`, and repeat.

Do not turn the evolving HTML report into a golden-file-only exercise. Structural tests protect correctness and escaping, while human review determines whether the internals are actually readable.

## Automated tests

### Model and decoding

- Root Scheduler, Generator, Invoker, Backfiller, EventLog, visibility, collection, and data-node classification.
- Multiple Backfillers and event logs.
- Full preservation of buffered starts.
- Pure and side-effect task ownership.
- Unknown component/task types and malformed blobs.
- Deterministic ordering of Backfillers, tasks, buffer rows, and equal-time events.
- Correct association of Backfiller EventLogs whose persisted path uses collection/map delimiters.
- Empty buffer and absent optional nodes.

### JSON

- Valid JSON with stable field names.
- Complete decoded component/task data.
- No base64-only representation when a supported payload can be decoded.
- No truncation of large buffers.
- Raw fallback and local error for undecodable values.

### HTML

- Expected component, buffer, task, and timeline sections.
- Rendering of empty and 1,000-entry buffers.
- Escaping of schedule IDs, notes, memo, payloads, paths, and event messages.
- Timezone control markup and timestamp source values.
- Stable event ordering.
- No network-loaded assets or executable content from decoded data.
- Writer and template errors are returned.

### CLI

- Required schedule ID.
- Accepted and rejected formats.
- stdout and output-file behavior.
- RPC error propagation.
- Missing mutable state.
- CHASM schedule fixture selection.
- Explicit workflow-backed schedule behavior.

## Verification

During implementation, run the smallest relevant tests first with the required tag:

```bash
go test -tags test_dep ./tools/tdbg -run 'TestScheduleDescribe'
```

Before handoff, run repository formatting/import checks and `make lint-code`. Run broader unit tests in proportion to the final change and any shared decoder modifications.

## Trade-offs and failure behavior

- A curated typed model requires mapping code, but it prevents persistence layout from dictating the UI and gives JSON consumers a coherent contract.
- Complete decoded data can make reports large. JSON remains complete; HTML uses collapsible details and filters rather than truncation.
- A 10x increase over the current maximum buffer would primarily affect browser rendering and report size, not server load: collection is one existing describe RPC and rendering is linear. If limits change materially, add table windowing after measuring it.
- The report may contain workflow inputs, memo, search attributes, IDs, and failures. It is a local debugging artifact and must not fetch remote assets or transmit data. Help text should make the sensitivity of generated files clear.
- Server or `tdbg` crashes leave logs, PID state, and the latest reports under `.tmp`. `start.sh` detects stale PIDs; `stop.sh` never signals a process it cannot validate.
- If the Backfiller finishes before capture, `seed.sh` fails its state predicate and recreates the fixture rather than silently producing an unrepresentative report.

## Delivery sequence

1. Add the fixture harness and prove repeatable current-main dumps with an active Backfiller, populated buffer, and all EventLogs.
2. Add the report model and node classification tests using the captured structure as a reference.
3. Add deterministic JSON output and verify complete decoded fidelity.
4. Add the first HTML layout with the buffer, component cards, tasks, and merged timeline.
5. Iterate on the HTML through `refresh.sh` and visual feedback.
6. Add workflow-backed behavior, remaining error cases, documentation, and final repository checks.

## Acceptance criteria

- One command produces valid JSON or a self-contained HTML report for a CHASM schedule.
- The report contains the complete decoded Scheduler, Generator, Invoker, Backfiller, task, buffer, visibility, and EventLog data available in mutable state.
- The buffer is the principal HTML surface and remains usable with 1,000 entries.
- Events from all component logs form a filterable deterministic timeline.
- Unknown data remains available as raw blobs with localized decode errors.
- The development scripts can safely start, stop, reset, seed, and refresh the fixture without manual process cleanup.
- A contributor can change the renderer and regenerate reviewable output with a single `refresh.sh` invocation.
