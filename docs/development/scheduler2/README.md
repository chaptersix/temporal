# Local scheduler2 verification

The current bounded prototype supports resumable automatic enumeration, SKIP,
BUFFER_ONE/ALL and keyed ALLOW_ALL starts, immutable retries and completion carryover.
The sections below preserve the contracts/evidence of successive local slices.
Only offline decoding registers scheduler2 in production-linked code; no service
routing or FX module selects it. Use explicit local library wiring in tests.

Run from the repository root:

```sh
python3 docs/development/scheduler2/run.py regressions --output /private/tmp/scheduler2-verification/regressions
python3 docs/development/scheduler2/run.py model --output /private/tmp/scheduler2-verification/model
python3 docs/development/scheduler2/run.py properties --output /private/tmp/scheduler2-verification/properties
python3 docs/development/scheduler2/run.py properties --exploration --seed 1 --output /private/tmp/scheduler2-verification/exploration
python3 docs/development/scheduler2/run.py simulation --output /private/tmp/scheduler2-verification/simulation
python3 docs/development/scheduler2/run.py race --output /private/tmp/scheduler2-verification/race
CGO_ENABLED=0 GOMAXPROCS=4 make lint-code-fast GOLANGCI_LINT_BASE_REV=85b80a1836dbc73741cb52586297dc35e6c937ca GOLANGCI_LINT_FIX=false
```

The runner emits exact commands, source SHA, dirty diff, per-source hashes, tool
versions, settings, outcome, duration and log paths. Missing commands/timeouts
fail. The fuzz/benchmarks profiles emit unsupported and exit nonzero. The proofs profile
runs the pinned local exact Lean build and axiom audit described below.
Go checks use test_dep. Normal runs disable CGO; race selects installed macOS
26.5 SDK because the default macOS 27 SDK cannot be linked by the installed linker.
No system SDK setting is modified. FizzBee release/tar hash is in contract.md.

## Trace and oracle boundaries

trace.schema.json specifies the versioned local replay format. The runtime
validator also checks dependencies and rejects unsupported initial states,
versions, events, times and completion result payloads. Completion is absent-result
only in M1; explicit null and success/failure carryover updates are M2. Frozen start
requests include arbitrary caller-supplied carryover and retain it byte-for-byte.
This supported-subset decision is D19; no result-presence equivalence is claimed.

I2 is automatic/<initial_time_ns>; I3 is incarnation/I2/action; I4 is I3/start.
These IDs are derived losslessly from recorded initial state and checked in each
projection. I5 is explicit attempt; I6 is explicit chain. One logical task belongs
to I4 and its committed retry timestamp; physical task identity/transition offsets
are tested separately by real CHASM validation. Initial cursor is zero, definition
revision one, limited budget one, no operation/task. Synthetic payloads only.

The Go replay adapter executes real Scheduler.Apply on fresh CHASM trees and
checks an independently authored event switch oracle after each observation.
The reducer uses the same domain event vocabulary but never computes expected
states. The FizzBee adapter mapping is in model/README.md: nanoseconds collapse
to pre-deadline/expiry atoms, whole request collapses to revision, and snapshot
publication maps to atomic CommitAdmission/FailCommit. It is a manual refinement
mapping, not an automated cross-language trace importer. Lean consumes no M1
calendar trace; WI-002 adds a separate executable enumeration draft. Differential API
trace execution belongs to M3. M1 replay permits only the recorded serial event
order; no claim about general concurrent ordering follows from that format.

The reference replay checks phase, IDs, accounting, revision, cursor, obligations
and full issued request. External acceptance is independent from local snapshots.
The CHASM fake store trusts atomic DB publication; tests exercise actual tree
serialization, task validation and fresh reconstruction, not database crash recovery.

## Self-tests and limits

V1 compares complete replay observations from fresh harnesses. V2 poisons old
objects and serialized buffers. V3 discards pre-publication state and task output,
retains post-publication obligations, and keeps remote acceptance outside snapshots.
V4 deletes only dependency-valid events while preserving the injected S2 failure,
saves minimized explicit JSON and replays it without a seed. V5 detects dropped
logical tasks, aliased snapshots, changed retry input and invalid event dependencies.
Rapid generates and shrinks command sequences; the deliberately observed mutation
shrink uses the deterministic dependency-aware deletion pass, not Rapid internals.

Routine reducer generation uses 32 events; exploration uses 128. CHASM simulation
uses the 12-event saved failure path plus 0–16 generated update/failed-commit events
(12–28 events). Recovery is the explicit authenticated completion in the suffix,
not a liveness proof. FizzBee separately checks temporal assertions under named
weak fairness. D20 records a conservative one-RPC gate per local library instance;
per-schedule concurrent executor partitioning is deferred beyond this single-
schedule experiment. Full calendar, public API, retention, failover, migration,
load and proof claims remain outside M1.

## WI-002 enumeration

The standalone pure core is not wired into CHASM admission yet. Its bounded
contract and intentional legacy divergences are in enumeration-contract.md.

```sh
python3 docs/development/scheduler2/run.py enumeration --exploration --seed 1 --output /private/tmp/scheduler2-verification/enumeration
CGO_ENABLED=0 GOMAXPROCS=4 go test -p 2 -parallel 4 -tags test_dep -count=1 ./chasm/lib/scheduler2/... ./service/worker/scheduler
CGO_ENABLED=0 GOMAXPROCS=4 make lint-code-fast GOLANGCI_LINT_BASE_REV=00b65430a GOLANGCI_LINT_FIX=false
```

The saved pending_jitter_v1.json fixture replays without generator reconstruction,
recompiling the definition and JSON continuation at every step. The pinned TZif
fixture checks selected US gap/fold cases; full timezone coverage is not claimed.
The independent Lean v4.24.0 draft and commands are in lean/README.md. Proposition
definitions and executable examples are not completed proofs or Go correspondence.
M2 policy/callback/backfill work and M3 proofs remain pending.

## WI-003 completion and carryover

```sh
python3 docs/development/scheduler2/run.py completion --exploration --seed 1 --output /private/tmp/scheduler2-verification/completion
python3 docs/development/scheduler2/run.py completion-model --output /private/tmp/scheduler2-verification/completion-model
```

Callback is an explicit local completion event, distinct from metadata-only
Complete. First completion freezes its carryover/pause effects. No-result success
clears previous values; encoded null and unknown protobuf fields are preserved;
failure retains previous success. Failure status must match its protobuf variant.
The current pause policy applies to FAILED/TIMED_OUT, including metadata completion.
The separate carryover node is materialized only for admission; retry requests
remain frozen. Initial uninitialized carryover keeps M1 caller-provided request
fields; fixtures seed initialized carryover explicitly. Empty Failure presence is
stored separately from its zero-byte encoding. Each wire value is bounded to 64KiB.

The saved completion_v1.json replays independently expected observations through
fresh CHASM serialization. The symbolic model checks safety and reachability at
100/200 action bounds, without liveness or automated refinement claims. Model
TogglePolicy maps to an Update preserving Paused, rather than an unpause command.
Authentication is trusted at this local boundary; production callback routing,
sequential admission, other overlap modes and retention remain future work.

## WI-004 sequential generation

```sh
python3 docs/development/scheduler2/run.py sequential --exploration --seed 1 --output /private/tmp/scheduler2-verification/sequential
python3 docs/development/scheduler2/run.py sequential-model --output /private/tmp/scheduler2-verification/sequential-model
```

ConfigureGeneration is explicit local wiring: an immutable ScheduleSpec/seed and
workflow start template, initial exclusive cursor, revision and catchup window.
A separate Generator component persists continuation and a token/deadline; its
registered pure task handles eight probes/four outputs per invocation. Next work
uses a 1ms delay (clamped to the supported clock end), or the pending actual time.
Empty/exhausted definitions remove their generation obligation. Update preserves
admitted snapshots while superseding unadmitted pending values under D24.

Strict SKIP treats pending, started and ambiguous operations as occupying one
slot; only verified completion permits a subsequent admission. Up to four records
are retained, evicting only the oldest completed record. Late retained/evicted IDs
cannot replace current carryover, counters or active identity. Skips advance the
nominal cursor without preparing carryover or debiting budget. Generation emits
only actual-due candidates and uses actual-time catchup; the reducer's direct Due
entry assumes its caller has checked actual-time eligibility. Automatic generated
workflow IDs append the UTC nominal RFC3339 timestamp.

The generation_v1.json replay executes registered pure tasks on real CHASM nodes,
with failed publication, reload and second-request carryover observations. The
independent sequential model uses six ordered occurrences and output limit one;
Go's four-output batch is a sequence of these dispositions under one transaction.
The model omits calendar arithmetic/catchup/rollback, which have separate Go tests.
No composition theorem or liveness guarantee after permanent ambiguous acceptance
is claimed. Other overlap modes, manual/backfill fairness and production routing
remain outside this slice.

## WI-005 / WI-006 buffered and keyed admission

```sh
python3 docs/development/scheduler2/run.py buffer --exploration --seed 1 --output /private/tmp/scheduler2-verification/buffer
python3 docs/development/scheduler2/run.py allowall --exploration --seed 1 --output /private/tmp/scheduler2-verification/allowall
python3 docs/development/scheduler2/run.py allowall-model --output /private/tmp/scheduler2-verification/allowall-model
```

Queues retain up to4 automatic occurrence identities without budget debit. Drain
uses current eligibility/template/carryover and queue-time workflow identity/policy.
Completion independently arms drain after generator exhaustion. ALLOW_ALL bypasses
the tracked blocker/queue but consumes bounded intent capacity; full-capacity new
admission drops/counts without debit, existing accepted queue waits without spinning.

Four retained operation records separate Pending/Started/Ambiguous from Completed
and StartOnly. ALLOW_ALL acknowledgment/early completion settles only the start
intent, never changes carryover/pause, and permits terminal eviction. Keyed older
results preserve the latest admission and unrelated blocking owner. Metadata and
requests live in separate DATA maps; an exact root PendingStarts deadline index
lets task validation avoid reading every operation. Real poisoned metadata tests
cover full CloseTransaction/reload. Physical persistence reads are not reduced.

No persisted-layout migration, public API routing, cancel/terminate or manual/backfill
claim follows. Those remaining gates are tracked in the active M2–M4 plan.

## WI-013 required Lean correspondence profile

This named local profile requires an explicit prebuilt Lean 4.24.0 oracle, its passed
build manifest (with adjacent `version.log`), and an explicit seed. It never builds,
downloads, substitutes or skips the oracle. A mismatched binary hash, missing required
test, input drift, nonzero test exit or timeout fails the profile. Use a fresh output
directory to preserve prior evidence. Existing runner profiles keep their commands
and default seed. The separate `proofs` profile runs the pinned local build and
axiom audit (no download), recording its nested build manifest, source/tool/binary
hashes and Lean version:

```sh
python3 docs/development/scheduler2/run.py proofs \
  --output /private/tmp/scheduler2-verification/lean-proofs
```

```sh
python3 docs/development/scheduler2/run.py lean-correspondence \
  --oracle /private/tmp/scheduler2-wi013-lean-build11/oracle \
  --oracle-manifest /private/tmp/scheduler2-wi013-lean-build11/manifest.json \
  --seed 1 --output /private/tmp/scheduler2-verification/lean-routine
python3 docs/development/scheduler2/run.py lean-correspondence --exploration \
  --oracle /private/tmp/scheduler2-wi013-lean-build11/oracle \
  --oracle-manifest /private/tmp/scheduler2-wi013-lean-build11/manifest.json \
  --seed 1 --output /private/tmp/scheduler2-verification/lean-exploration
```

The routine command runs the following with `CGO_ENABLED=0`, `GOMAXPROCS=4`, and
`SCHEDULER2_LEAN_ORACLE` set to the selected absolute executable path:

```sh
go test -p 2 -parallel 4 -tags test_dep,lean_correspondence -count=1 -timeout=10m \
  ./chasm/lib/scheduler2/internal/occurrence -run '^TestLeanCorrespondence' \
  -rapid.seed=1 -rapid.checks=100 -v
```

Exploration uses `-rapid.checks=10000` for **each of two Rapid properties**. Both modes
also run 384 bounded exhaustive traces, directed validation/boundary checks and all
three checked-in v1/v2 replay fixtures, including Update and historical Rebase.
Inherited `SCHEDULER2_LEAN_REPLAY` is cleared so it cannot silently replace that
coverage. Saved custom replay remains available through the direct Go test command.

The manifest records the executable SHA256, Lean version/tool hash, build-manifest
reference/hash and built source hashes, alongside driver/fixture/runner/config hashes,
exact commands, seed, Go version, environment and logs. Production source/testdata hashes, focused inputs and executable provenance
are checked again after execution; drift fails the correspondence profile. `profile.json` records separate generator bounds:
1–12 second interval-union traces with up to 40 scan iterations, or 8-second
window/replacement traces with up to 24 scan iterations. Historical dispositions
can add one extra call per scan; these are not coordinator event bounds.

Expected raw candidates come from independent integer-second interval membership
and UTC second exclusions, including fractional starts and exact timestamps. The
fixture never calls production next-occurrence search. Time binary encoding,
Farm32 jitter primitives and opaque identity obtained from Begin are shared
assumptions; this is not proof of hashing, timezone/calendar coverage or deployed
behavior. Formal proof building and mutation sensitivity remain separately recorded
evidence. Passing this profile alone does not accept WI-013 or M3.
