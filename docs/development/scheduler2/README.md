# Local scheduler2 verification

This is a deliberately bounded prototype: one predetermined automatic occurrence,
SKIP overlap, immutable start, early completion, and retained ambiguous expiry.
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
fail. The proofs/fuzz/benchmarks profiles emit unsupported, exit nonzero, and are
not required for the bounded M1 acceptance; they remain later-milestone work.
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
