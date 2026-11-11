# M1 contract, version 1

Source baseline: `85b80a1836dbc73741cb52586297dc35e6c937ca`.
Authorization: execute locally through M1; original checkout stays untouched.

## Selected semantics

D1: retain the existing `sch-redesign` integration worktree. Its branch reflog
records creation from upstream/main on 2026-11-10 at 18:33:52 -0600. The
comparison baseline `9ab3a9f770da20df7d94bcc0030f28eec7b0b947` is its ancestor.
No original-checkout implementation is imported. The original tracked diff is
captured only as provenance evidence, never applied.

D2: SKIP overlap, one automatic occurrence, one active operation. Admission is
eligible only when unpaused, within catchup, with budget and capacity available.
Other overlap modes, manual requests, backfill and public API wiring are M2/M3.
Source: chasm/lib/scheduler/internal/overlap.go:5 and invoker_tasks.go:560.

D3: decrement remaining action budget once at committed admission. Increment
started count once on acknowledgment OR valid early completion. Preserve the
admitted occurrence, action and operation identities through every retry.
Freeze the complete request (including definition and carryover) at admission;
an update affects future admissions only. Caller-owned buffers must be cloned.
This is intentional divergence DIV1: existing invoker_tasks.go:671,692-742
rebuilds retry requests using current definition/carryover. Completion is
monotonic; late acknowledgments, failures and duplicates cannot regress it.
Sources: scheduler.go:450-474, invoker_tasks.go:265, completion.go:109-155,
scheduler_nexus_completion_test.go:566-601.

D4: external deduplication is conditional, not everlasting. M1 assumes the fake
remote service retains accepted identity/input during the 60-second retry
horizon. After expiry stop issuing starts and retain ambiguous intent; do not
refund admission or call it known failure. Keep all terminal records in the
bounded experiment. Production retention, workflow-ID reuse and migration
protocols remain unresolved and unsupported. Sources:
service/history/api/startworkflow/api.go:118-120,464-483.

D12: trace version 1 separates input events from observations. Inputs are
advance_time, occurrence_due, update_definition, deliver_start, external_accept,
rpc_result, complete, commit_fail, crash, restart. Each record carries relevant
incarnation, occurrence, action, operation, chain, logical task and attempt IDs.
Initial state, definition revision, integer execution time in nanoseconds and
explicit synthetic payloads are recorded. Result presence is a separate boolean
from a null-valued payload. Outputs carry commit sequence, projection, obligations,
issued request and acceptance observations. Read observations occur after commit;
external acceptance may precede local acknowledgment and completion may precede
that acknowledgment. Unknown versions/events must fail, not skip. Formal schema
and executable adapters are WI-001 outputs; this vocabulary is the M0 agreement.

D13: one predetermined UTC occurrence with equal nominal/actual instant and zero
jitter; occurrence eligibility is cursor < nominal <= now. No general calendar,
DST, batching or Lean proof claim follows. Cursor advances only with a committed
disposition. SC2 overflow, SC3 sparse computation and SC5-7 general semantics
remain deferred until the occurrence core expands.

D14: prototype per-schedule limits: enumeration/admission/scan batch 1,
pending actions 1, live operations 1, backfills 0 (unsupported), terminal records
1 (single-occurrence test), executor concurrency 1, retry horizon 60 seconds,
retry backoff 1 second. Reject unsupported ingress; keep ambiguous intent at
expiry and never readmit it. Reducer owns admission limits; adapter owns executor
concurrency. Large unrelated payload loading tests may add inert fixture nodes.

D11: Go effective version 1.27.3; FizzBee v0.5.4 tar SHA256
2091d0a1bf5acc41015f8644cbaf5ec853ed0c88283a7f94d615135a6c8a441c.
Routine model: one schedule/occurrence/operation, two revisions, finite phases,
100 actions then 200 to check stabilization; explicit fair recovery assumptions.
Routine properties: 100 scenarios, 32 events, seed 1, recovery suffix 16 events.
Exploration: 10000 scenarios, 128 events, recorded seed, recovery suffix 32.
Serialize expensive checks: GOMAXPROCS=4, package parallelism 2, test parallelism 4,
Go timeout 10m, race timeout 15m; model timeout 5m per case. Timeout is failure,
not exhaustive completion. Rapid pin is still a WI-001 dependency acquisition.
Lean/Lake unavailable locally; completed proofs are deferred to M3, not waived.

## Trusted and unmodeled boundaries

Atomic database state/task publication, eventual fair task delivery, remote
retention during the selected horizon and production cache eviction after failed
commits remain trusted. Real CHASM tree tests must separately establish serializer,
task reconstruction and adapter commit discipline. No cross-cluster failover,
production routing, database crash recovery or complete API compatibility claim.
Selective loading means avoided value preparation/deserialization, not fewer DB
reads: mutable state fetches all nodes and Map hydrates key wrappers.

## Review resolutions (WI-000)

D4 retry deadline is committed admission execution time + 60 seconds. No new
issuance at now >= deadline. Expiry never discards identity or a valid late result:
a later acknowledgment establishes started count once; a later valid completion
establishes completion, also counting once. An in-flight cancellation does not
prove non-acceptance. Clock rollback cannot extend the stored deadline.

I1 is the CHASM execution RunID, explicitly supplied in the local fixture. I2 is
`automatic/<nominal Unix nanoseconds>` within I1; I3 is `I1/I2/action`; I4 is
`I3/start`, also the external request ID. I5 is the monotonically assigned fixture
physical delivery number, never part of I4. I6 is the accepted workflow chain's
first run ID. The frozen workflow ID remains part of the immutable request.
Callbacks/results must match I1 and I4. The first valid success or completion
binds I6; conflicting nonempty chain IDs are rejected without mutation. The
injected completion endpoint is trusted to authenticate the callback source;
production token validation is outside this local endpoint. Old-incarnation
messages and unknown operations are ignored without accounting changes.

Model fairness: under eventual environment recovery, continuously enabled
issuance/delivery, acknowledgment and completion transitions receive weak
fairness, except when retry horizon has expired. Faults may happen finitely in
recovery traces. An accepted operation may wait externally before completion;
its terminal state is not guaranteed unless completion delivery is assumed fair.
Expiry to retained ambiguity is a permitted disposition, not successful completion.
A bounded recovery suffix checks finite progress only. Any infinite-execution
liveness assertion must name its fairness settings in the model evidence.


## Accepted implementation refinements

D19/D20/D21 are recorded in the central decision ledger and README. Completion
payload updates are outside the M1 subset; replay explicitly rejects present
results. The local issuance limit is one RPC per library instance. Recovery uses
one valid completion for the single operation, replacing the proposed padded
16/32-event suffixes. CHASM generated scenarios contain 12–28 events; reducer
exploration uses 128. No failing assertion was weakened for these clarifications.
A failed RPC schedules from max(current time, previous retry time) to preserve
physical successor dispatch even when time rolls back in flight.


## First-slice compatibility matrix

| Input/observation | M1 expectation | Coverage / deferred boundary |
|---|---|---|
| Automatic due | Strict cursor advancement with committed disposition; SKIP; debit once | Reducer properties and CHASM admission/publication tests. General calendar deferred. |
| Definition update | Higher revision changes future admission settings; frozen request unchanged | Lost-ack/update/retry tests; public conflict-token API deferred. |
| Start acknowledgment | Bind matching chain and count once, including after expiry | Real task-bound-reference regression and reducer duplicate tests. |
| Ambiguous RPC error | Keep immutable operation; bounded retry and physical successor | Retry/cancellation/rollback tests; public error translation deferred. |
| Early/duplicate completion | Absent result, matching identity; monotonic completion and count once | Replay/refinement, reducer and CHASM tests; payload carryover changes deferred. |
| Unknown/old-incarnation result | No state/count change; conflicting bound chain rejected | Reducer identity tests; production token auth and delete/recreate API deferred. |
| Retry horizon | No new starts at/after admission+60s; retain ambiguity; valid late evidence accepted | Boundary and real executor tests; remote retention remains explicit assumption. |
| Metadata observation | Root/selected operation metadata only; no request/diagnostic payload decoding | Poisoned three-payload CHASM test; no visibility/DB-fetch reduction claim. |
| Public Schedule commands, List/Count and visibility ordering | Not routed to scheduler2 | Existing scheduler preserved; public API suite belongs M3. |
