# CHASM scheduler redesign and verification plan

## 1. Document control

| Field | Value |
| --- | --- |
| Status | Authorized local execution through M1 |
| Updated | 2026-11-10 |
| Current milestone | M1: lost-acknowledgment vertical slice |
| Implementation status | WI-000 accepted; WI-001 ready |
| Authorization | User authorized local execution, branches/worktrees and task commits through M1. No pushes, production routing, rollout or migration. |
| Provisional component directory | `chasm/lib/scheduler2` |
| Execution environment | Local development, model checking, simulation, and functional testing only |

### Update conventions

- Preserve stable IDs when editing; add IDs rather than renumbering existing records.
- Use statuses `pending`, `active`, `blocked`, `passed`, and `unsupported` for work and gates. Use `open`, `decided`, and `superseded` for decisions.
- Mark a gate `passed` only with evidence IDs. Record blocked dependencies explicitly.
- Keep the current design in the main sections. Record changes to decisions in the decision ledger.
- Record exact commands, revisions, assumptions, and artifact paths in the evidence ledger. Do not substitute a seed for a replayable trace.
- Update the current milestone, relevant checklist, decisions, and evidence together after each work session.
- This document is the index. Link larger models, traces, inventories, and reports from it as they are created; do not imply that planned artifacts already exist.
- Use the ADLC work-item statuses in section 12 for individual changes; phase and gate statuses retain the conventions above.

## 2. Goal and boundaries

Build a new CHASM scheduler alongside `chasm/lib/scheduler`, retaining the existing implementation for local comparison. Preserve the agreed public Schedule API contract while simplifying internal coordination.

Use one coordinator per schedule, explicit durable state, and a pure Go decision layer. Retain CHASM persistence, transactions, timer delivery, visibility, and side-effect execution. Independent schedules remain independent; external operations can execute concurrently within bounded limits.

Do not use remote execution, cloud test infrastructure, or production shadow traffic. Do not add a production third-party dependency. Do not change production routing. Migration and sentinel ownership require a separate verified protocol and are outside the initial prototype.

Confirm registration and generation conventions before adding independent component, persisted message, and task types. Never reuse existing identities for incompatible state.

## 3. Workspace and experiment manifest

| Field | Recorded value | Verification |
| --- | --- | --- |
| Requested worktree name | `chasm-scheduler-redesign` | Original request |
| Current worktree | `/Users/alex/repos/wt/sch-redesign/temporal` | `git worktree list`, 2026-11-10 |
| Current branch | `sch-redesign` | `git worktree list`, 2026-11-10 |
| Current HEAD at plan creation | `85b80a1836dbc73741cb52586297dc35e6c937ca` | `git rev-parse HEAD` |
| Initial comparison baseline | `9ab3a9f770da20df7d94bcc0030f28eec7b0b947` | `git rev-parse '9ab3a9f77^{commit}'` |
| Original checkout | `/Users/alex/repos/temporal`, branch `main` | `git worktree list`, 2026-11-10 |
| Current worktree status before this file | Clean | `git status --short` |
| Worktree creation base and provenance | Pending investigation | Current HEAD does not establish creation base |
| Original checkout modifications | Previously reported scheduler verification files and `spec_processor.go` modification; current contents unverified | Must inspect separately |

Earlier worktree creation attempts were reported blocked. The current worktree is now verified to exist, with a different name. This documentation task did not create or rename it. Do not recreate it merely to satisfy the old blocked step; resolve the naming and provenance discrepancy before implementation.

- [x] W1: Inspect original checkout status, diffs, and provenance without modifying it.
- [x] W2: Record worktree creation provenance and explain its relation to the comparison baseline.
- [x] W3: Inventory later fixes available on other refs and identify which each baseline includes.
- [x] W4: Explicitly record any verification material selected for copying, its source revision or diff, and why it is needed. Never silently treat uncommitted material as baseline behavior.

## 4. Architecture and state access

### A1. Transition boundary

Conceptually:

```text
Step(event-specific immutable state, event, time, limits) -> delta, effects
```

The adapter prepares only the state needed for the event, invokes the decision layer, and atomically applies deltas and records tasks. This boundary does not require copying or deserializing the entire schedule tree. External executors issue RPCs only for committed operations and return results through the same transition mechanism. No RPC or hidden wall-clock read belongs in the decision layer.

Use the correct CHASM execution clock, including virtual time behavior. Model rollback and deadline comparisons explicitly. Do not assume local generation checks fence already-issued external effects.

### A2. Partitioned persisted state

| Partition | Contents | Intended readers |
| --- | --- | --- |
| Root control | Revision, incarnation, lifecycle, cursor, next candidate, timer token/deadline, bounded summaries | Most transitions |
| Definition | Spec, policies, workflow settings; split large payload-bearing data where useful | Enumeration, admission, API reads, request preparation |
| Pending queue | Ordered occurrence metadata and overlap wait state | Admission and enumeration |
| Backfill map | Request identity, range, cursor, admission order | Selected batches and API operations |
| Operation map | Immutable request identity/input reference, phase, attempts, retry deadline | Selected executors and results |
| Tracked execution map | Action identity, workflow chain identity, completion phase | Overlap and callbacks |
| Completion carryover | Success payload and failure state | Relevant completions and request preparation |
| Recent history | Bounded API history | Describe and visibility projection |
| Diagnostics | Bounded human-readable events | Diagnostics |

Keep CHASM Field and Map children as separate nodes. Inspect lazy preparation/deserialization and mutable-state loading before claiming fewer persistence bytes are fetched. Measure node preparation/deserialization, modified nodes, serialized bytes, and persistence reads separately.

Use keyed access for results and callbacks and bounded batches for scans. Every summary or index must have a defined owner, invariant, and update/recovery rule. Measure an index versus a bounded scan for minimum retry deadlines before choosing. Never introduce an unbounded reconciliation scan on every transition.

History truncation must not affect admission, live identity, overlap, or retention anchors. Visibility providers must not load every operation or payload per commit. Read-only paths must not mutate cached live state. Metadata operations must avoid large carryover payloads and diagnostics; keyed callbacks must avoid unrelated operation payloads.

### A3. Identity and immutable requests

Define distinct identities and their derivation, scope, persistence, and retention:

| ID | Identity | Required distinction |
| --- | --- | --- |
| I1 | Schedule incarnation | Distinguishes deletion and recreation using the same public schedule ID |
| I2 | Occurrence | Distinguishes automatic occurrences and manual/backfill request occurrences |
| I3 | Admitted action | Owns one-time admission and budget accounting |
| I4 | External operation | Stable across retries; identifies an immutable external request |
| I5 | Physical attempt | Delivery/attempt metadata; never substitutes for operation lifecycle |
| I6 | Workflow chain | Tracks continue-as-new/reset and callback routing under the agreed contract |

Specify when pending actions adopt updated definitions and when request inputs become immutable. Stable retry identity must preserve the immutable request, including its configuration and carryover snapshot. References to snapshots must remain valid until no live operation needs them.

Define behavior when a retry outlives external deduplication guarantees. Local record retention cannot extend an external service's deduplication horizon. Test stale callbacks and results after delete/recreate.

## 5. Compatibility and dependency decisions

Phase 0 proceeds incrementally. Resolve the contract needed for the first vertical slice first; unrelated open decisions block only their dependent work. Neither V1 nor V2 is the sole oracle. Known defects are named expected divergences, never silently copied into the contract.

Maintain a source-linked defect inventory with: defect ID, minimal reproduction, intended semantics, affected/fixed revisions, baseline inclusion, and regression test. Maintain a compatibility table for commands, responses, workflow requests, errors, counts, ordering, and visibility consistency.

| Contract ID | Required coverage | Status |
| --- | --- | --- |
| C1 | Calendar, timezone, DST gaps/folds, jitter, exclusions, strict advancement, batch equivalence | open |
| C2 | Update boundaries, catchup, clock rollback, pending configuration adoption | open |
| C3 | Six overlap modes, manual overrides, ALLOW_ALL tracking and results | open |
| C4 | Pause/unpause, pause-on-failure, action budgets and accounting point | open |
| C5 | Deduplication, conflict tokens, manual trigger identity, external deduplication horizons | open |
| C6 | Result absence versus explicit null, failure carryover, immutable request snapshots | open |
| C7 | Workflow-ID reuse, early/duplicate callbacks, continue-as-new/reset | open |
| C8 | Retention, activity anchor, terminal records, delete/recreate, in-flight update/delete guarantees | open |
| C9 | Sentinels and migration ownership | open; initial migration unsupported |
| C10 | API errors, response fields, counts, ordering, eventual visibility observation points | open |
| C11 | Capacity, overload dispositions, manual/backfill fairness and recovery | open |

For C11, define limits and reject/defer/drop behavior independently for automatic occurrences, manual triggers, backfills, and API retries. Specify how blocked ingress resumes. State whether finite backfills must drain under sustained manual traffic and what scheduling policy guarantees it.

### Early CHASM guarantee investigation

Perform this read-only investigation before freezing the model or schema. Record source links and distinguish verified guarantees from trusted assumptions.

| Assumption ID | Investigate | Status |
| --- | --- | --- |
| H1 | Atomic state/task recording, transaction retries, cached mutation rollback | pending |
| H2 | Logical task lifecycle, physical redelivery, validation, reconstruction after reload | pending |
| H3 | Callback routing/token validation and schedule incarnation boundaries | pending |
| H4 | Failover, replication, stale executor behavior and ownership | pending |
| H5 | Field/Map loading, mutable-state persistence reads, visibility evaluation | pending |
| H6 | CHASM virtual time and task timestamp conversions | pending |
| H7 | Registration identities, protobuf generation, annotated source generation, local component selection | pending |

## 6. Milestones and execution sequence

Phases describe coverage areas, not a requirement to finish all modeling before exercising CHASM. Build a narrow end-to-end slice through Phases 0–4, then expand policy and API coverage incrementally.

| Milestone | Deliverable | Status | Gate evidence |
| --- | --- | --- | --- |
| M0 | Workspace provenance, first-slice contract, early CHASM guarantees | passed | E4–E8 |
| M1 | Independently registered local component surviving accepted-start/lost-ack/restart/early-completion traces with selective loading | active | WI-001; no passing evidence yet |
| M2 | Complete policy, update, backfill, and callback behavior with model mappings | pending | — |
| M3 | Differential API and shared functional coverage with no unexplained differences | pending | — |
| M4 | Load/state-growth report and explicit migration support boundary | pending | — |

### Gate ownership and proof scope

The implementing agent maintains each gate's evidence; a separate review pass checks contract coverage, assumptions, and unresolved failures before the gate is marked passed. This is a review responsibility, not authorization to spawn agents or use remote services.

| Gate | Required evidence | Deferred work |
| --- | --- | --- |
| M0 | First-slice semantics, trace schema, concrete resource/exploration configuration, and relevant CHASM assumptions | Unrelated policy decisions |
| M1 | First-slice model checks, reducer properties, actual CHASM restart/atomicity/loading tests, harness self-tests, applicable race/cancellation tests | Completed Lean proofs and full calendar coverage |
| M2 | Full implemented-policy properties/mappings; reviewed Lean statements and executable enumeration specification | Final Lean proofs and complete differential/API coverage |
| M3 | Lean resumable-enumeration and batching proofs, Go correspondence evidence, full spec-processing checks, explained differential/API results | Large load report |
| M4 | Reproducible load/state-growth evidence, retained assumptions, explicit migration boundary | Production rollout |

Start Lean definitions and theorem statements during occurrence-core design. Their completion gates M3, not the first CHASM vertical slice. A failed or incomplete required check remains visible; changing a gate's scope requires a decision-ledger entry.

### Phase 0: Compatibility contract and defect inventory

- [ ] P0.1: Complete W1–W4 and H1–H7 as needed for M1.
- [ ] P0.2: Create the historical-fix inventory and compatibility table with source links.
- [ ] P0.3: Resolve first-slice admission, immutable request, deduplication, callback, and restart semantics.
- [ ] P0.4: Identify remaining decisions and their dependent phases/tests.
- [ ] P0.5: Resolve first-slice spec-processing semantics, version the shared trace contract, and record concrete resource and exploration settings from section 7.

Exit: all M1 dependencies are resolved or explicitly block M1. Expand the full contract before implementing dependent behavior.

### Phase 1: Independent abstract model and schema

Use FizzBee for the scheduler protocol model. Initially abstract calendar evaluation as an ordered stream of nominal and actual times. That abstraction does not verify calendar evaluation; cover it separately under section 7's spec-processing track. Verify and pin local tooling before selecting commands. Do not add a production dependency.

Model durable versus tentative state; logical tasks versus physical deliveries; committed intent versus external acceptance versus local acknowledgment; early and duplicate completions; timer tokens; bounded capacity; overload and recovery; and explicit action phases independent of attempt counts.

| ID | Obligation |
| --- | --- |
| S1 | Cursor advancement accounts for every passed occurrence under policy |
| S2 | Retried admitted actions preserve stable external-operation identity and immutable request input |
| S3 | Duplicate/stale results cannot regress phase or repeat accounting |
| S4 | Admission obeys overlap, capacity, and action-budget semantics |
| S5 | Durable internally pending work has a task or explicit recovery path |
| S6 | Failed commits expose neither new state nor new external obligations |
| S7 | History retention cannot regress the activity anchor |
| S8 | Results and callbacks from an old incarnation cannot mutate a recreated schedule |
| L1 | Eligible work resolves under explicit fair delivery and eventual recovery assumptions |
| L2 | Finite backfills drain under stated capacity and admission fairness assumptions, including the sustained-ingress policy |
| L3 | Eligible idle schedules close after their deadline stabilizes |

Define permitted external waits separately from stuck states. Check liveness over infinite modeled executions with explicit fairness; task existence and finite recovery traces alone are not liveness proofs. Report bounds and explored state counts.

- [ ] P1.1: Independently specify transitions, identities, capacity, and invariants.
- [ ] P1.2: Check the first-slice small-state model.
- [ ] P1.3: Detect injected lost-wakeup, cursor-advance, duplicate-accounting, retry-identity, and retry-input defects.
- [ ] P1.4: Expand the model alongside policy implementation.
- [ ] P1.5: Draft Lean enumeration definitions and theorem statements against the spec-processing contract; review assumptions before implementation correspondence work.

Exit: bounded exploration passes, targeted mutations fail, and assumptions and unmodeled boundaries are recorded.

### Phase 2: Pure Go decision engine

Provisional responsibilities; finalize names against project conventions:

| Responsibility | Behavior |
| --- | --- |
| Step | Deterministic event dispatch and delta/effect generation |
| EnumerateOccurrences | Bounded enumeration retaining jittered candidates; automatic drops versus manual deferral |
| PlanAdmission | Overlap, catchup, capacity, fairness, and budget rules |
| ApplyOperationResult | Monotonic results merged by operation identity |
| ApplyCompletion | Early callbacks and redelivery |
| Reconcile | Required operations and next timer using affected state and bounded summaries/indexes |
| Project | Committed state mapped to independently specified abstract state |

Reuse calendar evaluation through a narrow interface initially. Keep occurrence progress independent of timer validity. Consume/replace timer tokens atomically with successor obligations. Completion must not depend on waking a separate generator. Preserve one-time budget accounting and specify ALLOW_ALL separately.

- [ ] P2.1: Implement first-slice occurrence/admission, start, result, and completion transitions.
- [ ] P2.2: Test accepted start → lost acknowledgment → update → retry with identical operation identity and request input.
- [ ] P2.3: Add all historical reproductions, overlap policies, update races, action exhaustion, early completion, stale failure after success, duplicate acknowledgment, full-buffer backfill, manual identity, clock rollback, retention truncation, delete/recreate, and legitimate quiescence.
- [ ] P2.4: Map every event kind to model transitions, including specifically named internal stuttering steps.
- [ ] P2.5: Use Rapid to generate structured specs and reducer event sequences, checking section 7 properties and S1–S8 after applicable transitions. Save minimized failures as deterministic regression traces.

Exit: deterministic tests pass and mappings cover implemented event kinds. Unbounded internal stuttering cannot satisfy progress obligations.

### Phase 3: CHASM component, adapter, and early instrumentation

Register independent component and task types. Keep existing scheduler implementation unchanged; select locally through explicit test wiring.

Implement partitioned state, event-specific loading, atomic deltas/tasks, stable request IDs, immutable retry inputs, bounded executors, timer generation checks, duplicate/out-of-order results, and task reconstruction after reload. Validate adapter obligations separately from scheduler rules.

Malformed public input returns established service errors without partial mutation. Transient failures preserve intent with bounded backoff. Permanent failures have explicit policy dispositions. Handle all errors; follow project logging and invariant-reporting conventions. Preserve callback token validation and keep sensitive payloads out of traces/logs.

- [ ] P3.1: Implement and regenerate first-slice schema/registration/adapter sources.
- [ ] P3.2: Test round-trip, task validation, transaction atomicity, cache behavior, and fresh-object reload through actual CHASM paths.
- [ ] P3.3: Instrument preparation/deserialization, modified nodes, serialized bytes, and persistence reads from the first slice.
- [ ] P3.4: Demonstrate metadata reads avoid large carryover/diagnostic payloads and keyed callbacks avoid unrelated operations.
- [ ] P3.5: Expand the adapter only after relevant contract/model decisions are ready.
- [ ] P3.6: Run applicable concurrent adapter/executor tests with the Go race detector; verify cancellation, timeout, and immutable snapshot ownership behavior.

Exit: actual CHASM paths meet atomicity, restart, and selective-loading obligations. Required generation and focused checks pass.

### Phase 4: Deterministic simulation and refinement

Run production reducer and adapter code locally. Control time, API commands, physical deliveries, workflow acceptance, RPC outcomes, callbacks, persistence failures, crashes, and restart. Restart from committed snapshots using fresh objects, never retained component pointers. Inject faults around commits and external acceptance/acknowledgment boundaries, not inside an indivisible durable commit.

The abstract transition relation must be independently specified rather than derived from reducer output. Projection exposes identities, accounting, and outstanding obligations as well as public observations. After each commit, compare projected state and effects against that relation. Permit only named stuttering transitions. Use fair recovery suffixes for bounded progress evidence without claiming an infinite-execution liveness proof.

- [ ] P4.1: Exercise one automatic occurrence and one explicitly selected overlap policy through admission, durable intent, acceptance, lost acknowledgment, retry, early completion, and restart.
- [ ] P4.2: Shrink failures into replayable event traces.
- [ ] P4.3: Save seed, initial state, tool/binary versions, configuration, source SHA, event ordering, and invariant expectations.
- [ ] P4.4: Replay historical traces, detect deliberate defects, and cover every production event path as it is added.
- [ ] P4.5: Drive the deterministic harness with Rapid-generated commands, delivery orders, and fault choices. Check invariants after each commit and append controlled recovery suffixes for bounded progress checks.
- [ ] P4.6: Pass harness self-tests for replay, fresh-object restart, failure boundaries, and dependency-aware shrinking before counting simulation runs as evidence.
- [ ] P4.7: Implement the local verification runner and named evidence-producing profiles defined in section 7.

M1 exit: P0–P4 first-slice gates pass; independently registered local component preserves immutable requests across ambiguous acceptance and restart and demonstrates selective node loading.

Phase exit: complete refinement mapping, mutation evidence, exercised bounds, and explicit unmodeled boundaries. Deterministic simulation is evidence, not a blanket correctness proof.

### Phase 5: Differential and API validation

Drive old and new implementations with equivalent commands and external outcomes. Compare normalized API observations and workflow requests at defined observation points. Normalize only documented nondeterminism; preserve semantic differences, eventual visibility constraints, and permissible concurrent ordering. Record which historical fixes each baseline contains.

- [ ] P5.1: Run the shared Schedule functional suite against scheduler2 through the public frontend API using the backend-selection design below. Classify every existing Schedule functional test and record its outcome.
- [ ] P5.2: Test calendar jitter, DST gaps/folds, exclusions, strict advancement, bounded computation, and batch partition equivalence separately.
- [ ] P5.3: Give every intentional divergence a named expectation and reproduction.
- [ ] P5.4: Complete the Lean resumable-enumeration theorem and Go correspondence checks defined in section 7; report theorem assumptions and the unproved implementation boundary explicitly.
- [ ] P5.5: Replace boolean backend selection with an explicit test backend descriptor and add a scheduler2 shared-suite entry point; preserve V1 and existing CHASM coverage.
- [ ] P5.6: Implement local test wiring for all Schedule API paths, including typed List/Count queries, setup, and cleanup. Prove component identity and detect unintended fallback.
- [ ] P5.7: Promote applicable CHASM-only public-contract tests into shared coverage and inventory implementation-specific tests and named divergences.

Exit: no unexplained contract differences. Every existing Schedule functional test is classified as shared and passing, implementation-specific, intentionally divergent with a contract decision, or explicitly blocked with a reason and dependency. Required shared coverage cannot pass M3 while blocked. Scheduler2 runs have evidence that the independent scheduler2 component executed; all remaining coverage gaps and intentional differences are recorded.

### Functional test reuse and backend selection

Current source inspection establishes these starting points; revalidate them before implementation:

- [Shared suite](tests/schedule_test.go) contains `TestScheduleV1SharedSuite` and `TestScheduleCHASMSharedSuite`, which run `ScheduleSuite` with a boolean backend parameter.
- `contextFactoryFor` and `chasmContextFactory` select the existing CHASM path using the experiment header. `scheduleCommonOpts` derives V1 worker setup from test naming, and `newScheduleEnv` cleanup uses the existing CHASM context.
- [Frontend workflow handler](service/frontend/workflow_handler.go) injects the existing scheduler service client for several commands but uses the existing component type directly for List/Count. A new context/header or client replacement alone is insufficient.
- CHASM-specific functional coverage includes public-contract assertions as well as tests tied to existing component protobufs, tasks, and configuration.

Define a test backend descriptor for V1, existing CHASM, and scheduler2. It supplies request-context creation, local cluster/service wiring, backend-specific configuration, and cleanup. Replace the boolean parameter with this descriptor without duplicating the shared test bodies. Derive worker requirements from the descriptor rather than test names. Keep public assertions common where the agreed contract is common.

Add a scheduler2 entry point that runs the same public frontend requests for Create, Describe, Update, Patch, Delete, List, Count, and matching-time queries, as well as workflow input/result, overlap, backfill, callback, and visibility scenarios. Configure selection at local test-cluster construction through the narrowest suitable repository test seam. Cover both injected scheduler-client calls and component-typed List/Count queries. If a seam must be introduced, preserve the existing default behavior and keep scheduler2 activation confined to local test wiring; do not add production rollout selection as part of this work.

Add a test-only assertion of the created component's independent scheduler2 registration identity and evidence of relevant dispatch paths. Fail unexpected fallback to V1 or the existing CHASM scheduler. Identity verification must not depend solely on the chosen header or successful API response. Verify List/Count query the intended component type. Isolate backend runs so shared-cluster configuration or namespace state cannot cause one backend's test to exercise another.

Route cleanup using the same descriptor as creation, including list pagination and deletion. Follow existing conventions for bounded cleanup and error reporting. Test cleanup sufficiently to ensure leaked scheduler2 schedules cannot contaminate later runs. This test setup must not require migration or sentinel handoff to be implemented.

Create a functional coverage matrix with one row per test/scenario: source symbol, contract IDs, applicable backends, classification, shared assertion or named divergence, exact invocation, result/evidence ID, and blocker if any. Include standalone Schedule tests and related files, not only suite methods.

| Classification | Treatment |
| --- | --- |
| Shared public contract | Run unchanged assertions across applicable backends; required scheduler2 cases must pass |
| CHASM-only public contract | Move reusable scenarios into shared coverage for existing CHASM and scheduler2; include V1 only where its agreed contract applies |
| Implementation-specific | Retain tests for old state/task/layout details against the old scheduler; add separate scheduler2 adapter tests for equivalent obligations |
| Intentional divergence | Reference an explicit contract decision and reproduction; keep each backend's named expectation visible |
| Blocked coverage | Record the missing capability, dependent gate, and next action; never represent a skip as a pass |

Do not weaken assertions merely to make all backends agree or accumulate ad hoc scheduler2 conditionals. Use named contract expectations for genuine differences. Keep real functional execution through frontend, persistence, tasks, callbacks, and visibility complementary to the deterministic crash/RPC harness; passing functional tests does not cover all fault boundaries.

### Phase 6: Load, state growth, and migration boundary

Measure metadata reads, due bursts, large payloads/backfills, long-lived tracked executions, callback storms, and sustained overload. Define baseline workload, offered arrival rate, payload sizes, schedule counts, and concurrency before defining a 10× run. Report latency, backlog, throughput, and resource use at both rates.

Bound work per transaction and executor concurrency. Measure contention, task/node counts, preparation/deserialization, serialization/write bytes, persistence reads, state growth, and restart cost. Compare equivalent traces with the old scheduler. Prevent unbounded request snapshots and terminal records.

Operation records improve recovery but consume state. Partitioning reduces unrelated decoding/writes but introduces indexing complexity. One coordinator simplifies ordering while retaining per-schedule serialization.

- [ ] P6.1: Produce reproducible workload definitions and baseline/new comparisons.
- [ ] P6.2: Verify capacity recovery and record retention/deduplication tradeoffs.
- [ ] P6.3: Report measured costs, retained benefits, proof boundaries, and unresolved risks.
- [ ] P6.4: Leave migration explicitly unsupported unless separately authorized and verified.

Any future migration must model sentinel reservation and ownership handoff separately, prevent destination execution of source-owned work, test interruption at every handoff boundary, and route callbacks correctly. No production rollout belongs to this prototype.

## 7. Local verification rules

### Prescribed toolchain

| Tool ID | Tool | Required role | Boundary |
| --- | --- | --- | --- |
| T1 | FizzBee | Bounded scheduler protocol exploration, fault counterexamples, and explicit fairness/liveness analysis | Abstract state model; does not verify actual calendar code |
| T2 | Lean 4 with Lake | Selected cursor, resumable enumeration, and batching proofs; executable specification | Proves Lean definitions under recorded assumptions, not automatically the Go implementation |
| T3 | Rapid (`pgregory.net/rapid`) | Primary structured property testing and stateful scenario generation/shrinking | Test-only dependency; generated sampling is not exhaustive verification |
| T4 | Go tests and independent reference enumerator | Bounded exhaustive production-code checks, DST examples, regressions, CHASM and shared functional tests | Record finite input/time bounds and timezone data |
| T5 | Custom deterministic Go harness | Execute actual reducer/adapter code under controlled delivery, acceptance, failures, and restart | Rapid supplies scenarios; the harness implements fault and persistence semantics |
| T6 | Native Go fuzzing | Complementary coverage-guided exploration of parsing and spec processing | Reuse property assertions where practical; does not replace structured stateful tests |
| T7 | Go benchmarks and `pprof` | Latency/allocation benchmarks and CPU/heap investigation, alongside explicit CHASM counters | Profiles do not replace persistence/node instrumentation |
| T8 | Versioned JSON traces and Markdown evidence index | Standalone deterministic replay and human-readable claim/evidence mapping | A seed alone is insufficient |

Keep this toolchain local. Pin versions and record exact invocations after inspecting repository tooling. If a selected tool is unavailable, record the blocker and proposed resolution; do not silently replace it or mark its gate passed. TLA+/Apalache is not an additional required tool; switching protocol model checkers requires a recorded decision.

Rapid-related comments exist in the current scheduler tests and test logger, but the root `go.mod` and `go.sum` had no Rapid entry when inspected for this planning discussion. Inspect the earlier harness's provenance before reusing it. Add Rapid only for tests during authorized implementation; this plan update installs nothing.

### Spec-processing verification track

Keep the occurrence-stream scheduler model separate from calendar implementation verification. Use integer timestamps and an explicit eligibility predicate for initial Lean definitions. Record supported resolution, range boundaries, overflow assumptions, and ordering semantics before stating theorems.

Resolve these semantics in C1/C2 before their dependent proofs or properties are accepted:

| Semantic ID | Required decision |
| --- | --- |
| SC1 | Inclusive/exclusive start, end, cursor, and backfill boundaries; supported timestamp resolution |
| SC2 | Representable timestamp range, overflow handling, and out-of-range errors |
| SC3 | Empty, impossible, exhausted, and temporarily computation-limited results as distinct outcomes |
| SC4 | Whether advancement and ordering refer to nominal time, actual jittered time, or a composite occurrence key |
| SC5 | Jitter bounds, deterministic inputs, collisions, tie-breaking, and candidates crossing batch/window boundaries |
| SC6 | Updates to a pending candidate: retained identity, eligibility, configuration, and actual time |
| SC7 | DST gaps/folds, timezone version, exclusion precedence, and duplicate calendar matches |

An open semantic decision blocks the corresponding test oracle/theorem acceptance; it does not justify choosing whatever behavior the existing implementation exhibits.

| Property ID | Property | Verification |
| --- | --- | --- |
| SP1 | Next occurrence is eligible, strictly advances, and skips no earlier eligible occurrence | Independent bounded Go oracle and Rapid; Lean theorem only for explicitly modeled eligibility/next-occurrence logic |
| SP2 | Stopping at a computation budget and resuming preserves completeness and uniqueness | First Lean theorem, bounded exhaustive Go tests, and Rapid |
| SP3 | Splitting an interval into batches preserves the occurrence sequence | Lean batching theorem and Rapid comparison across generated partitions |
| SP4 | Jitter is stable across retries/partitions and pending candidates survive cursor advancement | Lean candidate-state reasoning and Rapid |
| SP5 | Definition updates affect precisely the contract-defined occurrences | FizzBee transition model and Rapid event sequences |
| SP6 | Sparse/impossible specs yield within the per-step computation budget without losing resumable progress | Instrumented Go tests and Rapid; distinguish yielding from eventual enumeration completion |
| SP7 | DST gaps/folds, exclusions, and timezone boundaries match intended eligibility semantics | Pinned-timezone Go examples, bounded oracle, and boundary-biased Rapid generators |

The first Lean deliverable is a machine-checked theorem that resumable enumeration preserves completeness and uniqueness, including a pending jittered candidate, under explicit assumptions about eligibility and jitter. Follow with batch equivalence. Reject unfinished proofs (`sorry`/`admit`) and audit theorem axioms; record any additional trusted assumptions. Full timezone-library verification is outside this initial proof scope.

Use the executable Lean specification as a separate oracle for bounded exhaustive and Rapid comparisons with Go. This correspondence is testing evidence unless an actual refinement proof is supplied. Assuming a correct next-eligible-time function does not prove its implementation or DST behavior. Review theorem statements against the contract so a passing proof cannot substitute for the intended claim.

Implement a deliberately simple bounded Go reference enumerator that scans instants at the supported resolution over short intervals and evaluates eligibility directly. Do not call the production next-occurrence search to compute expected results. Document shared timezone primitives and other common dependencies as trusted boundaries. Pin timezone data/version for reproducible DST tests.

### Property testing and replay requirements

- Generate valid structured specs directly; test malformed inputs separately. Avoid excessive filtering that hides difficult cases.
- Bias generators toward DST transitions, interval boundaries, pending jitter, exhausted budgets, full buffers, updates, duplicate results, and acceptance without acknowledgment.
- Generate reducer commands and harness delivery/fault choices explicitly. All clocks, randomness, and external outcomes must be controlled by recorded inputs.
- Use Rapid's shrinking with dependency-aware scenario construction. Ensure references to operations, callbacks, and snapshots remain meaningful after shrinking; do not shrink away the precondition that makes a failure relevant.
- Check invariants after each committed transition and compare with independently specified expectations. Do not implement the oracle by calling the reducer being tested.
- Use controlled fair recovery suffixes to test bounded progress; randomized successful runs do not prove liveness.
- Persist the minimized explicit event sequence, initial state, source revision/diff, configuration, generator/tool versions, timezone data, expected invariant, and replay command in versioned JSON artifacts. Preserve Rapid's replay information as supplemental evidence.
- Replay saved traces without needing Rapid's generator or the same random seed. Promote failures to focused deterministic regressions.
- Define and record a fixed routine-test profile and a larger local exploration profile, including scenario counts, event limits, time budgets, and seed policy. Pin routine seeds for repeatability and record rotating exploration seeds.
- Mutate cursor advancement, candidate retention, retry identity/input, wakeups, and accounting to demonstrate that properties detect their targeted defects.

### Shared event and trace contract

Define a versioned JSON schema before building separate harnesses. Include schedule incarnation and I2–I6 identities, initial committed state, definition revision, clock domain/unit, explicit time advances, commands, logical task identity, physical deliveries, external acceptance, RPC outcomes, callbacks, commit outcomes, crashes, and restart snapshots. Represent absent versus explicit null results distinctly. Use synthetic payloads; do not persist secrets in traces.

Distinguish input events from observed output records. Output records include commit sequence, abstract projection, logical task obligations, issued requests, acceptance observations, API responses, and visibility observations. Define observation points and permitted partial orders explicitly. Do not encode every effect as a single total order when the contract permits concurrency.

Provide explicit adapters from this trace vocabulary to FizzBee, the reducer, CHASM simulation, and differential runners. The Lean adapter consumes only the enumeration subset. Models retain independently written transition rules and expected results; sharing serialization and event names must not share production decision logic. Reject unsupported schema versions and record explicit conversions. Replay must fail clearly on an unsupported event rather than skip it.

### Oracle disagreement policy

The reviewed public compatibility contract and recorded decisions define intended behavior. Historical implementations, independent Go enumeration, Lean definitions, and models provide evidence; none wins by majority agreement. On disagreement, preserve the smallest trace, identify shared dependencies, inspect source/history and contract wording, and either fix an implementation/oracle or resolve an open contract decision. Block only dependent gates until resolved. Record expected historical divergences by ID rather than normalizing them away. A proved theorem about the wrong specification does not resolve a contract disagreement.

### Verification infrastructure self-tests

| Check ID | Acceptance criterion |
| --- | --- |
| V1 | Replay the same trace in fresh processes/objects and obtain identical normalized commits, requests, and observations; normalization is explicitly documented |
| V2 | After snapshotting, discard all old objects; mutate or poison old in-memory state and verify reload/replay is unaffected |
| V3 | Pre-commit failure exposes no new durable obligations; post-commit crash preserves obligations; accepted-RPC/lost-ack paths retain external acceptance independently of local state |
| V4 | Shrink a known injected failure, validate trace dependencies, and reproduce the same invariant failure through the standalone replay path |
| V5 | Deliberately retain a cached pointer, drop a committed task, alter retry input, or reorder a required event and verify the relevant self-test/property detects it |

Infrastructure self-tests are prerequisites for claiming simulation evidence. Record which injected mutation each check detects and which boundaries remain simulated or trusted.

### Resource limits and exploration profiles

Before M1, commit a local configuration with explicit numeric limits for enumeration work, admission work, scan batches, pending actions, live operations, backfill requests, terminal records, retry horizon/backoff, and executor concurrency. Record units, scope, overload disposition, and the owner of each limit. These are prototype settings, not inferred production defaults. Before M2, cover limit-minus-one, limit, and limit-plus-one cases and demonstrate recovery after saturation.

Implement one local runner using repository conventions, with the following profile names. Finalize its path and exact invocation after tooling inspection; these are planned interfaces, not existing commands. Each profile emits a manifest with source/diff identity, pinned tools, complete settings, exact underlying commands, duration, result, and artifact paths. Missing tools, timeouts, and incomplete exploration are explicit outcomes, never passes.

| Profile | Required settings and output |
| --- | --- |
| `regressions` | Saved JSON traces and focused examples; report every replay outcome |
| `model` | State/domain bounds, fairness settings, completion status, explored counts, and counterexamples |
| `properties` | Fixed routine seeds, scenario counts, event limits, generator distributions, shrinking budgets, and minimized failures |
| `simulation` | Scenario/fault bounds, restart frequency, delivery choices, recovery-suffix length, invariant/refinement outcomes |
| `proofs` | Pinned Lean/Lake versions, theorem list, axiom audit, build result, and Go correspondence results |
| `fuzz` | Targets, corpus identity, per-target duration, execution counts, and saved failures |
| `race` | Relevant Go packages/tests, explicit timeout, and race-detector outcomes |
| `benchmarks` | Workload parameters, offered rates, payloads, warmup, run duration/repetitions, machine details, counters, and profiles |

Define a fast routine configuration and a larger local exploration configuration with actual numbers before running them. Benchmark profiles must define baseline and 10× workload rates and resource ceilings before comparison. Do not equate a model run cut off at a wall-clock limit with completed bounded exploration. Record revisions to budgets and the reason; do not weaken a setting merely to turn a failure into a pass.

### Go implementation failure coverage

Run applicable concurrent adapter/executor tests using `go test -race -tags test_dep` with verified package/test selectors; add `integration` only when appropriate. Test cancellation and timeout before issuance, during in-flight RPCs, after acceptance, and during retry scheduling. Cancellation of a local context must not be interpreted as proof of remote non-acceptance.

Test immutable snapshot ownership by mutating source protobufs/buffers after request preparation and verifying retry inputs remain unchanged. Exercise result/callback arrival against concurrent update/delete and executor shutdown through actual synchronization boundaries. Race-detector success does not establish protocol correctness; retain sequential model and deterministic simulation checks.

### Repository checks

Read applicable `AGENTS.md`, `.github/copilot-instructions.md`, `docs/development/testing.md`, and Makefile/tooling before implementation or test execution. Inspect surrounding code and existing test helpers first.

- All Go tests include `-tags test_dep`; add `integration` only for integration tests.
- Run focused package tests before applicable shared functional coverage.
- After implementation changes, run `make lint-code-fast` and required generation/build checks.
- Regenerate changed protobuf and `//go:generate` sources using verified repository commands.
- Verify local model-checker availability before selecting commands.
- Do not run code checks merely for this planning document.
- Preserve the original checkout and unrelated work. No remote execution or production traffic.

## 8. Decision ledger

| Decision ID | Question | Status | Resolution or next action | Blocks |
| --- | --- | --- | --- | --- |
| D1 | Worktree name and provenance | open | Existing `sch-redesign` is verified; reconcile requested name and creation base | M0 |
| D2 | First-slice overlap policy | open | Select one after inspecting semantics; keep scope to one automatic occurrence | P1.2, P2.1 |
| D3 | Admission/accounting point and immutable request boundary | open | Resolve C3–C6 for first slice | P1.2, P2.1 |
| D4 | External deduplication expiry and terminal retention | open | Establish external guarantees and disposition of ambiguous late retries | M1 |
| D5 | Capacity limits and sustained-ingress fairness | open | Define ingress dispositions and L2 assumptions | M2 |
| D6 | Retry deadline representation | open | Measure bounded scan versus maintained index | Expanded P3 |
| D7 | Local model checker | decided | Use FizzBee; verify/pin local installation and record commands before execution | P1.2 |
| D8 | Migration support | decided | Unsupported in initial local prototype | Future migration only |
| D9 | Property-testing tool | decided | Use test-only Rapid for structured specs, reducer sequences, and harness scenarios; preserve standalone replay artifacts | P2.5, P4.5 |
| D10 | Algorithm proof tool and scope | decided | Lean 4 for resumable enumeration with pending jitter and batching; Go correspondence initially tested, not proved | P5.4 |
| D11 | Tool versions and exploration budgets | open | Inspect local installations and repository conventions; pin versions, routine seeds, counts, and event bounds | Verification execution |
| D12 | Shared event/trace schema and observation points | open | Define versioned JSON vocabulary and independent adapters per section 7 | M0, P4.7 |
| D13 | Precise spec-processing semantics | open | Resolve SC1–SC7 incrementally against C1/C2; block dependent oracles/proofs | Relevant occurrence-core work |
| D14 | Prototype resource limits | open | Record numeric limits, units, owners, overload behavior, and boundary tests | M1 configuration; M2 full coverage |
| D15 | Verification runner path and commands | open | Follow repository tooling; implement the named profiles with evidence manifests | P4.7 |
| D16 | Proof completion gate | decided | Draft during core design; reviewed executable specification by M2; proofs and correspondence required by M3, not M1 | M2, M3 |
| D17 | Functional test backend selection | decided | Explicit V1/existing CHASM/scheduler2 descriptor; shared API assertions, backend-aware setup/cleanup, local-only wiring, and verified independent component identity | P5.5, P5.6 |
| D18 | Local frontend test seam | open | Inspect test-cluster injection hooks; cover scheduler client plus typed List/Count paths without production routing changes | P5.6 |

For new decisions, record alternatives, rationale, source/evidence IDs, dependent work, and any superseded decision ID.

## 9. Evidence ledger

Evidence kinds: `source-inspection`, `abstract-theorem`, `bounded-model-check`, `bounded-exhaustive-test`, `property-test`, `fuzz-test`, `simulation`, `unit-test`, `CHASM-test`, `functional-test`, `benchmark`, and `trusted-assumption`. Keep these distinctions in reports. For property tests record scenario counts and shrinking results; for Lean proofs record theorem names, axioms, assumptions, and the implementation correspondence boundary.

| Evidence ID | Kind | Claim | Command or source | Result / bounds |
| --- | --- | --- | --- | --- |
| E1 | source-inspection | Current and original worktrees exist | `git worktree list`, 2026-11-10 | Paths/branches recorded in section 3; creation provenance not established |
| E2 | source-inspection | Full comparison and current revision identities | `git rev-parse '9ab3a9f77^{commit}'`; `git rev-parse HEAD` | SHAs recorded in section 3 |
| E3 | source-inspection | Current worktree clean before plan creation | `git status --short`, 2026-11-10 | Empty output; original checkout not inspected |

For each future evidence record include:

```text
ID:
Kind:
Claim and contract/invariant IDs:
Source SHA and dirty-diff artifact:
Baseline SHA and included fixes:
Model and production transition IDs:
Exact command and local tool versions:
Configuration, initial state, and explored bounds:
Trace schema version, runner profile, and configuration artifact:
Oracle identities, shared dependencies, and disagreement disposition:
Result, state counts, or measurements:
Mutation and expected detection:
Trace/artifact path and replay command:
Trusted assumptions and unmodeled boundaries:
Comparison outcome and intentional divergence IDs:
```

## 10. Next authorized work

Local execution through M1 is now authorized by the user. Execution order:

1. Re-read this plan and repository instructions; verify live workspace status.
2. Resolve W1–W4 and the first-slice portions of H1–H7 and C1–C11.
3. Build the smallest independent model and fault-injection counterexamples.
4. Implement the narrow reducer and partitioned adapter with early instrumentation.
5. Complete M1 before expanding policies, backfills, and differential API coverage.
6. Execute each bounded change through the ADLC in section 12, updating its work-item record and evidence before selecting the next item.

## 11. Change log

| Date | Change |
| --- | --- |
| 2026-11-10 | Created structured plan from proposed architecture and review. Moved CHASM guarantees earlier, added first vertical slice, explicit identities and immutable retry inputs, modeled capacity/fairness, independent refinement checks, early performance instrumentation, and incremental contract resolution. Recorded verified current worktree state without changing either checkout's existing code. |
| 2026-11-10 | Prescribed FizzBee, Lean 4, Rapid, Go tests/fuzzing, deterministic harness, benchmarks/pprof, and JSON replay artifacts. Added SP1–SP7 spec-processing properties, a concrete Lean deliverable, property-test shrinking/replay rules, and explicit proof-versus-testing boundaries. No tools installed or implementation authorized. |
| 2026-11-10 | Added shared trace/observation contract, SC1–SC7 semantics, oracle disagreement policy, V1–V5 harness checks, resource/exploration settings, evidence-producing runner profiles, Go race/cancellation/aliasing coverage, and explicit milestone proof gates. Numeric settings and exact commands remain recorded M0/implementation deliverables rather than invented verified values. |
| 2026-11-10 | Made existing functional-suite reuse explicit: backend descriptor, scheduler2 suite entry point, local frontend/client and typed visibility wiring, backend-aware setup/cleanup, identity/fallback verification, and complete test classification. Required shared tests gate M3; implementation remains unauthorized by this document. |
| 2026-11-10 | Added the Agentic Development Lifecycle, work-item template and status transitions, evidence-based acceptance rules, and initial prerequisite/lost-acknowledgment work items. Existing phase IDs and milestone gates remain unchanged. |
| 2026-11-10 | Added bounded parallel delegation, file/interface ownership, isolated implementation worktrees, sequential integration, and combined-revision verification. Planning does not itself start agents or create worktrees. |

## 12. Agentic Development Lifecycle

The ADLC is a repeatable, evidence-gated loop for one bounded behavior or obligation. Phases describe what is built; this lifecycle describes how each change is specified, implemented, verified, reviewed, and integrated. It does not grant implementation, commit, production, or remote-execution authorization beyond the user's instructions.

### Lifecycle stages

| Stage ID | Stage | Agent responsibility | Required output |
| --- | --- | --- | --- |
| ALC1 | Select | Choose the smallest unblocked behavior or obligation | Work item with contract/invariant IDs, scope, dependencies, and acceptance criteria |
| ALC2 | Investigate | Inspect source, history, tests, and CHASM guarantees | Source-linked verified facts, affected paths, and unresolved decisions |
| ALC3 | Specify | Define intended behavior, invariants, and failure cases | Contract decision, relevant model/theorem updates, and independent expectations |
| ALC4 | Challenge | Construct counterexamples and regression cases | Failing reproduction or property for defects; targeted mutations or independently discriminating cases for new behavior |
| ALC5 | Implement | Make the smallest coherent code/adapter change | Implementation, required generated sources, and transition mappings |
| ALC6 | Verify | Run applicable local verification profiles | Reproducible evidence with bounds, outcomes, and unresolved failures |
| ALC7 | Review | Assess intended behavior, unintended effects, and evidence quality | Findings resolved or explicitly blocking acceptance |
| ALC8 | Integrate | Record the accepted increment and update project state | Commit reference when authorized, evidence links, gate status, and next work item |

Do not force a failing product test for documentation or investigation-only items. Their challenge and verification steps validate claims, sources, and unresolved assumptions. Record why code/model checks do not apply. For a new feature, a missing implementation or a discriminating counterexample can establish that its checks are meaningful; never introduce a production defect merely to obtain a failing run.

### Work-item states and acceptance

```text
proposed -> ready -> implementing -> verifying -> reviewing -> accepted
                       ^                |           |
                       +----------------+-----------+
                              findings require work
```

`blocked` is available from any nonterminal state. Record the previous state, concrete blocker, dependent decision, and unblock condition; resume the appropriate stage when resolved. `accepted` is terminal for the recorded scope. A later regression becomes a linked new work item rather than silently rewriting prior evidence.

| Transition | Required condition |
| --- | --- |
| proposed → ready | Scope and acceptance criteria defined; necessary authorization present; blocking dependencies resolved; applicable verification profiles selected |
| ready → implementing | Investigation/specification and meaningful challenge cases complete for the bounded scope |
| implementing → verifying | Implementation and required generated changes complete; mappings and tests ready |
| verifying → reviewing | Applicable checks passed; exact evidence recorded; infrastructure failures and unsupported checks disclosed |
| reviewing → accepted | Acceptance criteria satisfied, blocking findings resolved, and evidence/assumptions reviewed |

Investigation-only items use the same record but may mark implementation activities as not applicable. A required profile that failed, timed out, or could not run blocks acceptance unless the work item's scope is explicitly revised through a recorded decision. An unrelated milestone check does not block an otherwise complete item. Item acceptance does not imply milestone completion.

Integration records the exact source state. When committing is authorized, commit the accepted increment and record its SHA. If integration is pending, record that fact and the exact dirty-diff artifact instead of claiming a commit. No automatic push, merge, rollout, or migration follows from acceptance.

### Operating rules

- Keep each loop bounded to a behavior or obligation, such as stable immutable start input after accepted-RPC/lost-acknowledgment.
- Specify expected behavior before changing implementation. Historical behavior is evidence, not automatically the contract.
- Select verification by affected behavior: cursor changes require applicable spec properties and proof checks; adapter changes require actual CHASM, restart, and relevant concurrency checks. Run broader gates at milestone boundaries.
- Never weaken assertions or exploration settings merely to clear failures. Resolve genuine contract changes in the decision ledger with rationale and updated expectations.
- Distinguish product failures from infrastructure failures. A broken replay harness, missing tool, or incomplete model run cannot establish a passing gate.
- Review acceptance criteria and missing paths in addition to the diff. Check common dependencies between oracles, mutation sensitivity, immutable state ownership, and external-effect boundaries.
- Use a separate review pass with repository review instructions; this does not require or authorize additional agents. Review findings link to work items and evidence.
- Keep authorization separate from technical readiness. Local test success grants no permission for production changes or remote execution.
- Update the work item, evidence ledger, affected decisions, and milestone status together. Do not redo already sufficient checks unless new changes or unresolved concerns justify them.

### Work-item template

Create durable records under a repository-appropriate verification location selected during M0 and linked from this plan. The template is a planned record format; no work-item files or runner are implied to exist yet.

```yaml
id: WI-001
title: Preserve start intent across lost acknowledgment
status: proposed
milestone: M1
phase_ids: [P1.2, P2.2, P3.2, P4.1]
contract_ids: [C5, C6]
invariant_ids: [S2, S3, S5, S6]
dependencies: [WI-000]
authorization: pending implementation instruction
scope:
  included: []
  excluded: []
acceptance_criteria: []
source_findings: []
decision_ids: []
challenge_cases: []
verification_profiles: []
evidence_ids: []
review_findings: []
blocker: null
unblock_condition: null
integration_status: pending
commit: null
dirty_diff_artifact: null
next_action: Resolve external acceptance and retry semantics
```

### Initial work queue

| Work item | Scope | Acceptance criteria | Status |
| --- | --- | --- | --- |
| WI-000 | Resolve workspace provenance and the first-slice contract/CHASM assumptions | W1–W4 and relevant H1–H7 have evidence; D2–D4 resolved for the slice; trace vocabulary, resource limits, and exploration settings recorded; unrelated open questions explicitly deferred | proposed |
| WI-001 | Lost-acknowledgment vertical slice after WI-000 | One automatic occurrence under the selected overlap policy survives committed intent, external acceptance, lost acknowledgment, retry, early completion, and fresh-object restart; identity/input and accounting remain correct; model/property/CHASM evidence and selective-loading checks pass | proposed |

WI-001 is the first implementation loop and may be split into linked smaller items after investigation. Splitting must preserve the end-to-end acceptance obligation; partially completed subitems do not satisfy M1. Both items remain proposed until their authorization and readiness conditions are met.

### Parallel work and worktree policy

When the user authorizes execution of this plan, use one integrator and at most three concurrent delegated agents. Parallelize assigned, nonconflicting work; do not create agents solely to occupy available slots. The integrator owns shared contract decisions, interfaces, integration order, the central plan, and milestone acceptance. This planning update alone starts no delegated work.

| Stage | Suitable parallel assignments | Shared decisions and dependencies |
| --- | --- | --- |
| M0 | Historical defects/contracts; CHASM guarantees; functional-test wiring and local tool inventory | Integrator resolves first-slice contract and boundaries from findings |
| M1 | Independent model; reducer; harness scaffolding after event/interface agreement | Persisted identities, event schema, and adapter integration have explicit owners |
| M2–M3 | Spec processing/Lean; functional-suite adaptation; independent policy cases | Shared reducer edits and compatibility changes are coordinated |
| Review | Review a fixed contribution commit against acceptance criteria | Integrator sequences fixes and checks the combined revision |

Every delegation records a work-item ID, owner, input/base revision, owned files or directories, read-only versus write scope, dependencies, acceptance criteria, expected evidence, and completion handoff. Agents report conflicting assumptions to the integrator rather than independently changing shared contracts. Keep schema definitions and generated sources under one owner at a time. Other agents consume agreed versions and request changes through that owner.

Read-only investigations may share the existing checkout. Use separate branches/worktrees for independent concurrent code changes. Keep tightly coupled implementation and frequently changing interfaces with one owner rather than forcing parallel edits. Review a fixed commit or isolated worktree so findings have a stable target.

Before creating an implementation worktree, record the integration base commit and prerequisite changes. Worktrees do not inherit uncommitted changes: explicitly commit authorized prerequisites or record and deliberately transfer a reviewed patch with its provenance. Never silently copy original-checkout modifications into a baseline.

Maintain a worktree registry with work-item ID, owner, absolute path, branch, full base SHA, dependency commits, status, and final disposition. Verify every creation with `git worktree list` and inspect its HEAD/status before reporting it usable. If creation is blocked, record the blocker and use sequential work only when it can safely preserve unrelated changes; do not claim isolation that does not exist.

Each contribution handoff includes commits or an exact diff artifact, changed-file inventory, evidence IDs and commands, unresolved decisions, and known limitations. The integrator reviews contributions, integrates accepted changes sequentially, and runs affected checks on the combined revision. Passing checks on individual branches do not establish compatibility after integration. Resolve conflicts against the contract and rerun checks affected by the resolution.

Give concurrent functional runs separate ports, local persistence resources, namespaces, and artifact directories. Worktrees isolate files, not running services or shared caches. Record machine capacity limits and serialize expensive checks when resource contention would invalidate timing or benchmark evidence. Do not introduce remote execution to increase parallelism.

Do not automatically delete worktrees, branches, unfinished changes, or failure artifacts. Record whether each is retained, integrated, or awaiting disposition. Any cleanup must preserve unrelated work and follow the user's authorization. The integrator alone updates the central plan/evidence index; delegated agents return evidence artifacts and proposed updates to avoid concurrent document conflicts.

## 13. Local execution record (2026-11-10)

[Contract and decisions](docs/development/scheduler2/contract.md),
[source investigation](docs/development/scheduler2/investigation.md),
[WI-000](docs/development/scheduler2/work-items/WI-000.md),
[evidence E4–E8](docs/development/scheduler2/evidence/README.md), and
[worktree registry](docs/development/scheduler2/worktrees.md) are authoritative
execution details supplementing the initial ledgers.

WI-000 accepted after independent review resolved identity, deadline and fairness
clarity. D1–D4 and relevant D11–D14 portions decided for the bounded slice.
General calendar, capacity/fairness, retention and migration decisions remain open
for later milestones. No original-checkout implementation was imported.
WI-001 is ready; required model, property, CHASM and harness evidence remains pending.
