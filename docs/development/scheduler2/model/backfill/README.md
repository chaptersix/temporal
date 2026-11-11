# WI-009 independent backfill arbitration and cursor models

Evidence ID: E-BACKFILL-MODEL-001. Worktree base:
`c96e3526fc004affd95b6b6773f2606836e90f80`. Contract input is the independently
reviewed durable-vacancy correction in `design/backfill-arbitration.md`, including
the parent-confirmed dynamic tracked-policy predicate. No production implementation
is imported or used as a transition oracle.

```sh
python3 docs/development/scheduler2/model/backfill/run.py \
  --fizz /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz \
  --output /private/tmp/scheduler2-backfill-model-leases
```

Pinned FizzBee v0.5.4 explores each specification with 100 and 200 action bounds,
one concurrent atomic action, then six separate mutants. Each case has a 300-second
timeout and process-group cleanup. Baseline failure stops the runner immediately.
Sensitivity requires a specific named safety failure, or for NAIVE_RR the specific
named liveness failure. Parser errors, timeouts and unrelated failures never pass.
Exact commands, hashes and outcomes are in `evidence/results.json`.

## Arbitration fixture and conditional liveness

`arbitration.fizz` uses queue capacity4, one contended operation slot, two finite
backfill requests with one ready candidate each, and sustained manual/automatic
producers. Manual and backfill independently choose BUFFER, ALLOW or SKIP behavior.
Automatic work is persistently queued-policy or direct ALLOW in separate initial
scenarios. Policies remain fixed during a run. Manual/automatic identity bits
alternate for finite state; they represent a fresh head after each handoff and stay
stable while leased. Backfill identities0 and1 never repeat.

QueueTurn chooses all manual/backfill heads, including ALLOW/SKIP, and automatic
queued work. Acquisition reserves one physically vacant queue position for the
concrete head. All insertions require their lease, even when extra positions are
vacant. Successful insertion, direct admission or policy disposition releases it
and advances QueueTurn. No scan/drop rotates either resource turn. A backfill lease
pins its request until disposition/handoff.

OperationTurn includes Drain, leased manual/backfill and automatic direct work.
A tracked policy queues/disposes if a tracked blocker or existing queue is present;
otherwise it needs an operation slot. ALLOW always needs a slot. Need is recomputed
as state changes, while ownership remains stable. A leased ALLOW remains an operation
contender while the slot is full. Drain needs only an operation slot and can create
queue vacancy. Direct automatic ALLOW needs no vacancy lease, but honors OperationTurn.
Tracked completion and ALLOW settlement are distinct capacity-release transitions.
Neither lease acquisition nor a denied attempt advances the operation turn.

Three safety assertions check reserved vacancy, pinned candidate and exactly-once
ordered backfill handoff. Two witnesses check both requests resolving and actual
lease acquisition while operation-full followed by service. The temporal assertion
`always eventually FiniteBackfillsProgress` requires both finite requests to resolve.
Since completion is permanent, this means eventual completion in this finite fixture.

Unlike earlier safety-only models, this specification contains an actual liveness
assertion. Its proof is CONDITIONAL on FizzBee weak fairness for each of eight named
actions: AcquireVacancy; TransferManual; TransferBackfill; TransferAutomaticQueued;
DirectAutomaticAllow; Drain; MatchingCompletion; AllowSettlement. A continuously
enabled action cannot be postponed forever. This assumes eventual successful
publication and eventual external resolution of occupied slots. Infinite retries
that never commit and permanently unresolved workflows are excluded by those
assumptions. Physical queue delivery and a single concrete CPU coordinator must
establish the service assumptions separately.

This is not a universal sixteen-grant bound or wall-clock bound. It is finite-model
liveness for a fixed policy fixture with one contended operation slot. There is no
automatic proof lifting it to cap4 concurrent operation identities, arbitrary
numbers of backfills, arbitrary definition updates, or unbounded source streams.
In particular, queue-capacity progress here is produced by Drain, and operation
capacity by separate external transitions; capacity is not released in one combined
atomic step. Existing queued work is represented by a count and tracked execution,
not full mixed-queue cancellation/replacement semantics.

## Starvation and lease counterexamples

NAIVE_RR changes only capacity-free automatic work to reset QueueTurn to manual
while the queue is full. The checker finds a weak-fair infinite counterexample:
tracked completion permits Drain to open a vacancy; manual takes it and refills;
automatic dropping resets the turn; repeat. Backfill never receives the vacancy.
Task fairness and recurring capacity therefore do not imply capacity fairness.
The retained error graph is a liveness counterexample, not merely a long finite
trace or a timeout.

UNRESERVED_VACANCY allows an unrelated insertion to consume a leased last vacancy,
violating queue length plus reservation <=4. ROTATE_PINNED_BACKFILL changes focus
while a concrete candidate owns the lease, violating ownership/ordered handoff.
These isolate the two prerequisites missing from naive resource arbitration.

## Cursor factor and atomicity boundary

`cursor.fizz` uses raw nominals10,20,30 with actual=nominal+5, strictly before the
next raw nominal, inclusive lower bound10 and immutable upper bound25 or32. End32
specifically distinguishes nominal-in-range from actual-out-of-range. Revision0
excludes nominal20; one update to revision1 makes it eligible if it was not already
disposed. This finite ordered stream abstracts actual calendar evaluation.

Peek may advance SearchAfter and commit exclusions, but an eligible candidate is
retained in full with nominal/actual/revision until publication. Capacity denial
stutters. Handoff and DisposedThrough advance together. Definition update discards
unadmitted pending data and rebases strictly after the last disposed nominal,
never execution now100. Actual>end retains an endpoint witness without advancing
disposition beyond the range. A publication-failure witness records the full pending
candidate and requires a subsequent successful handoff of that same candidate.

Five safety assertions check disposition atomicity, actual upper bound, historical
rebase, nominal uniqueness and complete pending state. Four witnesses cover inclusive
boundaries, range-end evidence, failed publication then same-candidate handoff, and
update preserving unadmitted history. This factor has no temporal fairness/liveness
claim. Source duration, bounded scanning/Yield, timestamp overflow, exclusions beyond
this finite stream and all real calendar/jitter code remain separate obligations.

## Factorization and trusted limits

The arbitration factor starts with ready candidates; the cursor factor publishes
only when its abstract capacity gate opens. Their composition is a manual mapping,
not a verified refinement theorem. Extending finite ready-candidate progress to an
entire finite range requires eventual bounded scanning and eventual definition
stability, as well as the stated resource fairness. Binding/revoking an actual
vacancy lease during source revision changes is not explored by their Cartesian
product and needs integration tests. New request arrival/fair rotation beyond the
two initially ready requests likewise needs concrete coverage and a separate lifting
argument; source identities must stay pinned while entitled.

Failed publication and stale physical delivery are explicit durable-state stutters.
The models assume atomic DB publication, authenticated matching completion and
correct request/token validation. They do not verify protobuf persistence, selective
loading, executor locks, rollback clocks or real physical task delivery. No general
unconditional deadlock freedom is claimed: an entitled ALLOW can legitimately hold
vacancy while waiting indefinitely for an unresolved external operation.

| Mutation | Required failed assertion |
| --- | --- |
| `NAIVE_RR` | Liveness: `FiniteBackfillsProgress` |
| `UNRESERVED_VACANCY` | `ProtectedVacancy` |
| `ROTATE_PINNED_BACKFILL` | `PinnedCandidate` or `BackfillHandoffOnce` |
| `DISPOSE_ON_PEEK` | `DispositionRequiresCommit` or `PendingRetainsNominalActualRevision` |
| `UPDATE_USES_NOW` | `RebaseNeverExecutionNow` |
| `END_USES_NOMINAL` | `RangeUsesActualUpperBound` |

## Exploration history

The initial arbitration exploration found the expected genuine liveness failure,
but the runner recognized only safety-failure output and rejected the aggregate.
The detector was corrected to require the exact named liveness failure. A first
cursor fixture did not distinguish nominal/actual end membership, so the endpoint
mutant passed and the aggregate was rejected. Adding end32 made that distinction
observable. Before final checking, the queue was expanded from an initial one-slot
resource abstraction to capacity4 and tracked/direct admission was aligned with
the independently confirmed contract. Every final baseline and mutant was rerun;
all earlier manifests remain separately labeled exploration evidence.

## Accepted results

`/private/tmp/scheduler2-backfill-model-leases` exhausted identical graphs at both
100 and 200 action bounds, with zero queued nodes:

| Specification | Visited/valid nodes | Unique states |
| --- | ---: | ---: |
| Arbitration | 7889 | 7876 |
| Cursor | 145 | 144 |

All eight safety assertions, six reachability witnesses and the arbitration's one
conditional liveness assertion passed. All six mutations failed their named check;
NAIVE_RR exhausted 11821 valid nodes /11808 unique states and failed liveness.
Logs, exact commands/hashes and counterexample graphs are retained here. Full
exploration graphs remain at the recorded local paths. Python syntax and owned-file
whitespace checks passed. No production Go tests were run for this specification-only
contribution.


Review boundary: in the model's queued-automatic mode, even direct admission with
an empty queue first acquires the vacancy lease. Go automatic direct admission
requires only its operation turn, including a tracked policy with no blocker or
queue. The model therefore adds serialization in that case. Conditional liveness
for this fixture is not a trace refinement of every Go interleaving; the separate
service/rank argument and concrete mixed-ingress checks remain necessary.
