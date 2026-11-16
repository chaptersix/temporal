# WI-008 independent bounded manual-command models

Evidence ID: E-MANUAL-MODEL-001. Contract: WI-008 D43–D46 at
`0232beba726aceedafa1707acba9ca3fc3ea7ab2`. The specifications were authored from
the contract and import no production reducer, adapter or property-test code.

```sh
python3 docs/development/scheduler2/model/manual/run.py \
  --fizz /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz \
  --output /private/tmp/scheduler2-manual-model-reviewed
```

Pinned FizzBee v0.5.4 checks each specification at bounds 100 and 200, then eight
separate named mutations. Each case has a 300-second timeout and process-group
cleanup, including wrapper descendants. Results are written incrementally; any
baseline error or timeout stops the runner before redundant bounds. A mutant
passes sensitivity only by failing its named invariant. Exact commands, source
hashes, exit codes and graph counts are retained in `evidence/results.json`.

## Command identity and framework boundary

`commands.fizz` checks a fixed finite request stream A, empty, B, empty, C, A, D,
interleaved with duplicate A carrying a changed body, pending transfers, operation
retention and one framework expiry of A. Every accepted command has nominal100,
so timestamp-based identity collisions are observable. Nonempty IDs deduplicate
before capacity checks; empty IDs are separate calls. Acceptance allocates a
monotonic sequence and immutable body snapshot. Pending capacity is4; sequence
capacity is symbolically6, allowing exhaustion without exploring uint64 space.
The model does not prove actual integer overflow handling.

Independent accepted-command and successful-ID journals check unique manual IDs,
first-success-wins bodies and rejection without ID consumption. Expiry is an
explicit symbolic removal of A, not an implementation of CHASM age/count trimming.
A subsequent A acceptance must allocate another sequence. Transfer abstracts an
operation becoming terminal and places it into a separate cap4 retained-record
list. Eviction never removes framework dedup. A witness requires an actual
changed-body duplicate after its associated start record has been evicted.

The framework's lock, success-only request-ID recording and atomic publication
are trusted boundaries. This model does not call CHASM, prove retention duration,
or imply indefinite deduplication. Framework expiry/trim may admit another command;
sequence identity still cannot collide with previous manual operations. Failed
publication is an explicit durable-state stutter, not a rollback implementation
test. Actual engine dedup, payload comparison behavior, count/age trimming and
failed-commit retries require separate CHASM evidence.

## Admission, eligibility and independent resource progress

`admission.fizz` starts with one accepted command, all six explicit policies or
an inherited policy, a past or future nominal time (-10 or100), execution clock0,
queue occupancy0 or4, unfinished count1 or4, and a possible tracked blocker.
The current normalized schedule policy starts at BUFFER_ALL and may update once
to ALLOW_ALL or TERMINATE_OTHER. API policy normalization/validation is outside
this model; explicit SKIP covers its resolved behavior.

Full queue blocks every transfer before policy disposition. With queue space,
ALLOW_ALL requires an operation slot. Other policies may transfer/dispose with
an existing blocker or FIFO queue; free empty-queue admission needs a slot.
Independent external transitions release one queue position, one unrelated
unfinished operation, or the tracked blocker. These represent unrelated queue
and operation progress; they do not model those workflows themselves. A temporal
witness observes capacity wait, then queue space, then tracked transfer while all
four operation slots remain occupied. A separate witness observes a real capacity
wake followed by manual admission. Persisted token obligations are checked without
requiring an internal retry loop during external capacity wait.

Transfer resolves an inherited policy from current configuration; explicit override
wins. The resolved policy and workflow identity are frozen at transfer. Manual
identity remains sequence-based while the public workflow identity is a symbolic
timestamp suffix. All states are paused with zero action budget and catchup0;
manual admission remains possible without budget debit or automatic cursor advance.
Past nominal bypasses catchup; future nominal does not defer admission until that
time. The request captures current template revision and ALLOW_DUPLICATE reuse,
with a horizon of admission clock plus60, independent of nominal identity.

SKIP and excess BUFFER_ONE dispose immediately after transfer becomes possible;
other tracked policies queue when occupied. Drain samples the current template
while retaining queued identity/policy. The explicit-override witness requires
Update before Transfer, captured at that transition. The template witness requires
Update while already queued, followed by admission with the updated revision.

This fixture drains the manual entry only after unrelated queue entries are gone.
Mixed-queue CANCEL/TERMINATE replacement and their effect protocol remain WI-007
and concrete manual integration tests, not claims of this model. Post-manual-
admission operation accounting/completion is not modeled. Full start-request
payloads, carryover variants, actual timestamp validation/int64 conversion,
absent-time acceptance-clock defaulting, callback identities, serialization,
selective loading and physical task attributes need concrete tests. Execution
clock is fixed; clock rollback and transport retry behavior remain prior items.

## Factorization, assertions and mutations

The models share a conceptual transfer boundary: command acceptance owns durable
sequence/pending identity, while admission owns readiness and adoption. Command
retention abstracts successful transfer/completion; admission assumes one already
accepted command. Removing their Cartesian product does not establish a formal
composition theorem. There is no automated correspondence proof to Go.

Four command safety assertions and six witnesses combine with five admission
safety assertions and nine witnesses. The mutation suite checks distinct failures:

| Mutation | Required failed invariant |
| --- | --- |
| `DUPLICATE_REWRITES_BODY` | `FirstSuccessfulBodyWins` |
| `ID_FROM_NOMINAL` | `DistinctSameNominalIdentity` |
| `REJECT_RECORDS_ID` | `RejectionDoesNotConsumeID` |
| `DEBIT_MANUAL` | `ManualBypassesEligibilityAndDebit` |
| `ADVANCE_AUTO_CURSOR` | `AutomaticCursorIndependent` |
| `EARLY_INHERITED_POLICY` | `LatePolicyAndFrozenAdmission` |
| `QUEUE_TIME_TEMPLATE` | `LatePolicyAndFrozenAdmission` |
| `LOSE_CAPACITY_WAKE` | `DurableWakeWithoutSpin` |

No liveness assertions or fairness assumptions are present. Existential progress
witnesses establish reachability, not guaranteed scheduling. FizzBee's `IsLive: true`
line is vacuous here. Sustained mixed manual/automatic/backfill fairness is outside
WI-008 and remains an explicit later obligation.

## Review history

All exploratory runs completed before another runner started. The initial passing
model combined resource release; it was strengthened to independently release
queue positions, unrelated operations and the tracked blocker. Before final
review, SKIP/BUFFER_ONE disposition and queued-template sampling were made explicit.
Independent review then identified that an override witness could observe Update
after admission; the final witness captures Update before Transfer. All final
baselines and mutants were rerun. Earlier passing hashes remain exploration
history, not accepted evidence for the final specification.

## Manual implementation mapping

After independent authoring, inspected integration `f525eacab`:
`internal/decision/manual.go` maps sequence/pending fields to `ManualSequence` and
`Manuals`; the model's readiness predicate corresponds to `CanDispatchManual`.
Transfer corresponds to `DispatchManual`, including no-slot tracked buffering and
SKIP/excess-BUFFER_ONE disposal. `Trigger` in `chasm/lib/scheduler2/manual.go` supplies
CHASM `WithRequestID`; the command model abstracts that framework boundary rather
than replacing it with scheduler-owned indefinite dedup. Root ManualArmed/Token/At
map to the model's task/token with execution clock0. Real At validation, stale
physical envelopes and clock rollback are explicit adapter-test obligations.
`ManualNeedsTemplate` metadata-only disposal and timestamp validation are not
represented as physical reads or integer encodings in this model. This is a manual
mapping, not an implementation correctness proof.

## Accepted results

Accepted run: `/private/tmp/scheduler2-manual-model-reviewed`. Both action bounds
exhausted each graph with zero queued nodes:

| Specification | Visited nodes | Valid nodes | Unique states |
| --- | ---: | ---: | ---: |
| Commands | 259 | 259 | 259 |
| Admission | 34452 | 34060 | 17434 |

All nine safety assertions and fifteen existential witnesses passed; all eight
mutants failed their named invariant. Exact commands/hashes, compact logs and
counterexamples are retained alongside prior exploratory result manifests.
Full graphs remain at their recorded local paths. Python syntax and owned-file
whitespace checks passed. No production Go tests were run for this independent
specification-only contribution.
