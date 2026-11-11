# WI-004 independent sequential-admission model

Evidence ID: E-SEQUENTIAL-MODEL-001. Source base:
`b57e10e188403637de441e05b1029dcaf662f824`. Contract input: WI-004 D29–D31.
No production reducer, generator, calendar or serializer code is imported. This
finite model composes the accepted completion/immutable-input contract with
sequential occupancy and keyed retention; it does not replace WI-001–WI-003 checks.

```sh
python3 docs/development/scheduler2/model/sequential/run.py \
  --fizz /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz \
  --output /private/tmp/scheduler2-sequential-model-accepted
```

The pinned FizzBee v0.5.4 runner explores action bounds 100 and 200, then three
single-fault variants. Timeout is 300 seconds per case. Exact commands, source
hashes, exit codes, counts and outcomes are in `evidence/results.json`. Copied
logs strip trailing whitespace. Error graphs are explicit JSON counterexamples;
re-running reconstructs each mutated specification deterministically. Full graphs
remain at the local paths in the logs. No production defect was introduced.

## Bounds and abstract state

Six ordered nominal occurrences, actual time = nominal + one abstract tick; six
initial admissions available, four retained records, one active slot, one source
probe yield, and one failed generator publication. Six occurrences allow a trace
with at least five admissions (forcing eviction) and an independent trace that
also skips occupied work. The output batch limit is one, a refinement subset of
the concrete maximum four; probe yield abstracts a bounded eight-probe source
search. All sources are eligible and immutable for this item. WI-002 separately
covers exclusions, resumable calendar/jitter and revision update boundaries.

`cursor` is nominal progress. `dispositions` is an independent oracle journal,
one entry per committed admit/skip. `history`, `started` and `completed` are
unbounded-within-six ghost histories, not proposed retained production state.
`retained`, `phases` and `frozen` represent actual bounded keyed records. `active`
identifies the latest operation, including after it completes. Carryover is the
last completed operation's result identity; it survives record eviction.

Pending, Started and Ambiguous occupy the single slot until Completed. Admission
freezes current carryover and decrements budget once. Acknowledgment or completion
counts started once; early completion is allowed. An occupied due occurrence is
skipped without budget debit. Before a fifth retained admission, the oldest
Completed record is atomically removed. In reachable well-formed states, single
occupancy ensures an eligible eviction exists. The defensive concrete error for
an externally malformed all-unfinished/full state remains a Go validation test,
not a reachable model state. Duplicate latest and older retained/evicted results
cannot replace active identity or latest carryover.

The next frozen input is independently checked against the previous admitted
operation in ghost history, rather than against a second call to the admission
transition. This relation follows from strict sequential occupancy. Symbolic
successful completion results suffice to check propagation; absent/null/failure,
metadata-only completion and current pause policy are covered by WI-003's
separate completion model and concrete composition tests.

## Generator and publication abstraction

Each modeled valid delivery occurs at its current deadline. Its next actual
candidate is due; future clock rollback and catchup policies are separate tests.
After a bounded batch, deadline advances one tick. A probe-yield delivery changes
token/deadline without advancing cursor. Source exhaustion removes the timer.
Retired tokens represent old physical deliveries; stale delivery is a stutter.
Both token freshness and pending generator obligation are checked independently
from cursor progress. Deadline authentication and timer reconstruction remain
CHASM test obligations; the source-level eight-probe cost is not proved here.

Failed publication leaves cursor, records, tasks, budget and carryover unchanged.
Restart is an explicit stutter because all modeled state is durable. Tentative
serializer buffers and real persistence atomicity are not modeled. This is a
protocol invariant under atomic state/task publication, not a database recovery
proof. No liveness assertion or fairness assumption is made: a schedule may wait
forever for an external completion, including after ambiguous acceptance.
FizzBee's `IsLive: true` output without temporal assertions is not liveness evidence.

## Checked properties and challenges

Seven safety assertions cover committed cursor dispositions, single occupancy,
latest identity/carryover, oldest-completed retention, frozen next input,
eviction-independent accounting, and generator obligations. Six witnesses cover
fifth admission/eviction, ambiguous SKIP, token-only progress, next carryover,
an actual stale delivery after eviction, and failed publication before five
admissions. Witness flags are set at the qualifying event, not inferred from
unrelated conditions that happened at different times.

| Mutation | Required named detection |
| --- | --- |
| `ADVANCE_CURSOR_ON_FAILURE` | `CursorAccountsForDispositions` |
| `STALE_TIMER_ADVANCES` | `CursorAccountsForDispositions` or `GeneratorObligation` |
| `STALE_COMPLETION_REPLACES_ACTIVE` | `LatestIdentityAndCarryover` |

Parsing failures, timeouts and other errors do not count as mutation detection.
A finite graph stabilized at two action bounds does not establish an unbounded
implementation proof or automated cross-language refinement.

## Recorded evidence

Accepted run: `/private/tmp/scheduler2-sequential-model-accepted`. Both action
bounds exhausted the same graph: `Valid Nodes: 3076 Unique states: 3076`, with zero queued nodes.
All seven safety assertions and six witnesses passed. All three mutations failed
a named expected invariant. The earlier `-first` directory is retained but its
witness flags were subsequently strengthened; the accepted artifacts match the
source hashes recorded alongside them.
