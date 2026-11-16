# WI-005 independent bounded-buffer model

Evidence ID: E-BUFFER-MODEL-001. Base `e706a634074b72c22d666e23aee2876c8bfd957d`; contract input: WI-005 D32–D34.
The transition rules and oracle journals were authored independently from the Go
reducer, CHASM adapter and property tests. No production code is imported.

```sh
python3 docs/development/scheduler2/model/buffer/run.py \
  --fizz /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz \
  --output /private/tmp/scheduler2-buffer-model-second
```

The runner uses the pinned local FizzBee v0.5.4 release. Bounds are 100 and 200
actions, one concurrent atomic action, and 300 seconds per case. It then checks
four independent one-fault mutations. Each record contains the exact command,
specification hash, exit code, node counts and expected invariant. Parser errors,
timeouts and failures outside that named invariant set do not pass sensitivity
checks. Compact logs/results and explicit JSON counterexamples are in `evidence/`;
full graphs remain at their recorded local paths. Logs strip trailing whitespace.

## Finite state and independent observations

The initial admitted operation occupies the slot in Pending, Started or Ambiguous.
Five additional automatic arrivals (nominals 2 through 6) allow four waiters plus
an overflow. Time is fixed at tick 6; actual equals nominal. This abstracts due
source occurrences independently of calendar evaluation. Generator exhaustion
means all five arrivals have been disposed. The probe/output scheduling of the
generator and operation-record retention remain WI-002/WI-004 obligations.
The queue capacity is four, independent from retained-operation capacity.

Initial policy is BUFFER_ONE or BUFFER_ALL. One definition update flips it and
changes template revision 1 to 2; existing queue entries retain their queue-time
resolved policy and workflow identity `(revision, nominal)`. This pair represents
the fully suffixed workflow ID. BUFFER_ONE refuses later waiters once a queue
exists, including a queue inherited from BUFFER_ALL. BUFFER_ALL appends up to four.
New ingress never bypasses an existing queue. For simplicity, all arrivals enter
the queue before admission, even if free; this adds an explicit drain step instead
of claiming to model the concrete immediate-admission optimization.

An initial remaining budget of two is separate from the initial active operation's
already consumed admission. Ghost grant/accounting journals verify that enqueue
never debits budget and admission debits exactly once. Additional finite scenarios
change current eligibility on update: normal, paused, zero budget, or catchup zero.
A single recovery may unpause, restore catchup, or refill budget. Queue entries
remain while occupied. Once free, selected entries adopt current pause, budget and
catchup rules and are discarded without debit if ineligible. New ineligible ingress
is skipped. Disposed entries cannot be resurrected by recovery.

`enqueue_history` and `drained` form an independent FIFO conservation journal;
`admitted` and `completed` independently check at most one unfinished operation.
The current operation's phase alone is not used as evidence for that invariant.
On admission, an observation journal captures current template/carryover and
queue-time workflow identity/policy. Frozen request fields are checked against that
journal, so reusing a queue-time template produces a discriminating failure.
Later updates/completions cannot change already recorded frozen requests.

The symbolic carryover is the last completed operation's identity. WI-003 separately
verifies absent/null/value/failure and metadata-only completion. This model checks
when that carryover is sampled for the next admitted request; it does not repeat
protobuf or completion-payload semantics.

## Drain obligations and publication

Completion directly arms a persisted drain token whenever free with queued work,
including after generator exhaustion. Each drain delivery selects one FIFO entry
and admits at most one operation. One-entry batches are a refinement subset of
the concrete up-to-four-entry work limit. Dropping an ineligible entry rearms a
successor when more queued work remains. Pending or Ambiguous work cannot drain.
Retired tokens make old deliveries stutter. Duplicate/old callbacks stutter.

A failed publication leaves queue, budget, frozen requests and obligations unchanged;
restart preserves all modeled durable state. The failure witness requires a later
admission, and the refill witness requires an actual prior discard, so witness
flags do not conflate events in reverse order. Real serializer rollback, timer
attribute validation, selective loading and database atomicity remain separate
CHASM tests/trusted boundaries. This model has no wall-clock, retry transport,
manual/backfill ingress, public API or production routing.

## Assertions, mutants and scope

Seven safety assertions check budget debit, FIFO conservation, capacity, direct
drain wakeups/token freshness, immutable adoption, non-resurrection, and strict
occupancy/FIFO admission. Seven existential witnesses cover saturation, exhausted-
generator wakeup, late-bound template with old workflow identity, ambiguous
occupancy, discard before budget refill, failure before later admission and
preserved queued policy after an update.

| Mutation | Required failed invariant |
| --- | --- |
| `DEBIT_ON_ENQUEUE` | `NoEnqueueBudgetDebit` |
| `LOSE_COMPLETION_WAKE` | `DrainObligation` |
| `STALE_DRAIN_CONSUMES_QUEUE` | `FIFOConservation` |
| `REUSE_QUEUED_TEMPLATE` | `FrozenAdoption` |

There are no liveness assertions or fairness assumptions. FizzBee's `IsLive: true`
line is vacuous without temporal assertions and is not liveness evidence. Equal
exhausted graphs at both bounds support finite-model stabilization, not an
unbounded implementation proof or automated Go correspondence.

The initial `-first` run failed because Starlark does not support Python generator
expressions. The runner rejected these infrastructure failures. The assertion was
rewritten as an explicit loop, and every required case was rerun; no expected
behavior was weakened.

## Accepted result

Accepted run: `/private/tmp/scheduler2-buffer-model-second`. Both action bounds
exhausted the same finite graph: `Valid Nodes: 69207 Unique states: 69196`, zero queued nodes.
All seven safety assertions and seven witnesses passed. All four mutations failed
their required named invariant. Accepted source hashes and exact commands are
recorded in the accompanying evidence.
