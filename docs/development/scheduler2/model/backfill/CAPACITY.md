# WI-009 concrete cap4 arbitration supplement

Evidence ID: E-BACKFILL-CAPACITY-001. This supplement leaves the original model,
runner and evidence unchanged. It directly explores a tracked blocker together with
multiple unresolved ALLOW intents; it does not assert a refinement from the earlier
one-slot model.

```sh
python3 docs/development/scheduler2/model/backfill/run_capacity.py \
  --fizz /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz \
  --output /private/tmp/scheduler2-backfill-capacity-first
```

Pinned FizzBee v0.5.4 runs bounds100 and200, then four named mutants, with a
300-second timeout per case, process-group cleanup and baseline fail-fast. No
exploration timed out. Exact commands, transformed source hashes, exit codes and
counts are recorded in `/private/tmp/scheduler2-backfill-capacity-first/results.json`;
each case directory contains `run.log` and `graph/`, including mutant counterexamples.
These supplemental artifacts are separate from the original committed evidence.

## State and transitions

The queue capacity is4. Occupancy consists of a tracked-blocker boolean and an
ALLOW-intent count0–4, constrained by `int(tracked) + allows <= 4`. The initial
fixture has a full queue, one tracked blocker and three ALLOW intents. Independent
external MatchingCompletion and AllowSettlement transitions release their respective
resource; neither implicitly clears the other. Admissions consume a real counted
slot. ALLOW can admit with a tracked blocker when spare capacity exists; tracked
admission requires absence of a blocker and applicable queue/drain conditions.

There are two finite ready backfill requests, one candidate each, sustained manual
traffic and sustained automatic traffic. Manual/backfill independently choose
BUFFER, ALLOW or SKIP; automatic policy is queued or direct ALLOW. These choices
remain fixed during an execution. Queue lease, concrete candidate identity,
QueueTurn and OperationTurn retain the earlier protocol: one protected vacancy;
manual/backfill ALLOW acquire vacancy before operation capacity; Drain and automatic
direct ALLOW compete for operations; capacity-free scans/drops rotate neither turn.
Backfill focus remains pinned through its lease. Stable manual/automatic head bits
alternate after service to bound identities; this is not the real monotonic-ID or
stale-response protocol.

Four safety assertions check vacancy protection, pinned ownership, ordered unique
backfill handoff and the concrete total-operation cap. Six existential witnesses
cover both requests resolving, an actual full-operation lease wait followed by
handoff, tracked+three ALLOW saturation, four-ALLOW saturation, spare capacity with
a tracked blocker, and a full queue with tracked+two ALLOW intents. The last case
specifically separates queue fullness from operation fullness.

## Conditional fairness and composition limits

`always eventually FiniteBackfillsProgress` is a genuine temporal assertion over
this finite graph: eventually both backfill candidates have been handed off or
legitimately disposed. It assumes weak fairness separately for AcquireVacancy,
TransferManual, TransferBackfill, TransferAutomaticQueued, DirectAutomaticAllow,
Drain, MatchingCompletion and AllowSettlement. Thus continuously enabled service
cannot be withheld forever, publications eventually succeed, a persistent tracked
blocker eventually completes, and continuously present ALLOW intents yield settlement
capacity. It does not assume or prove completion of every individual ALLOW identity.

The implementation has one CPU coordinator, whereas these are separately fair
abstract actions. Composition requires showing that its persisted NextWork rotation
and fair physical delivery implement those service assumptions, and that no bypass
can consume reserved capacity. The intended argument is that a continuously enabled
class is selected by finite cyclic CPU service, while other classes cannot steal
its resource entitlement. This supplement does NOT mechanically verify that argument
or derive per-class fairness from mere fairness of the single coordinator task.
Concrete coordinator tests and independent source review remain necessary evidence.

This directly checks cap4 occupancy combinations, removing the need to lift the
one-operation capacity abstraction for this fixture. It still does not prove Go
refinement, arbitrary calendar enumeration, arbitrarily many backfill candidates,
changing policies/definitions, dynamic request arrival order, mixed-queue effect
replacement, physical callback identity, retention or database atomicity. Existing
cursor tests/models provide separate evidence; no composition theorem is claimed.
There is no universal sixteen-grant bound, wall-clock bound, or progress guarantee
while external capacity never recovers. Fixed policies and two already-ready finite
requests remain explicit limits.

## Results and sensitivity

Both bounds exhausted the same graph: **36687 valid nodes,36674 unique states**,
zero queued. All four safety assertions, six witnesses and conditional liveness
passed. The baseline checker reported `IsLive: true` for the named temporal property.

| Mutation | Required failure | Result |
| --- | --- | --- |
| `NAIVE_RR` | Liveness: `FiniteBackfillsProgress` | Detected;53745 valid nodes /53732 unique states |
| `UNRESERVED_VACANCY` | `ProtectedVacancy` | Detected |
| `ROTATE_PINNED_BACKFILL` | `PinnedCandidate` or `BackfillHandoffOnce` | Detected |
| `ALLOW_OVER_CAPACITY` | `ConcreteOperationCapacity` | Detected |

The last mutation bypasses the admission slot limit. The naive-RR mutation produces
an infinite starvation counterexample under the declared weak fairness, rather than
being classified from a timeout or a long finite trace. Syntax and whitespace checks
passed. No production Go files were changed by this supplement.

Source SHA256: `e508a66631c2f32f55b43072d137054c8c15b5163023a14b5357bfbf3ec1f219`.
Runner SHA256: `4a94f41ba95f3ff2f56fbed7193cfaa6ca0641dcbfb66cf4933433d0384289e5`.


Review boundary: in the model's queued-automatic mode, even direct admission with
an empty queue first acquires the vacancy lease. Go automatic direct admission
requires only its operation turn, including a tracked policy with no blocker or
queue. The model therefore adds serialization in that case. Conditional liveness
for this fixture is not a trace refinement of every Go interleaving; the separate
service/rank argument and concrete mixed-ingress checks remain necessary.
