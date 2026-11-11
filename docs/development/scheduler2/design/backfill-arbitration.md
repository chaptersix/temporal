# WI-009 coordinator and capacity arbitration design

Status: design review; no fairness implementation or liveness claim yet.

## Why runnable-task round-robin is insufficient

Consider a full execution queue, one tracked blocker, and continuous pending manual
and backfill ingress. Automatic overload-disposition work remains runnable while
both accepted producers wait. A naive four-class coordinator skips capacity-blocked
classes, executes automatic disposition and resets its next turn to drain. Every
completion then permits drain to free one queue position; the next manual turn takes
it. Backfill never obtains capacity although queue positions become available forever.
Fair delivery of each task and recurring capacity therefore do not prove plan L2.

One root coordinator task should own scheduling and publication. Existing generation,
manual and drain physical tasks must be invalidated or routed through that coordinator;
they cannot remain admission bypasses. Each successful transaction performs at most
one candidate handoff/disposition or bounded scan quantum, then reconciles the next
obligation. Callbacks/configuration release/recompute readiness, without admission.

## Resource entitlements under review

Separate CPU scan selection from resource allocation:

- Persist QueueTurn among manual/backfill/automatic producers. Contenders are pending
  candidates whose resolved policy requires an execution-queue position, including
  when the queue is full. Advance only on a successful queue insertion.
- Persist OperationTurn among queued drain/manual/backfill/automatic. Contenders are
  eligible direct admissions or eligible queued drain needing an operation slot,
  including when all slots are unfinished. Advance only on successful admission.
- Capacity-free scans, exclusions and policy/overload dispositions may progress but
  must not rotate either resource entitlement. This prevents automatic dropping from
  repeatedly resetting the backfill turn.
- A separate bounded scan/service round-robin chooses executable work, respecting
  resource ownership. Backfill request selection is also persisted and bounded.
- Other prerequisites still matter: drain with an active tracked blocker cannot use
  an operation slot; ALLOW_ALL transfer with a full queue retains WI-008's full-queue
  wait. These are not permission to rotate resource turns on unsuccessful service.

Queue drain must compete for operation slots so repeated ALLOW_ALL admissions cannot
steal every newly freed slot. It may create queue capacity needed by waiting producers.
The two entitlement rings need a reviewed argument that these dependencies cannot
cycle and that changing contenders cannot repeatedly reset an older waiting class.
Per-backfill rotation must preserve each request's pending candidate and not let scans
or newly accepted requests starve an existing ready candidate.

## Enumeration boundary

Historical requests use inclusive lower/upper bounds and current definition, with a
pending-preserving bounded Peek operation. Peek may dispose exclusions but keeps an
eligible candidate separate from DisposedThrough until the same transaction commits
its handoff/disposition. Capacity wait retains that candidate. Rebase begins strictly
after last disposed nominal, never execution now; request range end is immutable.
Range exhaustion from actual>end retains the witness without advancing disposition
past the range. Jitter is strictly below the next raw nominal, establishing this stop.

Actual automatic enumeration must likewise preserve a denied eligible candidate until
its grant; eager Run outputs cannot advance past an unrecorded candidate. Existing
WI-002 Run semantics remain valid for callers atomically publishing all outputs.

## Evidence required before acceptance

A finite model must include the starvation trace above as an adversarial schedule,
independent queue/operation capacity releases, sustained ingress, and matching
completion versus ALLOW_ALL settlement. Record fairness assumptions explicitly;
bounded witnesses are not infinite liveness. Concrete CHASM tests must cover all
admission paths, failed publication, stale predecessor tasks, cursor retention,
update rebasing, and capacity recovery. No production routing or migration changes.


## Reviewed correction: a durable vacancy lease

The insert-only QueueTurn proposal above is superseded. A second counterexample has
pending manual ALLOW, a full queue and four unfinished operations. Each tracked
completion lets drain use the freed operation slot and open a queue position; another
tracked producer refills it before the next operation completion. ALLOW never meets
both capacity conditions simultaneously. Treating its full-queue guard as an unrelated
prerequisite incorrectly removes it from entitlement.

Reviewed design retains the guard by treating vacancy as a resource even for direct
accepted admissions and policy-only disposal:

1. QueueTurn considers every ready manual/backfill transfer, including ALLOW and SKIP,
   plus automatic candidates needing queue insertion. With space, grant one durable
   vacancy lease naming a concrete command/candidate. Do not grant a source still
   scanning or a future automatic candidate. At most one lease exists.
2. Reserve that position: queue length + reservation <=4. Owner insertion replaces
   the reservation. Other insertions cannot consume it; the initial prototype
   conservatively waits for its own lease instead of allocating extra spare positions.
3. Direct accepted admission acquires the vacancy lease BEFORE requesting an operation
   slot. Once held, its operation contention persists even at full operation capacity.
   Drain needs only an operation slot and can create queue vacancy; automatic direct
   ALLOW retains its no-vacancy semantics but must obey OperationTurn.
4. Successful owner insertion, direct admission or policy-only disposition releases
   the lease and rotates QueueTurn. OperationTurn rotates only after actual admission.
   Failed publication, capacity waits, scans and overload drops rotate neither.
5. Owner identity remains stable through readiness changes; backfill focus/rotation
   stays pinned until handoff/disposition. Reevaluate policy/resource need without
   assigning the lease to a different command. Definition invalidation must explicitly
   revoke/rebase old candidate ownership, with no silently advanced disposition.

Vacancy-before-operation acquisition avoids a cycle: drain requires only operation
capacity and creates vacancy. Under recurring usable operation capacity, a concrete
leaseholder receives eventual service; a non-ready class cannot reset its claim.
An entitled ALLOW can delay another pending cancellation while all operations remain
unresolved. This head-of-line cost is explicit; no unconditional deadlock freedom,
finite sixteen-ready-grant bound, or progress without external capacity is claimed.
The independent reviewer rejected the earlier ready-only bound and found no further
counterexample under these stated assumptions. Model and implementation evidence
remain required; design review is not proof of integrated behavior.
