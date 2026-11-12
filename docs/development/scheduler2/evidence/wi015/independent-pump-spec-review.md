# Independent immediate-work and public-definition review

Reviewer: api_seam. Read-only source review coordinated with proof_review (pump) and
oracle_review (public occurrence profile). Snapshot hashes are recorded separately in
independent-pump-spec-review-snapshot.json; author manifests remain historical evidence.
No remaining behavioral blocker found in this snapshot. The pump design document's
stale proposal-only status was reported to its author for correction.

## Bounded immediate coordinator chain

Reviewed coordinator_immediate.go/tests, reconcileCoordinator and WorkHandler, library
binding, WorkTask schema, task publication call sites, and CHASM AddTask,
executeImmediatePureTasks, ExecutePureTask and EachPureTask. Immediate work is the
existing pure-task framework facility executed before persistence. It does not call
StartHandler or any workflow RPC; admission only publishes side-effect tasks. The unit
fixture installs a real start callback and asserts zero invocations during publication.

An initial immediate task carries at most2 quanta. Each valid handler derives a scoped
remaining budget, and any immediate successor carries one less. At zero, reconciliation
publishes a physical successor. A physical handler consumes the first quantum and can
publish one immediate successor. Context-only state would be lost because framework
immediate execution creates fresh contexts; the explicit task payload solves that.
Current token, armed status and due logical timestamp remain necessary. Immediate zero
scheduled time does not replace the logical Coordination.At. Pure physical task
validation receives persisted logical task attributes through EachPureTask, unlike the
side-effect physical envelope precision issue fixed earlier.

Repeated reconcile calls before close can enqueue duplicate current-token promotions,
but only the first succeeds: each successful quantum disarms the old obligation and
re-arms with a newer token (or remains disarmed), invalidating duplicates. No loop bypass
of NextWork/NextQueue/NextOperation was introduced; resource turns and retained vacancy
lease updates occur after every successful quantum. Existing token exhaustion preflight
remains before publication. Failed publication is covered by exact durable-node and
physical-task rollback assertions in the author's real CHASM tree test.

The budget bounds valid coordinator quanta per root chain, not total bytes, spec parsing,
request assembly, task-validation count, RPC latency or whole-transaction CPU independent
of input. Source reasoning relies on the current single active coordination token per
root. The existing Lean/single-quantum models do not prove atomic composition of this new
public runtime pump. Author reports focused PASS0.676s and full scheduler2 recursive
passes; reviewer did not rerun them. Two of three original timing ports passed at this
review, while weekly execution-timeout remained a separately diagnosed due-Start task
classification gap. No full compatibility acceptance follows from pump source review.

## Public definition limits and reconstruction

Reviewed occurrence New/NewPublic, bounded canonical compiler, public creation,
configureGeneration, currentEnumerator and backfillEnumerator. New retains64KiB,
64terms and1024ranges for internal verification fixtures. NewPublic removes those extra
prototype definition ceilings while retaining the same canonicalization, validation,
identity hash, candidate generation and continuation logic. It does not alter probe/output
bounds. Create chooses NewPublic; all later generation and automatic/backfill reconstruction
choose via persisted PublicRevision rather than transient frontend activation or an
in-memory constructor flag. PublicRevision is preserved across root replacement and
explicit update, so reload cannot silently select the smaller internal profile.

NewBoundedCompiledSpec still validates source and normalized definitions, nil intervals,
calendar range semantics, and malformed exclusions. MaxInt maxima remove only cardinality
ceilings; they do not skip validation. Public request ingress and configured transport /
persistence limits remain outside this compiler. Frontend BlobSizeLimit measures memo
and action input, not serialized schedule specs, so it is not a spec cardinality bound.
Every compile/raw calendar lookup may scale with accepted spec size. Fixed probe budgets
therefore do not imply fixed work independent of public input size; both source comments
and author evidence explicitly state that boundary.

Author tests cover100terms,1025ranges and >64KiB valid unknown data, preserving immutable
input and exact first candidate while internal construction rejects those profiles;
malformed inputs and excess probe budgets still reject. The four actual public ports
exercise50 calendars plus50 exclusions, configured mirrored exclusions and future
projections. Source consistently selects the public profile during repeated enumeration;
this review does not claim a separate new larger-spec shard-reload experiment. The
representable execution domain remains2000 through2100, not all protobuf timestamps.
No new Lean/generalized cardinality proof or broad public-domain parity is claimed.

## Due-start classification follow-up

Reviewed the subsequent narrow component.publishTransition change: only an already-due
Start with explicit live public pump capability uses TaskScheduledTimeImmediate. It is
still a side-effect task, therefore enters the transfer queue and does not execute inside
the pure-task transaction. Future wake/retry tasks retain their timer classification.
The StartTask payload's exact WakeTime remains unchanged; StartHandler.Validate compares
it to PendingStarts and startSnapshot/request still enforce due/deadline before RPC.
No early issuance or stale-wake fence relaxation was introduced. Internal profile task
classification remains unchanged. The added test inspects actual CHASM physical transfer
and timer categories, validates exact logical wake, and rejects the initial stale wake
after retry publication. The pump test was also strengthened to inspect serialized
WorkTask budget0/token/deadline and physical-task status, directly distinguishing the
coordinator fallback from a separately published side-effect task. No behavioral blocker
found in this follow-up; author's rerun remains separate evidence.
