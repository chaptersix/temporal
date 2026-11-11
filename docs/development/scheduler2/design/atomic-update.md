# WI-011 local atomic update contract

Status: independently source-reviewed; implementation and tests in progress.
D55 extends the local adapter without introducing frontend routing or changing
public conflict-token, visibility, or migration contracts.

## Source and decision

Reference scheduler.Update replaces Schedule controls and definition in one CHASM
transaction after closed/migration/conflict checks (chasm/lib/scheduler/scheduler.go,
Update). Frontend validation handles malformed schedules and remaining actions
(service/frontend/workflow_handler.go, UpdateSchedule). Existing scheduler2
ConfigureGeneration advances Root.Revision; a following decision.Update with the
same revision silently stutters. Two independent commits expose intermediate
source/control combinations. Neither sequence is the full-replacement contract.

UpdateConfig groups GenerationConfig with Paused, Limited, Remaining and
PauseOnFailure. UpdateConfiguration requires a live configured root, strictly newer
root and continuation revisions, and nonnegative Remaining even when unlimited.
The common generation preparation path validates/compiles/marshals all inputs,
computes automatic supersession at max(execution now, disposed cursor), previews
all replacement controls for idle eligibility, and checks generator/coordinator/idle
token arithmetic before publishing any root, DATA, generator or task mutation.
Generation.After is initialization-only and is ignored for an existing source.

Publication replaces controls, policy and source together, then reconciles against
the new source. This clears obsolete automatic/backfill lease keys. Still-valid
manual leases remain tied to their concrete candidate. Historical backfills retain
their fixed range and restart after their last disposed nominal on their next scan;
the automatic execution-time boundary is never substituted for that cursor.

Already admitted start/effect request bytes, retry deadlines and bound target chains
remain immutable. Queued identity and resolved policy remain frozen. Replacement
controls do not recreate, settle or cancel existing overlap effects; current effect
and waiter policies keep governing their accepted obligations. Later admissions
use the current template, catchup and carryover under those preserved identities.
Budget replacement is not a second debit; future automatic admissions debit once,
manual/backfill admissions retain their explicit bypass contract.

## D56 local request replay

The local Update wrapper uses CHASM WithRequestID, consistently with Trigger and
Backfill. Within the framework's bounded request-ID retention, a previously
committed request returns success without executing the body again, including a
changed body or closed root. A failed transaction does not consume that ID.
A new request ID with a stale/equal revision is rejected before writes. This local
request identity is distinct from definition revision and from public conflict
tokens; no UpdateSchedule compatibility claim follows from it.

## Review and verification obligations

Independent review found the preflight order coherent. The new generator always
provides an immediate resource-free scan, so the existing immediate coordinator
preflight bounds the later wake requirement. Idle checks use all proposed pause and
budget fields; after publication they see the same transaction clock and source.
Tests must discriminate whole replacement, rejected publication/retry, each token
limit, stale revisions, untouched admitted bytes, frozen queue policy, source/lease
supersession, budget/pause changes and local request replay.

C7 correspondence remains explicit: ChainID denotes first execution run ID, the
fence used by cancel/terminate history APIs. Local authenticated-event assumptions
do not establish continue-as-new/reset callback authentication or public routing;
those remain required M3 correspondence work, not inferred from this update API.
