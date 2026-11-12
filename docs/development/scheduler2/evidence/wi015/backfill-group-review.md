# Independent public SKIP backfill group review

Reviewer: api_seam. Source-only review of backfill_batch.go/tests, backfill.go,
coordinator.go, decision/manual.go and publicDispositionCounts against old
backfiller_tasks.go:allowedBufferedStarts/backfillerBufferCapacity. Parent owns test logs.

## Resolved finding: retained vacancy lease

<details>
<summary><strong>med, resolved</strong> — Zero configured group capacity could discard a retained vacancy lease.</summary>

`demands` omits the BACKFILL class entirely for a fresh group with capacity0.
`planWorkFor` clears OwnerKey when its class is absent. A backfill that already owns
vacancy while all operation slots are occupied can therefore lose its retained owner
when configuration reduces group capacity to0 and a callback reconciles. Restoring
capacity lets another class acquire vacancy, although no durable handoff or definition
invalidation occurred. Existing zero-capacity test starts without such a retained owner.

</details>

**Suggestion:** Preserve the demand/owner identity while independently marking it not
runnable until the future retry, or explicitly establish why configuration loss is an
accepted invalidation. Add an owner-held, operation-capacity-blocked→config0→callback→
restore regression with competing manual/automatic demand. Sent to proof_review before
any edits; reviewer did not change source.

## Source findings otherwise

- Capacity formula matches old max(0,maxBufferSize/2-max(0,buffered-retained10)-reserve)
  divided by max(1,active range count). Defaults produce450. Trigger requests do not
  count as ranges. Retained non-StartOnly records plus queue form the explicitly bounded
  stand-in for old BufferedStarts; do not infer unlimited buffer-equivalence from4-record
  tests. Config getter absence yields0; actual live library supplies the getter.
- Group remaining count and revision are per range. A first transfer captures capacity;
  subsequent same-revision SKIP candidates suppress regardless of fast callbacks. Explicit
  non-SKIP overrides bypass suppression. Revision rebase resets remaining count, retaining
  accepted immutable operation identity and range continuation rules. This preserves
  coordinator treatment of unadmitted revised candidates; it is not proof that an old
  monolithic already-disposed batch can be replayed with a new definition.
- Suppression does not load request payloads or acquire an operation slot. The reducer
  rejects the suppression event for non-SKIP policy and does not decrement action budget
  or increment successful starts. Projection counts exactly one skipped disposition;
  finishBackfill advances the range only after successful applyTransition. Current
  root range is reacquired after root replacement, avoiding writes through a stale pointer.
- Group membership changes are inside the same transaction as handoff/disposition.
  Existing rollback/reload test verifies failed publication leaves membership intact.
  Exclusion-only scan work does not decrement group cardinality; eligible candidate
  transfer does. Range selection/focus remains owned by coordinator arbitration.
- With no owner, zero capacity schedules a future1s wake without immediate scan spin,
  and restores progress when config changes. Future automatic wake takes the earlier
  deadline. Concurrent range-count tests verify450/count arithmetic; they do not yet
  exercise a full adversarial concurrent-range/lease interleaving.

Reviewed tests cover default450, injected2 producing3starts from5 candidates with
callbacks between steps, existing blocker, rollback/reload, zero-capacity retry/recovery,
revision/policy reset and concurrent capacity arithmetic. Parent reports shared
SkipOverlap PASS10.249s. This review does not approve its author's payload changes or
claim a complete old/new concurrent scheduling trace equivalence.

## Correction reviewed

The corrected demand carries `deferred` while retaining class, key and vacancy. Owner
validation therefore preserves the lease; new vacancy allocation, operation arbitration
and runnable work selection all exclude deferred demands. The future retry remains1s.
`TestPublicBackfillBatchBudgetRetainsVacancyLease` covers unfinished4, competing manual
demand, budget0, restoration and selection of the original owner. This regression tests
planner previews directly; it is not an actual callback/database persistence test. No
remaining source blocker was found in this correction. Test execution belongs to the
author/integrator; this reviewer did not rerun or alter source.
