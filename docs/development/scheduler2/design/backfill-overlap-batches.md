# Public backfill overlap batches (WI015 investigation)

Status: implemented with targeted and full package validation; independent review/acceptance pending.

## Observed incompatibility

The full scheduler2 shared suite's SKIP backfill case submits five historical ticks and expects one start. The original CHASM backfiller enumerates a bounded batch and atomically enqueues all selected starts. Invoker ProcessBuffer resolves all their overlap decisions before issuing any RPC. Consequently a fast workflow cannot complete between selection of two candidates in that same batch. Scheduler2's accepted coordinator deliberately performs one scan or candidate handoff per transaction; a workflow can complete between two range handoffs, reopening SKIP admission. The current per-candidate implementation therefore creates more starts for the same public test.

The old semantics do **not** establish a permanent one-start limit for an arbitrarily large range: a later backfill batch can run after an earlier batch's workflow completes. A whole-range latch would be incorrect.

## Exact reference capacity

`chasm/lib/scheduler/backfiller_tasks.go:allowedBufferedStarts` counts active range backfillers (excluding immediate trigger backfillers) and delegates to:

```
pending = max(0, len(invoker.BufferedStarts) - recentActionCount)
available = max(0, MaxBufferSize/2 - pending - GeneratorBufferReserveSize)
limit = available / max(1, activeRangeCount)
```

`recentActionCount` is 10. Default MaxBufferSize is1000 and GeneratorBufferReserveSize is50, giving450 candidates for one empty-system range,225 for two. Zero capacity waits with backoff; it is not silently raised to one. These are logical overlap-batch cardinalities, independent of the new implementation's bounded per-transaction enumeration or admission work.

## Proposed representation and ownership

For public roots only, sample the live runtime tweakable getter at the first SKIP handoff of a new logical group. Use the reference capacity formula, with new representation's logical buffered count = execution queue length plus retained tracked operation records (exclude StartOnly). The accepted prototype has at most four queue entries and four operation records, so this count is at most8; the legacy retained-history allowance of10 remains explicit rather than replaced with an invented subtraction. The cardinality mapping is consequently conservative about old available capacity under the prototype's smaller storage bounds; it is not a claim of identical buffer contents.

Persist only group remaining-count and defining revision in BackfillState. A group applies only while the currently resolved policy remains SKIP. The first candidate uses normal overlap admission/disposition. Each subsequent eligible candidate in the group receives a durable SKIP disposition even if the first workflow completes between transactions. Excluded raw occurrences do not count as candidates. Group exhaustion permits a new capacity sample and new first-candidate overlap decision. Range exhaustion removes the record; cancellation/deletion removes authority with it. A blocked first candidate does not consume group membership. Zero cardinality waits with a bounded retry wake.

Definition rebase resets grouping before processing candidates from the new revision. An unspecified override resolving to a different current policy also ends the old SKIP group. This preserves the accepted current-definition/backfill rebase contract: unlike old materialized batches, no unenumerated old-definition candidates are retained. Explicit non-SKIP overrides retain their existing behavior. Internal public_revision=0 executions keep per-candidate semantics, so existing finite model fixtures remain honest about their scope rather than silently being reinterpreted.

A dedicated decision-event marker expresses authorized in-group SKIP, checked only for DispatchBackfill with resolved SKIP. Demand construction suppresses operation-slot contention for that disposition but retains the existing vacancy entitlement and exactly one disposition per successful handoff. The root coordinator remains sole owner. Publication failure rolls back group metadata and candidate disposition together. Public overlap-skipped counts include group dispositions even when the first workflow is already complete.

## Required verification before acceptance

- Deterministic completion between every candidate: five ticks in one public batch yield one start; a range crossing a deliberately small configured batch boundary may yield more starts.
- Existing initial blocker: whole current group skips even when blocker completes after its first disposition.
- Independent ranges, dynamic capacity/reserve settings and zero-capacity recovery; no positive minimum fabricated.
- Definition rebase and current-policy changes reset the group as documented; cancellation/deletion removes it; request deduplication cannot restart it.
- Failed publication/reload retains group/candidate identity, and exact candidate/counter progression.
- Existing coordinator fairness, replay, model mapping and source selectivity remain valid. Add explicit public grouping refinement scope to evidence; do not reuse internal per-candidate property expectations as proof of public grouping.
- Re-run unchanged shared SKIP backfill case and full coordinator package tests/lint; include mutation sensitivity for dropped group persistence or ignored marker if practical.


## Implementation and verification checkpoint

`backfill_batch.go` owns the context config key and source-derived capacity calculation. The library binds the live namespace tweakable getter before registration. `demands` retains vacancy arbitration, removes operation-slot contention for authorized group dispositions, and publishes a one-second future retry when a fresh group has zero budget. `transferBackfill` publishes the reducer disposition and remaining counter in the same transaction; subsequent `DisposeRange` commits the exact candidate. Runtime fields are persisted in BackfillState and serialized across reload. A marker is legal only for SKIP DispatchBackfill; it cannot debit RemainingActions or allocate a start identity.

Validation commands ran in the execution worktree with `CGO_ENABLED=0 GOMAXPROCS=4` and `-tags test_dep` (plus `integration` for functional):

- `make proto`: PASS regenerated additive fields.
- `go test -tags test_dep ./chasm/lib/scheduler2 -run '^TestPublicBackfill' -count=1`: PASS0.730s after correcting test-only setup issues (carryover-bearing legacy fixture rejected by new public-template invariant; mutable node pointer captured after tentative failed publication). No production fix was inferred from those fixture errors.
- `go test -tags test_dep -p 2 ./chasm/lib/scheduler2/... -count=1`: PASS root19.054s, decision0.928s, occurrence1.101s. Includes existing coordinator Rapid/replay and added rebase/policy/concurrent-capacity cases.
- `go test -tags test_dep,integration -p 2 ./tests -run '^TestSchedule2SharedSuite$/TestBackfill/SkipOverlap' -count=1 -timeout=3m`: unchanged shared case PASS10.249s.
- Final style/refactor baseline `go test -tags test_dep ./chasm/lib/scheduler2 -run '^TestPublic(Backfill|Projection)' -count=1`: PASS0.675s.
- Read-only Go overlay mutation disables `suppressBackfill`: deterministic default450 group incorrectly starts all5 (expected1), configured2 group starts all5 (expected3). Named assertion failures demonstrate sensitivity; no tracked source was rewritten. Same test baseline passes. This directed mutation has no shrink claim.

Raw commands/results, exact overlay, and source fingerprints live under `evidence/wi015/backfill-batches/`. Initial lint found two own assertion-style issues and the projection branch's complexity increase; corrected them by using Positive and extracting the pure overlap counter helper. Parent payload-storage findings were reported separately. Final consolidated lint is required after parent changes.

Model domain mapping: existing finite coordinator/FizzBee and Rapid models are the internal `PublicRevision=0` per-candidate design, unchanged. They do not prove the added public group counter. New public tests explicitly force callback completion after every admitted candidate, publication failure before every coordinator grant, and serialized reload; reference expected starts are ceil(candidateCount/groupSize), with the first group wholly suppressed when an external blocker is present. They check no action-budget debit/cursor movement, exact OverlapSkipped counts, rebase reopening, and zero-budget recovery. Enumeration Lean proofs are unaffected finite-stream candidate/disposition obligations, not a proof of public group policy.


Independent review correction: api_seam identified that removing a zero-budget demand also cleared an existing vacancy owner, violating the accepted lease invariant. The final implementation retains the demand identity with `deferred=true`, excludes it from new lease acquisition and service/operation selection, and retains an already-held lease until normal handoff/invalidation. It still publishes a future one-second retry. New `TestPublicBackfillBatchBudgetRetainsVacancyLease` covers operation-capacity exhaustion, config budget loss, competing manual ingress, and restoration to the same owner. All `Test(PublicBackfill|Coordinator)` passed4.146s after this correction (`scheduler2-wi015-backfill-lease.log`). This explicitly preserves entitlement even when external configuration prevents progress; it does not claim progress while budget remains zero forever.

Final full `make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false` (lint03) had no backfill/projection findings; its sole remaining finding was other-owner payload_storage.go confusing unnamed same-type results. Reported to parent for consolidated correction.
