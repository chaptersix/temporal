# WI-015 public mutation increment

Implementation in the execution worktree, uncommitted pending integrator gates.
The first transport implementation was preserved before shared-file edits under
`transport-source-snapshot/` with SHA256 manifest and source stored as `.go.txt`.

## Behavior

The live test frontend now creates initialized generators for empty/nonempty and
paused/unpaused schedules; the inert WI-014 constructor remains unchanged in scope.
Initial patch controls join creation in one engine transaction. Update replaces future
generation, public definition, optional memo/search attributes, compact list projection
and public token under one WithRequestID transaction. Patch performs all pause/unpause,
manual trigger and backfill parts under one WithRequestID transaction. Time-skipping
config is applied through the actual CHASM mutable context. MatchingTimes uses the
shared compiled spec, actual configured search iteration limits, stable namespace-ID
seed and existing fixed 1000-result cap (old scheduler.go:104).

Root PublicRevision is independent of definition Revision. New public roots start at1;
initial patch keeps token1. Explicit successful Update/Patch advances it once, after
an overflow precheck. Supplied nonnil conflict tokens compare inside Update transaction;
retained successful request IDs bypass repeat validation/mutation through the framework.
Nil memo/SA preserves; supplied empty clears; supplied values replace. Inert selected
backends remain explicitly unsupported for execution controls without legacy fallback.

Optional ScheduleMutationFrontend routes through the same common frontend validation
as the existing selected paths. Direct selected errors return unchanged. The real start
wrapper validates detached generated callback bytes with configured workflow callback
kinds and ValidateAdditions before invoking the actual frontend, which also performs its
normal authenticated validation. No runtime dependency is persisted in the root. An
error here remains potentially ambiguous for an earlier issued request, per D59-E1.

Backfill and trigger UNSPECIFIED policies remain unresolved until the existing reducer
uses current schedule policy, then SKIP. The draft's premature backfill-to-SKIP conversion
was caught in source review and removed. This follows old NewRangeBackfiller and
backfiller_tasks.go:processBackfill/resolveOverlapPolicy behavior.

Parent-owned component integration increments PublicRevision for limited automatic
admission and newly paused failure callbacks, and freezes scheduled-by/scheduled-time
search attributes. Independent source review found the counter overflow check precedes
publication; root replacement preserves the counter; duplicates/ordinary success do not
advance it. Generated attributes overwrite reserved values on the detached frozen request.
The parent calls projectPublicPause to publish the old scheduler's exact lowercase-status
failure note without a second counter bump.

## Verification

- `/private/tmp/scheduler2-wi015-public-mutation-initial.log`: focused creation/token/
  inert tests pass; frontend compiled but regex matched no tests.
- `/private/tmp/scheduler2-wi015-public-mutation-focused.log`: focused component tests
  pass; frontend compiled but regex again matched no tests. These are not frontend
  behavior evidence.
- Corrected command: `CGO_ENABLED=0 go test -tags test_dep ./service/frontend -run
  '^TestWorkflowHandlerSuite$/TestSelectedSchedule' -count=1`.
  `/private/tmp/scheduler2-wi015-public-mutation-frontend.log`: PASS0.906s. Covers inert
  unsupported controls and selected mutation error propagation/no fallback, with common
  invalid-input rejection before dispatch.
- `CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2 -run
  'Test(ExecutingPublic|PublicMutation|PublicRevision|PublicAdmission)' -count=1`.
  `/private/tmp/scheduler2-wi015-public-mutation-atomic.log`: PASS1.581s. Tests initial
  paused+trigger patch, independent counters, conflict and overflow rejection, malformed
  final backfill after earlier pause/manual writes, injected publication failure, retained
  successful request-ID replay with changed invalid body, and late visibility rejection
  after generation changes. Failed transitions preserve the exact durable tree and do
  not consume request IDs. The option-aware mock engine uses real CHASM request-ID/tree
  state but does not establish History database rollback.

The integrator owns live public Create/Patch workflow transport, independent source
review and combined lint/race gates. A later import/staticcheck-only unused-context fix
was applied before the combined lint; no behavior changed.

## Remaining full-WI-015 obligations

This increment is not acceptance of all M3: complete live public information projection,
shared suite parity, execution-time configuration semantics, aggregate large-state
capacity, complete recovery/error matrix, and remaining supported-range/backfill-capacity
compatibility are still governed by D59. Ordinary remote permanent errors do not become
terminal drops. Parent owns payload-bound reconciliation and model/evidence updates.


## Checkpoint follow-up

Final scoped source/log hashes and archived logs are in
`public-mutation-checkpoint/manifest.json`. Independent oracle review caught Describe's
hardcoded catchup default/minimum: executing roots now return persisted
Generator.CatchupWindow, retaining the inert profile fallback. A configured one-hour
default and two-minute minimum survive reload and agree with Describe. Scoped command
`CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2 -run
'Test(PublicDescribeUsesFrozenConfiguredCatchup|PublicCatchupWindowDefaults|ExecutingPublic|PublicMutation)'
-count=1` passed0.732s (`scheduler2-wi015-public-mutation-final.log`).
Parent reports actual public Create/Patch transport passed10.481s; parent owns that log
and its provenance. Combined lint/checkpoint review remains integrator-owned.
