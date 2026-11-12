# WI-014 independent public component review

Scope: `public.go`, `public_api_test_dep.go`, `public_test.go`, and the component/library
additions in `/private/tmp/sch-redesign-api`, inspected against D58 and current legacy
scheduler/frontend and CHASM engine/visibility sources. Review performed independently
of the implementation owner. No tests were run by this reviewer; parent owns combined
builds and the actual dedicated-cluster functional test.

## Findings

No additional definite implementation defect found in this source review after the
D58 nil-action correction. The initial contract assertion that nil actions were accepted
conflicted with `WorkflowHandler.validateSchedulePayloadSize`, which rejects missing
StartWorkflow. The parent corrected D58 and NewPublicSchedule to require a supplied
valid inert action; the frontend retains common payload validation.

## Source evidence

- NewPublicSchedule validates the paused, no-inclusion, no-patch profile before root
  construction. It rejects invalid remaining-action and overlap values, initializes
  through NewExecution, and binds the allocated execution RunID/clock. The public
  action, including its UserMetadata, remains within a detached Schedule clone.
- PublicSchedule, PublicInfo and PublicList use separate DATA fields. Memo reads only
  PublicList plus root state; Describe clones stored public data and custom Visibility
  values before returning them. Current root pause/budget/policy values drive Describe.
- Default overlap SKIP and catchup365d/minimum10s match the frozen D58 profile and old
  scheduler defaults. Describe uses actual ExecutionInfo.ApproximateStateSize and
  GetTimeSkippingInfo. Initial UpdateTime matches the old CHASM zero-time convention.
- The returned initial conflict token combines actual run identity and revision1.
  Create uses WithStartRequestID and maps ExecutionAlreadyStartedError to public
  WorkflowExecutionAlreadyStarted. Existing CHASM StartExecution performs construction
  before conflict/dedup resolution; rejected changed retry payloads therefore need not
  reach dedup, consistent with retaining validation before the replay boundary.
- Delete performs typed UpdateComponent and Close. It rejects already closed roots,
  retains durable history, and never calls DeleteExecution. Existing lifecycle code
  exposes Closed as Completed; the native running predicate excludes it eventually.
- The library preserves independent scheduler2 identity and adds only BusinessID's
  ScheduleId alias. It does not shadow ExecutionStatus. TemporalSchedulePaused uses
  the existing predefined search attribute, as in the old scheduler.
- Both public List and Count invoke independent generic APIs typed to scheduler2.
  List decodes ScheduleListInfo and copies custom memo/search attributes. Neither
  adapter path invokes the old scheduler, workflow scheduler, or sentinel creation.
- NewPublicTestFrontend is test_dep-only and registers NewNilLibrary through the actual
  History registry supplied by the construction hook. This empty paused profile emits
  no generator or action tasks that would require the absent side-effect handlers.

## Evidence limits and required integration checks

The component unit tests exercise caller/reader ownership for notes and memo, persisted
reload, close visibility to Describe, compact list reads with a poisoned definition
partition, and rejected creation without new DATA partitions. They do not independently
prove the engine's public retry/recreate semantics, namespace alias mapping, persistence
archetype, visibility pagination, opaque-token validation or actual History startup.
The helper overwrites the action with an inert action, so these tests also do not yet
prove action/UserMetadata roundtrip or specifically exercise nil-action rejection.

The actual dedicated-cluster functional test must verify registry registration before
service/task start and public gRPC→History/SQLite behavior. In particular, cover real
custom ScheduleId values different from business IDs, old-archetype collisions,
List/Count division rejection, closed-row disappearance, retained old references and
new-run recreation. Add action/UserMetadata, catchup defaults/minimum and actual size
observations to the public fixture where practical. These are evidence gaps, not claims
of demonstrated failures. No production routing or generated SchedulerService transport
parity is established by this seam.

## Actual-cluster registration correction

The first public cluster run reached successful Create/Describe and independent typed
Read, then Admin DescribeMutableState rejected scheduler2 archetype3600483531. Source
inspection traced this to AdminHandler.validateAndResolveArchetypeID consulting the
frontend's distinct registry; the original factory registered only History's registry.
The persisted identity assertion remains intact.

WithScheduleFrontend now accepts explicit offline frontend libraries alongside its
runtime factory. The test supplies scheduler2.NewNilLibrary. A scoped frontend FX
construction hook registers these libraries before server.Start, propagates registration
errors, and is removed after graph construction. Missing invocation fails startup when
libraries were supplied. History runtime registration remains owned by the factory.
Each registry is cluster/service local. Ordinary clusters supply no libraries; production
TestHooks.Get is a no-op. No default scheduler selection or live registry mutation is
introduced.

Focused rerun command (from `/private/tmp/sch-redesign-api`):

```sh
CGO_ENABLED=0 go test -tags test_dep,integration ./tests -run '^TestSchedule2PublicPausedProfile$' -count=1 -v
```

Output: `/private/tmp/scheduler2-wi014-public-registry.log`. Result is recorded below
when the process completes. The parent owns subsequent pagination additions and broader
verification.

Focused rerun **PASS**: TestSchedule2PublicPausedProfile (0.17s), package1.402s,
exit0. The formerly failing Admin persisted-archetype assertion now passes unchanged,
as do the current replay/duplicate/close/recreate, public validation/no-state, query
isolation and same-backend cleanup checks in that test. This verifies the construction
hook runs before serving in this one-History dedicated SQLite cluster.

## Final WI-014 component and public-test review

Re-read the complete current public.go, public_api_test_dep.go, public_test.go and
expanded tests/schedule2_public_test.go against corrected D58, with the surrounding
CHASM lifecycle, start/dedup, visibility mapping and old scheduler projections already
inspected above. This review does not approve the reviewer's own frontend/testcore seam;
that portion requires the parent's independent review.

No new definite component/adapter contract flaw found. The expanded real-cluster test
adds discriminating checks beyond merely selecting a named adapter:

- A real old scheduler with the same business ID is visible through its independently
  typed Count. Public scheduler2 List/Count return only the new root; deleting and
  recreating scheduler2 leaves the old root open and closes it through its own type.
- Admin database mutable state and typed root identity agree on the independently
  registered scheduler2 archetype and allocated RunID. No old scheduler/sentinel exists
  before the deliberate collision fixture is created.
- Same-request replay preserves original notes/token; a different request ID gets the
  public duplicate error; delete/recreate produces another token and retains the closed
  prior typed run. Native visibility eventually removes the closed current row.
- Two scheduler2 rows traverse page-size-one pagination with bounded termination and
  exact IDs; synthetic equality/IN/negative predicates, invalid opaque tokens and both
  List/Count division rejection are exercised.
- A namespace custom ScheduleId alias stores a value different from the business ID.
  Both List and Count find the custom value; querying the business ID gives zero,
  distinguishing custom precedence from synthetic-ID translation.
- Missing action and the three unsupported creation profiles return the appropriate
  error and selected Describe cannot find a new root. Authorization denial occurs
  before the observed adapter Create counter advances.
- Actual action and memo roundtrip, nonzero persisted size and empty action/future
  observations are checked. Added component catchup tests cover default365d,
  minimum10s and explicit1h behavior.

Two remaining preservation assertions were recommended to the parent: include nonempty
Action.StartWorkflow.UserMetadata in the fixture so action equality exercises that
field, and inspect returned SearchAttributes payload/alias in both Describe and List
for the custom-SA row. Existing query assertions prove indexing and predicate precedence,
but do not by themselves prove public attribute return projection. These are targeted
evidence gaps, not demonstrated implementation failures; cloning/Visibility reads are
correct by inspected source. Parent owns any added assertions and final race/lint logs.

Acceptance remains WI-014's paused persistence profile only. Full execution, public
Update/Patch/MatchingTimes, live projections, real callback delivery and shared-suite
parity remain explicit next-work dependencies documented separately. This review does
not claim M3 completion or generated SchedulerService transport coverage.

### Returned-attribute gate found a real boundary defect

The parent added the recommended nonempty action summary/details and returned custom-SA
assertions. The latter failed in both public-final and race-final: List leaked
TemporalNamespaceDivision and TemporalSchedulePaused alongside the custom ScheduleId.
This supersedes the earlier source-review assessment that returned visibility data
was sufficient: typed visibility results still include attributes that the existing
frontend cleans before exposing ScheduleListEntry.SearchAttributes.

The selected List success path now applies the same existing
WorkflowHandler.cleanScheduleSearchAttributes helper to every returned entry. Direct
adapter errors still return without fallback. A focused frontend regression asserts
system-field removal, custom ScheduleId preservation, nil normalization for a row with
only system attributes, and opaque-token preservation. The parent owns reruns and
independent review of this seam correction; the public assertion was not weakened.
