# WI-014 local public API seam investigation

Read-only investigation in the WI-012 functional worktree; no routing, registration
or production code changed, and no tests run. This proposes the next independently
reviewable slice, not a decision to enable scheduler2 or claim public compatibility.
Source symbols below are stable locators; line numbers may move with WI-012 edits.

## Existing wiring and constraints

| Source | Observed boundary | Consequence |
| --- | --- | --- |
| `chasm/lib/scheduler/fx.go: Module` | Provides handler, library and executors; invokes registry.Register | Existing service/module owns its registration; scheduler2 is not runtime-installed by its nil offline registry entry. |
| `chasm/lib/scheduler/library.go: RegisterServices` | Registers generated SchedulerService_ServiceDesc with existing handler | Do not register a second handler under that same gRPC service name. Reusing transport request messages does not select a new component identity. |
| `chasm/lib/scheduler/handler.go: CreateSchedule/DescribeSchedule/DeleteSchedule` | Calls typed CHASM start/read/update with existing Scheduler; maps duplicate start and sentinel errors | A replacement client alone needs its own typed implementation. Reusing this handler still executes old scheduler. |
| `service/frontend/workflow_handler.go: WorkflowHandler.schedulerClient` | Injected generated client used for Create/Describe/Update/Patch/Delete/MatchingTimes | Possible local dependency seam, but it does not cover List/Count. |
| `WorkflowHandler.listSchedulesChasm/countSchedulesChasm` | Calls `chasm.ListExecutions[*chasmscheduler.Scheduler,*schedulepb.ScheduleListInfo]` / `CountExecutions[*chasmscheduler.Scheduler]` | Must supply scheduler2-specific typed query path; changing experiment metadata or injected CRUD client cannot fix this. |
| `WorkflowHandler.prepareSchedulerQuery` | Existing CHASM base query includes existing scheduler visibility semantics; rewrites ScheduleID/search-attribute queries | Do not blindly reuse the old union/memo extraction policy. Define scheduler2-only query scope for the isolated slice, leaving mixed-backend compatibility pending. |
| `service/history/fx.go: HistoryChasmRuntimeProvider` | Existing test hook sees engine, visibility manager and registry during graph construction | Concrete runtime registration/context bridge available in test_dep builds; currently testcore captures only engine and visibility handles. |
| `tests/testcore/onebox.go: Start/ChasmContext` | Installs the provider hook while constructing server; ChasmContext binds actual engine/visibility, rejects more than one History host | Local test bridge can exercise actual History transactions and SQLite, with an explicit single-History-host constraint. This is not an arbitrary FX override hook. |
| `tests/testcore/TestClusterConfig.AdditionalServerOptions` | Propagates temporal.ServerOption values | No general per-service fx.Decorate/Replace escape hatch was identified. Add a narrow typed test hook if dependency replacement is needed; do not assume AdditionalServerOptions accepts fx.Option. |
| `common/testing/testhooks/noop_impl.go` | Without test_dep, Get returns no hook; test hook creation/use is disabled | Test-only selection can preserve production defaults, provided all new paths are gated by an actually installed hook. |
| `scheduler2/library.go: NewLibrary/NewNilLibrary` | Independent library name scheduler2 and component/task identities; no RegisterServices override, visibility aliases or memo/search registration yet | Needs explicit runtime registration and public/visibility adapter. NilLibrary permits decoding, not execution service integration. |

## Recommended narrow seam (proposal for parent review)

Use a dedicated local test cluster and a typed optional Schedule frontend adapter,
installed only through test_dep hooks. The adapter surface should contain the five
public operations in this slice, including List/Count; it must not masquerade as an
existing CHASM handler by changing a name or returning canned success. A hook absent
from ordinary builds/clusters leaves current WorkflowHandler paths unchanged.

Keep standard frontend interceptors, namespace lookup, authorization, request/payload
validation and error translation in the request path. The local selection boundary
must precede existing V1 sentinel creation, migration routing and old typed visibility
queries. For explicitly selected scheduler2 requests, an adapter error propagates;
NotFound/Unimplemented must not fall back to V1/current CHASM. A dedicated namespace
or immutable per-cluster backend selection is preferable to a mutable process global.
Do not add a production dynamic-config rollout flag for this experiment.

The existing History runtime provider can register the scheduler2 library before
server task processing starts and capture its engine/visibility handles. Extend that
testcore callback deliberately instead of mutating a live Registry after start.
The test frontend adapter may use those handles to call typed CHASM operations.
That proves public gRPC frontend → adapter → actual History engine/persistence; it
bypasses the generated SchedulerService transport hop and must say so. If that hop
is required for this slice, add a separately named scheduler2 service and generated
client/registration, rather than duplicate SchedulerService registration. The latter
is a larger independent integration decision, not required to prove a first local
public/persistence vertical slice.

An alternative is an injected generated CRUD client plus separate injected typed
visibility functions. It can preserve current common validation more narrowly, but
still needs an explicit non-fallback selection boundary before sentinel/legacy routing.
Do not ship only client replacement: List/Count and cleanup would continue querying
the old component and give misleading success.

## Minimal supported public state

Start with paused/manual-only definitions (empty spec), no initial trigger/backfill,
and no executed actions. Use public Create → Describe → eventual List/Count → Delete
→ Describe NotFound → eventual absence, then duplicate and recreate cases. Reject
unsupported nonempty specs/initial patches explicitly until actual dispatch and
callback integration exists; never accept work without executable task handlers.
This limited test profile is not the shared ScheduleSuite passing against scheduler2.

| Operation | State and behavior required | Discriminating checks |
| --- | --- | --- |
| Create | Engine-allocated RunID via NewExecution; durable Schedule API definition, namespace/business identity, creation/update timestamps, pause/budget/policy, generator template, conflict token; requestID creation dedup | Same ID/same request replays original success; different request on live businessID yields proper AlreadyExists/SDK translation. No old scheduler/sentinel created. Initial public state and custom metadata persist atomically. |
| Describe | Read-only projection of durable definition, state/policies, creation/update times, counts, buffer/running/recent/future projections, conflict token, memo/search attributes and StateSizeBytes | Empty initial counts/lists must be justified by empty fixture, not hard-coded for general state. StateSizeBytes from execution information; no mutation of root/definition/token on repeated reads. Closed returns NotFound before payload access. |
| List | scheduler2 typed visibility registration, ScheduleID business alias, ScheduleListInfo memo plus custom memo/search attributes, active lifecycle filter and opaque pagination | Only correct scheduler2 identities; preserve exact businessID including old-prefix-looking IDs. Page-size/token handling and eventual observations; no query of old scheduler type. |
| Count | Same scheduler2 archetype/query/filter semantics as List | Count matches visible set after convergence; no hard-coded zero or count of all CHASM archetypes. |
| Delete | Exact current reference, persisted Close fence / explicit deletion semantics; visibility close/removal and in-flight authority boundary | Describe immediately rejects closed state; eventual List/Count removal, repeated delete contract specified, recreate gets new RunID and old ref cannot affect it. No migration/sentinel invocation. |

The current scheduler2 Root/Definition are an execution protocol representation,
not a complete public Schedule document. New DATA fields may be needed for durable
API definition/custom metadata and visibility, with root projections for inexpensive
queries. A synthetic Describe response reconstructed from only revision/paused is
insufficient for round-trip assertions. Define public conflict tokens separately
from local source Revision and framework request IDs; never conflate all three.

For Create either explicitly support supplied memo/search attributes through
`chasm.Visibility` or reject them in this initial subset. Existing scheduler
`CreateScheduler` merges custom attributes/memo; `Describe` removes internal
ScheduleInfo from returned custom memo. scheduler2 registration needs corresponding
providers/search definitions and list projection. Copying old internal caches,
worker routing or migration fields is unnecessary and risks implicit coupling.

## Identity proof and local evidence

1. Register scheduler2 independently; assert registry component/task IDs differ
   from existing scheduler IDs and that the runtime has the expected handlers.
2. Call the real public workflowservice client using the explicitly configured
   local descriptor. Inspect persisted mutable state for that namespace/businessID
   with the scheduler2 archetype ID and verify its root type/library and allocated
   RunID. Read with typed `*scheduler2.Scheduler` through actual History context.
3. Attempt the equivalent old `*scheduler.Scheduler` lookup (and V1 scheduler workflow
   lookup where appropriate); require absence. Assert no sentinel creation during
   these local calls. Do not use successful Describe alone as identity evidence.
4. Verify typed List/Count against the persisted new identity, including a distinct
   old-backend schedule in the same namespace when testing exclusion. This slice
   can deliberately use scheduler2-only visibility, but broader mixed-backend API
   semantics remain a named M3 decision, not an accidental difference.
5. Reload/evict History cached state between Create and Describe/Delete; record
   SQLite/backend/namespace config, exact source SHA, commands and output. Recreate
   under the same public ID and deliver old refs to verify terminal isolation.
6. Test an explicitly selected unsupported operation and missing ID. No fallback
   call should reach existing clients. Cleanup must use the same explicit backend,
   typed visibility query and bounded paginated deletion as creation.

## Deferred dependencies that must stay visible

- UpdateSchedule conflict-token behavior, whole public definition replacement,
  memo/search update, Patch request dedup and validation, matching-times API,
  counts/status ordering and all shared tests remain M3 dependencies after this slice.
- StartHandler needs an actual frontend workflow starter with namespace, immutable
  request and callback registration. Effects need real cancel/terminate clients.
  A no-op starter or successful fake callback cannot validate executed schedules.
- Public callbacks need authenticated exact execution/operation routing and request
  identities surviving retry/reset/continue-as-new. Existing internal result tests
  and test_dep bridges do not implement that public contract. Metadata completion
  alone cannot invent observed start time or failure/result carryover.
- Visibility publication is asynchronous; separate immediate Describe assertions
  from eventually consistent List/Count. Query aliases, custom attributes and
  continuation tokens require backend-specific tests, not arbitrary sleeps.
- Default catchup/overlap normalization, ID/request size checks and correct errors
  must reuse or match existing frontend contract. Existing error fallback helpers
  must not silently redirect selected scheduler2 traffic.
- Migration/sentinel ownership and production rollout remain unsupported. Do not
  register scheduler2 into default History/frontend modules or change default
  routing to obtain a functional pass. Test-only registration is explicit.
- Proof/correspondence/spec processing and full functional inventory completion are
  separate M3 gates; passing this vertical slice does not close them or M4 load work.
