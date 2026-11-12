# WI-014 local frontend seam

Specified by D58 (`local-api-contract.md`); frontend/testcluster implementation is
now present for combined verification. Original source investigation used base51448e84a
plus specification fd8e7dd83. Verification results belong in the WI-014 evidence ledger.
The selected scope is public frontend gRPC → local adapter → actual History engine
and persistence. It does not exercise the generated SchedulerService transport hop.

## Exact interface proposal

Define the following narrow interface in common/testing/testhooks (workflowservice
request/response names below are the public API types). Use typed methods, not an
any request dispatcher or a replacement WorkflowServiceServer.

```go
type ScheduleFrontend interface {
    CreateSchedule(context.Context, namespace.ID, *workflowservice.CreateScheduleRequest) (*workflowservice.CreateScheduleResponse, error)
    DescribeSchedule(context.Context, namespace.ID, *workflowservice.DescribeScheduleRequest) (*workflowservice.DescribeScheduleResponse, error)
    DeleteSchedule(context.Context, namespace.ID, *workflowservice.DeleteScheduleRequest) (*workflowservice.DeleteScheduleResponse, error)
    ListSchedules(context.Context, namespace.ID, *workflowservice.ListSchedulesRequest, string) (*workflowservice.ListSchedulesResponse, error)
    CountSchedules(context.Context, namespace.ID, *workflowservice.CountSchedulesRequest, string) (*workflowservice.CountSchedulesResponse, error)
    SearchAttributesMapper() *chasm.VisibilitySearchAttributesMapper
}
// The final string arguments above are validated, isolated visibility queries.
// Namespace names remain available in the public requests.
var FrontendSchedule = newKey[ScheduleFrontend, global]()
```

Thread the cluster-owned testhooks.TestHooks dependency into WorkflowHandler through
its constructor/FX (the current NewWorkflowHandler signature does not contain it).
Update direct constructor callers explicitly. Look up FrontendSchedule at request time; do not capture its
absence during construction, since service graph construction order is not an API.
Hook existence selects the dedicated cluster for all Schedule requests, regardless
of experiment headers, rollout percentages, schedule ID, or test name. A selected
adapter error is returned directly; it never means “try another scheduler.”
Non-test_dep Get always returns false (common/testing/testhooks/noop_impl.go:29).
No new production configuration or rollout flag is needed.

In tests/testcore, `WithScheduleFrontend(factory ScheduleFrontendFactory, frontendLibraries ...chasm.Library)` selects a
dedicated cluster. Its parameter is threaded through testClusterParams, TestClusterConfig
and temporalParams into temporalImpl:

```go
ScheduleFrontendFactory func(chasm.Engine, chasm.VisibilityManager, *chasm.Registry) (testhooks.ScheduleFrontend, error)
```

Wire it into the existing HistoryChasmRuntimeProvider callback installed by
onebox.go:290. Require exactly one History host before constructing this profile.
Keep the existing engine/visibility capture; invoke the factory once, register the
scheduler2 library before server.Start, and install FrontendSchedule on the same
cluster-owned TestHooks. Capture a factory error and fail Start before server.Start;
the existing runtime-provider callback cannot return an error. Keep hook cleanup
until cluster shutdown (the current capture hook itself is intentionally removed
after graph construction). No package-global hook or live registry mutation. Frontend has a separate registry;
its construction hook registers explicitly supplied offline frontendLibraries before
server.Start so Admin and visibility can resolve scheduler2 identity. Registration
errors or a missing expected hook fail startup. The public profile passes
scheduler2.NewNilLibrary for this registry.

The factory implementation belongs in a test_dep helper package. It owns immutable
engine/visibility handles, binds them onto each incoming context using
chasm.NewEngineContext/NewVisibilityManagerContext, and preserves the request's
cancellation, deadline and metadata. Its mapper comes from the registered scheduler2
component in this History registry. This avoids relying on the frontend registry
having scheduler2 registered merely because History does. Construction must fail
if registration or mapper lookup fails. Validate actual registry initialization
ordering when implementing; never solve a registration-order failure by mutating
a running registry. Do not register another SchedulerService_ServiceDesc.

## Frontend insertion points and retained checks

References below are service/frontend/workflow_handler.go at the inspected source.
Public gRPC still runs the existing frontend interceptor stack, including
AuthorizationInterceptorProvider (service/frontend/fx.go:137), namespace validation,
rate limiting, telemetry and error handling. The hook is inside the handler, not
an alternative public server. An authorization-denied integration case should prove
that an adapter is never invoked on rejection.

| Method | Insertion and required ordering |
| --- | --- |
| Create,3917–3989 | Resolve hook presence before time-skipping validation so selected scheduler2 is treated as CHASM for that check, independently of old rollout. Keep nil/enabled checks, prefixed workflow-ID length validation, request-ID checks, overlap/patch timestamp validation, canonicalization, time-skipping validation, start-workflow validation and header metrics. At final dispatch, selected path resolves namespace ID, runs search-attribute validation and payload validation currently at3652–3669, then calls adapter. Do not enter createScheduleCHASM: its sentinel write begins3672. Extract those common checks or invoke identical helpers explicitly; no sentinel helper reuse. |
| Describe,4528 | After nil/EnableSchedules checks, resolve namespace ID and return selected adapter result before chasmSchedulerEnabled/fallback. |
| Delete,5134 | After nil/EnableSchedules checks, resolve namespace ID and return selected adapter result before either stack is attempted. Do not enter dual-stack deletion/metadata winner selection. |
| List,5228 | Retain nil/enabled, page-size normalization, namespace resolution, DisableListVisibilityByFilter, query-validation metrics. Selected query preparation precedes final typed adapter dispatch; do not invoke old listSchedulesChasm or old prepareSchedulerQuery(true,...). |
| Count,5433 | Same common validation/query path as List, without page-size handling. Return selected typed count; no old query/dispatch fallback. |
| Update4752/Patch4904/MatchingTimes5040 | Selected profile must return explicit Unimplemented after existing common validation and before any old dispatch until implemented. Add presence guards at their dispatch boundary, not only the five successful methods. An absent hook retains every existing branch. |

Create AlreadyExists requires explicit public mapping to
WorkflowExecutionAlreadyStarted, matching3669–3710 without the sentinel/routable-error
conversion. Preserve all other service errors, particularly NotFound, InvalidArgument,
Unavailable and cancellation. Parent defines public duplicate-request and conflict-token
semantics; this seam must neither invent success nor translate errors into fallback.

## Query preparation must be independently scheduler2

The existing base service/worker/scheduler/fx.go:42 explicitly ORs TemporalScheduler
with chasm.SchedulerArchetypeID. It is unsuitable. chasm/visibility_manager.go:75–82
also warns that an explicit TemporalNamespaceDivision overrides generic type filtering.
Therefore merely substituting a generic type while keeping the old base is wrong.

Add a selected query preparation helper using the same namespace search-attribute
provider, mapper provider, scheduler.ValidateVisibilityQuery, metrics and logger as
prepareSchedulerQuery:5278. Supply adapter.SearchAttributesMapper(), never
wh.registry.ComponentByID(chasm.SchedulerArchetypeID). Base query is exactly:

```
ExecutionStatus = 'Running'
```

The earlier proposal used `TemporalSystemExecutionStatus`; source review rejects
that choice. `query/resolve.go:73` recognizes it only for the existing
`chasm.SchedulerArchetypeID`, and `TestQueryConverter_TemporalSystemExecutionStatus`
plus `TestResolveSearchAttributeAlias` explicitly reject other archetypes.
Scheduler2 must use the native `ExecutionStatus` field and must not register a
CHASM alias/override that shadows it. `chasm/tree.go:1952` maps both running and
paused lifecycle states to persisted RUNNING, and completed lifecycle to COMPLETED;
that native field provides the desired open-schedule filter. This is not a change
to the old scheduler's query path.

Append a nonempty validated user predicate in parentheses. Before adding the base,
reject any USER predicate for which the query converter reports
`SeenNamespaceDivision()`. The existing reserved-alias check alone is insufficient:
`NamespaceDivision = 'some-other-archetype'` resolves via Temporal-prefix fallback
to `TemporalNamespaceDivision`, but the former alias is not reserved. The current
scheduler validator records `SAColumn.Alias`, so it does not reject this spelling,
and the final converter suppresses its automatic type filter when the resolved
namespace-division field was seen. This source-derived counterexample was not
executed during this investigation; it must become a regression test.

Add an isolated-query validation sibling sharing the existing validator internals:
keep `ValidateVisibilityQuery` behavior unchanged for existing backends, and expose
`ValidateIsolatedVisibilityQuery` (or an equivalently named focused function) that
also rejects the explicit resolved namespace-division flag from the user's query.
`getQueryFields` can return both its existing alias set and the converter's flag;
the existing validator ignores the new flag, while the selected path requires it
false. Use the existing converter and mapper rather than an ad hoc SQL identifier
scan. The automatically injected default division is not an explicit user field:
`SeenNamespaceDivision()` already distinguishes it. Direct/prefixed identifiers,
parenthesized/negated predicates and namespace aliases that resolve to that field
must all reject in the isolated profile. This keeps typed filtering mandatory. Typed calls in the adapter are exclusively
chasm.ListExecutions[*scheduler2.Scheduler,*schedulepb.ScheduleListInfo] and
chasm.CountExecutions[*scheduler2.Scheduler]. They bind the scheduler2 archetype.
Do not call RewriteScheduleIDQuery with either boolean: false adds the legacy prefix;
true constructs legacy/current OR comparisons (schedule_id_query_rewriter.go:15–29).
Register BusinessID alias `ScheduleId` on scheduler2 and pass the original user
predicate unchanged to the standard converter with that mapper and the current
namespace mapper/type map. No new AST transform is needed for identity queries;
this preserves the namespace custom-attribute precedence detailed below. Verify
equality, IN, negative comparison, invalid attributes, collision semantics and
page token errors against actual visibility.

The mapper-only interface intentionally does not allow the adapter to provide an
arbitrary base query. Frontend selected-query helper and typed dispatch together
own isolation. Test with old V1/current CHASM rows sharing schedule IDs: neither
List nor Count may include them, including nonempty queries and subsequent pages.
Do not infer V1 entries from empty Spec in this adapter; its list memo is exclusively
the scheduler2 projection. Preserve custom memo/SA and opaque pagination tokens.

## Resolved ScheduleId precedence

`common/persistence/visibility/store/query/resolve.go:ResolveSearchAttributeAlias`
checks these sources in order:

1. A mappable namespace custom alias, resolved by `Mapper.GetFieldName` and the
   returned field's type in `NameTypeMap`.
2. The supplied CHASM mapper, including `WithBusinessIDAlias("ScheduleId")`.
3. A directly named system/predefined field.
4. Special aliases and Temporal-prefix fallback.

`ScheduleId` is mappable (`sadefs.IsMappable`) and is not implicitly reserved merely
because scheduler2 registers it as a business-ID alias. Thus a namespace mapping
`ScheduleId -> Keyword01` wins over the component's `ScheduleId -> WorkflowId`.
The standard `MapperProvider.GetMapper` returns a `backCompMapper` whose
`GetFieldName` falls back to identity for legacy custom fields registered directly
in cluster metadata. A directly registered custom `ScheduleId` therefore also wins
under the normal provider; a `NoopMapper` similarly resolves it through the type
map. Without either custom registration, the component mapper selects unprefixed
`WorkflowId`, and typed archetype filtering still excludes old schedulers.

This agrees with the intent of `IsUserDefinedSearchAttribute` and the old
`RewriteScheduleIDQuery` custom-attribute exception, without importing its V1
prefix or mixed-backend OR/AND rewrites. Existing resolver tests already assert
custom-over-CHASM precedence (`TestResolveSearchAttributeAlias_WithChasmMapper`,
case `AliasForKeyword01`), synthetic/custom ScheduleId resolution, and business-ID
alias resolution. These are source/test inspections, not newly executed results.

Preserve the caller's mapper contract: `MapperProvider` returns an explicitly
provided custom mapper unchanged. Such a mapper that reports an error for a field
registered directly in the type map does not receive the standard compatibility
fallback and may fall through to the CHASM alias. Do not invent a scheduler2-only
fallback or silently claim support for this inconsistent custom configuration.
The local profile should use the standard provider; if an injected custom mapper
is used in a test, it must faithfully resolve its configured custom fields. Exact
support for a custom mapper deliberately declining a registered alias remains the
same shared resolver contract, not a new scheduler2 divergence.

Required discrimination tests in the actual selected path:

| Input setup | Required observation |
| --- | --- |
| No custom ScheduleId; actual business ID A, another row B | `ScheduleId = A`, IN, !=, NOT IN and prefix predicates use the unprefixed business ID and never match V1-prefixed/old-archetype rows. |
| Namespace alias ScheduleId -> Keyword01; custom values deliberately different from business IDs | The predicate follows custom values; same-ID data is insufficient to distinguish a mistaken synthetic mapping. |
| Legacy directly registered custom ScheduleId with standard provider | Same custom-value precedence; back-compat identity mapping must be exercised. |
| Custom attribute removed/no longer mapped | Subsequent queries resolve according to the current namespace/type map, not a stale captured adapter mapper. |
| User division filter under bare, Temporal-prefixed or mapped alias | Isolated validator rejects, so no predicate can disable scheduler2 type filtering. |
| `ExecutionStatus` native base with scheduler2's mapper | Open paused scheduler2 rows are visible; completed/deleted roots are absent after eventual visibility. The old-only TemporalSystemExecutionStatus spelling is not used. |

The reserved `TemporalScheduleId` spelling has separate existing frontend query
validation behavior; do not broaden it merely because the lower-level resolver
can translate it. Public rejection/acceptance follows the shared validator. The
synthetic unreserved `ScheduleId` and a custom field of that name are the required
collision cases for this slice.

## Required proof and remaining decisions

The concrete test profile factory must construct actual scheduler2 registration,
visibility providers/alias and API adapter; absence is a startup failure. Assert
persisted archetype identity and typed scheduler2 root after Create, absence of old
scheduler/V1/sentinel state, typed list/count under eventual visibility, Delete and
same-ID recreate with old reference fenced. Public success/header/name alone is
insufficient. Verify normal clusters retain unchanged behavior with hook absent and
selected errors never invoke old clients. Use independent persisted-state and query
observations rather than checking the adapter's own selected flag.

D58 owns the precise public stored definition/projection, supported paused
profile, conflict tokens, memo/search-attribute acceptance, duplicate Create,
delete/recreate behavior and unsupported-input errors. This proposal leaves those
to the parent implementation contract. Active dispatch, callbacks/authentication,
all public controls, shared-suite parity and the generated service transport hop
are not established by the five-method paused slice. The initial source investigation ran no tests. Implementation adds selected dispatch/
no-fallback and exact query preservation frontend tests, plus opt-in isolation and
custom ScheduleId precedence query tests; combined runs are owned by the parent.

Implementation evidence correction: `validateSchedulePayloadSize` rejects a nil
StartWorkflow action, even though the earlier action validator accepts nil. Retaining
common validation therefore requires an explicitly supplied valid inert StartWorkflow
action in the local public profile; no frontend template is fabricated.
