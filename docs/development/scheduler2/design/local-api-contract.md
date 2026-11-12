# D58 — Local public frontend and persisted scheduler2 identity

Specified for WI-014 from current frontend, existing scheduler handler/Describe,
CHASM visibility/query and lifecycle sources. This enables only an explicitly
constructed dedicated test cluster. Default production/frontend dispatch is unchanged.

## Selection, validation and transport

Accept the typed optional cluster-owned ScheduleFrontend seam in local-api-seam.md.
The test factory registers scheduler2 before task processing, binds actual History
engine/visibility to request contexts, and supplies its actual registered mapper.
Keep frontend interceptors, namespace/enabled checks and common validation. Create
also retains payload/search-attribute validation before the selected adapter. No
sentinel creation, migration routing or fallback is permitted for selected requests.
Unsupported Update/Patch/MatchingTimes must return Unimplemented at their selected
dispatch boundary after common validation. Errors never request old-backend fallback.
Public frontend gRPC and real History/SQLite execute; generated SchedulerService
transport is not exercised by this local direct-engine seam.

## Initial public profile and persisted fields

This increment supports paused definitions without inclusion sources, no initial
patch, and no actions executed. A valid StartWorkflow action is required by the
existing frontend payload validator, even though argument validation alone accepts
nil. The action is preserved verbatim but never issued. Nonempty inclusion
sources, unpaused creation or nonempty initial patches return Unimplemented before
publishing state. This is a sequencing limit, not an accepted final API divergence;
all those paths remain required M3 work. Accepted empty specs are still validated
and canonicalized by the frontend; no fabricated workflow template is used.

Initialize the root with NewExecution, binding the allocated RunID and execution
clock, paused/budget/policy controls from a detached Schedule clone. Persist public
Schedule and creation metadata in a separate DATA partition; use the CHASM
Visibility child for custom memo/search attributes. No generator or external intent
is necessary for this empty paused profile. Preserve action, notes, custom memo/SA
and user metadata without aliasing caller objects. Existing frontend size validation
remains authoritative; new DATA layout does not bypass it. No proto interface change
is needed if registered public proto DATA types suffice; regenerate if schema changes.

Describe returns detached public metadata, root control values, zero action/buffer/
running counters for this profile, execution-clock CreateTime, unset/default
UpdateTime, and actual ApproximateStateSize. Match current public defaults: unspecified
overlap resolves to SKIP; default catchup is365d with existing10s minimum behavior.
These are explicit local-profile defaults; configuring runtime/public knobs remains
M3 work. Empty specs have no future actions. Read paths and nil-library offline
inspection cannot read absent runtime configuration or mutate the component.
List memo is ScheduleListInfo from the same detached definition/control projection;
custom memo/SA come from Visibility, not a fabricated old V1 memo.

## Identity, replay and lifecycle

Create uses typed StartExecution with WithStartRequestID. Same retained start request
ID replays framework success without republishing/changing metadata; a different ID
on an existing active scheduler returns public WorkflowExecutionAlreadyStarted.
Malformed frontend input still fails before this dedup boundary. The adapter owns
that explicit public error mapping; frontend selected dispatch propagates it directly.
Conflict token is opaque and stable for this profile, incorporating allocated RunID
and definition revision; Create and Describe agree. This is not yet the full Update
conflict-token implementation. The future Update contract must account for completion/
patch-driven token changes in existing behavior, not assume revision alone suffices.

Delete follows current Schedule API semantics: atomically Close through typed
UpdateComponent, retain history, and return NotFound if already closed or missing.
Do not hard-delete persisted history via DeleteExecution. Recreate after close uses
a new RunID/token; exact old refs stay fenced by retained terminal authority. List
and Count eventually omit closed roots through native lifecycle status.

## Visibility isolation and query compatibility

Use native `ExecutionStatus = 'Running'`; the old
TemporalSystemExecutionStatus alias is restricted to the old scheduler archetype.
Do not register a shadowing ExecutionStatus alias. Typed ListExecutions/CountExecutions
use scheduler2's type and generic archetype filter. Validate the user's predicate
before adding the native base and explicitly reject a resolved namespace-division
predicate via converter.SeenNamespaceDivision, including bare NamespaceDivision.
Otherwise a user predicate can disable the generic archetype filter. Implement this
as an opt-in validator path; unchanged default callers retain existing behavior.

Keep raw ScheduleId predicates: existing ResolveSearchAttributeAlias gives actual
namespace custom attributes precedence, then scheduler2's BusinessID alias resolves
the synthetic ScheduleId. Do not call the old mixed-backend query rewriter. Test
custom values deliberately different from business IDs, ordinary equality/IN/negative
comparisons, prefixed and bare division attempts, old-component collisions, multiple
pages and opaque invalid tokens. Both List and Count must retain typed isolation.

## Evidence before acceptance

Real public create/describe/list/count/delete/recreate and request replay, invalid
input/no-partial-state, independent persisted archetype + typed root assertions,
absence of old scheduler/sentinel records, query isolation, alias collision, common
validation/auth-denial dispatch checks, default-path regression, same-backend cleanup,
applicable race and lint. Public action/callback/Update execution remains a separately
visible dependency; neither this profile nor test hooks can count as full M3 parity.

Source reconciliation: validateSchedulePayloadSize enforces StartWorkflow after validateStartWorkflowArgsForSchedule; the earlier nil-action interpretation considered only the latter and is corrected here without changing public validation.
