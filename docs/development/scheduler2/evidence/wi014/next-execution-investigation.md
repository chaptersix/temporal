# M3 public execution investigation for the next work item

Source investigation only; WI-015 is not specified by this document. No execution code
or test selection changed. The intended endpoint remains complete M3 public scheduler2
execution and shared-suite coverage, not acceptance of the WI-014 paused slice as parity.
Paths below are relative to the repository at the WI-014 working tree.

## Existing mechanisms and missing connections

| Boundary | Actual source evidence | Required connection |
| --- | --- | --- |
| Public creation | `chasm/lib/scheduler2/public.go:NewPublicSchedule` rejects nonempty inclusion sources, unpaused state and initial patches; initializes no Generator | Convert the validated public definition into GenerationConfig and atomically install public DATA, controls, generator and initial patch. Remove these profile exclusions only when real handlers exist. |
| Runtime registration | `scheduler2/library.go:NewNilLibrary` is used by the public test factory; NewLibrary accepts concrete StartHandler/EffectHandler | Dedicated History factory must register NewLibrary with real clients; frontend keeps offline identity registration. Preserve pre-start registration and actual independent archetype assertions. |
| Start issuance | `scheduler2/tasks.go:StartHandler.Execute` loads a detached frozen request, issues outside the lock, then applies an incarnation/operation-fenced event | Bind a real workflow frontend client (as the old scheduler does), preserve request identity across retry, and retain actual allocated workflow chain/run identity. No fake start function in public acceptance tests. |
| Callback attachment | `scheduler2/component.go:freezeAdmission` writes operation RequestId and manual/backfill reuse policy, but generates no callback | Generate a callback from the scheduler component ref and operation ID before freezing the request. Same frozen callback must be reused after failures/reloads. |
| Callback receipt | `scheduler2/completion.go` validates reducer events but Scheduler does not implement `chasm.NexusCompletionHandler` | Implement HandleNexusCompletion and translate the actual persisted success/failure union into a fenced decision.Callback. |
| Effects | `scheduler2/effects_tasks.go` accepts cancel/terminate functions; `effects.go:freezeEffect` already carries namespace, workflow ID, first-run chain, cancel request ID, reason and identity | Wrap actual History cancel/terminate requests with namespace ID, preserving FirstExecutionRunId; never substitute the current latest run without the chain fence. |
| Public mutation | `scheduler2/update.go`, `manual.go`, `backfill.go` expose separate local Update/Trigger/Backfill helpers; frontend selected Update/Patch/Matching are Unimplemented | Add typed public adapter operations and combine each public request into one atomic mutation/dedup boundary, including public metadata and visibility. |
| Public observation | `public.go:DescribePublic` returns initial counters/empty future history; Memo uses a creation-only compact list partition | Project live operation/queue/completion/generator state into existing public contracts without materializing unrelated payloads. Update compact list info on relevant transitions. |

## Actual transport and callback contract

The reference is `chasm/lib/scheduler/invoker_tasks.go:startWorkflow` (request assembly
around665–760), not an invented direct History StartWorkflowExecution shortcut. It calls
the frontend workflow client, which supplies normal workflow validation and service
behavior, and includes headers, input, memo, action search attributes, task queue, user
metadata, priority, retry/timeouts, versioning override when enabled, schedule identity,
time-skipping config/propagation, and last result/failure. Automatic starts use
REJECT_DUPLICATE; manual starts use ALLOW_DUPLICATE. Scheduler2's public template builder
must explicitly set the automatic reuse policy because freezeAdmission currently only
sets the manual/backfill override. Preserve the old scheduled-start search-attribute
projection, including nominal schedule time and schedule identity, instead of copying
only user action attributes.

`chasm.GenerateNexusCallback` accepts serialized Context.Ref plus request ID. Its envelope
form carries the request ID across continue-as-new. Existing scheduler calls it with
its configured EncodeInternalTokenWithEnvelope policy. The new local wiring must record
its selected envelope policy explicitly and verify continue-as-new/retry chains; do not
rely on an untested legacy token accidentally retaining the ID. Attach callbacks even to
ALLOW_ALL requests, following the existing start contract; reducer semantics should
ignore their late completion for start-only history/carryover.

Real completion runs through `service/history/hsm/callbacks/executors.go` for
`temporal://internal`, then `service/history/handler.go:CompleteNexusOperationChasm` and
`applyChasmNexusCompletion`. The latter dispatches through chasm.NexusCompletionHandler,
using component-creation consistency first. Its fallback to current run is expressly
restricted to workflow refs; scheduler2 must retain old-incarnation fencing across delete
and recreate, not add a scheduler-specific fallback. Use this actual transport for the
public test, not a direct Apply(Callback) shortcut.

`chasm/lib/scheduler/completion.go:completedResultFromNexusCompletion` is the source for
status restoration: success→COMPLETED; failure details distinguish canceled, timed out,
terminated and failed. Validate absent outcomes/failures before mutation; preserve the
single success Payload/no-result distinction and deterministic encoding expected by
scheduler2.prepareCompletion. Map completion RequestId to operation ID and use the
current targeted root incarnation, retaining the event's identity checks. Test callback
before start acknowledgment, duplicate callback, completion after update, and callback
after close/recreate. Preserve pause-on-failure exclusions for cancel/terminate.

## Gaps requiring an explicit compatibility decision from source

1. **Start error classification.** StartHandler currently turns every RPC error into
   RetryableFailure. The old executor distinguishes AlreadyStarted, transient/context
   deadline/rate-limit errors, retry exhaustion, and permanent errors (`invoker_tasks.go`,
   result classification around400–450 and isRetryableError around829). Specify reducer
   outcomes and observations for each class before connecting real clients. Persistent
   malformed action failures must not retry indefinitely or be counted as success.
2. **Prototype limits versus accepted API payloads.** configureGeneration caps the entire
   serialized request at64KiB and catchup at365d. Frontend validates configured blob
   limits, and public catchup handling has a default/minimum rather than that upper
   bound. Effects have a128KiB prototype request bound. Inventory these and decision
   completion/queue bounds against accepted public input; either reconcile storage and
   bounded processing or record an explicit, tested contract decision. Do not silently
   turn valid existing public inputs into prototype errors.
3. **Public conflict-token semantics.** WI-014's run+revision token is stable only for its
   inert profile. Existing behavior may change tokens on patch/completion as well as
   definition updates. Resolve from existing scheduler handler/state tests, then keep
   dedup and conflict validation in the same public transaction. Separate local Update,
   Trigger and Backfill calls cannot implement an atomic multi-action Patch request.
4. **Time and runtime configuration.** ConfigureGeneration takes seed, After, catchup,
   revision and policy, while public data alone does not supply the execution cursor
   anchor. Match existing jitter seed (`scheduler/scheduler.go:jitterSeed`), initial
   cursor, update supersession and pause/unpause semantics. Wire idle duration and
   relevant namespace knobs. StartSnapshot reconstructs observation time from skipped
   duration, but this alone does not establish real time-skipping config propagation.
5. **Public projections.** Existing ScheduleSuite asserts running workflow identity,
   buffer size, action counts/status, bounded recent actions, future times, list fields,
   carryover and query behavior. New public DATA must stay synchronized with accepted
   updates and control changes, while frozen already-admitted action requests remain
   immutable. StateSizeBytes must continue using persisted execution accounting.

## Proposed dependency-ordered implementation contract

1. Specify and record the next decision/work item: exact public request→template,
   callback envelope, start error classes, runtime configuration/limits, mutation token
   rules and live projections. Keep frontend common validation and selected no-fallback
   behavior. Amend generated schemas only if these contracts require stored fields.
2. Build the real local runtime construction seam for workflow frontend start and
   namespace-ID-wrapped History effects. Register live handlers in History and offline
   library identities in frontend before start. Capture clients through test graph
   construction rather than package globals or post-start handler replacement.
3. Implement callback preparation and NexusCompletionHandler together with start
   error classification. Exercise real workflow start→completion→scheduler transaction
   before enabling broad generation. Keep reducer/event and physical transport evidence
   distinct, with fault injection at publication/acknowledgment boundaries.
4. Implement public Create with full template/configuration and atomic initial patch;
   then atomic Update/Patch, Trigger and Backfill integration, and read-only MatchingTimes.
   Preserve request dedup, invalid-input rollback, close/recreate fencing and common
   validation errors. Typed adapter interfaces may expand; no selected operation may
   fall back to old scheduler or workflow.
5. Implement live Describe/List projections and queryable attributes, including changing
   overlap modes and ALLOW_ALL start-only semantics. Preserve compact list reads,
   opaque pagination tokens, native lifecycle filtering and custom ScheduleId precedence.
6. Add the real scheduler2 descriptor to the existing backend fixture only with actual
   execution wiring. Run the shared ScheduleSuite against that backend, retaining V1
   and old-CHASM baselines and same-backend cleanup. Do not replace the suite with a
   smaller scheduler2-only success test or mark failing cases unsupported.
7. Close remaining CHASM execution-specific obligations (time skipping, idle close,
   recovery and physical queue delivery), then run required race/lint and update the
   plan, work-item record and evidence ledger. M3 completion requires all accepted
   obligations, including authentication/error dispatch and generated transport limits
   being stated accurately; local direct-engine public routing remains local only.

## Concrete reusable tests and evidence boundaries

`tests/schedule_backend_test.go` supplies backend name/context/options/SDK options,
worker requirement, and same-backend cleanup over all pages. Use it without a rollout
header masquerading as independent scheduler2 identity. The common `ScheduleSuite`
methods in `tests/schedule_test.go` already cover Basics, Input, LastCompletionAndError,
LastCompletionResultAfterNoResultWorkflow, ScheduleContinuesAfterWorkflowRetryFailure,
AllowAllDescribeContract, BufferSizeReportedWhenBuffered, BufferOneDeferredFiresAfterCompletion,
PatchTriggerRetryDedupsRequestID, UpdateIntervalTakesEffect, ListScheduleMatchingTimes,
list/count/pagination/ScheduleId, duplicate SDK errors, deleted operations, unpause,
paused idle/empty behavior, jittered wakeup recovery, TriggerImmediately and Backfill.
Use the test inventory in the plan to cover remaining inherited suite cases as well.
CHASM-specific cases cover time skipping, pause-on-cancel/terminate and catchup defaults.

Retain component tests for immutable admissions, forged/stale task envelopes, failure
publication, bounded enumeration and callback-before-ack. Add real History/SQLite
observations for independent root identity, workflow run/first-run IDs, persisted
callback routing, cancellation/termination chain fences, retry/continue-as-new,
close/recreate late completions, shard reload and queued task delivery. A callback event
unit test cannot substitute for receipt through History's callback transport; a public
successful Create cannot substitute for actual workflow issuance.
