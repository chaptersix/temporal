# Independent public mutation review

Reviewed against `.github/copilot-instructions.md` and `docs/development/testing.md`. This is non-author review of public mutation/API seam code; the reviewer authored separate byte-ceiling changes. Static source review only in this pass; parent owns consolidated tests/lint. General public execution readiness is not established.

<details><summary><strong>med</strong> — Describe reports different catchup defaults from execution.</summary>

`publicGeneration` resolves DefaultCatchupWindow and MinCatchupWindow from namespace tweakables. `DescribePublic` instead uses literal365d/10s. With an omitted catchup policy and a configured1h default, generation uses1h while Describe reports365d. A custom minimum similarly diverges. The existing scheduler's Describe uses effective tweakables. Current default tests only construct the inert profile with built-in defaults, so they miss this difference.

</details>

**Suggestion:** For an executing schedule project the persisted generator's resolved CatchupWindow, or persist the effective policy during create/update. Test non-default default/minimum tweakables through executing creation and update and assert Describe equals persisted generation.

<details><summary><strong>med</strong> — Accepted time-skipping configuration is not propagated to started workflows.</summary>

The selected frontend validates and accepts TimeSkippingConfig; create/patch call SetTimeSkippingConfig. Unlike the existing scheduler invoker, scheduler2 does not populate StartWorkflowExecutionRequest.TimeSkippingConfig or TimeSkippingStatePropagation. `startSnapshot` calls GetTimeSkippingPropagateState only to capture skipped duration for activity timestamps, discarding the actual child propagation fields. Thus downstream workflows do not inherit the configured virtual-time state. Existing public physical-transport tests do not cover time skipping. MutableStateImpl.IsTimeSkippingFeatureEnabled also recognizes only the original scheduler archetype; its applicability to the dedicated scheduler2 path must be reconciled, although this review has not established an active scheduler2 caller of that method.

</details>

**Suggestion:** Attach detached propagation config/state at the defined start boundary, preserve retries' intended clock semantics, reconcile archetype eligibility, and test actual public create/patch plus started-workflow virtual time. If not supported yet, reject the config explicitly rather than accept it as implemented.

<details><summary><strong>small</strong> — Initial time-skipping config is applied twice.</summary>

`newExecutingPublicSchedule` sets InitialPatch.TimeSkippingConfig before creating the root, then `applyPublicPatch` sets it again. MutableStateImpl.SetTimeSkippingConfig records an update metric and reinitializes fast-forward/session fields each call. Existing scheduler creation applies it once.

</details>

**Suggestion:** Keep the initial application before clock-dependent creation, and apply the remaining initial patch without resetting the same configuration. Test a recording context to establish one application.

## Reviewed behavior with no additional finding

Frontend Update retains action/overlap/spec/search-attribute/payload validation before selected dispatch. Patch retains notes/policy/timestamps/time-skipping validation and patch-size checks. The selected implementation is invoked directly; errors do not fall back to another archetype. MatchingTimes mirrors existing (start,end] iteration and1000-output bound with the same compiled-spec machinery.

Mutation handlers use typed CHASM UpdateComponent and request-ID options; duplicate IDs map to success as in the existing scheduler. Conflict tokens are incarnation-qualified, unconditional update uses nil, and explicit mismatch rejects before mutation. Counter exhaustion is preflighted. Generator replacement, visibility replacement, public definition and token publication are within the same framework transition. The option-aware harness covers late visibility rejection, malformed final backfill after earlier controls, failed publication, duplicate delivery and request-ID rollback. This establishes modeled transaction behavior with real CHASM serialization, not physical History/database fault coverage. Pause followed by unpause uses the existing last-control-wins ordering. Public snapshots clone messages and virtual clock-dependent operations use ctx.Now.

Missing live Describe/List runtime fields remain a known implementation gap, not approval of static results as complete. Further evidence should cover Update dedup after lost response, non-default configuration, matching-times outputs, and virtual-clock public requests through the actual selected frontend. Existing tests are parallel, use require, and keep assertions in the test goroutine.

## Proposed disjoint ownership for live projections

One agent can own new `public_projection.go` and its tests: compact read-only Describe/List assembly and finite future-time computation, receiving explicit lifecycle metadata. Parent retains component/schema and admission/acknowledgment/callback hooks; API author retains public_mutation/public_api/default fixes.

Recent history needs a bounded DATA record independent of the four retained operation slots, with actual scheduled time, observed start time, actual workflow RunID and status. The first-run chain fence is not a substitute for the actual run ID. Parent must introduce and populate these fields at successful acknowledgment and completion-before-acknowledgment boundaries before projection can claim correctness. Retain10 recent Describe/5 List records. Running excludes pending/ambiguous and start-only/terminal records; ALLOW_ALL contributes recent results but not running. Cumulative ActionCount uses Started, not operation-map length. Missed/overlap counters require explicit reducer outcome accumulation. Future times must honor definition replacement/remaining and remain read-only. List should not materialize request/input/carryover payloads. No implementation started by reviewer pending parent ownership assignment.

## Source snapshot SHA-256

- `chasm/lib/scheduler2/public_mutation.go`: `36f39a7dcdb8eb63ed65dd89398f2763f8a33d93dc805b7201b7120dd837c7b1`
- `chasm/lib/scheduler2/public.go`: `4a2220589d3be50a73dad910f8266966d3504dd73ecebe05bc4af910fbd3e718`
- `chasm/lib/scheduler2/public_api_test_dep.go`: `ea7e2a5dbabeeb3ff469c7c396d35360342b90f5a9a0d27f71d84fdcecf515f5`
- `chasm/lib/scheduler2/public_mutation_test.go`: `bb4caf4d6e42ee27192e5472950e3b2a2b0c4de482f190e4e1eeb86fa2ed89cf`
- `chasm/lib/scheduler2/public_test.go`: `157f660062910baad96d8006f81c488f590894c4089ba25d6352c518b70dee8c`
- `chasm/lib/scheduler2/public_completion_test.go`: `8fc94d2b6e2d15c075e23df0ee5cfc644f84f8fc6d2eddc6dbb4c8b6b7e63ac4`
- `chasm/lib/scheduler2/public_revision_test.go`: `c44ba941f5926a3602dd4ab53877c5313b1fa639dc9fd075d9406e2277db8417`
- `chasm/lib/scheduler2/activity.go`: `06a97579de6e4fbbd60743e7752681cdf57ab48748b78d08c4d8fabcacec1bbe`
- `chasm/lib/scheduler2/component.go`: `9203b372cc90baa21a3582da61b10a8d41c023da2c3d40817545ddffed7ac609`
- `service/frontend/workflow_handler.go`: `a1162b0cc6bd9860948e90153fecde9686d923334b242f17d01417de8f3c4571`
- `service/history/workflow/timeskipping.go`: `0120672e7e700327e53c460160af9d944a735bdb7293770da26779c8667682df`
- `chasm/lib/scheduler/scheduler.go`: `d1b06bd1a5c38225831921a3ef8a66bb5edab8986ff8bc1525a9a6cb53d6e7cc`
- `chasm/lib/scheduler/invoker_tasks.go`: `a8f9b45c5548bfc7df831043eda25c1d7037008e1f734889ed2543112c87e5e5`
- `tests/schedule2_execution_test.go`: `027584a52a848ccec6b6a6c1c80ed13550bd3605c8d16cd93cbfb87c51ba8201`
