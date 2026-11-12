# Live public projections: implementation evidence

Author evidence; parent/sibling review remains required. `PublicRuntime` is a separate DATA partition with compact operation-ID/result records. It retains at most ten recent acknowledged results plus the separately bounded four retained operation records. It contains no workflow input, header, memo, result/failure payload or frozen request. PublicList carries five recent results and five future times; Describe returns ten and ten. Operation eviction does not truncate recent history to four.

The actual StartWorkflowExecution response now reaches the handler as a whole response on the public path. ChainID remains FirstExecutionRunId. Public action RunId comes from response.RunId, independently. Existing local starter constructor remains supported without inventing public RunId from its chain-only result. ScheduleTime is the admitted jittered actual time (queued admissions preserve their original actual time); ActualTime is the successful RPC observation in execution time, matching existing invoker semantics. Callback updates status, while an actual late acknowledgment can enrich a callback-before-ack result. Completed and ALLOW_ALL start-only actions are excluded from RunningWorkflows. Detached Describe/List clones protect persisted state.

## Source-resolved identity boundary

Legacy `invoker.recentActions` includes only starts with a real response RunId. Its completion handler records status without fabricating an initial start RunId. Workflow callback links describe the source's current run, which can differ after continue-as-new. This patch does not substitute either that final callback run or the canonical chain fence for the actual start response identity. If the callback arrives first and the true acknowledgment is permanently lost, ActionCount/status metadata is retained but no recent/running entry claims an unknown initial RunId. A late actual acknowledgment enriches it while operation metadata remains retained. This boundary is explicit, not fake completeness. No callback transport/link fields were changed by this projection patch.

## Counters and previews

ActionCount uses cumulative Started acceptance evidence, not retained-map length. BufferSize uses queued occurrences plus unconfirmed starts. OverlapSkipped classifies SKIP and BUFFER_ONE policy rejection, including consumed manual/backfill controls. BufferDropped distinguishes buffer capacity from BUFFER_ONE overlap rejection, rather than relabeling the reducer's combined prototype count. MissedCatchupWindow records eligible late automatic arrival and discarded expired automatic waiters; paused/budget-limited/manual/range exclusions do not count as catchup misses.

Future preview uses the shared compiled-spec adapter, stable namespace/schedule seed, max(execution now, public update time, cursor), remaining-action clamp, at most ten calls and the registered runtime SpecBuilder's live configured warn/max iteration settings per call. Exhaustion or a compute bound returns a partial preview, matching existing preview behavior; this is not an exhaustive liveness claim. The same builder is installed in CHASM component context before runtime registration and used by matching-times queries. Offline NewNilLibrary uses the repository dynamicconfig defaults via NewNoopCollection. Configuration is not persisted or frozen; each compiled GetNextTime reads the live property. Describe computes without publication. Compact list preview refreshes on public definition/control and transition publication; Memo does not load action definitions or request payloads. Paused schedules still show definition previews as the existing scheduler does.

## Validation

`make proto` completed successfully; source schema regenerated both state.pb.go and helper output. Initial targeted public/executing tests passed1.592s. Final focused command `CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep ./chasm/lib/scheduler2 -run '^TestPublicProjection' -count=1` passed0.668s (`public-projection-focused.log`). Tests cover pending buffer view, distinct chain/actual-run IDs, callback-before-ack enrichment, duplicate deliveries, exact jittered/observed times, detached messages,14starts with10/5 retention beyond operation eviction, compact visibility without definition loading, read-only future preview and remaining clamp, policy/capacity/catchup counter distinction.

The parent owns final full-suite and actual shared-backend validation. Sibling reported real parent/child time-skipping test pass9.367s exercising the public actual-run projection. Initial lint found complexity/default-switch issues, repaired; second lint found one remaining counter-helper complexity issue, repaired by extracting expired-waiter counting. Final consolidated lint remains parent-owned. These lint-driven refactors do not change counter behavior after the focused test. No aggregate payload-capacity claim follows from compact metadata retention.

## SHA-256 snapshot

- `chasm/lib/scheduler2/public_projection.go`: `4ee01858057b4727d6c0dec3a85ae6b9e63b5461bf097277206016c664ba1c7a`
- `chasm/lib/scheduler2/public_projection_test.go`: `244829fb78ce7beaf336935ae1ac9f004f2f1118245df648d1283598750b18b6`
- `chasm/lib/scheduler2/public.go`: `64b4dba7bfb1f8ae0fec331ad7c5a76d6787af31ecff0e60b32544ba3f93b2a8`
- `chasm/lib/scheduler2/public_mutation.go`: `9a77ef738c0dded50d313f7eae91b1c04b63d8c293881896b09dc69d3da92256`
- `chasm/lib/scheduler2/public_api_test_dep.go`: `c792c91a49bfe0ef144ced65ca8001ed3c839100887f0730ad3f721dfc48f7b3`
- `chasm/lib/scheduler2/tasks.go`: `30281fc25b70e3a74f9665fb1b315f631cf9b708c61f47211b1e819d538cf333`
- `chasm/lib/scheduler2/component.go`: `fc4bd043c4cf4a9fdd8b3e1f04b73bbfea8eb3a2010509d70113b6c6a558f295`
- `chasm/lib/scheduler2/internal/decision/decision.go`: `bd91223e09ffdf3c5ac1cb8c1a154842bdf030f1da8bd356330296d26055f7ec`
- `chasm/lib/scheduler2/proto/v1/state.proto`: `d02b15bdebce00c3783e1399e28b6930fe614505f220d05d7999efd2ef15b1cc`
- `chasm/lib/scheduler2/gen/scheduler2pb/v1/state.pb.go`: `927b9e6ade5c777d49feba6d98242a3de725366bc44d3ba9f772381149b503bd`
- `chasm/lib/scheduler2/gen/scheduler2pb/v1/state.go-helpers.pb.go`: `1b9dbae06bed81763c4c55d88c3ef32d7eed6caf9ba79d1e6fe6b45756c9d6f2`

## Configured-compute follow-up

Independent review correctly found the original preview used a literal10000 instead of runtime configuration. Replaced it with the registered live SpecBuilder via the existing `chasm.WithContextValues` mechanism. The earlier focused/shared baseline evidence predates this fix and remains labeled accordingly. Added TestPublicProjectionUsesLiveSpecComputeLimits, changing a local builder limit from1 to100 without replacing the registered builder, checking both Describe helper and List preview behavior. Final validation is pending concurrent payload-owner compilation readiness.

Follow-up source hashes (prior snapshot retained above):

- `chasm/lib/scheduler2/library.go`: `c844fe28a5aeef9c3bca4b97039aa17409d4007538cdf7a922099b16c5e96488`
- `chasm/lib/scheduler2/public_api_test_dep.go`: `341ebf732dd460fb7c71740727244d1fa3025e1b6310b48a6a37ccbab22350b9`
- `chasm/lib/scheduler2/public_projection.go`: `392e261a36099db0c7e5e7f21895cd4628f66dd7d78f3ab01536250b85ff9550`
- `chasm/lib/scheduler2/public_projection_test.go`: `3db311a66732fb0b2793ad6c2aea779328f309596a6a75c3add682deb3d2e562`

## Final configured preview and memo follow-up validation

`go test -tags test_dep ./chasm/lib/scheduler2 -run '^TestPublicProjection' -count=1` with CGO_ENABLED=0/GOMAXPROCS=4 passed0.658s against the current payload-reference implementation. Log `public-projection-config-final.log`. Initial dynamic-limit test failed because its excluded structured calendar omitted the other required ranges and matched no dates; corrected the fixture to explicit complete ranges. The configured limit test now proves low-limit exhaustion and live property change for both projection paths without changing production behavior to force acceptance.

The real full shared suite's TestLimitMemoSpecSize exposed omitted list-spec truncation. Source comparison: current CHASM ListInfo uses10; legacy getListInfo reads TweakablePolicies.SpecFieldLengthLimit (default10), not a dynamicconfig setting. The library now registers an accessor to that existing tweakable source and trims only cloned List Interval/StructuredCalendar/ExcludeStructuredCalendar arrays. Full Describe and future enumeration use the complete definition. New test injects limit3 without global writes, verifies all3 arrays truncate, and verifies Describe retains all13 intervals. No new policy/configuration setting was invented. Parent owns final actual shared rerun and lint.

Final follow-up hashes:

- `chasm/lib/scheduler2/library.go`: `51f676b279759ae4dcf55df370aeaee8d631436351f05d6ec00e630995687b9e`
- `chasm/lib/scheduler2/public_api_test_dep.go`: `c941abb8c7ad6ea07bdd81ee26829f1936d219ae15ad1bc74a0e9ce208697163`
- `chasm/lib/scheduler2/public_projection.go`: `be54b325a981fe2627b7440dcde794fb36339248571774682c1ffa39ebf142c5`
- `chasm/lib/scheduler2/public_projection_test.go`: `f0633cd79ee59a2ce475cfc1bf98cb92e60230cf06f9fa78cd755ea4cd138f2b`
