# Independent public projection review

Review by proof_review of oracle_review source. Read-only review; no implementation changes or WI acceptance. Parent reported shared subset PASS 22.344s; not independently rerun here.

## Finding requiring correction

**Medium: future previews ignore the configured compute budget.** `public_projection.go:85` constructs a SpecBuilder with constant warning/max iteration limits of 10000, whereas `public_api_test_dep.go` constructs the runtime-configured builder for matching-time requests. Legacy `scheduler/generator.go:computeFutureActionTimes` uses its injected builder. Therefore a legitimate sparse/over-excluded spec may have matching times but an empty Describe/List preview when the configured limit permits the search; conversely a lower operator budget does not constrain preview work. Both public reads and transition-time list publication use this hardcoded path.

Independent standalone source reproduction: a one-second interval excluding all November 12, 2026, queried at that day's midnight, returns compute-limit error with max10000 but November13 midnight with max100000. Exact reproducer and output are `preview-limit-review.go.txt` and `preview-limit-review.log`. Command: `CGO_ENABLED=0 GOMAXPROCS=4 go run /private/tmp/scheduler2-preview-limit-review.go` from execution worktree. This reproduces the builder semantics underlying the source mismatch; it does not pretend to be a full public API test. Fix should bind the effective configured budget to all preview paths and add a non-default-budget regression, keeping bounded search.

## Reviewed behavior

- Rich start response preserves FirstExecutionRunId as the authoritative chain fence and carries RunId separately as public metadata. No alias to a mutable proto response is persisted: strings are copied to the event, projection allocates its own result, and snapshots decode a fresh request. The actual execution clock observation uses the separately reviewed captured propagated offset.
- Callback-before-ack records terminal status without inventing a response RunId. A later matching acknowledgment enriches a retained operation with actual run identity/time without reverting its terminal status. Duplicate acknowledgments cannot replace an already recorded RunId/time. Different-chain terminal events do not pass the projection's equality check.
- `publicRecent` and RunningWorkflows clone output messages; the test mutating returned RecentActions demonstrates isolation. RunningWorkflows uses only tracked Started operations, consistent with legacy excluding start-only ALLOW_ALL. Recent actions sort by actual observation time and retain ten; List retains five recent results.
- ActionCount counts positive acceptance evidence, including callback-first evidence. BufferSize counts queued admitted candidates plus unresolved Pending/Ambiguous intents, excluding tracked running operations. Pending manual/backfill commands not yet converted to action candidates are excluded; legacy also represents those separately as backfillers, so this is not alone a count defect.
- Retention keeps latest ten acknowledged starts plus metadata for at most four retained operations. The 14-action test checks actual operation eviction and recent/list history survival. Terminal payload reclamation is separate; retained public history is metadata only.
- Future search uses the same seed composition and max(now,cursor,updateTime) progression pattern as legacy, and clamps count by remaining action budget. It does not mutate persisted enumeration. The new tests cover minute schedules/remaining limits but not configured search budgets or jittered preview equivalence.

## Explicit evidence limits and follow-ups

Callback-first plus permanently lost start response yields ActionCount without a fabricated RecentAction. Once the terminal operation is evicted, a very late acknowledgment cannot enrich its public result. Current tests cover callback-before-ack only while retained. This is an explicit bounded retention/evidence limitation, not evidence that all accepted actions always appear in RecentActions.

The real-start wrapper validates FirstExecutionRunId but not empty RunId; actual server response normally supplies both. Unit tests exercise direct reducer events, not a malformed rich response. An empty actual RunId response would settle the intent while leaving RecentActions/RunningWorkflows absent; add a guard/test if malformed-client responses are in supported scope.

List future count five matches V1 default FutureActionCountForList but differs from original CHASM's current memo of ten generator future values. Describe count ten agrees. Record this intended bounded presentation choice; do not claim byte-identical List cardinality to both old implementations.

## Reviewed source fingerprints

- `chasm/lib/scheduler2/public_projection.go` SHA256 `392e261a36099db0c7e5e7f21895cd4628f66dd7d78f3ab01536250b85ff9550`
- `chasm/lib/scheduler2/public_projection_test.go` SHA256 `244829fb78ce7beaf336935ae1ac9f004f2f1118245df648d1283598750b18b6`
- `chasm/lib/scheduler2/tasks.go` SHA256 `30281fc25b70e3a74f9665fb1b315f631cf9b708c61f47211b1e819d538cf333`
- `chasm/lib/scheduler2/public_api_test_dep.go` SHA256 `341ebf732dd460fb7c71740727244d1fa3025e1b6310b48a6a37ccbab22350b9`
