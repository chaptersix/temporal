# WI-015 independent component/callback source review

Reviewer: api_seam. Worktree: `/private/tmp/sch-redesign-execution`.
Scope: parent-owned `public_completion.go`, `public_completion_test.go`, callback
freezing in `component.go:freezeAdmission`, live binding in
`public_api_test_dep.go`, and `tests/schedule2_execution_test.go`. Read D59,
related decision/completion/task code, old scheduler callback/start code, repository
code guidelines and testing guide. This review does **not** approve this reviewer's
runtime injection, internal chain metadata, or logical-wake changes. The integrator
reviews those separately. No tests were rerun for this source-only review.

## Findings requiring test strengthening

<details>
<summary><strong>med</strong> — Continue-as-new evidence does not assert the canonical first-run ID.</summary>

The transport test checks that callback ChainID differs from the final RunID, while
StartWorkflowExecution's actual FirstExecutionRunId is only formatted into a diagnostic
string. Any nonempty unrelated ID would satisfy the current continue-as-new assertion.
D59 requires the callback to preserve the actual canonical first execution identity.

</details>

**Suggestion:** Keep a structured observed start response and assert the persisted
operation ChainID equals its FirstExecutionRunId in both cases. Retain the final-run
inequality for continue-as-new as an additional assertion.

<details>
<summary><strong>med</strong> — The transport test does not observe reconciliation after the withheld acknowledgment.</summary>

The explicit acknowledgment gate meaningfully proves actual callback completion while
the Start RPC response remains withheld. After releasing it, however, the test does not
wait for the actual StartHandler merge and verify that the late acknowledgment preserves
Started=1, Unfinished=0, terminal phase, chain and carryover. The source reducer correctly
ignores terminal-operation results, but the physical-path gate currently ends before
that behavior is observed.

</details>

**Suggestion:** Observe completion of the actual side-effect merge or physical task
retirement after release, then re-read and compare terminal state and carryover. A
read immediately after releasing the gate alone would race ahead of the merge.

## Source conclusions for the explicitly scoped first increment

No additional blocking implementation defect was found in the reviewed callback
translation/freezing/live-client selection for this directed transport increment.

- Admission uses `ctx.Ref(s)` and the admitted operation ID with the explicitly
  specified request-ID envelope; generated callback bytes are included in the
  deterministic frozen request. Public DATA presence selects this path, preserving
  deterministic internal fixtures. The freeze/reload test checks token decoding and
  complete frozen request equality. ALLOW_ALL still receives a callback and omits
  carryover, matching the old request policy.
- Completion filters closed roots, unmatched IDs and terminal operations before
  reading outcome payloads. Active matching operations reject absent first-run metadata
  without inventing a current-run identity. Success/failure unions become existing
  decision.Callback events; failure status conversion distinguishes timeout, cancel,
  terminate and failure. The reducer handles chain conflicts and completion-before-ack.
  Nil/empty success follows the existing empty-result semantics; invalid failure rejects.
- Mutable reads can change CHASM last-update version stamps in the unit harness.
  Normalizing only LastUpdateVersionedTransition leaves data and task comparisons intact;
  it does not excuse logical mutations or task publication on rejection/duplicate.
- Live binding calls the actual frontend Start API and uses FirstExecutionRunId only.
  A missing canonical ID remains retryable ambiguity, not a guessed RunId. Cancel and
  terminate use actual History clients with namespace-ID wrappers. Registration remains
  confined to explicit test construction; normal service routing is not enabled.
- The test uses real workers and actual workflow/callback transport with directed
  generation/trigger controls. Its acknowledgment gate is stronger than sleeping or
  hoping a callback wins a race. The corrected WorkerTaskQueue matches the worker.

## Acceptance limits retained

This review does not accept all WI-015 or M3. The contract explicitly permits a first
transport increment, while general public execution remains unfinished. In particular:
prototype payload ceilings and aggregate persistence duplication; permanent/start-error
classification; public Create/Update/Patch and initial-patch atomic semantics; public
conflict-token updates and live projections; callback validation against configured
limits before issuance; and full shared-suite execution remain required. Physical
cancel/terminate chain fencing, failure/cancel/timeout outcomes, lost response/dedup,
late callback after close/recreate, reload/recovery and time skipping need their specified
acceptance evidence. Callback transport tests using directed admission must not be
reported as public Patch parity. Default routing stays unchanged.

The reviewed completion tests cover success/failure before acknowledgment, missing-chain
rejection, and a duplicate with malformed/missing payload after terminal completion.
Additional malformed-outcome, terminal status, ALLOW_ALL and close/recreate adapter tests
would make the new boundary's exact guard ordering explicit; existing reducer tests alone
do not prove the transport translation. These are required as the full WI-015 matrix is
completed, rather than evidence that the current two-case transport test is comprehensive.

## Related verification provenance (not this review's execution)

Preserve `/private/tmp/scheduler2-wi015-wake-tests.log`: recursive command failed due to
WI-016 London fold property (family4), while scheduler2 root, internal/decision and all
service/history passed. Parent and other reviewers are diagnosing that property;
this review neither overwrites the failure nor treats the scoped passes as a recursive
pass. Parent owns the latest physical transport run and its result; no result is inferred
from source inspection here.


## Follow-up source review: findings resolved

The updated transport test now captures a typed workflowStartOutcome and asserts its
FirstExecutionRunId equals the completed operation's ChainID before releasing the gate.
It then triggers a second physical action and awaits Started=2, Unfinished=0, unchanged
carryover and a distinct chain. StartHandler's single issuance permit is released only
after the first handler's result transaction returns, so the second physical start is
a meaningful barrier for late acknowledgment reconciliation. Both findings above are
resolved by source inspection. Parent owns execution evidence for the updated test.

Three import-only gci findings in reviewer-owned chain transport tests were corrected
by grouping standard-library imports before third-party imports. No test behavior changed.
