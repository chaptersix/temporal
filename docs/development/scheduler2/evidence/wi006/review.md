# WI-006 independent review

Contract and source reviewed by history_contract at37cb2c020,39b5099c3 and50a9e6540.
No remaining source/test blocker. Final race/profile/lint results gate acceptance.

The initial keyed metadata poison test failed during real CloseTransaction: root
StartTask cleanup prepared unrelated Operation components. D38 changes operation
metadata/requests to DATA maps and validates tasks using a cloned bounded root
PendingStarts index. Issuance independently checks selected metadata. Reviewer
confirmed targeted changes preserve unrelated keys; eviction removes both data
records/index; immutable requests only change on admission. Poisoned sibling test
now passes through commit/reload. Exact index oracle compares persisted Pending
metadata against min(RetryAt,Deadline), including retries/terminalization/eviction.

Old summary equality oracles were extended to independently assert Unfinished and
blocking identity. Initial oracle-adaptation failures are not production bugs.
The new metadata isolation failure is a real implementation regression, preserved
in isolation-initial.log and detected by the hydration source mutant. Initial
Requests collection-node poison panic was a test adaptation bug: filter Data != nil
while preserving leaf poison count. Initial test lint failures were imports,
slices.Sort convention and unnecessary Fatalf; all fixed without suppression.

Model maps retained keyed intents, count/blocker summaries, first terminal evidence,
terminal-only retention and drain readiness. Concrete event-addressed Step receives
one selected record; tests compose it with keyed storage and verify index equality.
PendingStarts is an implementation task index rather than a separate model premise.
Model starts from a saturated fixture; Go tests cover construction, all four pending
tasks, source capacity drops and capacity recovery. One representative ambiguity,
one queued waiter and symbolic payloads are explicit bounds. No automatic refinement,
infinite liveness or actual storage-backend crash-atomicity claim is made.

Review corrected ChangeEligibility mapping to queued catchup ineligibility, not
global pause/budget exhaustion, because model ingress stays enabled afterward.
Existing WI-005 tests cover paused/budget disposal. Model source and results unchanged.
Rapid oracle covers keyed ALLOW_ALL/SKIP summaries and keeps terminal history; real
CHASM tests separately cover cap4 eviction, buffering, callbacks and selective loads.

Final acceptance source58ad6c3d6: allowall profile10000 scenarios seed1 and full
scheduler2/nil-registry race pass, scoped normal/minimal lint passes after test
convention fixes. All six Go mutants and three independent model mutants detected.
Five model specification hashes verified after integration. All existing scheduler2
tests pass with the new layout. Task worktrees clean/retained; no original writes.
