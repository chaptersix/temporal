# Independent effect-bound review

Reviewer: index_finish. Source hashes: independent-manifest.sha256. No root-owned implementation edits made. Verdict: accept retaining the existing bounded effect-task protocol rather than introducing a second claim dispatcher, with the already planned addressed/fixed request storage and framework/physical task acceptance gates still required before keyed public activation.

Source proof:

- `decision.reconcileEffects` clears intents when there is no blocker or the blocker changes, and adds only an absent Cancel or Terminate kind for the current blocker. This constructs at most two effects. Terminate can supersede Cancel without creating another identity.
- `validateEffects` checks metadata/request cardinalities before any metadata loop, then validates nonnil metadata, allowed kind/phase, exact target-derived ID, unique kind and common nonempty blocking target. Its two point membership checks and matched-count equality reject orphan requests without raw-map enumeration. Pending metadata must own its request.
- `stateFor` validates before decoding effects. `prepareEffects` validates decision metadata before loops; transient new Pending requests can then be frozen by the existing preflight. EffectHandler.Validate and effectRequest validate persisted metadata before inspecting it. Closed task/request paths return without iteration or authority.
- Publication uses only at-most-two before/after effects, and deletes vanished request owners through their before IDs. Every legitimate prior request is owned by prior metadata under the validated matched-count invariant, so this fixed cleanup is equivalent to the former map scan on valid states.
- The handler's existing one-element permit serializes detached request read, RPC and result update. Request preparation checks current target/phase/retry/deadline; frozen RPCs retain their first-execution-run fence. Result events carry execution incarnation. This slice does not change existing response-loss, late-result or RPC semantics.

The earlier missing-Pending and orphan-request findings are resolved. Existing generic malformed-fixture tests could mask duplicate/orphan branches behind a missing Pending request. New `effects_independent_test.go` establishes a valid Pending request baseline and asserts the exact duplicate, orphan and missing errors, including stateFor's early boundary.

Independent execution:

`CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm/lib/scheduler2 ./chasm/lib/scheduler2/internal/decision -run 'Test.*(Effect|Overlap|Idle)' -count=1 -v`

PASS scheduler2 1.696s, decision 0.830s; decision properties ran100 Rapid cases. Complete output is `independent-run.log`. This verifies actual handlers/reducers and boundary behavior in repository tests; it does not establish SQLite physical-envelope delivery or a complete public bounded-layout gate.

Start dispatcher contract cross-check: the reviewed document explicitly rejects active claim/PendingCount underflow, applies decision.Expire before dropping elapsed-deadline readiness, removes only the matching claim when an operation leaves pending, and fences delayed negative results against token/ID/sequence/logical wake. Positive evidence remains valid for the original operation after expiry. The matching-claim rule also prevents settling an old operation from clearing a different active claim; an explicit sentence was suggested for clarity. Physical-deadline conversion inside the claim lease uses the issuance skipped-duration snapshot and compares against execution time, avoiding extension by workflow-lock wait.
