# WI-005 review checkpoint

Independent history_contract reviewed implementation010b39757 against D32–D34.
No blocking findings. Verified FIFO/no debit, current eligibility/adoption, independent
drain token/deadline, selective payload preparation before writes and old callbacks.
Acceptance awaits independent tests, publication/restart checks and final validation.
Assumptions: generator/admission creates bounded queues; CHASM rolls back failures.

Independent chasm_guarantees mapping at010b39757:

- Occupied/queued Due maps to AutomaticIngress: eligibility/capacity and queue-time
  nominal/actual/workflow ID/resolved policy agree.
- Free+empty immediate Due maps to atomic AutomaticIngress followed by Drain,
  with no intervening environment action.
- Concrete drain at index k maps to k ineligible model Drain steps and one eligible
  Drain, holding time/template/carryover/policy/budget fixed. Intermediate rearms
  are hidden; all-ineligible clearing maps to at most four model drops.
- Drain tokens project to live/retired freshness classes, not numeric equality.
  Batched concrete transitions allocate fewer tokens than single-entry model steps.
- First callback/Complete maps to FirstCompletion and direct reconcileDrain,
  independently of generator state. Configuration/policy/budget changes map to the
  selected model environment update. Failed publication/reload is model stuttering.

This is a manual transaction-boundary mapping, not one-to-one trace equivalence
or an automated implementation proof. No batch/immediate optimization defect found.

Selective-loading scope: the drain path prepares current request/carryover only for
actual admission. Generation retains WI-004's combined Definition node and decodes
its template to derive workflow identity even on queued/dropped ingress. No global
avoidance of template deserialization or physical DB reads is claimed.

Initial default-base lint failed with44 findings, including new Apply complexity
and existing applyResult/other-package findings. c3da87ced extracts prepareDrain;
`CGO_ENABLED=0 GOMAXPROCS=4 make lint-code-fast
GOLANGCI_LINT_BASE_REV=256ac1df6` passes normal/minimal configurations. Existing
package tests pass again after the refactor. No finding was suppressed.

Final independent review at443587c3e found no blocking findings. Added queue
ownership coverage resolves the test gap and detects the removed-clone mutant.
Saved replay now reads frozen persisted requests after completion instead of calling
the issuance helper, which correctly returns nil for completed operations. Initial
harness failure remains in replay-initial.log. Final buffer profile (10000 cases),
full scheduler2/nil-registry race profile and scoped normal/minimal lint pass.
All five Go mutants and four independent model mutants detected; source restored.
Actual storage-backend crash recovery remains outside this CHASM node harness.
