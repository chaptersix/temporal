# Real accepted overlap effect response loss

Four real SQLite/History cases: CAN/reset × CANCEL_OTHER/TERMINATE_OTHER. Actual selected frontend creation, typed and persisted scheduler2 identity; no reducer substitution or production edits.

The first scheduler action is acknowledged, then its workflow actually continues as new or resets (first workflow task completion is observed before reset). The current run differs from the original first run. The next public manual trigger creates an actual effect. A History client wrapper pauses before issuance so the persisted effect and its exact first-run fence can be inspected, then calls actual History and requires success. The response remains held and is ultimately replaced with Unavailable.

For cancellation, the workflow deliberately ignores cancellation until a signal using a disconnected workflow context. The owning schedule shard is closed and durable effect metadata is read unchanged. Releasing the lost reply forces actual retry; the entire captured History request must equal the original, including chain and request IDs. Explicit workflow completion then settles the authority.

Termination closes immediately. Its real completion callback can settle authority and start the second action while the successful effect reply is still withheld. After shard reload the test observes Started2/Unfinished1 before releasing the lost reply. This is callback reconciliation, not claimed duplicate terminate retry. A timer post-dispatch observation ensures a physical effect handler has returned through reconciliation before final assertions. Registry lookup occurs after the selected library is installed, and nonzero task type is required.

Both manual triggers have the same explicit nominal timestamp: their generated workflow ID is identical, while request identities and chain roots are distinct. The original changed run must be CANCELED/TERMINATED, the second scheduled chain RUNNING. Delivering the exact old request again through real History returns NotFound and leaves that replacement chain RUNNING. This is actual first-run fencing, not a mock expectation. Existing recreation-physical witnesses separately cover a new schedule incarnation and stale physical effect envelope.

Final four-case run06 PASS8.098s. Prior run02 PASS8.122s without replacement assertions; run03 exposed a fixture assumption (a second scheduled workflow already owns the reused ID, so an unnecessary third Start returned AlreadyStarted). Explicit same-timestamp triggers and inspection of the actual second scheduled action replaced that fixture assumption; run04 PASS9.195s. Run05 failed only a new observation bug: task type was looked up before library registration, producing zero. Lookup-after-registration plus NotZero fixed it; no runtime change. Run01 was a compile-only missing TestEnv converter method, replaced by SDK default converter.

Scoped lint found no effect-file findings but is not reported as globally passing: concurrent clock-test builtin shadows failed both attempts. Parent owns final consolidated lint and independent review. No process restart, exhaustive failure permutations, or both callback implementation cross-products are claimed. The actual accepted-response-loss boundary is forced for both effect kinds; cancel demonstrates frozen retry, terminate demonstrates safe callback settlement.

## Independent review strengthening

Parent found run06's generic post-dispatch channel could be satisfied by a retry while the original reply was still reconciling. Run06 remains evidence for the actual transport assertions, not an exact original-delivery completion barrier. The final hook designates the first matching physical invocation before calling execute via sync.Once, and only that invocation closes the channel after execute returns. Retry invocations cannot satisfy it. Final run07 PASS8.163s on the strengthened source; final fingerprint supersedes but preserves the run06 fingerprint in manifest.json.

Final scoped make lint-code (normal/minimal/vet) PASS, zero issues, lint03.log.gz. Earlier failed lint attempts remain archived.
