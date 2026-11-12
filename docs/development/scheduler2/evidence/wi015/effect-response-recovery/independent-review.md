# Independent effect response recovery review

Reviewer: root. Reviewed the complete test, including the corrected original dispatch barrier after run07.

The fixture issues a real History cancel/terminate against the continued/reset chain, observes success, suppresses that response, closes the scheduler shard and reconciles. Cancellation keeps the workflow live until an explicit signal, allowing exact protobuf retry comparison after reload. Termination may settle through its actual callback while the reply is held, so the test correctly permits reconciliation without claiming a duplicate RPC. Replaying the exact old first-chain request against the same workflow ID in a new scheduled chain returns NotFound and leaves that replacement RUNNING.

Initial review found that any matching effect dispatch could signal the completion barrier. A retry could therefore satisfy the barrier before the original response had been merged. The final code selects the first matching invocation before calling execute and only that invocation closes the completion channel after execute returns. Retry completions cannot satisfy it. Final run07 reports PASS8.163s for all four cases; earlier passing runs do not establish this strengthened assertion.

No remaining blocking behavior finding in this fixture. Evidence is scoped to the tested local SQLite paths, not universal backend or failure ordering coverage. New WI018 framework changes require their own regression evidence.
