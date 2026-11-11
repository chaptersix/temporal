# WI-003 review and acceptance

Final verified source: aee04d35b22d8085e133a09f58df426249f5a89c.
Independent history_contract review inspected behavior, source compatibility and
model assumptions, and reported no acceptance blocker at the final verified source. Independent tooling_harness authored tests/replay; independent
chasm_guarantees authored the finite model without importing the implementation.
Integrator owned the core, adapter, schema/generation and combined verification.

Resolved findings:

1. Unknown-field-only Payload must remain present. Replaced metadata/data-length
   emptiness check with proto.Size; opaque unknown-field regression passes.
2. Failure status must match the top-level FailureInfo. A canceled failure labeled
   FAILED could otherwise pause the schedule. Adapter now rejects contradiction;
   empty/default failure maps FAILED, canceled/timeout/terminated variants map
   their own status. Malformed/mismatched inputs leave durable payloads untouched.
3. Added valid protobuf payloads at limit-minus-one, limit and limit-plus-one for
   success and failure, including successful recovery after rejected oversized
   input. Failure presence remains distinct from zero-byte encoding.
4. Concurrency ownership test uses cancellation-aware await.Rcv; worker assertions
   remain in the test goroutine. Initial import lint finding corrected.

The first test attempt overlapped full code generation's temporary output move
and failed missing-package resolution. It did not establish a product failure or
require dependency installation. After make proto finished, combined tests pass.
The initial transient log and lint failure remain as evidence, not passing gates.

Passing evidence: five model safety assertions/seven witnesses at100/200 bounds,
173584 nodes/162732 unique states and empty frontier at both; three model mutants
caught. Three source mutants (duplicate overwrite, null collapse, contradictory
status acceptance) fail their named Go tests. Original bytes restored after each;
final completion property suite and full race suite pass on restored source.
Rapid explores10000 scenarios, one first callback plus1–16 duplicate statuses,
0–32 opaque bytes and both pause policies. Directed CHASM tests cover the protobuf
boundary, serialization, failed publication, resource boundaries, aliases and
selective loading. A saved explicit completion_v1.json replays twice through
fresh CHASM nodes, independent expected carryover/phase/accounting/pause values.

Compatibility: Callback requires explicit outcome/status; Complete is metadata-
only and may retain legacy zero status. Metadata completion is terminal and wins
against later payload callbacks. Current policy controls pause; updates after a
completion cannot cause a duplicate callback to reapply effects. Success clears
old failure and replaces result, including clearing on no-result (DIV5 versus V1).
Retry input remains the admission snapshot. No budget refund occurs.

Limits: one-operation SKIP only. Initial uninitialized carryover retains caller
request fields; initialized carryover is explicitly seeded in fixtures. Sequential
admission and full policy/retention integration are not implemented. No production
callback authentication/routing, public conflict token, automated refinement or
new liveness claim. Atomic DB publication remains a harness assumption; actual
CHASM serialization, reload, task invalidation and failed publication are tested.
The model abstracts protobuf validation and excludes unpause; its source hashes
were checked byte-for-byte after integration, so an identical repeat was unnecessary.
