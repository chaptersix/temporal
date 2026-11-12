# Proof review repair checkpoint

The earlier usage-limit checkpoint was resumed. Final repair build11 passed pinned Lean 4.24.0 module compilation and fail-closed axiom auditing. No WI-013 acceptance is claimed by the implementation author.

Immutable binary: `/private/tmp/scheduler2-wi013-lean-build11/oracle`.
Binary SHA256: `3e0b7213a8e63024cc7a219c003cbe4fff22e8b683029ce4694c15b58112db6d`.
The archived `build11/manifest.json` records exact source/tool/runner hashes and commands. All 93 named theorem audits passed with dependencies restricted to `propext`, `Quot.sound`, and `Classical.choice`. Source scan found no sorry/admit/native_decide/project axioms. Failed build09 preserved its PublicComposition record-layout parse error; direct repair check passed and builds10/11 passed. Earlier build04 parser error and successful builds05–08 remain separate immutable evidence.

## Repaired obligations

- Full public validState initialization and probe/disposition/terminal/Run preservation: Validity.lean.
- Actual public Run success, serialized suffix reconstruction after every call, Ready preservation, existence/completeness/uniqueness from actual initialization with arbitrary supported positive budgets: Public.lean `public_resume_kernel`, `public_resume_complete`, `public_initial_complete`.
- Empty-source Run/Peek and repeated continuation validity: `empty_scan_success`, `public_empty_resume`.
- Exact Peek rewind validity and Peek+Dispose full-state equality: `peek_run_valid`, `peek_dispose_run`, `public_peek_dispose`; serialized Peek reconstruction and readiness; capacity waiting stutter; public future-pending and first-output corollaries.
- Update/Rebase readiness via actual initial state at successful boundary: `replacement_initial`, `replacement_ready`; original automatic/historical boundary theorem retained.
- Full-range Impossible implies no eligible bounded source; fully excluded sufficient kernel scan reaches Impossible: explicit classification theorems. Public empty sources instead return Empty.
- Generic malformed-state rejection at the contextual validation stage for Run/Peek, Dispose, replacement; signed-negative parser precedence repaired and parent independently tested.
- Original exact full-state probe-resume and adjacent/finite window batching: independent KernelComposition implementation, integrated into audit. Complete Result equality includes machine/state/output/probes/stop. `disposedTrace` recovers the exact consumed raw prefix including exclusions and revisions; its composition proves original disposition-accounting obligation without inventing a public journal field.
- Actual serialized public exact pair and finite partition equivalence: PublicComposition `public_probe_resume_exact`, `public_window_batching_exact`, `public_finite_window_batching_exact`.

## Scope and review limits

Ready is an invariant superset, not proof of source provenance for every arbitrarily supplied pending candidate; the initial-state and preserved reachable-continuation chain supplies that route. Completeness folds report terminal cache, not the last public response stop; exact pair/finite wrappers deliberately preserve the actual last scan response. Sufficient whole-call public equivalence requires the whole call's budget to fit the public caps (uniform remaining length at most 127); arbitrarily large finite source completeness uses repeated supported calls. Kernel all-excluded classification does not make public no-inclusions sources Impossible. Repeated Peek without eventual Dispose intentionally does not progress; scheduler fairness is external.

The JSON parser, Go production calendar/jitter/exclusion computation, persistence, physical timer delivery, and scheduler fairness are not proved here. Runtime oracle hash is unchanged from parent-tested build08, allowing explicit artifact-identity reuse of correspondence/mutation evidence; parent owns those records and WI-013 acceptance. Independent oracle_review recorded review of Public/Validity/PublicComposition in independent-proof-review.md. Non-author proof_review recorded KernelComposition review in independent-composition-review.md; parent also independently inspected it. These scoped reviews do not substitute for integrator WI-013 acceptance.
