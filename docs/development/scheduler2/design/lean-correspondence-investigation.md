# M3 Lean proof and correspondence investigation

Read-only production/specification investigation over WI-012 base `352dafa83`.
No Lean or Go source was changed. This records proposed WI-013 decisions for review,
not an accepted proof contract or completed M3 gate.

## Required result

Plan section7 requires a machine-checked resumability theorem preserving
completeness and uniqueness, including pending jittered candidates, followed by
batch equivalence. It also requires the separate Lean executable to be used for
bounded exhaustive and Rapid comparison against Go. Comparison is testing evidence,
not an implementation refinement proof. Full timezone-library verification is
outside the initial theorem scope; full spec-processing checks, including pinned
DST/exclusions and boundary-biased generation, still belong to M3.

The existing `lean/Enumeration.lean` is a useful independent executable core with
proposition definitions. No proof of those definitions follows from compilation.
The current propositions were inspected; this investigation did not find a concrete
counterexample to them under their stated hypotheses. That is not a proof or an
exhaustive satisfiability check.

## Statement gaps and proposed strengthening

| Existing definition | Required addition or qualification |
| --- | --- |
| `CompleteEnumeration` | Completeness currently starts from `initial` only. Add a declarative remaining-sequence interpretation for reachable continuations, including the retained pending value, and prove sufficient-resource completion from that interpretation. |
| `ResumeEquivalence` | Covers a first stop of `yielded`, preserving output/dispositions/probes/state/stop. Prove this proposition, then derive finite multi-call completeness and uniqueness for positive work/output budgets. A repeated output-limited stop must also be supported; a proof only about probe yields does not cover all public resumptions. |
| `OutputUnique` | Within-call strict ordering is insufficient alone. Derive strict ordering and no duplication across concatenated resumed outputs, not merely each batch separately. |
| `BatchEquivalence` | Prove the two-window proposition, then finite nondecreasing-window partition equivalence with a sufficiently funded or fully resumed call at each boundary. Preserve the complete pending candidate across actual-time windows. |
| `PendingStable` | Prove existing statement and connect it to the continuation invariant. Include excluded future candidates, whose disposal must wait until their actual/nominal time is due. |
| `UpdateBoundary` / `UpdateRejectsStale` | Prove existing local replacement rules. Separately specify historical rebase: Go `RebaseRange` restarts at disposedThrough, whereas automatic `Update` restarts at max(updateTime, disposedThrough). |
| `ProbeBound` | Prove public probe bound and a structural-recursion sufficiency lemma: valid executions cannot hit the private tick-exhaustion `malformedOrder` branch. This does not bound calendar CPU cost. |

Strengthen reachability/invariants before proving semantic terminal claims.
Current `WellFormedState` checks revision, cursor inequality and a pending-member
condition, but permits `pending=none`, `disposedThrough<searchAfter`, arbitrary
`fullRange`, and an `eligibleSeen` bit unrelated to past probes. Go rejects the
first shape (`occurrence.go:270`); reachable-state history is needed to interpret
Impossible versus Exhausted honestly. A proof under an invariant must also prove
initialization and each allowed step preserve that invariant; do not assume the
desired conclusion as a state hypothesis.

The ordered-source hypotheses already include nonnegative jitter, strict actual
time before every later raw nominal, no duplicate nominals, in-range actual times,
and exclusion actual=nominal. They deliberately assume the raw calendar/jitter
stream is correct. Proving them for the production evaluator is not accomplished
by the enumeration theorem.

## Concrete abstraction counterexamples

These are counterexamples to an *unqualified exact Lean/Go correspondence*, not to
the existing restricted propositions. Executable Lean guards were checked locally
as described below; the Go differences were established by source inspection.

1. **Same revision, changed stream.** Initialize revision1 with raw nominal1, then
   scan a different revision1 source containing nominal2. Lean emits nominal2.
   Go `Run` and `Peek` require both revision and DefinitionID equality
   (`occurrence.go:164`, `backfill.go:43`). The adapter must authenticate immutable
   source identity or the executable must represent it; revision alone is not the
   concrete fence.
2. **Zero budget.** Lean `scan source through 0 1 initial` returns yielded.
   Go `Run` rejects probes0/outputs0 (`occurrence.go:167`). Zero internal residual
   fuel is useful in proofs; it cannot be serialized as a valid public Go call.
3. **Terminal re-probe.** A one-candidate source scans to completion in two probes;
   scanning its terminal Lean state again takes one end-of-source probe. Go caches
   Terminal and returns without probing (`occurrence.go:178`). Unrestricted restart
   equivalence cannot preserve summed probe counts or compare raw terminal-state
   representation. Existing ResumeEquivalence avoids this by requiring yielded.
4. **Epoch-zero sentinel.** Lean initial after0 excludes a candidate at nominal0.
   Thus subtracting the Go lower epoch and using Nat0 as both the first candidate
   and the pre-range sentinel loses the first supported occurrence. Use an explicit
   sentinel/order embedding, or shift valid nominal seconds by one.

Other source-established mapping requirements:

- **Definition end is nominal; supported upper time is actual.** Go `probe`
  (`occurrence.go:226–247`) rejects a raw nominal after `e.end`, but permits an
  eligible jittered actual after that definition end if the actual remains in the
  prototype's supported range. A candidate nominal1000ms/actual1500ms from a
  definition ending at1000ms must remain available for a later through1500ms scan.
  Setting Lean `upperMillis=1000` would instead reject it with actualOutsideRange.
  This is a reasoned counterexample to that hypothetical adapter assignment, not
  a tested defect in an existing adapter (none exists yet). Filter the raw source
  by the nominal definition end and separately use the supported actual upper
  bound, or represent both fields explicitly. The terminal cache and fixed
  backfill actual end must also remain distinct from these two bounds.
- Go carries nanosecond Origin/SearchAfter/DisposedThrough and supported range
  `[2000-01-01,2101-01-01)`; Lean uses nominal seconds and actual milliseconds.
  For membership/window comparisons, a documented shifted epoch plus floor of
  cursor seconds and floor of through milliseconds can preserve the integer
  candidate comparisons. Exact fractional cursor values must remain in an adapter
  side projection or the spec must use a finer unit. Do not claim byte/state equality
  after rounding. Lower nominal bounds use ceiling, not floor.
- Inclusive backfill BeginRange subtracts1ns; PeekRange translates Waiting to
  range Exhausted without globally exhausting the source. Eligible candidates stay
  pending until DisposeRange; no emitted list is returned. Current Lean scan emits
  immediately. These are distinct transitions requiring an explicit adapter or
  independent executable extension, not a rename of `scan`.
- Terminal cache, Origin and DefinitionID are currently erased; malformed-state
  validation and concrete service-error classes are not modeled. Supported valid
  trace correspondence and rejected-input tests need separate, explicit contracts.
- Go maximum probes1024/outputs128 are public API checks. Lean Nat parameters are
  unbounded mathematical resources. The proof can quantify over Nat while the Go
  comparison generator restricts public calls to their valid ranges.

## Proposed proof and execution architecture

Keep Lean4.24.0 pinned, initially Std-only. Introduce a declarative suffix/remaining
interpretation, then prove ordering/filter lemmas, initial/step preservation,
pending stability and termination of the bounded loop. Prove a generalized loop
composition lemma over accumulators and remaining fuel/slots; derive the public
yield and output-limit resumptions and the sufficient-resource completion theorem.
Derive finite-window batching from that semantic theorem rather than duplicating
the scanner as its own oracle. If a simpler executable fold replaces the tick-based
loop, prove its equivalence to the retained executable or explicitly review and
version the spec change before using its theorem as evidence.

Export named theorems and run `#print axioms` for each deliverable. Reject sorry,
admit, project axioms, and unchecked replacements such as `native_decide` producing
a trusted axiom. Record any Lean foundational axioms used; compiler success and
example evaluation do not replace this audit.

Add a versioned enumeration-only JSON driver implemented independently in Lean,
with explicit source revision/identity, initial continuation, scan windows/budgets,
update/rebase events and expected observations. Reject unknown fields/events or
versions according to the chosen schema. Compare production Go against emitted
Lean observations after every step. Persist minimized failing input traces and
replay without Rapid. Runtime compilation should not occur per generated case:
compile the oracle once, feed batches, and record binary/source/tool hashes.

Use two layers so calendar correctness is not assumed away: abstract ordered-stream
exhaustive fixtures check the executable/protocol relation; concrete short-window
spec traces derive an expected stream through the independent direct eligibility
oracle, then compare actual production enumeration to Lean. Do not extract the
expected stream by calling production NextTimeCandidate. Shared timezone and hash
primitives remain named assumptions. Calendar-specific SP7 generators and examples
remain required even if all abstract stream checks pass.

## Proposed WI-013 scope and dependency order

1. Record the strengthened theorem/projection contract, sentinel and fractional
   mapping, valid-input scope and terminal-cache handling. No user decision appears
   necessary: current source/contract determines these routine choices, but they
   need independent review before implementation.
   Modernizing the executable with explicit immutable identity, terminal cache and
   nanosecond time fields is permitted and may simplify exact observation mapping;
   retain the independently specified semantics and strengthen proofs accordingly.
   No source-backed boundary may be weakened merely to fit the old abstraction.
2. Complete SP2 and SP3 machine-checked proofs with pending-state lemmas, meaningful
   completeness/uniqueness corollaries and the axiom audit. Include update/pending
   lemmas needed by the executable correspondence; do not substitute only easy
   probe-bound/update lemmas for the required resumability/batching result.
3. Deliver the versioned Lean oracle, bounded exhaustive and Rapid Go comparisons,
   saved replay, and sensitivity mutants for lost pending/cursor skip/duplicate
   emission. Include automatic and historical continuation operations in the
   recorded mapping; stage implementation if needed without claiming the omitted
   operations correspond.
4. Record exact bounds/counts, seed, timeouts, all failed runs and trusted edges.
   Independent statement/source review is required before item acceptance.

This is a coherent proof/correspondence work item, independent of WI-012 backend
wiring. Full boundary-biased calendar/DST checks, shared functional/differential API
coverage and routing/reset integration remain explicit M3 work if not completed
inside WI-013. M4 load/state-growth and migration boundaries are unaffected.

## Local investigation evidence

Tool: `/private/tmp/scheduler2-lean-toolchain/lean-4.24.0-darwin_aarch64/bin/lean`;
reported Lean4.24.0, commit `797c613eb9b6d4ec95db23e3e00af9ac6657f24b`.
Copied the unchanged Enumeration source to `/private/tmp/scheduler2-wi013-audit.lean`
and appended four `#guard` evaluations for the concrete cases above. Command:

```sh
/private/tmp/scheduler2-lean-toolchain/lean-4.24.0-darwin_aarch64/bin/lean /private/tmp/scheduler2-wi013-audit.lean
```

Exit0, no output. This checks those four Lean behaviors, not any general theorem,
Go execution, proof architecture feasibility, or runtime bound. No production tests
were run for this investigation. The temporary audit file should be archived or its
guards promoted into versioned correspondence regressions when WI-013 begins.
