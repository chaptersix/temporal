# WI-013 independent proof review

Reviewed in `/private/tmp/sch-redesign-proofs` against WI-013, D57, plan section7
SP1–SP6 and the original `Enumeration.lean/Proposed` statements. This reviewer did
not author the Lean implementation, did not edit it, and ran no tests or compiler
while the parent held a mutation window. Review uses current source and archived
build03 commands/axiom output; it does not infer completion from compilation.

## Conclusion

The kernel contains genuine executable proofs of finite positive-budget
completeness, cross-call uniqueness and occurrence-sequence batching. In particular,
`resumable_completeness` does not assume a final consumed stream or the desired
output. Its finite call bound follows from executable progress. These are substantial
SP2/SP3 results and should be retained.

WI-013 is not yet fully proved against its own state-validity/reachability contract.
The public wrapper bridge is conditional on successful calls; current frontier,
bounds and authority lemmas do not establish all conditions of `validState`, nor
that a valid reachable public sequence continues to return success. Historical
Peek/Dispose coverage is also too weak to establish that its retained candidate and
cursor protocol inherits the proved Run sequence theorem. Exact source acceptance
needs a fresh build/audit for the two current Controls additions.

## Findings requiring work before proof acceptance

<details>
<summary><strong>med</strong> — Public successful resumption is assumed rather than established.</summary>

`Controls.scan_success_kernel` (line56) assumes `scan ... = .ok result`.
`scan_reconstructible` and `scan_conservation` (lines109/118) inherit that premise.
The main completeness theorem executes `Kernel.resume`, which retains its private
Machine between calls; the actual oracle reconstructs it from serialized state and
passes identity, budget, validity and range checks on each call. Reconstruction is
proved, but public validity and success are not.

`Frontier` (line5), `Bounds` (line148), `loop_authority` and cache preservation cover
only parts of `Oracle.validState` (line60). Missing connected conditions include
positive revision/nonempty identity, supported cursor/candidate values, matching
pending revision, integer-second nominal alignment, excluded actual equality,
pending eligibility evidence, and Impossible/cache consistency. Initialization also
needs a formal valid-source hypothesis; `beginMachine` permits arbitrary revision
and candidate metadata. For example, an ordered raw candidate with revision2 and a
revision1 initial state satisfies the current frontier/order/bounds premises but
can yield a revision2 pending candidate that fails public validation on resumption.
That is outside the intended valid-source domain, but no source predicate/theorem
currently connects that domain to the public predicate.

WI-013/D57 explicitly require state validity/reachability initialization and
preservation, so factored lemmas about a weaker frontier do not close that duty.
The README correctly discloses the limit; disclosure does not satisfy the gate.
</details>

**Suggestion:** Define source/continuation validity with explicit trusted raw-stream
assumptions. Prove begin and successful scan preserve the full public-valid reachable
state. Establish that supported positive-budget calls from that domain return `.ok`
(and no range/internal-limit error), including cached/empty branches. Lift kernel
resumability/batching through serialized restore into an actual public call-chain
success/completeness/uniqueness corollary. Do not assume all calls succeed or assume
the desired remaining/output equality as a validity clause.

<details>
<summary><strong>med</strong> — Historical Peek/Dispose has only local field lemmas.</summary>

`peek_no_output` and `peek_retains` (Controls lines97/103) describe `asPeek` for an
arbitrary Result. They do not establish that the result is the corresponding bounded
scan, that the rewound disposed cursor is correct after excluded candidates, or that
serialized pending/source frontier is preserved. `dispose_success` proves the local
record update, but not that disposing a due Peek result reproduces one Run emission.
These are actual operations used by the unified coordinator, not an unused API.

The gap matters even when Run's theorem is correct: an incorrect cursor rewind in
`Oracle.asPeek` could pass both current Peek lemmas while duplicating or skipping
historical work on the next call. Go correspondence is useful independent test
evidence but does not establish the requested state-preservation theorem for these
allowed continuation steps.
</details>

**Suggestion:** Prove that a successful due Peek followed by valid Dispose reproduces
the bounded one-output Run state/candidate, with exact excluded-prefix disposition,
frontier/reconstruction and validity preservation. Prove repeated waiting/capacity
retention is a stutter and successful rebase establishes the new valid frontier.
Then lift completeness/uniqueness to the finite Peek/Dispose protocol under explicit
fair handoff assumptions, or give a checked reduction to the existing Run theorem.
This need not prove external admission fairness or the whole scheduler in Lean.

## Original proposition reconciliation

| Original statement | Current evidence | Acceptance interpretation |
| --- | --- | --- |
| `NextIsFirst` | Conservation, output-due and strict-order lemmas imply the intended first-output property on the ordered kernel domain, but no named public corollary exists | Include it in the public lift or explicitly state the derived theorem; never label the old definition as proved by name alone. |
| `CompleteEnumeration` | Kernel sufficient and finite positive-budget completeness are stronger about budgets; public existence/success gap above remains | Existing old statement included an existence guard precisely to rule out an always-error scanner. Preserve that protection in the wrapper theorem. |
| `OutputUnique` | `resume_unique` and `initial_resumability` prove strict nominal order, including concatenated outputs | Genuine result under stated initial order; needs public/source-domain connection above. |
| `ResumeEquivalence` | Current results prove completed sequence equivalence, not exact split-fuel state/stop/probe equality or disposition-journal equality | Do not claim this original proposition is proved. Investigation explicitly proposed proving it; reconcile that promise before WI-013 acceptance. |
| `BatchEquivalence` | Current adjacent/finite-window proofs establish occurrence-sequence equality, not final full-state equality | Plan SP3 explicitly asks occurrence sequences, so these theorems satisfy that sequence claim. They do not by themselves fulfill the stronger originally proposed state equation. Record the exact relationship; if retaining the investigation's original-statement promise, prove the corresponding state theorem. |
| `PendingStable` | `pending_stable` proves the complete kernel Machine/result stutter for a future candidate | Genuine and covers excluded candidates; public successful lift still needed. |
| `UpdateBoundary`, `UpdateRejectsStale` | Actual helper boundary/stale lemmas are proved, with historical disposed-only rebase explicitly distinct | Correct local claims; complete new-state validity/reachability is not yet derived. |
| `ProbeBound` | Executable kernel bound and structural recursion sufficiency proved | Candidate work only, not calendar CPU cost. Public budget rejection is executable/tested. |

The stronger full-state/fuel-composition promises must not disappear by silently
calling every original statement historical. The minimal plan SP3 wording alone
does not demand equal probes across windows, and the current sequence theorem is
not deficient merely because it does not prove that different claim. The item record
and investigation must consistently identify which additional state equations remain
required. This review does not authorize narrowing an accepted gate.

## Source, parser and axiom boundaries

Current wire-v1 explicitly states that parsing validates JSON types/required fields,
not all mathematical raw-stream hypotheses. Therefore accepting an unordered raw
fixture is not reported here as a wire-contract violation. Lazy out-of-supported-range
actual candidates intentionally reach the probe-time error. Source ordering,
nonnegative jitter and candidate metadata hypotheses still must appear in the formal
valid-domain predicate used by the proof bridge. The JSON parser and arbitrary
malformed-state rejection need not be universally verified to satisfy the stated
SP2/SP3 theorem, provided their concrete correspondence coverage remains explicit.

Build03 audit output lists only `propext`, `Quot.sound` and, for selected proofs,
`Classical.choice`. No project axiom/sorryAx/native_decide dependency appears in its
listed theorem audit. These are recorded Lean foundational assumptions, not a claim
of axiom-free proof. The core proofs use induction, executable equations and list/order
lemmas; no desired-output hypothesis was found in the final unconditional kernel
completeness theorem.

Current `Controls.lean` differs from build03 by the new scan reconstruction and
conservation lemmas. Build03 recorded Controls SHA256
`a47829def792ec0db8393226a93052f5914ee6a84688cbd80ef195254c8274b7`;
the reviewed current hash is listed below. Other manifest source hashes matched at
review. Audit.lean does not yet print axioms for these two new exported claims.
A fresh complete source build/audit is required; an old successful binary does not
certify later proof additions.

`build.py` records `#print axioms` output but does not reject forbidden dependencies
or compiler warnings automatically. Manual review of build03 establishes the current
listed claims' dependencies; future automated gate use should fail on sorryAx or
unexpected project axioms and verify the required theorem list is fully audited.
This is evidence-runner hardening, not an assertion that a forbidden axiom was found.

## Reviewed source fingerprints

```json
{
  "Audit.lean": "c0a2ca8b21025ff41b7ce7506263967919096dd24563ff31acf899f1023b67a3",
  "Controls.lean": "6879c408511995ae05c5d106fe648a193a54d21880bf380153bd84e309c766b0",
  "Frontier.lean": "c7b90bda984551e14b2e2e1b8a255e476f6fcc074838aba5890511440ca32a76",
  "Kernel.lean": "4ae8bfa669039aefd8cd74478ba72fa759d1623012138538f772d261fecc62e3",
  "Oracle.lean": "d94d226b6a104c1ad1d2bfca7bb1257299e0946cff59c9f116b92fa1d52e3846"
}
```

No production code or Go test was reviewed as an implementation-correctness claim
in this pass. M3 full calendar/SP7, Go comparison sensitivity/replay and API coverage,
and all M4 requirements remain independently open.

## Repair review — public validity and serialized lifecycle (2026-11-11)

This appendix supersedes the two blocking public/lifecycle findings above for the
fingerprinted source below. It does not erase the initial review or certify that
older builds contained these repairs. The reviewer independently read WI-013/D57,
the original Proposed obligations, Controls, Validity, Public, PublicComposition,
Oracle and the production Run/Peek/DisposeRange/Update/RebaseRange boundaries.
**Scope separation:** this reviewer authored KernelComposition and earlier Go
replay repairs, so does not independently approve either implementation here.
The parent separately reviews KernelComposition. This appendix reviews the public
bridges' use of its stated interface, not that imported proof's correctness. No
production source was edited, and no Go tests were run during parent mutation windows.

### Findings resolved

1. **Public success, full validity and serialized continuation: resolved.**
   `CandidateValid` and `SourceValid` state independent input requirements, including
   supported/aligned times, matching revisions, nominal order, actual monotonicity
   and excluded-candidate semantics. `initial_valid`, `loop_valid` and `run_valid`
   establish/preserve the entire executable `validState` check. `initial_ready`
   and `ready_run` establish/preserve identity, fullRange and terminal-cache
   conditions as well. `ready_scan_success` derives an actual `.ok` result from
   these invariants and supported public inputs; it does not assume success,
   completed consumption or the desired output. The proof eliminates lazy actual
   range failure using independently valid candidates and eliminates internal-limit
   failure using the structural recursion bound. Cached/empty behavior is handled
   explicitly rather than silently excluded from the public protocol.

   `publicResume` passes only `first.machine.state` into every subsequent `scan`;
   `public_resume_kernel` proves reconstruction recovers the execution-local suffix.
   `public_initial_complete` therefore supplies actual public success, due output
   and strict nominal uniqueness from initialization across finite positive-budget
   calls, including output caps and probe yields. Its rank/call-count premise is a
   concrete sufficient resource bound, not a disguised completeness assumption.
   The older Controls success premises are now legitimate interface lemmas beneath
   this existence proof, rather than the sole support for public completeness.

2. **Peek/Dispose lifecycle and replacement: resolved by checked reduction.**
   `peek_run_valid` proves validity after the actual one-output Run-to-Peek
   transformation, including excluded-prefix cursor bounds and eligibleSeen.
   `peek_dispose_run` proves Dispose rejoins the entire one-output Run state.
   `public_peek_dispose` derives actual Peek and Dispose success and Ready
   preservation from supported inputs; its nonempty one-output guard selects the
   due-handoff branch and does not assume eventual completeness. No-output Peek
   uses the same Run result by the executable `asPeek` definition.
   `peek_reconstructible` and `ready_peek_run` retain the serialized continuation
   invariant. `public_capacity_wait` is exact zero-output/zero-probe state stutter
   for repeated due pending Peek. `public_pending_stable` proves the future-pending
   public Waiting response without changing state or probes. Thus the admitted
   handoff step reduces to Run, while capacity waits intentionally do not progress;
   actual admission fairness/atomic persistence remain caller obligations.

   `replacement_boundary`, `replacement_initial`, and `replacement_ready` retain
   Update's max(time, disposed) boundary, Rebase's disposed-only boundary, and the
   superseded pending identity, then initialize the new definition's invariants.
   Success premises here express preservation of a successful control transition,
   consistent with the original UpdateBoundary proposition; they are not used to
   evade Run completeness. Stale rejection and malformed-state precedence remain
   separately stated. Historical fixed range endpoints remain caller-owned, as in Go.

3. **Named public obligations and exact call linkage: resolved at the bridge.**
   `public_next_first` provides an actual successful one-output call and identifies
   its first due result when nonempty. `publicPair` composes two actual scans using
   only serialized state; `public_probe_resume_exact` and
   `public_window_batching_exact` lift the imported exact kernel equations while
   proving public success under independent valid-call requirements.
   `ready_resume_reconstructible` and `public_finite_window_batching_exact` do the
   same for finite public partitions plus an actual final scan. Their sufficient
   resource predicates constrain lengths/budgets and endpoints, not outputs or
   final-state equality. The latter retains the final public call's stop.

### Semantics and evidence limits retained

- `Ready` is an invariant superset, not proof that every arbitrary pending value
  originated in the immutable raw source. This matches Go's structural validation,
  which does not authenticate pending membership. Initial/replacement chains
  provide the reachable path; arbitrary Ready results are relative to the supplied
  pending value. The updated README states this distinction.
- `eligibleSeen` is monotone and emission implies seen. Full-range initial
  Impossible implies no eligible bounded-source candidate; conversely a fully
  excluded source reaches kernel Impossible at the supported upper window with
  sufficient probes. Public lifting requires inclusions and valid budgets. A
  no-inclusion public definition returns Empty, separately proved by EmptyReady
  and `public_empty_resume`; the vacuously excluded empty raw list is not a license
  to conflate Empty with Impossible.
- `publicResume` and kernel `resume` summarize the final terminal cache, not the
  last Waiting/Yield/Limited response. The public exact pair/final-scan wrappers
  intentionally preserve the actual final response instead. The README now says so.
- Public sufficient single-call batching has the real 128-output/1024-probe bounds;
  the uniform pair theorem permits at most 127 remaining raw candidates. This is
  not a claim that a larger source fits one public call. Finite positive-budget
  completeness remains applicable through repeated supported calls.
- Universal claims concern the independent Lean finite-stream implementation.
  Production calendar search, exclusion/hash/timezone implementation, JSON parsing,
  source identity hashing, serialization transport, persistence and timer delivery
  are not proved here. Production linkage requires separately recorded bounded Go
  correspondence/replay evidence. Signed-input/error projection is explicit in
  wire-v1 and contextual malformed-state theorems; no broad parser verification
  claim was found. The review does not independently certify this reviewer's own
  earlier Go v2 replay changes.
- The exact consumed-prefix disposition correspondence is imported from
  KernelComposition and independently reviewed by the parent. No fictitious
  disposition field was added to Go's concrete result.

**Review result:** no remaining blocking semantic finding in the independently
reviewed public validity, control or serialized lifecycle source. This is a scoped
source-review result, not WI-013/M3 acceptance. Final immutable build/axiom audit
and Go comparison/mutation evidence remain integrator-owned gates. At this snapshot,
build11 was running against frozen source; no unfinished build is counted as passed.
The runner now fails on missing audited theorem results, forbidden axioms and
unfinished-proof diagnostics, closing the earlier fail-open audit-runner concern.

### Repair-review source fingerprints

```json
{
  "Controls.lean": "6879c408511995ae05c5d106fe648a193a54d21880bf380153bd84e309c766b0",
  "Validity.lean": "1b51cc5617287863470af71783ad5a2d293a8f2ddf0a0a5765a806b0813309ac",
  "Public.lean": "1c50bf0c7367e907076a91e656256e04a65f9adeceb1a34fdd9043fe1d9b6257",
  "PublicComposition.lean": "ce1fe975301d9690a7f237036e0216886fc9e9746dd03e5ba4387c28777709f2",
  "Oracle.lean": "143cca85dbfd48e23bc10cb7ddb711209051f1ae2169d3e06df3b71ff9ac560d",
  "Frontier.lean": "c7b90bda984551e14b2e2e1b8a255e476f6fcc074838aba5890511440ca32a76",
  "Audit.lean": "4e00918c590948f81b96cc7e9aef7e56222bbb45c6f501314d327d15c057fcdd",
  "build.py": "e8897c7be6993af079d561e5507d7a3a6e1f2d4ac3f3f5d64071f061c0ba4ba4",
  "README.md": "5f561d833f396706155833ce52bb7376a654d8df393e7678088bdc32767fd778",
  "wire-v1.md": "9399ad5f5c17701e8d0066cf89a195b40139d56c584e11e9b2ba00a84b7f2ebc"
}
```

### Build11 evidence linkage

After the source-review snapshot, independently inspected the completed immutable
`/private/tmp/scheduler2-wi013-lean-build11/manifest.json` and `audit.log`. Status is
passed; all 11 recorded commands exited zero without timeout, all 93 required
axiom reports are present, and reported dependencies are limited to propext,
Quot.sound and Classical.choice. Manifest source hashes match both their recorded
after-build hashes and the current frozen files. This establishes compilation/audit
linkage for the reviewed source; it does not convert the excluded self-authored
modules into independent source review or replace Go correspondence.

- Manifest SHA256: `5e954a372fcac342118ffd8b69d8cee7317f519fbb66ddcfa597385640bf48a2`
- Audit log SHA256: `e8e853edd5799600c5fcba0ebc82d156fd719f95c97d4061e04614c040b723ca`
- Oracle binary SHA256: `3e0b7213a8e63024cc7a219c003cbe4fff22e8b683029ce4694c15b58112db6d`
