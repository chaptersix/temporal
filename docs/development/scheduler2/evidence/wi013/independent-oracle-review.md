# WI-013 independent oracle and replay review

Read-only review of the correspondence driver, standalone JSON replay, mutation
runner, public Go occurrence interfaces and Lean wire wrapper. Repository review
and testing instructions were read. No tests, builds or mutations were run by this
reviewer; parent owns an exclusive mutation window and its execution evidence.
This is source review, not acceptance of prior green runs or WI-013/M3.

## Findings

<details>
<summary><strong>med</strong> — Generated update failures cannot be saved as complete standalone replay traces.</summary>

`TestLeanCorrespondenceRapidWindowsAndUpdates` changes the compiled definition at
step1 but reports failures with plain `t.Fatal(err)`. Its initial definition,
replacement definition, preceding windows and complete command order are not
serialized. `leanReplay` has only one immutable `Fixture`; SavedReplay compiles it
once and therefore cannot reproduce a successful Update/RebaseRange to a newer
revision. The three mutation checks currently exercise only the static-source
Rapid property. This leaves WI-013's generated replacement/range correspondence
without the required explicit, generator-independent replay path.
</details>

**Suggestion:** Add a versioned replay definition registry and an explicit source
reference on each step, validate those references, and record the same concrete
trace from both generated properties. Retain compatibility with existing v1 traces
or provide an explicit conversion. Add saved automatic-update and historical-rebase
traces and prove they replay without Rapid. Do not reconstruct omitted definitions
from the final mismatch request alone.

<details>
<summary><strong>med</strong> — Negative wire numbers bypass the documented operation-specific error precedence.</summary>

`Oracle.natField` maps every negative decimal string directly to `input-invalid`
during parsing. That happens before `scan`'s identity check and before `replacement`'s
state/stale validation. The concrete Go path and the driver's stated projection
can therefore disagree. Source-derived examples (not executed in this review):

- Valid source plus a continuation with wrong DefinitionID and `run.probes=-1`:
  Go Run checks identity first, producing `identity-mismatch`; Lean parses probes
  first, producing `input-invalid`.
- Update to a valid newer source with `state.revision=-1`: Go validates the supplied
  state first, producing `state-invalid`; Lean's state parser returns `input-invalid`.
- A stale Update with `at` below the shifted sentinel: Go's combined rejection is
  explicitly mapped stale-first by `compareLeanStep`; Lean rejects the negative
  time before checking stale revision.

Current negative tests use zero and max+1 budgets plus one malformed positive
origin, so they do not discriminate this mismatch. This is an executable oracle
validation gap, not a request to change production Go semantics.
</details>

**Suggestion:** Preserve signed input/error information until the operation's
validation stage, or implement contextual parsing that exactly preserves Go's
precedence for the documented wire projection. Add directed correspondence cases
for negative state/budget/time fields and combinations with identity/staleness;
do not broaden error normalization to hide the difference.

## Evidence quality and limits

- The raw source is independently enumerated by per-second UTC interval membership
  and structured-second exclusions. It does not call production NextTimeCandidate
  or canonicalization for expected candidates. Farm32 and time.MarshalBinary are
  explicitly shared primitives. Finite nominal bounds and lookahead preserve the
  next nominal used for last-candidate jitter, including spec-end crossing.
- DefinitionID is obtained from Go Begin and treated as opaque authority, not an
  independent hash proof. A genuine changed-seed/same-revision test discriminates
  identity reuse. This named boundary is acceptable for enumerator correspondence;
  it cannot establish collision resistance or correctness of canonical hashing.
- Comparison includes the entire mapped state, outputs, stop, probes, superseded
  candidate and error class after each operation. Go input ownership is checked.
  Although the variable is named expected for the Go observation, the counterpart
  is an independently executable Lean result from an independently built stream;
  this is not the production reducer computing its own oracle.
- 384 exhaustive traces are exactly16 exclusion subsets ×2 fractional starts ×2
  jitter settings ×3 probe budgets ×2 output budgets. Generated cases are bounded
  UTC interval fixtures, not full calendar/DST/SP7 coverage. The valid-budget edge
  test exercises actual1023/1024 probe saturation and127/128 output saturation.
- The external binary is mandatory under the explicit lean_correspondence tag;
  absent binary fails. Ordinary unit runs do not claim oracle evidence. One
  bounded subprocess is reused per test, with explicit EOF/Wait before context
  cancellation. The known testing.T.Context cleanup bug was corrected.
- Static-source Rapid failures call `leanRapidFailure` directly with Rapid.T, so
  assertions participate in shrinking. The runner extracts the final explicit
  trace, reruns it against the mutant through non-Rapid SavedReplay, restores exact
  source bytes in finally, and requires that restored replay pass. Compilation,
  panic, timeout, missing oracle response and cleanup failures cannot count as
  semantic mutation detection. Actual parent logs still need to establish that
  each required case ran and the retained trace is Rapid's final reduced input.
- The saved replay currently reconstructs initial state through Begin from explicit
  fixture inputs. That is valid for its generated reachable-state domain; arbitrary
  malformed-state records and definition replacement require schema support as
  described above. Unknown schema versions and operation names reject.
- The Go response decoder rejects unknown keys but uses typed zero values; it does
  not independently establish required-field presence for every zero-valued wire
  field. Oracle source visibly emits those fields. Do not describe this decoder
  as a complete wire-schema validator.

## Disposition

The two findings above block WI-013 correspondence/replay acceptance as currently
specified. Existing passing bounded comparisons remain useful evidence with their
recorded scope. Proof-statement review, final source-specific runs, malformed wire
checks and all three mutation/shrink/replay outcomes remain separate acceptance
requirements. Full SP7 and public API/shared functional gates remain M3 duties.

## Reviewed snapshot

- `chasm/lib/scheduler2/internal/occurrence/lean_correspondence_test.go`: `396203c83770c04d1723adc8d5f12c529ea7f32567f7e4bdc0c8e067c7a324a5`
- `docs/development/scheduler2/evidence/wi013/correspondence-mutations/check.py`: `1a69106998f17d3f954dfcb7bcc702a1084e811870211dc17403eb658af84ecf`
- `docs/development/scheduler2/lean/exact/Oracle.lean`: `d94d226b6a104c1ad1d2bfca7bb1257299e0946cff59c9f116b92fa1d52e3846`
- `docs/development/scheduler2/lean/exact/wire-v1.md`: `5a819a429e04a4e9299371b71f3b87370cc12ac5099266482826b4af739d893e`

## Follow-up implementation (verification pending)

After the parent's exclusive mutation window completed with restored source, this
reviewer became implementation owner for the Go replay/negative-regression repair.
The original findings and reviewed snapshot above remain unchanged. This follow-up
is not an independent review of its author's repair and makes no passing-test claim.

The driver now emits replay version2 from both generated properties. `definitions`
is an explicit name→fixture registry; each step has a `definition` reference and
`expect_success` for positive generated commands. Update/RebaseRange switches source
by reference and retains prior definitions for exact replay. The standalone reader
compiles each declaration once, rejects missing/unknown source references and
unsupported versions, checks trailing input, and preserves strict version1 fixture
compatibility. New `lean_correspondence_update_v2.json` and
`lean_correspondence_rebase_v2.json` exercise successful pending supersession and
subsequent enumeration. The per-request Lean oracle wire remains version1; replayv2
is a separate multi-command artifact format, not an incompatible wire change.

Nine directed negative/error-precedence cases are added in
`TestLeanCorrespondenceNegativeInputPrecedence`. The Lean owner is repairing
contextual signed-input handling separately. Parent should retain the old oracle's
mismatch, then run final source against the fresh immutable binary, including saved
v1/v2 replay, both generated properties and mutation/shrink/replay checks. Exact
source fingerprint after this repair:

- `lean_correspondence_test.go`: `6acc15898e81612c90fba9bf47747ecd6e44a7a5d0d53c76bd307e6c4252b050`.

No tests were run by this reviewer; gofmt and source diff whitespace checks passed.
