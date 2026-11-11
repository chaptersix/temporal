# WI-002 executable enumeration specification and proposed statements

Owner: independent enumeration-spec agent. Source base `00b65430a`.
Contract input: WI-002 ordered-source and definition-update contract supplied by
the integrator. These definitions do not import Go production logic or call its
calendar evaluator. `Enumeration.lean` uses only Lean's standard library.

`Proposed` contains **proposition definitions**, not theorem declarations or
completed proofs. The compiler checking that these are well-typed does not prove
them. There are no assumed theorem declarations or unfinished proof placeholders.
Completed SP2/SP3 proofs and their axiom audit remain M3 gates. Executable examples
are discriminating fixtures, not universal proofs or Go correspondence evidence.

Compile locally once the integrator pins and supplies Lean:

```sh
cd /private/tmp/sch-redesign-enumeration-spec/docs/development/scheduler2/lean
lean -o Enumeration.olean Enumeration.lean
LEAN_PATH=. lean Examples.lean
```

No Lake dependency download or third-party library is required. Compiler version,
checks and disposition are recorded separately by the integrator. Generated
`.olean` files are build artifacts, not source contributions.

## Executable semantics

`Candidate` stores nominal integer seconds, actual integer milliseconds and an
eligibility/exclusion bit. `Source` stores immutable revision, lower nominal and
upper actual range boundaries, presence of inclusion terms, and a finite raw
candidate stream. Its ordered list is the trusted source abstraction; a linear
`find?` oracle produces the next raw nominal strictly greater than `searchAfter`.
This oracle's CPU complexity does not represent Go's bounded calendar-call cost.
One logical source probe is counted per lookup, including an end-of-source lookup.
The adapter assumption is that each Go probe uses at most two raw calendar calls.

`State.searchAfter` can advance when a candidate is discovered;
`State.disposedThrough` advances on candidate disposition or when an explicit
update disposes the skipped interval through its boundary. The pending
candidate retains its complete original value across work yields and output
windows. Output windows include their upper actual-millisecond endpoint. Search
boundaries exclude their nominal-second endpoint. Exclusions are never emitted. An excluded raw candidate has actual = nominal
in milliseconds. A future excluded candidate remains pending until its window;
its disposition must not move disposedThrough into the speculative future.
This lets an intervening definition update reconsider that future nominal.

Probe fuel and output slots are independent. Emitting an existing pending value
uses no probe. A private structural recursion counter starts at 2 * fuel + slots + 2;
each recursive transition consumes a probe, an output slot, or disposes one
excluded value. The counter covers at most one initial buffered excluded value. The impossible zero-counter
branch is an explicit error rather than an assertion or fabricated completion.
`ProbeBound` proposes the intended public fuel guarantee.

Stop meanings:

- `yielded`: no probe fuel remains when another source lookup is needed.
- `waiting`: a pending candidate is beyond the inclusive output window.
- `limited`: no output slot remains, taking precedence over further search.
- `empty`: no inclusion terms are configured.
- `impossible`: full effective source range was searched, with no eligible value.
- `exhausted`: no next raw candidate remains in this source suffix.

`fullRange` is established only when initialization starts strictly before the
source lower bound; `eligibleSeen` persists across resumptions. A range-invalid
actual timestamp is an error, not exhausted. Sources cannot silently change
revision. The Go 2000–2100 range is mapped to explicit valid natural-number
bounds; there is no machine-integer overflow in Lean's Nat domain.

An explicit definition update records any pending candidate as `superseded`,
clears it, and sets both cursors to max(update boundary, disposedThrough).
This records the skipped old-revision interval as disposed. An update must have
a strictly newer revision; equal or older revisions return `staleRevision`. Superseding
a future unadmitted candidate does not advance disposedThrough to that candidate.
The new revision may therefore produce the same nominal value again. Admitted
WI-001 operations are outside this state and remain immutable.

## Proposed claims and assumptions

| Proposition | Scope |
| --- | --- |
| `NextIsFirst` (SP1) | An emitted singleton equals the first declaratively eligible value; no earlier eligible candidate is skipped. |
| `ResumeEquivalence` (SP2) | Splitting probe fuel at a yield preserves concatenated output/dispositions, total probes, final state and stop. |
| `BatchEquivalence` (SP3) | Splitting inclusive output windows with sufficient work/slots preserves concatenated output and final state. |
| `PendingStable` (SP4) | A pending value beyond the window remains identical, with no output, disposition or probe. |
| `UpdateBoundary` / `UpdateRejectsStale` (SP5) | A newer update sets both cursors to max(update boundary, disposedThrough), clears pending, and records supersession; stale revisions are rejected. |
| `ProbeBound` (SP6 precursor) | A successful call performs at most the supplied logical probe budget. |

`WellFormedSource` explicitly requires strictly increasing raw nominal values,
actual >= nominal, actual strictly before later raw nominal values, bounded actual
values, unjittered excluded candidates, and no raw values when inclusion terms are absent. `WellFormedState`
records source revision, cursor relation, pending membership and its
retention relation to the cursor. These are hypotheses to proposed propositions,
not globally asserted facts. Review may strengthen reachable-state hypotheses
before proofs; it must not silently weaken the intended property.

The finite source completely represents the chosen effective range. This is an
assumption about the oracle, not evidence that Go calendar search recognizes
impossibility correctly. DST, timezone conversion, overflow, source probe cost,
jitter generation/stability and calendar exclusions remain unproved. SP7 requires
separate pinned-timezone Go examples. Comparing Go to this executable spec is
future correspondence testing, not implied by compiling Lean.

## Source identity and state validation boundary

The Go implementation carries an origin marker and a canonical definition plus
seed fingerprint. This abstract source uses revision as a trusted identity for
the complete ordered candidate stream, including jitter choices and exclusions.
Reusing one revision for different stream contents violates that assumption.
`fullRange` abstracts origin tracking; it must only be set when a scan starts
before the effective lower bound. The Go adapter's malformed-state validation
is outside this core: the proposed claims quantify over `WellFormedState`.
Compiling these definitions does not verify canonicalization or seed hashing.

## Terminal-state abstraction

Go caches terminal state to avoid repeated source probes after exhaustion. This
Lean core erases that cache: a resumed terminal state may perform a fresh end-of-
source probe and report the same terminal result. Output/disposition semantics
are the proposed mapping; terminal resumption probe counts are not asserted equal
to Go. `ResumeEquivalence` compares Lean executions only, with its first call
required to yield rather than terminate. No Go correspondence proof is claimed.
