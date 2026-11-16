# Proposed exact enumeration wire v1

Status: proposed for integrator review before Go adapter implementation.
One input JSON object per line; one response per line, in input order. The oracle
is compiled once. All integer quantities use decimal strings (no JSON float loss).
Unknown version, keys, operation names and malformed numbers are errors, never skips.

`version` is the integer1. `source` contains `identity` (opaque nonempty string),
`revision`, `lower`, `nominalEnd`, `upper`, `hasInclusions` (boolean), and `raw`
(array of candidates). Candidate fields: `nominal`, `actual`, `revision`, `excluded`.
All time fields are shifted exact nanoseconds: zero represents
2000-01-01T00:00:00Z minus1ns, and supported time1 is the lower endpoint.
`upper` is the last supported actual instant, distinct from `nominalEnd`.
Raw candidates belong to the nominal definition range and are strictly ordered;
eligible actual is at least nominal and strictly before the next raw nominal in
the theorem's valid ordered-source domain. Excluded actual equals nominal. Source
identity authenticates the entire immutable definition/seed, not merely revision.
Parsing validates types and required fields, not the theorem's source hypotheses.
In particular a raw actual beyond the supported upper bound is allowed as a negative
fixture and must fail only when probed, matching Go's lazy candidate evaluation.
`upper` must equal the fixed supported-domain maximum; it is not a custom spec end.

`state` is null only for `begin`/`beginRange`; otherwise fields are `identity`,
`revision`, `origin`, `searchAfter`, `disposedThrough`, `pending` (null or complete
candidate), `fullRange`, `eligibleSeen`, `terminal` (active/empty/impossible/exhausted).
No hidden mutable state persists between requests. A request can therefore replay
an explicit saved continuation or supply the previous response state.

`op` selects exactly one of:

| Operation | Additional request fields |
| --- | --- |
| begin | after |
| beginRange | start, end |
| run | through, probes, outputs |
| peek | through, probes |
| peekRange | end, probes |
| disposeRange | none |
| update | at |
| rebaseRange | none |

For update/rebase, `source` is the new immutable definition and `state` belongs to
the old definition. Public run probes1..1024 and outputs1..128 are checked; theorem
internal residual zero is not a public input. Supported times, state shape and
identity checks precede execution according to the accepted contract.
Structural state validation follows Go; it must not require pending membership in
raw, which Go does not check for an explicitly supplied continuation. Proofs of
reachable state have stronger provenance invariants than this public validation.
Positive correspondence traces start from begin/accepted updates and preserve
immutable identity. Negative traces can supply malformed state or conflicting
identity explicitly; they compare the concrete rejection class, not theorem claims.

Success response: `{version:1, ok:true, state:..., status:..., outputs:[...],
probes:"...", superseded:null|candidate}`. For non-scan controls status equals the
new state's terminal status, outputs is empty and probes is zero. For peek calls
outputs remains empty; eligible pending is disposed only by disposeRange. Scan
status additionally permits waiting/yield/limited. PeekRange maps waiting to range
exhausted without replacing the source terminal cache. `superseded` is populated
only by update/rebase when an old pending candidate exists.

Error response: `{version:1, ok:false, error:"..."}`. Stable error classes are
wire-invalid, source-invalid, input-invalid, state-invalid, identity-mismatch,
stale-revision, actual-outside-range. Error yields no successor state; the caller
retains the supplied state. Go error-message text and gRPC spelling are not compared.
The Go adapter must map concrete errors to these documented semantic classes.

No source timestamp rounding, automatic status normalization or omitted pending
fields is allowed. Source raw lists are independent expected fixtures, not copied
from production next-occurrence output. Candidate disposition can be derived from
cursor movement plus outputs and superseded; it is not a Go result field and is
not synthesized into a fictitious concrete journal.

Signed decimal strings are parsed without selecting a semantic error early.
For invalid inputs only, negative times project to an out-of-domain natural sentinel,
negative revisions/budgets to zero, and invalid terminal names to an invalid-cache
sentinel. Operation guards then preserve identity/budget/state precedence. PeekRange
compares original signed end/origin before that projection. Valid timestamps and
budgets are unchanged; these sentinels do not make malformed data valid.
