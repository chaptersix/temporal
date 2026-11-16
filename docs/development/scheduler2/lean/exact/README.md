# Exact executable enumeration and proofs — WI-013

This is the independently implemented D57 successor to `../Enumeration.lean`.
The old executable and proposition definitions remain historical; their names are
not silently relabeled as proved by this directory. The integrator owns WI-013
acceptance and the Go comparison evidence.

## Executable and proof connection

`Kernel.lean` actually executes raw probing, pending retention, exclusions,
disposition and output limits. Probing moves one immutable candidate from a local
source suffix to pending. Disposition removes pending; eligible disposition emits
the candidate without recomputing its jitter. The private recursion bound is
`2 * rest.length + pending.length + 1`. `run_no_internal_limit` proves that bound
suffices when the input terminal cache is not the internal error sentinel.

`Frontier.lean` proves that initialization establishes the cursor/source frontier,
that probing and disposition preserve it, and that reconstructing the source suffix
from the final serialized cursor equals the actual execution-local suffix.
`Oracle.lean` directly uses these `beginMachine`/`restore` definitions. Its successful
active Run is proved equal to the kernel by `Controls.scan_success_kernel`.
The oracle also supplies public validation, cached terminal/empty handling,
historical peek/dispose, update/rebase and strict JSON transport.

The finite raw stream is an independent input. Correct production calendar,
exclusion and jitter evaluation is **not** inferred from these proofs. The Go
correspondence layer must obtain expected streams independently of production
next-occurrence search. Exact semantic times are shifted nanoseconds: sentinel0
is the lower supported instant minus1ns, supported instants start at1. Source
nominal end and supported actual upper bound are distinct. A candidate may jitter
past the definition's nominal end while remaining within the supported time domain.

## Machine-checked claims

| Theorem | What is proved | Hypotheses/boundary |
| --- | --- | --- |
| `loop_conservation`, `resume_conservation` | Every initially unconsumed eligible candidate is emitted or retained, including across finite call sequences | Actual executable recursion; no desired-output equality assumed |
| `run_probe_bound` | Work does not exceed supplied probe budget | Candidate probes, not calendar calls or CPU steps |
| `sufficient_completeness` | A sufficiently funded single call emits exactly the independently defined due prefix | Cache active or remaining stream empty; raw length+1 probes and remaining length+1 output slots |
| `resumable_completeness` | Arbitrary positive probe/output budgets eventually emit the complete due prefix | Fixed inclusive final window; finite call count greater than `2*rest.length+pending.length`; covers repeated probe yields and output-limit stops |
| `resume_unique` | Concatenated resumed output has strictly increasing nominal identities | Initially eligible stream is nominally ordered |
| `initial_resumability` | From actual initialization, output equals the raw stream filtered by after<nominal, eligibility, actual≤through, and is unique | Nominal strict order, actual monotone order, positive budgets and the explicit call bound; no assumed final state/output |
| `finite_window_batching`, `adjacent_window_batching` | Earlier bounded windows followed by a sufficient positive-budget final suffix produce the exact unsplit occurrence sequence | Earlier endpoints≤final; includes arbitrary finite nondecreasing partitions. Actual monotone raw order and cache validity |
| `begin_frontier`, `loop_frontier`, `run_reconstructible` | Source/cursor invariant initialization/preservation and exact local suffix reconstruction after serialization | Nominally ordered source and valid starting frontier; pending provenance is positional, not an assumed output |
| `begin_bounds`, `loop_bounds`, `loop_authority` | Origin≤disposed≤search and unchanged identity/revision/origin/fullRange across scans | Initial bounds/frontier for the bound theorem |
| `pending_stable` | A complete future pending candidate is retained exactly, with zero output/probes | Active cache, positive output slots, candidate actual beyond window; eligible and excluded candidates both included |
| `begin_identity`, `replacement_boundary` | Actual public helper initializes exact fields; successful update/rebase records superseded pending, advances revision and resets both cursors at the specified boundary | Automatic boundary=max(update time, old disposed); historical boundary=old disposed; no pending-nominal shortcut |
| `replacement_stale`, `dispose_success`, `peek_no_output`, `peek_retains` | Valid stale updates reject; successful disposal removes exactly the eligible pending candidate; peek emits nothing and restores the candidate intact without changing probe accounting | These are actual helper functions called by the oracle, not a complete universal Peek/Dispose composition theorem |
| `scan_success_kernel` | Successful active public Run is the proved executable kernel | Source has inclusions; concrete input validation and probe-time range error checks succeeded |

Source ordering is sufficient to derive actual monotonicity from the contract's
nonnegative jitter strictly before the next raw nominal. The proof accepts the
explicit actual-order hypothesis rather than assuming calendar correctness.
`initial_resumability` proves source-to-output completeness directly; the earlier
`resume_complete_when_consumed` lemma is only an intermediate conditional result
and is not substituted for it.

## Public validity, serialization and original obligations

`Validity.lean` proves the **entire** public `validState` predicate at initialization
and after probing, disposition, terminal classification and arbitrary kernel Run.
`Public.lean` defines `Ready`: validity, identity/revision agreement, fullRange
agreement, allowed non-empty terminal cache, and an empty reconstructed suffix for
cached termination. `initial_ready` establishes it and `ready_run` preserves it.
It is an invariant superset, not a claim that every arbitrary Ready pending candidate
came from the source. `public_initial_complete` starts at the actual initializer;
results for arbitrary Ready states are relative to their serialized pending value.

`ready_scan_success` proves actual public Run succeeds from those invariants and
independently valid source candidates under supported windows and public budget
bounds. No successful result is assumed. `public_resume_kernel` discards the private
suffix after **every** public call and proves cursor reconstruction gives the kernel
continuation. `public_resume_complete` and `public_initial_complete` then prove
exact due output and uniqueness with arbitrary positive public budgets and the
explicit finite rank/call bound. Empty sources have separate `EmptyReady`,
`empty_scan_success` and `public_empty_resume` proofs, covering both Run and Peek.

The completeness folds `resume`/`publicResume` report the final state's **terminal
cache** as their aggregate stop; they do not claim to preserve the final public
call's Waiting/Yield/Limited response. `PublicComposition.publicPair` deliberately
composes the two actual scan results and preserves the second response's stop.

`KernelComposition.run_probe_resume` proves exact whole-Result equality after a
first call yields: complete machine/state, outputs, summed probes and final stop.
`run_window_batching_exact`, `run_window_batching_uniform` and
`finite_run_window_batching_exact` prove full-state window batching under explicit
sufficient resource bounds, including arbitrary finite earlier windows below the
final endpoint. These resolve the old draft's additional full-state obligations,
not merely SP3 occurrence-sequence equality. `public_probe_resume_exact` and
`public_window_batching_exact` lift exact equality to serialized public calls with
supported input budgets (including supported residual and combined budgets).
`public_finite_window_batching_exact` supplies the finite public partition
corollary through an actual final scan with exact final response stop. Public
sufficient single-call comparisons require the whole call to fit the 128-output
limit (the uniform pair bound therefore allows at most 127 remaining candidates).
Arbitrarily large finite sources use repeated supported-budget completeness; a
single-call public equivalence cannot assume an unsupported whole budget.
The executable API has no disposition journal field. `disposedTrace` is the unique
consumed raw prefix, including excluded candidates; `run_disposedTrace_spec` and
`run_probe_resume_dispositions` prove its exact preservation/composition. Candidate
exclusion flags and revision identities are retained, so classifying this trace
as excluded/emitted dispositions preserves the original journal distinctions.

`peek_run_valid` proves that Peek's rewind to the last excluded predecessor leaves
a valid state. `peek_dispose_run` proves Dispose returns the **entire** exact
one-output Run state. `public_peek_dispose`, `ready_peek_run`, and
`peek_reconstructible` connect that reduction to actual public validation and
serialized continuation. `public_capacity_wait` proves due buffered Peek is an
exact zero-probe, zero-output state stutter; `public_pending_stable` proves the
future-pending public response is exactly Waiting. `public_next_first` proves
one-output public Run returns the first independently defined due candidate when
it returns an occurrence. `replacement_ready` proves successful Update and
historical Rebase reset both continuation invariants via actual initialization;
`replacement_boundary` still establishes the distinct automatic/historical
boundaries. Arbitrary scheduler admission fairness is external: repeated Peek
without eventual Dispose intentionally makes no progress.

`initial_impossible_classification` proves full-range Impossible implies the
bounded raw source has no eligible occurrence. Conversely,
`initial_all_excluded_impossible` proves a supported full-range scan of an entirely
excluded source reaches Impossible with sufficient probe budget and positive
output capacity in the kernel. Public use additionally requires hasInclusions=true
and supported budgets; a public source with no inclusions returns Empty, as proved
separately, even though its abstract raw list is also vacuously all-excluded. `scan_invalid_state`, `dispose_invalid_state`, and
`replacement_invalid_state` prove malformed states reject at the appropriate
contextual validation step. The signed JSON normalization/parser is executable and
correspondence tested, not a verified parser. Independent Go calendar/exclusion/
jitter correctness, persistence, timer delivery, and admission fairness remain
outside these finite-stream enumeration theorems.

## Build, audit and evidence

Pinned local toolchain: Lean4.24.0, no Lake/network/mathlib dependency. Run:

```sh
python3 docs/development/scheduler2/lean/exact/build.py --out /private/tmp/unique-wi013-build
```

The runner compiles each module, checks `Audit.lean`, compiles one persistent JSON
binary, and records exact commands, source/tool/binary SHA256, elapsed times and
failures. Timeouts kill the process group. Use a fresh output directory to preserve
accepted binary evidence. The binary reads one JSON request per line and emits one
response per line; see `wire-v1.md`. Public zero/over-limit budgets are rejected,
although internal theorem residual budgets can be zero.

Every build uses a new output directory and distinct manifest. Build03 predates
the public validity and exact composition repairs; build07 adds Validity and
build08 adds the initial serialized public completeness bridge. The final repair
build adds all Public and KernelComposition/PublicComposition claims described
above; its archived manifest identifies exact source/schema/runner fingerprints.
The runner rejects missing theorem audit results and any axiom outside propext,
Quot.sound and Classical.choice; it also rejects unfinished proof diagnostics. Compiler failures
are retained under `evidence/wi013/proofs/`; parser failures can mention Lean's
temporary elaboration `sorry` terms, but were never counted as successful proofs.
Accepted source contains no sorry/admit/project axioms/native_decide. The exported
theorems audited so far depend only on Lean's `propext`, `Quot.sound`, and for
selected ordering/batching/bounds proofs `Classical.choice`. These foundational
dependencies are recorded, not concealed as an axiom-free claim.

Compile/example/axiom success is distinct from Go correspondence, mutation
sensitivity and independent review. Full SP7/calendar processing, shared functional
and differential/API checks remain M3 gates; load/state growth and migration remain
M4. No production deployment or migration is implied.
