# WI-013 integrator acceptance review

Independently read KernelComposition.lean in full and Go replay-v2 driver changes.
The exact Run equality follows from rank induction and a proved resource-free normal
form; budgets assume resources, not desired outcomes. Probe resume assumes an actual
yield, subtracts prior output slots, sums consumed probes and retains final stop.
Finite window equivalence and consumed raw-prefix journal composition cover the
original stronger state/accounting obligations. Independent composition review agrees.
Public/Validity/Controls wrappers have a separate non-author review with build11 linkage.

Replay-v2 serializes distinct immutable definition fixtures and each step's selected
definition; saved replay compiles these independently, rejects unknown names/trailing
input, and requires positive generated commands to succeed. Slice fields are not
mutated after capture. The replacement-specific lost-superseded fault now shrinks and
replays under v2; it fails against mutated Go and passes restored Go. The three original
pending/cursor/duplicate mutants also satisfy that criterion. Production bytes restored.

Named proofs and correspondence profiles passed. Runner requires explicit oracle,
manifest and seed, checks passed build/binary identity, required tests and source drift.
Final routine02 exercises the new postcheck. Existing profile defaults are unchanged.
All build11 Lean source hashes verified against current files. Its runtime binary is
byte-identical to build07/build08 used for mutations, two10000-case properties and race.
Direct pinned Lean compilation checks the same kernel without extra package dependencies;
no compiler/examples-only acceptance or new project axiom is used.

WI-013 finite-stream proof/correspondence accepted. Calendar/TZif semantics, JSON parser
verification, universal Go refinement, persistence/physical timers and fairness are
outside the theorem claim. SP7 calendar/DST checks and full public M3 remain open.
