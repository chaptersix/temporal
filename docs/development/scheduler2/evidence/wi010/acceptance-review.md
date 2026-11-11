# WI-010 independent acceptance review

The final independent review maps D51–D54 to the activity oracle, clock adapter
tests, cap4 idle-enabled eviction, saved early-completion/late-observation replay,
real command/queue/backfill/effect and pause transitions, prewrite token/arithmetic
checks, finite conditional L3 model and actual History recreation traces.

The review initially withheld acceptance for failed History delete publication,
old callback/completion/expiry after recreation, and admitted historical backfill
observation with idle enabled. These were added and independently reviewed. The
review recommends acceptance conditional on the expanded race and final lint.
The parent verifies those command outcomes separately; no human gate is required.

Remaining limits are explicit: captured persistence rather than a database; exact
engine reference plus handler dispatch rather than production queue processing;
test_dep internal events rather than a public authenticated callback endpoint;
finite factored models rather than Go refinement proofs. The effect transition test
contains a queued waiter, while separate predicate tests isolate effect eligibility.
Poisoned DATA proves avoided deserialization, not fewer database reads.

The broader M2 audit identifies WI-011 current mappings and composed local update
semantics. M3 proof/API correspondence and M4 load/state-growth remain open.
