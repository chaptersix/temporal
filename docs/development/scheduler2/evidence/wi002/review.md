# WI-002 challenge and review

Reviewed production/test contribution: eae24bae88ffb9520f4345951c75f4649621f349;
final combined source: 637fe096df2a9b7de6db3e4865e5d9a53f10cc02.
Independent history_contract reviewer reported no blocking findings. Final Go
change extracts pending disposition without changing semantics; integration also
adds a standalone saved replay and the separately authored Lean draft.

Resolved findings:

- Legacy compilation can construct malformed exclusion matchers before rejecting
  them. NewBoundedCompiledSpec validates all ranges, nil entries and durations
  before matcher construction. Existing callers keep their previous behavior.
- Persisted continuation validation now checks revision plus canonical definition
  and seed fingerprint, cursor order, pending shape and terminal invariants.
- Fractional StartTime must exclude the preceding integer second. New core uses
  start minus one nanosecond; legacy discrepancy is recorded as DIV2.
- Excluded future candidates remain pending until their window. Otherwise update
  could advance disposition into the future and lose new-definition matches.
- Lean update now advances both cursors to max(boundary, prior disposition),
  rejects stale revision, and never advances to the discarded pending nominal.
- Rapid generator initially emitted phase >= period, which is invalid input.
  Generator range corrected; original shrunk artifact preserved and explicit
  invalid-phase regression added. This was a generator defect, not a passing
  production scenario or a hidden implementation failure.
- Lint complexity/import findings resolved by extracting probe, validation and
  pending-disposition helpers. Initial failing logs are retained. One subsequent
  lint attempt encountered another local runner's lock; no lock was removed.

Three source mutations fail their named tests: lost pending value, yield reported
as exhaustion, and omitted raw-next jitter bound. Each mutant is restored in a
finally block and the original source hashes verified. Restored baseline passes.
The versioned pending_jitter_v1.json is an explicit standalone five-step replay;
Rapid seed 1 supplements, rather than replaces, saved input evidence.

The Lean author and integrator reviewed executable definitions/assumptions. The
finite ordered source is trusted, terminal cache is abstracted away, revision
stands for the complete definition/seed, and malformed-state checks are outside
that model. Proposition definitions are not proofs; Go correspondence is pending.
Final review found NextIsFirst alone did not require complete enumeration or
unique output. Added CompleteEnumeration (existence of a successful sufficient-
budget result equal to the declarative answer) and OutputUnique (strictly
increasing nominal output). Recompiled definitions and examples; draft remains
unproved. The independent reviewer confirmed the finding resolved and reported
no remaining blocker. Lean source revision is recorded separately in checks.json.
No Lean axiom/sorry/admit theorem placeholder is introduced.

Residual boundaries: persisted state is trusted, structural checks are not
authentication; named timezones use host tzdata while selected examples pin TZif;
probe limits do not bound wall-clock CPU; calendar/timezone/Farm32 primitives
are shared trusted dependencies. Full historical DST, protocol update model,
policy/backfill/callback integration, formal proofs and production routing are
outside WI-002. No full M2 acceptance is claimed.
