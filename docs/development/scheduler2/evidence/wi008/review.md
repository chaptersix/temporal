# WI-008 acceptance review

Accepted source12740ef1d, prerequisite0c07f4a24, contract0232beba7.
Implementation97e9a7eca separates command deduplication, manual occurrence sequence,
workflow timestamp identity and immutable start requests. No public routing changed.
Reviewfixf525eacab avoids template loading for SKIP/BUFFER_ONE policy-only disposal
and gives equal-nominal terminal records deterministic ID tie ordering.

Independent review found no remaining blocking source or evidence issue after:

- Poisoned Definition regressions exercise metadata-only disposal through CHASM
  publication/reload. Same-time retention tie and sequence/task-token limits pass.
- Queued manual start preserves transferred identity/policy while adopting current
  template/carryover; ALLOW_DUPLICATE and manual eligibility remain set; retry stays
  immutable through later updates. Mixed automatic entries dispose when ineligible.
- Actual stored ManualTask rejects execution before deadline, advances token at
  same-instant successor publication and rejects stale validation/execution.
- Model override witness now requires definition update before transfer. Final
  models reran100/200; nine safety/fifteen witnesses and eight mutants pass.

Final TestManual profile passes10000 scenarios1–128 commands plus saved version1
replay of public Trigger bodies/configuration and failed publication/restart.
Full scheduler2/registry race and scoped normal/minimal lint pass. Eight Go mutants
fail their named tests, and source restoration hashes match. Initial lint failures
are retained; fixes preserve independent oracle semantics.

Evidence boundaries: new dedup harness uses actual Node.HasRequestID/AttachRequestID
and serialized CHASM transactions under an option-aware mock engine, specifically
the DATA-node layout. Separate existing History-engine tests cover both layouts,
empty IDs, expiry and count eviction with mocked persistence. Neither is a physical
storage crash or full frontend API claim. M3 still requires public functional wiring.

The factored models abstract sequence exhaustion at6, dedup expiry symbolically,
capacity progress and operation accounting after admission. Mixed-queue effect
replacement is covered by WI-007 and concrete tests, not the admission model fixture.
No automated composition/refinement proof or starvation freedom is claimed. Finite
backfill progress under sustained ingress remains WI-009's capacity-arbitration gate.
Manual timestamps must round-trip int64 nanoseconds; out-of-range inputs reject
explicitly rather than wrapping. The60s immutable start horizon is a recorded
prototype difference from existing CHASM attempt-count behavior.

Original checkout received no task writes; prior untracked paths remain present.
All worktrees are retained; commits local. M2 remains partial and M3/M4 pending.
