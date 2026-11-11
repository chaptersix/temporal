# WI-007 acceptance review

Accepted source688eba86d, implementing6175ef959 and contractd8a84beff.
Independent source review found no blocking defect in queue replacement, binding,
request fencing, retries, completion-only occupancy release or selective loading.
Independent test/model review identified and resolved actual evidence weaknesses:

- Model witnesses now capture stale response after successor admission, actual
  in-flight cancel response after terminate demand and update before retry issuance.
  All three factored models reran at100/200 after correction; integrated hashes match.
- Real physical-task tests cover deadline−1ns and exact deadline, and invalidation
  after retry, completion and supersession. Effect outcomes retain occupancy.
- The permit test waits for the second handler's permit-select context evaluation
  before cancellation. Removing the permit deterministically fails with a second
  RPC; removing the expiry guard fails the exact-deadline test. Eight total Go
  mutants fail their named assertions and original source hashes are restored.

Final effects profile passed10000 Rapid scenarios,1–128 commands, plus CHASM
publication/reload, keyed delayed binding after ALLOW_ALL, frozen updates, poisoned
payloads, clock rollback, saved replay and actual executor cancellation. Full
scheduler2/registry race and scoped normal/minimal lint pass. Initial fixture,
protobuf-cache comparison and lint failures remain visible with their resolutions.
No implementation behavior was weakened to satisfy those tests.

Model limits:12 safety/fourteen existential witnesses, one target/two overlap demands,
finite clocks/attempts, separate selection and delayed-horizon fixtures. Factoring
has a manually argued projection boundary; no automated composition/refinement or
liveness proof. All-ineligible queues and rollback are concrete-test obligations,
with general queue disposal already exercised in WI-005. CHASM publication/reload
is not physical database crash-atomicity evidence. Payload isolation means avoided
value decoding, not reduced physical database reads. No migration is supported.

Original checkout had no task writes. Prior untracked paths still exist; one became
tracked by concurrent original-checkout commit03ecd1d22. That unrelated change was
observed and preserved without intervention. Worktrees remain available; all task
commits are local. M2 remains partial; M3/M4 remain open.
