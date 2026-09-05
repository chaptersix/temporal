# Scheduler migration sweep, 2026-09-04

- Imported buffers: confirmed but already fixed by temporalio/temporal#11557. Preserved counterexample and native control here; no duplicate PR.
- Invoker phase transfer: chaptersix/temporal#53 and #55, stack #58.
- Delete versus forward creation: chaptersix/temporal#56. Evidence only; requires #44 durable ingress/incarnation protocol.
- Rollback ownership after continue-as-new: chaptersix/temporal#57, appended to stack #49.
- Backfiller capacity: chaptersix/temporal#51 and #52, stack #54.

The reports in each submitted branch document exact test commands, deterministic seeds, safety/compatibility boundaries and coverage limits. Local tests do not claim physical History crash or namespace-failover coverage. Range-map ordering has no established native total-order contract; no unsupported sorting fix is proposed. Known upstream fresh reverse boundary, recent timestamps and completion payload behavior remain excluded.

## Final local verification

All six PRs (#51, #52, #53, #55, #56, #57) were submitted as drafts, supplied with canonical reports/diagrams/failure images, and marked ready after local verification. GitHub stack APIs confirm #51/#52 in stack #54, #53/#55 in #58, and #57 appended above #47 in #49. #56 is standalone evidence against main because its production safety gate did not pass. All destinations are chaptersix/temporal.

- Capacity: full `./chasm/lib/scheduler/...` and `./service/worker/scheduler` packages pass, including replay tests and deterministic 450/451/1000-range drain tests. Lint passes.
- Phase: full scheduler packages and V1 replay tests pass, including phase rejection, transferable-state controls, drain-then-retry, and old-pending fail-closed behavior. Lint passes.
- Delete: full frontend package passes with the counterexample opt-in disabled; enabled counterexample fails at the documented invariant. Lint passes.
- Chain: full scheduler packages and V1 replay tests pass; owned-current/descendant, foreign-chain, missing-first-run, response/receipt/source-close loss and closed-descendant cases pass. History's continue-as-new identity test passes with both replication settings. Lint passes. `make proto` succeeds and a repeat leaves generated files unchanged.
- Imported buffer: native dispatch control passes, opt-in stranded-buffer test fails; exact upstream fix #11557 confirmed. No duplicate PR.
- Every Go test used `-tags test_dep`. `git diff --check` passes. A three-second migration fuzz smoke run passes while exercising 75 of 168 cached baseline inputs; this is not exhaustive fuzz coverage.

No physical crash, commit-ambiguity persistence, real namespace failover, or multi-cluster integration test is claimed. The reports distinguish component-engine fault injection and source-level durability reasoning from those unperformed experiments. Existing jitter/spec and migration conversion tests ran as part of affected package suites; no unsupported deterministic range-order guarantee was introduced.
