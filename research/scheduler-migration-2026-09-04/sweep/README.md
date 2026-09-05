# Scheduler migration sweep, 2026-09-04

- Imported buffers: confirmed but already fixed by temporalio/temporal#11557. Preserved counterexample and native control here; no duplicate PR.
- Invoker phase transfer: chaptersix/temporal#53 and #55, stack #58.
- Delete versus forward creation: chaptersix/temporal#56. Evidence only; requires #44 durable ingress/incarnation protocol.
- Rollback ownership after continue-as-new: chaptersix/temporal#57, appended to stack #49.
- Backfiller capacity: chaptersix/temporal#51 and #52, stack #54.

The reports in each submitted branch document exact test commands, deterministic seeds, safety/compatibility boundaries and coverage limits. Local tests do not claim physical History crash or namespace-failover coverage. Range-map ordering has no established native total-order contract; no unsupported sorting fix is proposed. Known upstream fresh reverse boundary, recent timestamps and completion payload behavior remain excluded.
