# WI-010 activity and idle increment: intermediate evidence

Source base bea4b6547; implementation remains uncommitted in
/private/tmp/sch-redesign-lifecycle. The exploration manifest fingerprints tested
Go/proto/testdata files. Its recorded event_limit=128 is incorrect generic runner
metadata: ActivityNodeProperties executes at most35 scenario events plus setup.
The runner/profile are corrected for subsequent runs; the original manifest is
preserved rather than rewritten. Seed1,10000 scenarios passed.

Idle-token overflow tests pass for automatic exhaustion, backfill exhaustion and
start acknowledgment. Each commits after the rejected transition and reloads to
check unchanged data/tasks except framework transition bookkeeping. Full normal
scheduler2/registry baseline, scoped lint (base bea4b6547) and actual History
start/delete/recreate race test using NewExecution pass. Full scheduler2/registry race also passes (activity-idle-race.log).
Subsequent composed test additions need their own focused verification.

The independent factored model passes both100/200 action bounds: activity146
unique states, idle1104. Five mutants are detected, including lost rearm by a
temporal lasso. See model/lifecycle/README.md for fairness and abstraction limits;
this is no Go refinement or database/physical-queue proof.

Independent source review found no blocking code defect. The acceptance audit
identified remaining composed coverage: idle-enabled early completion/late first
ack and publication failures; real removal of command/queue/effect holds; broader
History recreation delivery matrix; positive open-state selective loading; saved
lifecycle replay; implementation sensitivity. These remain required work.

Initial baseline failed old expected summary fields and an inconsistent new test
budget fixture; those fixtures were corrected before the passing baseline. Initial
lint rejected direct time.Now. The local documented exception is restricted to
post-RPC observation, matching reference wall-time-plus-snapshot-offset semantics.
No public API compatibility, full WI-010 acceptance or M2 completion is claimed.


The mutation runner's first attempt detected the first two mutants, then stopped
because a replacement substring matched both summary branches. It restored each
changed file; no test/build failure was counted as detection. The admission-only
replacement was narrowed to include its newline, and the runner was restarted.
The partial manifest/logs are retained under activity-idle-mutations/initial.

The corrected runner detects all five named assertion failures and restores every
source file byte-for-byte. Real transition tests for manual/queue/backfill/effect
hold removal pass (idle-obligations.log); effect scenarios include a queue hold,
so they do not independently isolate the effect predicate. Two saved JSON replays
now cover idle rearm/rollback/stale delivery and early completion/late first start
observation. Their final focused race/lint verification is pending.

Focused race now passes both saved JSON replays and all four obligation tests
(composed-race.log). The scoped lint target includes untracked packages, but its
diff filtering can omit new untracked lines; task files were staged and lint was
rerun to include these additions. No commit or integration has occurred yet.

Staged-source lint passes (staged-lint.log). Raw assertion logs intentionally retain
Go/testify indentation; git diff whitespace diagnostics in those logs are output
artifacts, not Go/source whitespace errors.

The expanded failed-delete test initially injects an unclassified persistence
error. History marks shard status unknown, so subsequent reload cannot run.
The failure log is preserved in history-delete-initial-failure.log. This failed
run is not accepted as rollback/retry evidence; the definite-not-committed
persistence error fixture is under source investigation. Historical-backfill
observation with idle enabled passes focused race (historical-anchor-race.log).

The final History race passes after using InvalidPersistenceRequestError, which
service/history/shard/context_impl.go explicitly classifies as definitely not
committed. No shard reset or recovery bypass was introduced. The test now checks
failed delete persistence, unchanged A/state/tasks, reload Running with valid
obligation, successful retry, and old Callback/Complete/Expire through an exact
A reference after B exists. history-delete-race.log is the accepted evidence.
Ambiguous-outcome shard recovery is a separate framework boundary, not covered
by this definite-rejection fixture. Final lint initially caught one epoch variable
name in the added backfill test; it was renamed observedNano.
