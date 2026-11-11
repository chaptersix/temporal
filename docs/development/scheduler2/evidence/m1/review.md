# Independent M1 acceptance review

Reviewer: chasm_guarantees delegate; integrator owns gate. Final code revision:
f9d62cf99ba5a6e72d987b9bdb9f315c98efd40e. All review work was local and read-only.

Initial adapter review found three blocking issues: task-bound refs rejected valid
late acknowledgment after expiry; pre-read clock rollback could consume a physical
delivery without rescheduling; executor limit was unenforced. Commit 4315dd889
fixes these with an exact-incarnation root result ref, retryable not-due response,
and cancellable one-RPC gate. Real Node.ExecuteSideEffectTask regressions pass.

Final review at 549fd46de found in-flight clock rollback could calculate an
unchanged retry timestamp, again omitting successor dispatch. Commit f9d62cf99
advances from max(now, prior RetryAt), clamped to the stored deadline. Reinstating
the old expression fails TestTaskRollbackDuringRPCPublishesSuccessor with expected
2s versus actual 1s (rollback-mutant.log); the fixed combined checks pass.
The regressions selector now includes the independent nil-library identity test.

Reviewer inspected final source and combined manifests and reported no remaining
M1 implementation blocker. Bounded single-schedule scope, manual FizzBee mapping,
independent Go projection oracle, absent completion results and trusted atomic DB
publication are explicit. The reviewer conditioned acceptance on final central
plan/decision/evidence synchronization, completed by this documentation update.

This acceptance is not a full Schedule API, database crash recovery, failover,
calendar, liveness-without-fairness, load, Lean proof or production-readiness claim.
