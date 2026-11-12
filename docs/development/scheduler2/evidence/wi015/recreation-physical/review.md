# Actual close/recreate fencing of old Start and completion

TestSchedule2RecreationFencesOldStartAndCallback captures the actual Start transfer
envelope through the existing History interceptor and calls the real workflow frontend.
It holds the successful response while public Delete closes the old scheduler and Create
reuses the same business ID with a new incarnation. The captured request ID equals the
old operation ID; decoding its real completion token confirms old run ID and request ID.

Only then is the old response released. A new-incarnation physical Start provides a
causal barrier: the single issuance permit is released only after the old handler attempts
to merge the delayed result. The old workflow subsequently completes by real signal;
DescribeWorkflowExecution must report its callback SUCCEEDED, proving delivery completed
rather than merely waiting. The new root is exactly unchanged after that old callback.

The test then closes the actual scheduler shard and re-enqueues the captured old Start
through Admin.AddTasks. The real transfer interceptor's post-dispatch callback supplies
an explicit delivery barrier. No additional frontend Start RPC occurs (exactly2 total),
the replacement root is unchanged, and the old root remains Closed/Started0. Typed and
Admin database scheduler2 identity is checked for the new root. No direct reducer call
substitutes for real task dispatch or callback transport.

Commands: `CGO_ENABLED=0 go test -tags test_dep,integration ./tests -run
'^TestSchedule2RecreationFencesOldStartAndCallback$' -count=1 -timeout 3m` gave recreation02
PASS1.485s before explicit token decoding. recreation01 was a compile-only fixture typo
(used nonexistent WithTestHook instead of existing env.InjectHook); recreation03 was
blocked by unrelated in-progress capacity schema generation. Both failures are retained.
Final token-bound source compiled with `CGO_ENABLED=0 go test -c -tags test_dep,integration
./tests -o /private/tmp/scheduler2-recreation.test` and executed using
`/private/tmp/scheduler2-recreation.test -test.run
'^TestSchedule2RecreationFencesOldStartAndCallback$' -test.count=1 -test.timeout 3m`: PASS.
The explicit binary avoided concurrent schema generation; hashes/logs are in manifest.

This witness covers old Start envelope/late successful response and real old callback
across close/recreate and actual shard reload. It does not yet certify old cancel/terminate
physical timer replay; those separate causal fixtures are in preparation. No production
routing, migration, rollout or external cluster was used. Parent owns final lint/acceptance.

## Actual stale cancel/terminate timer dispatch

TestSchedule2RecreationFencesOldEffectTimers adds separate CANCEL_OTHER and
TERMINATE_OTHER cases. Each starts a real blocking workflow, creates the overlap effect
through public Patch, and captures its physically dispatched CHASM timer before the
original effect RPC. The payload EffectId/WakeTime matches persisted effect metadata;
target operation and first-run fence match the old acknowledged workflow. The test
closes/recreates the same schedule ID and starts one new-incarnation workflow, then closes
the owning shard and re-enqueues that exact old timer using Admin.AddTasks.

A namespace-scoped HistoryTimerTaskInterceptor observes the replay's actual executor
return. It uses the existing activeStandbyExecutor TestHooks holder, adds no constructor
or runtime registration, and invokes the unchanged dispatch implementation through a
callback. With no installed hook, the prior path/namespace selection is unchanged; only
timer tasks in the selected namespace can be intercepted. Tests verify active/standby
pass-through preserves the complete delegate ExecuteResponse, other namespace ignores
the hook, and transfer tasks remain unchanged. As with the existing transfer interceptor,
the test may deliberately omit execute() to withhold the original delivery.

After the replay barrier, the actual History cancel/terminate clients have zero calls,
the new root equals its pre-replay snapshot, and the original workflow remains RUNNING.
This is real persisted timer/queue dispatch after reload, not a direct EffectHandler or
reducer call. It demonstrates stale-envelope invalidation before RPC, not a lost-response
retry of an effect that already reached a workflow. That latter ambiguity scenario remains
a separate obligation.

`CGO_ENABLED=0 go test -tags test_dep ./service/history/queues -run
'Test(ExecutorSuite|TimerInterceptor)' -count=1`: PASS0.653s.
`CGO_ENABLED=0 go test -tags test_dep,integration ./tests -run
'^TestSchedule2RecreationFencesOldEffectTimers$' -count=1 -timeout 3m`: PASS3.631s.
Final combined run after exact effect-payload/chain assertions:
`CGO_ENABLED=0 go test -tags test_dep,integration ./tests -run
'^TestSchedule2RecreationFencesOld(StartAndCallback|EffectTimers)$' -count=1 -timeout 3m`:
PASS3.854s. Source/log hashes are recorded separately as final_source_sha256; the earlier
start-only checkpoint remains intact. Scoped make lint-code is running separately.

## Lint follow-up

Scoped `CGO_ENABLED=0 make lint-code LINT_CODE_TARGETS='./service/history/queues ./tests'
GOLANGCI_LINT_FIX=false GOLANGCI_LINT_BASE_REV=HEAD` completed successfully, including
minimal-profile lint and errortype vet. Because diff filtering can omit new unstaged
files, supplemental full-package lint was attempted without diff filtering. The first
used an older PATH binary unable to decode Go1.27 export data; it is an environment/tool
failure, not a source failure. The repository-pinned2.14 binary then ran successfully
and reported pre-existing baseline issues across these packages. The only finding in
owned changed/new files was multiline-if style in the existing passive-hook block moved
by extraction. Collapsing that condition preserves semantics; final executor regressions
PASS0.677s. Earlier tested source hashes remain, with the post-lint executor hash recorded
separately. Parent owns consolidated acceptance lint for the complete staged source.
