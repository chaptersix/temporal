# Actual SQLite shard reload with lost Start response

`TestSchedule2ShardReloadAfterLostStartResponse` uses a dedicated actual test cluster,
public Create/Patch, typed scheduler2 identity and Admin database-archetype assertions.
The wrapper calls the real workflow frontend; after its successful response, it withholds
that response while the workflow blocks on a signal. Root inspection sees Started0,
Unfinished1 and a retained pending operation whose ID equals the real Start request ID.

The test calls env.CloseShard on the scheduler's owning shard, releases the lost response
as Unavailable, then reads the same pending incarnation/operation after cache eviction.
A gate prevents the recovered physical retry from acknowledging before that observation.
The actual redelivered request must be protobuf-equal to the original frozen request,
including the callback and request ID. The real retry response returns the same canonical
first-run and run ID. Root reconciliation then reaches Started1/Unfinished1. Only after
this barrier does the test signal the real workflow to finish. Its callback reaches the
reloaded scheduler, producing Started1/Unfinished0 and exactly one completed public action
with the original run ID. Actual workflow history contains one WorkflowExecutionStarted.

Command: `CGO_ENABLED=0 go test -tags test_dep,integration ./tests -run
'^TestSchedule2ShardReloadAfterLostStartResponse$' -count=1 -timeout 3m`.

- recovery01 FAIL3.284s at a fixture expectation that a deduplicated successful Start
  sets Started=false. Actual History Starter.generateResponse returns Started=true for
  request-ID replay, preserving the original successful-start semantics. No source fix
  or weakened identity assertion was needed; fixture now asserts Started=true.
- recovery02 PASS3.387s, including exact frozen request/canonical chain checks and real
  completion after reload. Both logs are archived with hashes in recovery-checkpoint.json.

Evidence is physical queue/History/SQLite recovery, not a mocked reducer or synthetic
state reload. It covers one lost accepted start response, one shard eviction, retained
pending intent, real idempotent replay and completion. It does not independently deliver
a forged/stale physical envelope, exercise cancel/terminate response loss, restart the
process, or certify cross-cluster failover. Parent owns final combined lint/race gates.
