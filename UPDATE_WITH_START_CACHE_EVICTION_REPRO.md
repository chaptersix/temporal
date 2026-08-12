# Update-with-Start waiter stranded by workflow-cache eviction

## Summary

A functional proof of concept on Temporal Server `v1.32.0-159.8` demonstrates that an Update-with-Start request waiting for the `Completed` lifecycle stage can remain blocked until History's 20-second long-poll soft timeout even though the update has already completed durably.

The reproduced sequence is:

1. History admits an Update-with-Start update and the original RPC waits on its in-memory `*Update`.
2. The workflow context is evicted after History releases its cache lease.
3. At this revision, the eviction callback deregisters the shard finalizer but does not call `WorkflowContext.Clear()`, so the old update registry and waiter are not aborted.
4. The worker completes the previously polled workflow task. History reloads the workflow into a new context, resurrects the update from the worker's acceptance message, and durably records acceptance and completion.
5. A separate poll for the same update ID observes `Completed` immediately.
6. The original Update-with-Start waiter remains attached to the evicted registry and returns `Admitted` only when `history.longPollExpirationInterval` expires approximately 20 seconds after the request began.

This proves the mechanism exists in the pinned server revision. It does not, by itself, prove that any particular customer completion event and delayed RPC belonged to the same request.

## Revisions

- Temporal Server tag: `v1.32.0-159.8`
- Temporal Server commit: `1469ced131ef1eb82fbba7d29bbbcb65d0749aee`
- POC branch: `codex/uws-cache-eviction-repro`
- Later change relevant to this mechanism: [PR #11151](https://github.com/temporalio/temporal/pull/11151), commit [`680e9588ab178c6d9c4135590ae94ebd271c0869`](https://github.com/temporalio/temporal/commit/680e9588ab178c6d9c4135590ae94ebd271c0869)

PR #11151 added `item.wfContext.Clear()` to the workflow-cache eviction callback for pagination-buffer resource cleanup. It was not described as an Update-with-Start fix, but the call also aborts waiters in the evicted update registry instead of leaving them stranded.

## Functional test

The POC adds:

- `TestWorkflowCacheEvictionLeavesCompletedUpdateWaiterBehind` in `tests/update_workflow_test.go`.
- A functional-test helper that captures the onebox History workflow cache and deletes the exact workflow-run entry.
- A cache test helper that locates the run and invokes the real cache `Delete` path.

The helper bypasses capacity and TTL selection, but `Delete` invokes the same `OnEvict` callback used by LRU and TTL eviction. The test sequences eviction only after the Update-with-Start workflow task has been polled, at which point the Update-with-Start API and workflow-task-start APIs have released their workflow-cache leases.

No dynamic configuration is changed. The test intentionally uses the revision's default 20-second `history.longPollExpirationInterval`.

### Why the test calls Frontend directly

Application code using Go SDK `v1.40.0` would look approximately like this:

```go
startOp := sdkClient.NewWithStartWorkflowOperation(
    client.StartWorkflowOptions{
        ID:                       workflowID,
        TaskQueue:                taskQueue,
        WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
    },
    Workflow,
)

handle, err := sdkClient.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
    StartWorkflowOperation: startOp,
    UpdateOptions: client.UpdateWorkflowOptions{
        UpdateID:     updateID,
        UpdateName:   "Update",
        Args:         []interface{}{arg},
        WaitForStage: client.WorkflowUpdateStageCompleted,
    },
})
if err != nil {
    return err
}
var result Result
if err := handle.Get(ctx, &result); err != nil {
    return err
}
```

Internally, SDK `v1.40.0` implements this in [`updateWithStartWorkflow`](https://github.com/temporalio/sdk-go/blob/v1.40.0/internal/internal_workflow_client.go#L1924-L2048) by repeatedly calling `WorkflowService.ExecuteMultiOperation` with the same Start and Update operations until the update response is durable. Its [`updateIsDurable`](https://github.com/temporalio/sdk-go/blob/v1.40.0/internal/internal_workflow_client.go#L2390-L2396) predicate treats `Admitted` and `Unspecified` as non-durable and continues the loop under the original caller context.

Consequently, the SDK deliberately hides the intermediate server response central to this reproduction. When the stale server waiter returns `Admitted` after approximately 20 seconds, the SDK sends another `ExecuteMultiOperation`. Because completion is already durable, that later request can return `Completed`; application code generally observes extra latency followed by the result rather than an `Admitted` handle.

The functional test therefore calls `env.FrontendClient().ExecuteMultiOperation` directly. This is still the public Workflow Service endpoint and still exercises Frontend conversion, the frontend-to-History client, History's multi-operation API, and the update waiter. It omits only the SDK's outer “retry until durable” loop, allowing the test to assert that the first RPC specifically returned `Admitted` after durable completion.

### Why the test needs a cache hook

There is no public or administrative API that evicts one workflow-run entry through ordinary History cache eviction. Existing alternatives exercise materially different behavior or make the proof unnecessarily expensive:

- `CloseShard` runs the shard finalizer. Its registered workflow callback calls `WorkflowContext.Clear()`, aborting the old registry and waking the waiter with `Unavailable`; that is explicitly not the suspected path.
- Capacity eviction without configuration requires exceeding the default 128,000-entry host cache.
- TTL eviction without configuration requires waiting for the default one-hour cache TTL.
- Calling `WorkflowContext.Clear()` directly would manufacture the fixed behavior instead of reproducing the missing-notification behavior.

The hook captures the real workflow cache constructed inside the onebox History service, locates the exact key for `(namespace ID, workflow ID, run ID)`, and calls the embedded cache's real `Delete` method. `Delete`, capacity eviction, and TTL eviction all converge on the same internal removal routine and therefore invoke the same `OnEvict` callback. The helper bypasses only victim selection and elapsed-time/capacity preconditions.

The test waits until `PollWorkflowTaskQueue` has returned before invoking the hook. By then Update-with-Start has released its workflow lease before waiting, and History has also released the workflow-task-start lease. The selected entry is therefore in the unpinned state in which normal LRU or TTL eviction could remove it. The helper does not independently enforce or inspect that internal reference count, so the ordering is an important part of the test contract.

### Test sequence

1. Start `ExecuteMultiOperation([Start, Update])` in a goroutine with an update wait policy of `Completed`.
2. Poll the initial workflow task and retain its task token and update protocol request.
3. Evict that exact workflow run from the History workflow cache.
4. Respond to the retained workflow task with update acceptance and completion messages.
5. Wait for `RespondWorkflowTaskCompleted` to succeed. This establishes that its persistence transaction completed.
6. Assert that the original `ExecuteMultiOperation` call has not returned.
7. Read workflow history and assert that it contains:

   ```text
   1 WorkflowExecutionStarted
   2 WorkflowTaskScheduled
   3 WorkflowTaskStarted
   4 WorkflowTaskCompleted
   5 WorkflowExecutionUpdateAccepted
   6 WorkflowExecutionUpdateCompleted
   ```

8. Call `PollWorkflowExecutionUpdate` for the same workflow, run, and update ID; assert that it returns `Completed` with the expected result.
9. Wait for the original Update-with-Start call; assert that at least 19 seconds elapsed and that its response is `Admitted` with no outcome.

The combination of steps 5-8 establishes that completion was durable and visible through a newly loaded registry before the original waiter returned.

## Results

Initial focused run:

```text
Slow gRPC call {"duration": 20.006487917, "method": "/temporal.api.workflowservice.v1.WorkflowService/ExecuteMultiOperation"}
--- PASS: TestUpdateWithStartSuite/TestWorkflowCacheEvictionLeavesCompletedUpdateWaiterBehind (20.05s)
PASS
ok  go.temporal.io/server/tests  21.690s
```

Two-run stability check:

```text
ok  go.temporal.io/server/tests  41.585s
```

All three executions reproduced the behavior.

## Pinned-version A/B proof of the fix

To isolate the causal change from the other work in PR #11151, the same POC was copied to a second worktree at the identical server commit `1469ced131ef1eb82fbba7d29bbbcb65d0749aee`. The only production-code difference in that worktree is:

```diff
diff --git a/service/history/workflow/cache/cache.go b/service/history/workflow/cache/cache.go
@@
                 logger.Debug("cache failed to de-register callback in finalizer",
                     tag.Error(err), tag.ShardID(item.shardId))
             }
+            item.wfContext.Clear()
         },
```

The cache hook, API ordering, worker acceptance/completion response, durable-history assertions, and separate completed-update poll remain identical. Only the final expectation is inverted: the original Update-with-Start must return `Completed` with its outcome before the unchanged 20-second soft timeout.

| Variant | Production difference from `v1.32.0-159.8` | Original RPC result | Focused test time |
| --- | --- | --- | --- |
| Baseline | None | `Admitted`, no outcome | `21.624s` package time; RPC waits approximately 20s |
| Pinned + `Clear()` | One added `item.wfContext.Clear()` | `Completed`, expected outcome | Approximately `0.08s` per test case; `1.751s` package time for two runs |

Commands:

```bash
# Baseline
go test -tags test_dep ./tests \
  -run '^TestUpdateWithStartSuite$/^TestWorkflowCacheEvictionLeavesCompletedUpdateWaiterBehind$' \
  -count=1

# Identical tag plus the one-line production change
go test -tags test_dep ./tests \
  -run '^TestUpdateWithStartSuite$/^TestWorkflowCacheEvictionWakesCompletedUpdateWaiter$' \
  -count=2 -v
```

This A/B result demonstrates that calling `WorkflowContext.Clear()` from ordinary workflow-cache eviction is sufficient to close the reproduced stale-waiter mechanism. It also supports, more narrowly than testing all of PR #11151, attributing the behavior change to the exact line introduced by that PR. It does not establish that PR #11151 intended to fix Update-with-Start or that this mechanism explains any particular external incident.

## Running the POC

From a checkout containing the patch:

```bash
go test -tags test_dep ./tests \
  -run '^TestUpdateWithStartSuite$/^TestWorkflowCacheEvictionLeavesCompletedUpdateWaiterBehind$' \
  -count=1 -v
```

The test takes approximately 20 seconds plus cluster startup. If the environment restricts the default Go build cache, set `GOCACHE` to a writable directory. The functional onebox cluster also needs permission to bind local ephemeral ports.

## Interpretation

At the pinned revision, ordinary cache eviction removes the only cache-owned reference to the old workflow context but does not clear that context. A later workflow-task completion can therefore operate on a newly loaded workflow context and update registry. Durable completion resolves the reconstructed `*Update`, not the original `*Update` still held by the first RPC.

The first RPC receives no completion or abort notification. `WaitLifecycleStage` reaches its soft timeout and returns the highest stage known to that old update instance: `Admitted`.

Current `main` calls `WorkflowContext.Clear()` from `OnEvict`. The pinned-version A/B test confirms that adding only this line closes the reproduced hole: eviction aborts the old registry waiter with `Unavailable`, the retrying frontend-to-History client attaches to reconstructed state, and the public RPC returns the durable `Completed` outcome promptly.

## Limitations

- Cache selection is deterministic and test-directed rather than caused by filling the default 128,000-entry cache or waiting for its one-hour TTL.
- The test invokes the real `Delete` and `OnEvict` implementation, but does not independently inspect the entry's internal LRU reference count. The API sequencing places eviction after all relevant leases have been released.
- The test calls the raw Workflow Service API to expose the first server response. Go SDK `v1.40.0` retries after receiving `Admitted`, so an SDK-level reproduction would normally present as approximately 20 seconds of extra latency followed by the durable result, provided the caller context remains valid.
- The POC establishes a server behavior, not the identity of requests in external customer evidence.

## Possible regression-test form

For a production regression test on a revision containing PR #11151, retain the deterministic cache-eviction helper and invert the final expectations:

- The original stale waiter should be aborted promptly rather than survive until the soft timeout.
- The frontend-to-History retry should attach to reconstructed state.
- The public Update-with-Start response should return `Completed`, not `Admitted`.

The long-poll duration can then be shortened with test configuration if reducing suite time is important. The direct eviction helper should remain narrowly scoped to a dedicated functional-test cluster.
