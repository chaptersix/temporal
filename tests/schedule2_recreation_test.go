package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
)

type recreationStarter struct {
	workflowservice.WorkflowServiceClient
	mu      sync.Mutex
	calls   int
	first   chan recoveryStartObservation
	release <-chan struct{}
}

func (c *recreationStarter) StartWorkflowExecution(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest, options ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
	c.mu.Lock()
	c.calls++
	first := c.calls == 1
	c.mu.Unlock()
	response, err := c.WorkflowServiceClient.StartWorkflowExecution(ctx, request, options...)
	if first {
		c.first <- recoveryStartObservation{request: common.CloneProto(request), response: common.CloneProto(response), err: err}
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return response, err
}

func TestSchedule2RecreationFencesOldStartAndCallback(t *testing.T) {
	first := make(chan recoveryStartObservation, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseStart := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseStart()
	captured := make(chan tasks.Task, 8)
	replayed := make(chan struct{}, 8)
	var replayMu sync.Mutex
	var replayRun string
	var startType uint32
	var starter *recreationStarter
	backend := scheduleBackend{name: "scheduler2-recreation", context: schedule2Backend.context, assertPersisted: assertSchedule2Persisted, options: []testcore.TestOption{
		testcore.WithDedicatedCluster(),

		testcore.WithScheduleFrontend(func(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
			starter = &recreationStarter{WorkflowServiceClient: runtime.FrontendClient, first: first, release: release}
			runtime.FrontendClient = starter
			frontend, err := scheduler2.NewPublicExecutionTestFrontend(runtime)
			startType, _ = runtime.Registry.TaskIDFor(&scheduler2pb.StartTask{})
			return frontend, err
		}, scheduler2.NewNilLibrary()),
	}}
	env := newScheduleEnv(t, []scheduleBackend{backend})
	env.InjectHook(testhooks.NewHook(testhooks.HistoryTransferTaskInterceptor, func(task tasks.Task, execute func()) {
		chasmTask, ok := task.(*tasks.ChasmTask)
		if !ok || chasmTask.Info.TypeId != startType {
			execute()
			return
		}
		replayMu.Lock()
		isReplay := task.GetRunID() == replayRun
		replayMu.Unlock()
		if !isReplay {
			select {
			case captured <- task:
			default:
			}
		}
		execute()
		if isReplay {
			select {
			case replayed <- struct{}{}:
			default:
			}
		}
	}))
	ctx := t.Context()
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
		return nil
	}, workflow.RegisterOptions{Name: "recreation-workflow"})
	sid := "recreation-" + uuid.NewString()
	schedule := &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, Action: startWorkflowAction(env, "old-"+sid, "recreation-workflow"), State: &schedulepb.ScheduleState{Paused: true}}
	createSchedule(ctx, t, env, sid, schedule)
	patchSchedule(ctx, t, env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))
	var accepted recoveryStartObservation
	select {
	case accepted = <-first:
	case <-time.After(30 * time.Second):
		t.Fatal("old start did not reach actual frontend")
	}
	require.NoError(t, accepted.err)
	var oldTask tasks.Task
	select {
	case oldTask = <-captured:
	case <-time.After(30 * time.Second):
		t.Fatal("old physical start envelope was not captured")
	}
	oldRun := oldTask.GetRunID()
	require.NotEmpty(t, oldRun)
	engineCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)
	key := chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: sid}
	read := func(run string) (*scheduler2pb.RootState, error) {
		selected := key
		selected.RunID = run
		return chasm.ReadComponent(engineCtx, chasm.NewComponentRef[*scheduler2.Scheduler](selected), func(s *scheduler2.Scheduler, _ chasm.Context, _ struct{}) (*scheduler2pb.RootState, error) {
			return common.CloneProto(s.RootState), nil
		}, struct{}{})
	}
	old, err := read(oldRun)
	require.NoError(t, err)
	require.Equal(t, oldRun, old.Incarnation)
	require.Zero(t, old.Started)
	require.EqualValues(t, 1, old.Unfinished)
	require.Equal(t, old.OperationId, accepted.request.RequestId)
	require.Len(t, accepted.request.CompletionCallbacks, 1)
	token := accepted.request.CompletionCallbacks[0].GetNexus().GetHeader()["temporal-callback-token"]
	refBytes, requestID, err := chasm.UnpackNexusCallbackToken(token)
	require.NoError(t, err)
	callbackRef, err := chasm.DeserializeComponentRef(refBytes)
	require.NoError(t, err)
	require.Equal(t, oldRun, callbackRef.RunID)
	require.Equal(t, old.OperationId, requestID)
	_, err = env.FrontendClient().DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
	require.NoError(t, err)
	schedule.Action = startWorkflowAction(env, "new-"+sid, "recreation-workflow")
	createSchedule(ctx, t, env, sid, schedule)
	recreated, err := read("")
	require.NoError(t, err)
	require.NotEqual(t, oldRun, recreated.Incarnation)
	assertScheduleCallbackBackend(t, env, backend, sid)
	// The accepted old RPC response is delivered only after the replacement exists.
	releaseStart()
	patchSchedule(ctx, t, env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))
	await.Require(ctx, t, func(at *await.T) {
		current, err := read("")
		require.NoError(at, err)
		require.Equal(at, recreated.Incarnation, current.Incarnation)
		require.EqualValues(at, 1, current.Started)
		require.EqualValues(at, 1, current.Unfinished)
	}, 30*time.Second, 100*time.Millisecond)
	// The new start is a causal barrier: the single issuance permit is released
	// only after the old handler attempts to merge its delayed response.
	before, err := read("")
	require.NoError(t, err)
	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{Namespace: env.Namespace().String(), WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: accepted.request.WorkflowId, RunId: accepted.response.RunId}, SignalName: "complete"})
	require.NoError(t, err)
	await.Require(ctx, t, func(at *await.T) {
		description, err := env.FrontendClient().DescribeWorkflowExecution(at.Context(), &workflowservice.DescribeWorkflowExecutionRequest{Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: accepted.request.WorkflowId, RunId: accepted.response.RunId}})
		require.NoError(at, err)
		require.Equal(at, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, description.WorkflowExecutionInfo.Status)
		require.Len(at, description.Callbacks, 1)
		require.Equal(at, enumspb.CALLBACK_STATE_SUCCEEDED, description.Callbacks[0].State)
	}, 30*time.Second, 100*time.Millisecond)
	afterCallback, err := read("")
	require.NoError(t, err)
	require.Equal(t, before, afterCallback)
	// Re-enqueue the actual captured old-incarnation envelope through Admin/History.
	// CloseShard forces queue/state reload before dispatch rather than a reducer call.
	env.CloseShard(env.NamespaceID().String(), sid)
	replayMu.Lock()
	replayRun = oldRun
	replayMu.Unlock()
	blob, err := serialization.NewSerializer().SerializeTask(oldTask)
	require.NoError(t, err)
	shard := tasks.GetShardIDForTask(oldTask, int(env.GetTestClusterConfig().HistoryConfig.NumHistoryShards))
	_, err = env.AdminClient().AddTasks(ctx, &adminservice.AddTasksRequest{ShardId: int32(shard), Tasks: []*adminservice.AddTasksRequest_Task{{CategoryId: int32(oldTask.GetCategory().ID()), Blob: blob}}})
	require.NoError(t, err)
	select {
	case <-replayed:
	case <-time.After(30 * time.Second):
		t.Fatal("old envelope was not dispatched after reload")
	}
	starter.mu.Lock()
	calls := starter.calls
	starter.mu.Unlock()
	require.Equal(t, 2, calls, "stale old envelope must not issue another Start RPC")
	afterReplay, err := read("")
	require.NoError(t, err)
	require.Equal(t, before, afterReplay)
	old, err = read(oldRun)
	require.NoError(t, err)
	require.True(t, old.Closed)
	require.Zero(t, old.Started)
}
