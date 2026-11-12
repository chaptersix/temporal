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
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type recoveryStartObservation struct {
	request  *workflowservice.StartWorkflowExecutionRequest
	response *workflowservice.StartWorkflowExecutionResponse
	err      error
}

type lostScheduleStartResponse struct {
	workflowservice.WorkflowServiceClient
	mu               sync.Mutex
	calls            int
	accepted         chan recoveryStartObservation
	retryRequests    chan *workflowservice.StartWorkflowExecutionRequest
	retried          chan recoveryStartObservation
	loseResponse     <-chan struct{}
	retryAfterReload <-chan struct{}
}

func (c *lostScheduleStartResponse) StartWorkflowExecution(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest, opts ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.mu.Unlock()
	if call > 1 {
		select {
		case c.retryRequests <- common.CloneProto(request):
		default:
		}
		select {
		case <-c.retryAfterReload:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	response, err := c.WorkflowServiceClient.StartWorkflowExecution(ctx, request, opts...)
	observed := recoveryStartObservation{request: common.CloneProto(request), response: common.CloneProto(response), err: err}
	if call == 1 {
		c.accepted <- observed
		select {
		case <-c.loseResponse:
		case <-ctx.Done():
		}
		// The workflow really started, but its successful response never reaches the scheduler.
		return nil, serviceerror.NewUnavailable("test lost accepted Start response")
	}
	select {
	case c.retried <- observed:
	default:
	}
	return response, err
}

func TestSchedule2ShardReloadAfterLostStartResponse(t *testing.T) {
	accepted := make(chan recoveryStartObservation, 1)
	retryRequests := make(chan *workflowservice.StartWorkflowExecutionRequest, 8)
	retried := make(chan recoveryStartObservation, 8)
	loseResponse, retryAfterReload := make(chan struct{}), make(chan struct{})
	var loseOnce, retryOnce sync.Once
	releaseLost := func() { loseOnce.Do(func() { close(loseResponse) }) }
	releaseRetry := func() { retryOnce.Do(func() { close(retryAfterReload) }) }
	defer releaseLost()
	defer releaseRetry()
	backend := scheduleBackend{name: "scheduler2-recovery", context: func(ctx context.Context) context.Context { return ctx }, assertPersisted: assertSchedule2Persisted, options: []testcore.TestOption{
		testcore.WithDedicatedCluster(),
		testcore.WithScheduleFrontend(func(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
			runtime.FrontendClient = &lostScheduleStartResponse{WorkflowServiceClient: runtime.FrontendClient, accepted: accepted, retryRequests: retryRequests, retried: retried, loseResponse: loseResponse, retryAfterReload: retryAfterReload}
			return scheduler2.NewPublicExecutionTestFrontend(runtime)
		}, scheduler2.NewNilLibrary()),
	}}
	env := newScheduleEnv(t, []scheduleBackend{backend})
	ctx := t.Context()
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) (string, error) {
		workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
		return "recovered", nil
	}, workflow.RegisterOptions{Name: "reload-start"})
	sid := "recovery-" + uuid.NewString()
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, Action: startWorkflowAction(env, "workflow-"+sid, "reload-start"), State: &schedulepb.ScheduleState{Paused: true}})
	assertScheduleCallbackBackend(t, env, backend, sid)
	patchSchedule(ctx, t, env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))
	var first recoveryStartObservation
	select {
	case first = <-accepted:
	case <-time.After(30 * time.Second):
		t.Fatal("real Start RPC did not accept workflow")
	}
	require.NoError(t, first.err)
	require.True(t, first.response.Started)
	require.NotEmpty(t, first.response.FirstExecutionRunId)
	require.Equal(t, first.response.FirstExecutionRunId, first.response.RunId)
	type state struct {
		incarnation, operationID, chainID string
		started, unfinished               int64
		pending                           bool
	}
	ref := chasm.NewComponentRef[*scheduler2.Scheduler](chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: sid})
	read := func(readCtx context.Context) (state, error) {
		engineCtx, err := env.GetTestCluster().Host().ChasmContext(readCtx)
		if err != nil {
			return state{}, err
		}
		return chasm.ReadComponent(engineCtx, ref, func(s *scheduler2.Scheduler, cc chasm.Context, _ struct{}) (state, error) {
			result := state{incarnation: s.Incarnation, operationID: s.OperationId, started: s.Started, unfinished: s.Unfinished}
			_, result.pending = s.PendingStarts[s.OperationId]
			if op, ok := s.Operations[s.OperationId]; ok {
				result.chainID = op.Get(cc).ChainId
			}
			return result, nil
		}, struct{}{})
	}
	before, err := read(ctx)
	require.NoError(t, err)
	require.Zero(t, before.started)
	require.EqualValues(t, 1, before.unfinished)
	require.True(t, before.pending)
	require.Equal(t, before.operationID, first.request.RequestId)
	// Closing the actual owning shard evicts its execution cache and queue ownership.
	// The retry gate keeps acknowledgement from racing the durable reload observation.
	env.CloseShard(env.NamespaceID().String(), sid)
	releaseLost()
	await.Require(ctx, t, func(at *await.T) {
		after, err := read(at.Context())
		require.NoError(at, err)
		require.Equal(at, before, after)
	}, 30*time.Second, 100*time.Millisecond)
	var retry *workflowservice.StartWorkflowExecutionRequest
	select {
	case retry = <-retryRequests:
	case <-time.After(30 * time.Second):
		t.Fatal("persisted Start intent was not redelivered after shard reload")
	}
	require.True(t, proto.Equal(first.request, retry), "redelivery must use the exact frozen request and callback")
	releaseRetry()
	var second recoveryStartObservation
	select {
	case second = <-retried:
	case <-time.After(30 * time.Second):
		t.Fatal("retried Start did not return")
	}
	require.NoError(t, second.err)
	// Start request-ID deduplication returns the original successful-start semantics.
	require.True(t, second.response.Started)
	require.Equal(t, first.response.FirstExecutionRunId, second.response.FirstExecutionRunId)
	require.Equal(t, first.response.RunId, second.response.RunId)
	await.Require(ctx, t, func(at *await.T) {
		got, err := read(at.Context())
		require.NoError(at, err)
		require.Equal(at, before.incarnation, got.incarnation)
		require.Equal(at, before.operationID, got.operationID)
		require.Equal(at, first.response.FirstExecutionRunId, got.chainID)
		require.EqualValues(at, 1, got.started)
		require.EqualValues(at, 1, got.unfinished)
		require.False(at, got.pending)
	}, 30*time.Second, 100*time.Millisecond)
	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{Namespace: env.Namespace().String(), WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: first.request.WorkflowId, RunId: first.response.RunId}, SignalName: "complete"})
	require.NoError(t, err)
	await.Require(ctx, t, func(at *await.T) {
		got, err := read(at.Context())
		require.NoError(at, err)
		require.EqualValues(at, 1, got.started)
		require.Zero(at, got.unfinished)
		description, err := env.FrontendClient().DescribeSchedule(at.Context(), &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
		require.NoError(at, err)
		require.Len(at, description.Info.RecentActions, 1)
		require.Equal(at, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, description.Info.RecentActions[0].StartWorkflowStatus)
		require.Equal(at, first.response.RunId, description.Info.RecentActions[0].StartWorkflowResult.RunId)
	}, 30*time.Second, 100*time.Millisecond)
	events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: first.request.WorkflowId})
	var starts int
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			starts++
		}
	}
	require.Equal(t, 1, starts)
	assertScheduleCallbackBackend(t, env, backend, sid)
}
