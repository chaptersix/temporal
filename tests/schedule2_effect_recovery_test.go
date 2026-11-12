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
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type lostEffectResponse struct {
	historyservice.HistoryServiceClient
	mu             sync.Mutex
	calls          int
	entered        chan proto.Message
	accepted       chan error
	retried        chan proto.Message
	issue, release chan struct{}
}

func (c *lostEffectResponse) invoke(ctx context.Context, request proto.Message, call func() error) error {
	c.mu.Lock()
	c.calls++
	first := c.calls == 1
	c.mu.Unlock()
	if !first {
		select {
		case c.retried <- proto.Clone(request):
		default:
		}
		return call()
	}
	c.entered <- proto.Clone(request)
	select {
	case <-c.issue:
	case <-ctx.Done():
		return ctx.Err()
	}
	err := call()
	c.accepted <- err
	if err != nil {
		return err
	}
	select {
	case <-c.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return serviceerror.NewUnavailable("test lost accepted overlap effect response")
}
func (c *lostEffectResponse) RequestCancelWorkflowExecution(ctx context.Context, r *historyservice.RequestCancelWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
	err := c.invoke(ctx, r, func() error {
		_, err := c.HistoryServiceClient.RequestCancelWorkflowExecution(ctx, r, opts...)
		return err
	})
	return &historyservice.RequestCancelWorkflowExecutionResponse{}, err
}
func (c *lostEffectResponse) TerminateWorkflowExecution(ctx context.Context, r *historyservice.TerminateWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
	err := c.invoke(ctx, r, func() error { _, err := c.HistoryServiceClient.TerminateWorkflowExecution(ctx, r, opts...); return err })
	return &historyservice.TerminateWorkflowExecutionResponse{}, err
}

func TestSchedule2LostEffectResponseAcrossChainChange(t *testing.T) {
	// Dedicated shard ownership and explicit response gates intentionally serialize cases.
	for _, chain := range []string{"continue-as-new", "reset"} {
		for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER} {
			t.Run(chain+"/"+policy.String(), func(t *testing.T) { testSchedule2LostEffectResponse(t, chain, policy) })
		}
	}
}

func testSchedule2LostEffectResponse(t *testing.T, chain string, policy enumspb.ScheduleOverlapPolicy) {
	effects := &lostEffectResponse{entered: make(chan proto.Message, 1), accepted: make(chan error, 1), retried: make(chan proto.Message, 8), issue: make(chan struct{}), release: make(chan struct{})}
	var issueOnce, releaseOnce sync.Once
	issue := func() { issueOnce.Do(func() { close(effects.issue) }) }
	release := func() { releaseOnce.Do(func() { close(effects.release) }) }
	t.Cleanup(issue)
	t.Cleanup(release)
	var effectType uint32
	backend := scheduleBackend{name: "scheduler2-effect-recovery", context: schedule2Backend.context, assertPersisted: assertSchedule2Persisted, options: []testcore.TestOption{
		testcore.WithDedicatedCluster(), testcore.WithScheduleFrontend(func(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
			effects.HistoryServiceClient = runtime.HistoryClient
			runtime.HistoryClient = effects
			frontend, err := scheduler2.NewPublicExecutionTestFrontend(runtime)
			effectType, _ = runtime.Registry.TaskIDFor(&scheduler2pb.EffectTask{})
			return frontend, err
		}, scheduler2.NewNilLibrary()),
	}}
	env := newScheduleEnv(t, []scheduleBackend{backend})
	require.NotZero(t, effectType)
	ctx := t.Context()
	effectDispatched := make(chan struct{})
	var firstEffectDispatch sync.Once
	env.InjectHook(testhooks.NewHook(testhooks.HistoryTimerTaskInterceptor, func(task tasks.Task, execute func()) {
		original := false
		if effect, ok := task.(*tasks.ChasmTask); ok && effect.Info.TypeId == effectType {
			firstEffectDispatch.Do(func() { original = true })
		}
		execute()
		if original {
			close(effectDispatched)
		}
	}))
	const workflowType = "effect-recovery"
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		detached, _ := workflow.NewDisconnectedContext(ctx)
		var command string
		workflow.GetSignalChannel(detached, "command").Receive(detached, &command)
		if command == "continue" {
			return workflow.NewContinueAsNewError(ctx, workflowType)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}, workflow.RegisterOptions{Name: workflowType})
	sid := "effect-recovery-" + uuid.NewString()
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, Action: startWorkflowAction(env, "workflow-"+sid, workflowType), State: &schedulepb.ScheduleState{Paused: true}})
	assertScheduleCallbackBackend(t, env, backend, sid)
	scheduledTime := timestamppb.New(time.Now().UTC().Truncate(time.Second))
	firstPatch := triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	firstPatch.TriggerImmediately.ScheduledTime = scheduledTime
	patchSchedule(ctx, t, env, sid, firstPatch)
	read := func() (*scheduler2pb.RootState, error) {
		cc, err := env.GetTestCluster().Host().ChasmContext(ctx)
		if err != nil {
			return nil, err
		}
		return chasm.ReadComponent(cc, chasm.NewComponentRef[*scheduler2.Scheduler](chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: sid}), func(s *scheduler2.Scheduler, _ chasm.Context, _ struct{}) (*scheduler2pb.RootState, error) {
			return common.CloneProto(s.RootState), nil
		}, struct{}{})
	}
	var original *commonpb.WorkflowExecution
	await.Require(ctx, t, func(at *await.T) {
		d, err := env.FrontendClient().DescribeSchedule(at.Context(), &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
		require.NoError(at, err)
		require.Len(at, d.Info.RunningWorkflows, 1)
		original = common.CloneProto(d.Info.RunningWorkflows[0])
	}, 30*time.Second, 100*time.Millisecond)
	env.WaitForHistoryEvents(`1 WorkflowExecutionStarted
2 WorkflowTaskScheduled
3 WorkflowTaskStarted
4 WorkflowTaskCompleted`, env.GetHistoryFunc(env.Namespace().String(), original), 10*time.Second, 10*time.Millisecond)
	signal := func(command string) {
		payloads, err := converter.GetDefaultDataConverter().ToPayloads(command)
		require.NoError(t, err)
		_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{Namespace: env.Namespace().String(), WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: original.WorkflowId}, SignalName: "command", Input: payloads})
		require.NoError(t, err)
	}
	if chain == "reset" {
		_, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{Namespace: env.Namespace().String(), WorkflowExecution: original, Reason: "effect recovery chain fence", WorkflowTaskFinishEventId: 3, RequestId: uuid.NewString()})
		require.NoError(t, err)
	} else {
		signal("continue")
	}
	var current string
	await.Require(ctx, t, func(at *await.T) {
		d, err := env.FrontendClient().DescribeWorkflowExecution(at.Context(), &workflowservice.DescribeWorkflowExecutionRequest{Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: original.WorkflowId}})
		require.NoError(at, err)
		current = d.WorkflowExecutionInfo.Execution.RunId
		require.NotEqual(at, original.RunId, current)
		require.Equal(at, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, d.WorkflowExecutionInfo.Status)
	}, 30*time.Second, 100*time.Millisecond)
	nextPatch := triggerPatch(policy)
	nextPatch.TriggerImmediately.ScheduledTime = scheduledTime
	patchSchedule(ctx, t, env, sid, nextPatch)
	var request proto.Message
	select {
	case request = <-effects.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("no actual History effect request")
	}
	before, err := read()
	require.NoError(t, err)
	require.Len(t, before.Effects, 1)
	require.Equal(t, original.RunId, before.Effects[0].ChainId)
	switch r := request.(type) {
	case *historyservice.RequestCancelWorkflowExecutionRequest:
		require.Equal(t, original.RunId, r.CancelRequest.FirstExecutionRunId)
		require.Empty(t, r.CancelRequest.WorkflowExecution.RunId)
	case *historyservice.TerminateWorkflowExecutionRequest:
		require.Equal(t, original.RunId, r.TerminateRequest.FirstExecutionRunId)
		require.Empty(t, r.TerminateRequest.WorkflowExecution.RunId)
	default:
		t.Fatalf("unexpected effect request %T", request)
	}
	issue()
	select {
	case err = <-effects.accepted:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("effect not accepted by actual History")
	}
	env.CloseShard(env.NamespaceID().String(), sid)
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER {
		durable, err := read()
		require.NoError(t, err)
		require.Len(t, durable.Effects, 1)
		require.Equal(t, before.Effects[0], durable.Effects[0])
		release()
		select {
		case retry := <-effects.retried:
			require.True(t, proto.Equal(request, retry), "retry must preserve exact target and first-run fence")
		case <-time.After(30 * time.Second):
			t.Fatal("accepted cancel was not retried after lost response/reload")
		}
		signal("complete")
	} else {
		// Termination completion may settle authority before the lost reply returns.
		await.Require(ctx, t, func(at *await.T) {
			s, err := read()
			require.NoError(at, err)
			require.EqualValues(at, 2, s.Started)
			require.EqualValues(at, 1, s.Unfinished)
		}, 30*time.Second, 100*time.Millisecond)
		release()
	}
	await.Require(ctx, t, func(at *await.T) {
		s, err := read()
		require.NoError(at, err)
		require.EqualValues(at, 2, s.Started)
		require.EqualValues(at, 1, s.Unfinished)
	}, 30*time.Second, 100*time.Millisecond)
	select {
	case <-effectDispatched:
	case <-time.After(30 * time.Second):
		t.Fatal("original effect delivery did not finish reconciliation")
	}
	d, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: original.WorkflowId, RunId: current}})
	require.NoError(t, err)
	expected := enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER {
		expected = enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED
	}
	require.Equal(t, expected, d.WorkflowExecutionInfo.Status)
	// Both manual commands use the same nominal time, so the second scheduled
	// action reuses the workflow ID with a distinct chain and request identity.
	replacement, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: original.WorkflowId},
	})
	require.NoError(t, err)
	replacementRun := replacement.WorkflowExecutionInfo.Execution.RunId
	require.NotEqual(t, original.RunId, replacementRun)
	require.NotEqual(t, current, replacementRun)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, replacement.WorkflowExecutionInfo.Status)
	switch r := request.(type) {
	case *historyservice.RequestCancelWorkflowExecutionRequest:
		_, err = effects.HistoryServiceClient.RequestCancelWorkflowExecution(ctx, r)
	case *historyservice.TerminateWorkflowExecutionRequest:
		_, err = effects.HistoryServiceClient.TerminateWorkflowExecution(ctx, r)
	default:
		t.Fatalf("unexpected effect request %T", request)
	}
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
	d, err = env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: original.WorkflowId, RunId: replacementRun},
	})
	require.NoError(t, err)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, d.WorkflowExecutionInfo.Status)

}
