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
	"go.temporal.io/server/api/historyservice/v1"
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
	"google.golang.org/protobuf/proto"
)

type observedRecreationEffects struct {
	historyservice.HistoryServiceClient
	mu    sync.Mutex
	calls int
}

func (c *observedRecreationEffects) RequestCancelWorkflowExecution(ctx context.Context, r *historyservice.RequestCancelWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.HistoryServiceClient.RequestCancelWorkflowExecution(ctx, r, opts...)
}
func (c *observedRecreationEffects) TerminateWorkflowExecution(ctx context.Context, r *historyservice.TerminateWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.HistoryServiceClient.TerminateWorkflowExecution(ctx, r, opts...)
}

func TestSchedule2RecreationFencesOldEffectTimers(t *testing.T) {
	for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER} {
		t.Run(policy.String(), func(t *testing.T) {
			var effects *observedRecreationEffects
			var effectType uint32
			backend := scheduleBackend{name: "scheduler2-effect-recreation", context: schedule2Backend.context, assertPersisted: assertSchedule2Persisted, options: []testcore.TestOption{
				testcore.WithDedicatedCluster(), testcore.WithScheduleFrontend(func(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
					effects = &observedRecreationEffects{HistoryServiceClient: runtime.HistoryClient}
					runtime.HistoryClient = effects
					frontend, err := scheduler2.NewPublicExecutionTestFrontend(runtime)
					effectType, _ = runtime.Registry.TaskIDFor(&scheduler2pb.EffectTask{})
					return frontend, err
				}, scheduler2.NewNilLibrary()),
			}}
			env := newScheduleEnv(t, []scheduleBackend{backend})
			ctx := t.Context()
			captured := make(chan tasks.Task, 8)
			dispatched := make(chan struct{}, 8)
			var mu sync.Mutex
			replay := false
			env.InjectHook(testhooks.NewHook(testhooks.HistoryTimerTaskInterceptor, func(task tasks.Task, execute func()) {
				effect, ok := task.(*tasks.ChasmTask)
				if !ok || effect.Info.TypeId != effectType {
					execute()
					return
				}
				mu.Lock()
				deliver := replay
				mu.Unlock()
				if !deliver {
					select {
					case captured <- task:
					default:
					}
					return
				}
				execute()
				select {
				case dispatched <- struct{}{}:
				default:
				}
			}))
			env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
				workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
				return nil
			}, workflow.RegisterOptions{Name: "effect-recreation"})
			sid := "effect-recreation-" + uuid.NewString()
			schedule := &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, Action: startWorkflowAction(env, "old-"+sid, "effect-recreation"), State: &schedulepb.ScheduleState{Paused: true}}
			createSchedule(ctx, t, env, sid, schedule)
			patchSchedule(ctx, t, env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))
			engineCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
			require.NoError(t, err)
			key := chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: sid}
			read := func() (*scheduler2pb.RootState, error) {
				return chasm.ReadComponent(engineCtx, chasm.NewComponentRef[*scheduler2.Scheduler](key), func(s *scheduler2.Scheduler, _ chasm.Context, _ struct{}) (*scheduler2pb.RootState, error) {
					return common.CloneProto(s.RootState), nil
				}, struct{}{})
			}
			var old *scheduler2pb.RootState
			await.Require(ctx, t, func(at *await.T) {
				old, err = read()
				require.NoError(at, err)
				require.EqualValues(at, 1, old.Started)
				require.EqualValues(at, 1, old.Unfinished)
			}, 30*time.Second, 100*time.Millisecond)
			description, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
			require.NoError(t, err)
			require.Len(t, description.Info.RunningWorkflows, 1)
			oldWorkflow := description.Info.RunningWorkflows[0]
			patchSchedule(ctx, t, env, sid, triggerPatch(policy))
			var envelope tasks.Task
			select {
			case envelope = <-captured:
			case <-time.After(30 * time.Second):
				t.Fatal("actual old effect timer was not enqueued/dispatched")
			}
			require.Equal(t, tasks.CategoryTimer, envelope.GetCategory())
			require.Equal(t, old.Incarnation, envelope.GetRunID())
			pending, err := read()
			require.NoError(t, err)
			require.Len(t, pending.Effects, 1)
			payload := &scheduler2pb.EffectTask{}
			require.NoError(t, proto.Unmarshal(envelope.(*tasks.ChasmTask).Info.Data.Data, payload))
			require.Equal(t, pending.Effects[0].Id, payload.EffectId)
			require.Equal(t, pending.Effects[0].RetryAt, payload.WakeTime)
			require.Equal(t, old.OperationId, pending.Effects[0].TargetId)
			require.Equal(t, oldWorkflow.RunId, pending.Effects[0].ChainId)
			_, err = env.FrontendClient().DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
			require.NoError(t, err)
			schedule.Action = startWorkflowAction(env, "new-"+sid, "effect-recreation")
			createSchedule(ctx, t, env, sid, schedule)
			patchSchedule(ctx, t, env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))
			var before *scheduler2pb.RootState
			await.Require(ctx, t, func(at *await.T) {
				before, err = read()
				require.NoError(at, err)
				require.NotEqual(at, old.Incarnation, before.Incarnation)
				require.EqualValues(at, 1, before.Started)
				require.EqualValues(at, 1, before.Unfinished)
			}, 30*time.Second, 100*time.Millisecond)
			assertScheduleCallbackBackend(t, env, backend, sid)
			env.CloseShard(env.NamespaceID().String(), sid)
			mu.Lock()
			replay = true
			mu.Unlock()
			blob, err := serialization.NewSerializer().SerializeTask(envelope)
			require.NoError(t, err)
			shard := tasks.GetShardIDForTask(envelope, int(env.GetTestClusterConfig().HistoryConfig.NumHistoryShards))
			_, err = env.AdminClient().AddTasks(ctx, &adminservice.AddTasksRequest{ShardId: int32(shard), Tasks: []*adminservice.AddTasksRequest_Task{{CategoryId: int32(envelope.GetCategory().ID()), Blob: blob}}})
			require.NoError(t, err)
			select {
			case <-dispatched:
			case <-time.After(30 * time.Second):
				t.Fatal("old effect envelope was not dispatched after reload")
			}
			effects.mu.Lock()
			calls := effects.calls
			effects.mu.Unlock()
			require.Zero(t, calls, "invalid old timer must not call cancel or terminate")
			after, err := read()
			require.NoError(t, err)
			require.Equal(t, before, after)
			workflowState, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: oldWorkflow.WorkflowId, RunId: oldWorkflow.RunId}})
			require.NoError(t, err)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, workflowState.WorkflowExecutionInfo.Status)
		})
	}
}
