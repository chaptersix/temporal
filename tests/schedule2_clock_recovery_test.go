package tests

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSchedule2ClockSkipReloadAndOldPhysicalTimer(t *testing.T) {
	t.Parallel()
	for _, oldFirst := range []bool{false, true} {
		name := "shifted_first"
		if oldFirst {
			name = "old_physical_first"
		}
		t.Run(name, func(t *testing.T) { t.Parallel(); testSchedule2ClockRecovery(t, oldFirst) })
	}
}

func testSchedule2ClockRecovery(t *testing.T, oldFirst bool) {
	t.Helper()
	env := newScheduleEnv(t, []scheduleBackend{schedule2Backend}, testcore.WithHistoryTaskRecorder(), testcore.WithDynamicConfig(dynamicconfig.CHASMSchedulerTimeSkippingEnabled, true), testcore.WithDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true))
	ctx := t.Context()
	sid := "clock-recovery-" + uuid.NewString()
	var workflowRuns atomic.Int32
	registerCountingWorkflow(env, "clock-recovery", &workflowRuns)
	const interval = 24 * time.Hour
	firstTick := time.Now().UTC().Add(20 * time.Second).Truncate(time.Second)
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(interval), Phase: durationpb.New(time.Duration(firstTick.UnixNano() % interval.Nanoseconds()))}}}, Action: startWorkflowAction(env, "workflow-"+sid, "clock-recovery")})
	engineCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)
	key := chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: sid}
	runID, err := chasm.ReadComponent(engineCtx, chasm.NewComponentRef[*scheduler2.Scheduler](key), func(s *scheduler2.Scheduler, _ chasm.Context, _ struct{}) (string, error) { return s.Incarnation, nil }, struct{}{})
	require.NoError(t, err)
	require.NotEmpty(t, runID)
	key.RunID = runID
	assertSchedule2Persisted(t, env, sid)
	recorder := env.GetTestCluster().GetHistoryTaskRecorder()
	require.NotNil(t, recorder)
	filter := testcore.TaskFilter{NamespaceID: key.NamespaceID, WorkflowID: sid, RunID: runID}
	var original *tasks.ChasmTaskPure
	await.Require(ctx, t, func(at *await.T) {
		for _, record := range recorder.GetRecordedTasksByCategoryFiltered(tasks.CategoryTimer, filter) {
			if task, ok := record.Task.(*tasks.ChasmTaskPure); ok && task.GetVisibilityTime().Sub(firstTick).Abs() < time.Second {
				cloned := *task
				original = &cloned
				return
			}
		}
		require.Fail(at, "original physical timer was not persisted")
	}, awaitTimeout, pollInterval)
	registry := chasm.NewRegistry(env.Logger)
	require.NoError(t, registry.Register(scheduler2.NewNilLibrary()))
	workType, ok := registry.TaskIDFor(&scheduler2pb.WorkTask{})
	require.True(t, ok)
	logicalTask := func(stored *persistencespb.WorkflowMutableState) *persistencespb.ChasmComponentAttributes_Task {
		for _, node := range stored.GetChasmNodes() {
			for _, task := range node.GetMetadata().GetComponentAttributes().GetPureTasks() {
				if task.TypeId == workType && task.GetScheduledTime().AsTime().Equal(firstTick) {
					cloned := common.CloneProto(task)
					cloned.PhysicalTaskStatus = 0
					return cloned
				}
			}
		}
		return nil
	}
	initialState, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: sid, RunId: runID}, Archetype: chasm.FullyQualifiedName("scheduler2", "scheduler")})
	require.NoError(t, err)
	originalLogical := logicalTask(initialState.GetDatabaseMutableState())
	require.NotNil(t, originalLogical)
	originalPayload := &scheduler2pb.WorkTask{}
	require.NoError(t, proto.Unmarshal(originalLogical.GetData().GetData(), originalPayload))
	require.Positive(t, originalPayload.Token)
	var shiftedID, replayID atomic.Int64
	shiftedReady := make(chan struct{}, 1)
	shiftedDone := make(chan struct{}, 1)
	replayDone := make(chan struct{}, 1)
	replayReady := make(chan struct{}, 1)
	releaseReplay := make(chan struct{})
	var replayOnce sync.Once
	releaseOld := func() { replayOnce.Do(func() { close(releaseReplay) }) }
	defer releaseOld()
	releaseShift := make(chan struct{})
	var releaseOnce, gateOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseShift) }) }
	defer release()
	env.InjectHook(testhooks.NewHook(testhooks.HistoryTimerTaskInterceptor, func(task tasks.Task, execute func()) {
		if task.GetRunID() != runID {
			execute()
			return
		}
		if oldFirst && task.GetTaskID() == original.TaskID {
			select {
			case <-releaseShift:
			case <-ctx.Done():
			}
			execute()
			return
		}
		if task.GetTaskID() == shiftedID.Load() {
			gateOnce.Do(func() {
				shiftedReady <- struct{}{}
				select {
				case <-releaseShift:
				case <-ctx.Done():
				}
			})
			execute()
			select {
			case shiftedDone <- struct{}{}:
			default:
			}
			return
		}
		if oldFirst && task.GetTaskID() == replayID.Load() {
			select {
			case replayReady <- struct{}{}:
			default:
			}
			select {
			case <-releaseReplay:
			case <-ctx.Done():
			}
		}
		execute()
		if task.GetTaskID() == replayID.Load() {
			select {
			case replayDone <- struct{}{}:
			default:
			}
		}
	}))
	patchSchedule(ctx, t, env, sid, &schedulepb.SchedulePatch{TimeSkippingConfig: &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Id: uuid.NewString(), Duration: durationpb.New(10 * time.Second)}}})
	var shifted *tasks.ChasmTaskPure
	var skipped time.Duration
	await.Require(ctx, t, func(at *await.T) {
		stored, err := env.AdminClient().DescribeMutableState(at.Context(), &adminservice.DescribeMutableStateRequest{Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: sid, RunId: runID}, Archetype: chasm.FullyQualifiedName("scheduler2", "scheduler")})
		require.NoError(at, err)
		require.True(at, proto.Equal(originalLogical, logicalTask(stored.GetDatabaseMutableState())), "clock skip must preserve the exact logical task token, payload, deadline, and versioned fence")
		skipped = stored.GetDatabaseMutableState().GetExecutionInfo().GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
		require.Greater(at, skipped, 9*time.Second)
		expected := original.GetVisibilityTime().Add(-skipped)
		for _, record := range recorder.GetRecordedTasksByCategoryFiltered(tasks.CategoryTimer, filter) {
			if task, ok := record.Task.(*tasks.ChasmTaskPure); ok && task.TaskID != original.TaskID && task.GetVisibilityTime().Sub(expected).Abs() < time.Second {
				cloned := *task
				shifted = &cloned
				shiftedID.Store(task.TaskID)
				return
			}
		}
		require.Fail(at, "shifted physical timer was not persisted")
	}, awaitTimeout, pollInterval)
	require.True(t, shifted.GetVisibilityTime().Before(original.GetVisibilityTime()))
	env.CloseShard(key.NamespaceID, sid)
	token, err := chasm.ReadComponent(engineCtx, chasm.NewComponentRef[*scheduler2.Scheduler](key), func(s *scheduler2.Scheduler, _ chasm.Context, _ struct{}) (uint64, error) {
		return s.Coordination.Token, nil
	}, struct{}{})
	require.NoError(t, err)
	require.Equal(t, originalPayload.Token, token, "reloaded coordinator owns the same logical task")
	// Re-enqueue the original envelope without changing its physical fire time.
	// Its later wall-clock delivery must not replace the surviving logical wake.
	blob, err := serialization.NewSerializer().SerializeTask(original)
	require.NoError(t, err)
	shard := tasks.GetShardIDForTask(original, int(env.GetTestClusterConfig().HistoryConfig.NumHistoryShards))
	_, err = env.AdminClient().AddTasks(ctx, &adminservice.AddTasksRequest{ShardId: int32(shard), Tasks: []*adminservice.AddTasksRequest_Task{{CategoryId: int32(tasks.CategoryTimer.ID()), Blob: blob}}})
	require.NoError(t, err)
	await.Require(ctx, t, func(at *await.T) {
		for _, record := range recorder.GetRecordedTasksByCategoryFiltered(tasks.CategoryTimer, filter) {
			if task, ok := record.Task.(*tasks.ChasmTaskPure); ok && task.TaskID != original.TaskID && task.GetVisibilityTime().Sub(original.GetVisibilityTime()).Abs() <= 2*time.Millisecond {
				replayID.Store(task.TaskID)
				return
			}
		}
		require.Fail(at, "re-enqueued original physical envelope was not persisted")
	}, awaitTimeout, pollInterval)
	select {
	case <-shiftedReady:
	case <-time.After(awaitTimeout):
		t.Fatal("shifted physical timer was not dispatched after shard reload")
	}
	description, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
	require.NoError(t, err)
	require.Zero(t, description.Info.ActionCount, "no action before the surviving timer executes")
	require.Empty(t, description.Info.RecentActions)
	require.Zero(t, workflowRuns.Load(), "no workflow body before a due logical task executes")
	if oldFirst {
		select {
		case <-replayReady:
		case <-time.After(awaitTimeout):
			t.Fatal("old physical envelope did not reach actual dispatch")
		}
		token, err = chasm.ReadComponent(engineCtx, chasm.NewComponentRef[*scheduler2.Scheduler](key), func(s *scheduler2.Scheduler, _ chasm.Context, _ struct{}) (uint64, error) {
			return s.Coordination.Token, nil
		}, struct{}{})
		require.NoError(t, err)
		require.Equal(t, originalPayload.Token, token, "old physical envelope must encounter the same still-live logical task")
		require.Zero(t, workflowRuns.Load())
		releaseOld()
		select {
		case <-replayDone:
		case <-time.After(awaitTimeout):
			t.Fatal("old physical execution did not return")
		}
	} else {
		release()
		select {
		case <-shiftedDone:
		case <-time.After(awaitTimeout):
			t.Fatal("shifted timer execution did not return")
		}
	}

	await.Require(ctx, t, func(at *await.T) {
		description, err = env.FrontendClient().DescribeSchedule(at.Context(), &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
		require.NoError(at, err)
		require.EqualValues(at, 1, description.Info.ActionCount)
		require.Len(at, description.Info.RecentActions, 1)
		require.Equal(at, firstTick, description.Info.RecentActions[0].ScheduleTime.AsTime())
		require.False(at, description.Info.RecentActions[0].ActualTime.AsTime().Before(firstTick))
		require.EqualValues(at, 1, workflowRuns.Load())
	}, awaitTimeout, pollInterval)
	if oldFirst {
		release()
		select {
		case <-shiftedDone:
		case <-time.After(awaitTimeout):
			t.Fatal("shifted duplicate execution did not return")
		}
	} else {
		select {
		case <-replayDone:
		case <-time.After(awaitTimeout):
			t.Fatal("re-enqueued old physical timer was not dispatched")
		}
	}

	description, err = env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
	require.NoError(t, err)
	require.EqualValues(t, 1, description.Info.ActionCount)
	require.Len(t, description.Info.RecentActions, 1)
	require.Equal(t, firstTick, description.Info.RecentActions[0].ScheduleTime.AsTime())
	require.Equal(t, "workflow-"+sid+"-"+firstTick.Format(time.RFC3339), description.Info.RecentActions[0].StartWorkflowResult.WorkflowId)
	require.EqualValues(t, 1, workflowRuns.Load())
	assertSchedule2Persisted(t, env, sid)
	t.Logf("run=%s original_task=%d original_fire=%s shifted_task=%d shifted_fire=%s replay_task=%d offset=%s", runID, original.TaskID, original.VisibilityTimestamp, shifted.TaskID, shifted.VisibilityTimestamp, replayID.Load(), skipped)
}
