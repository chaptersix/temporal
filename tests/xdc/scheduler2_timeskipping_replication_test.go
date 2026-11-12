package xdc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type scheduler2TimeSkippingReplicationSuite struct {
	xdcBaseSuite
}

func TestScheduler2TimeSkippingReplicationSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(scheduler2TimeSkippingReplicationSuite))
}

func (s *scheduler2TimeSkippingReplicationSuite) SetupSuite() {
	s.enableTransitionHistory = true
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableChasm.Key():                       true,
		dynamicconfig.CHASMSchedulerTimeSkippingEnabled.Key(): true,
	}
	s.setupSuite(testcore.WithClusterHistoryTaskRecorder(), testcore.WithClusterScheduleFrontend(scheduler2.NewPublicExecutionTestFrontend, scheduler2.NewNilLibrary()))
}

func (s *scheduler2TimeSkippingReplicationSuite) SetupTest() {
	s.setupTest()
}

func (s *scheduler2TimeSkippingReplicationSuite) TearDownSuite() {
	s.tearDownSuite()
}

// A replicated skip changes the virtual-clock offset without changing the logical
// timer. The standby must write a new physical timer using the replicated offset.
func (s *scheduler2TimeSkippingReplicationSuite) TestStandbyRestampsCHASMTimerAfterSkip() {
	ctx, cancel := context.WithTimeout(s.T().Context(), 2*time.Minute)
	defer cancel()
	chasmCtx, err := s.clusters[0].Host().ChasmContext(ctx)
	s.Require().NoError(err)

	const interval = 24 * time.Hour
	firstTick := time.Now().UTC().Add(4 * time.Hour).Truncate(time.Second)
	scheduleID := "chasm-skip-replication-" + uuid.NewString()
	namespaceName := s.createGlobalNamespace()
	_, err = s.clusters[0].FrontendClient().CreateSchedule(chasmCtx, &workflowservice.CreateScheduleRequest{
		Namespace:  namespaceName,
		ScheduleId: scheduleID,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{
				Interval: durationpb.New(interval),
				Phase:    durationpb.New(time.Duration(firstTick.UnixNano() % interval.Nanoseconds())),
			}}},
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "workflow-" + scheduleID,
					WorkflowType: &commonpb.WorkflowType{Name: "unused"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "unused", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			}},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err)

	nsResp, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: namespaceName})
	s.Require().NoError(err)
	runID, err := chasm.ReadComponent(chasmCtx, chasm.NewComponentRef[*scheduler2.Scheduler](chasm.ExecutionKey{
		NamespaceID: nsResp.GetNamespaceInfo().GetId(), BusinessID: scheduleID,
	}), func(_ *scheduler2.Scheduler, cc chasm.Context, _ struct{}) (string, error) {
		return cc.ExecutionKey().RunID, nil
	}, struct{}{})
	s.Require().NoError(err)
	s.Require().NotEmpty(runID)
	registry := chasm.NewRegistry(s.logger)
	s.Require().NoError(registry.Register(scheduler2.NewNilLibrary()))
	expectedArchetype, ok := registry.ComponentIDFor(&scheduler2.Scheduler{})
	s.Require().True(ok)
	activeState, err := s.clusters[0].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: namespaceName, Execution: &commonpb.WorkflowExecution{WorkflowId: scheduleID, RunId: runID}, Archetype: chasm.FullyQualifiedName("scheduler2", "scheduler"),
	})
	s.Require().NoError(err)
	activeArchetype, err := chasm.ExecutionArchetypeID(activeState.GetDatabaseMutableState())
	s.Require().NoError(err)
	s.Require().Equal(expectedArchetype, activeArchetype)
	standbyContext, err := s.clusters[1].Host().ChasmContext(ctx)
	s.Require().NoError(err)

	filter := testcore.TaskFilter{NamespaceID: nsResp.GetNamespaceInfo().GetId(), WorkflowID: scheduleID, RunID: runID}
	standbyRecorder := s.clusters[1].GetHistoryTaskRecorder()
	s.Require().NotNil(standbyRecorder)
	var originalFireTime time.Time
	var originalTaskID int64
	await.Require(ctx, s.T(), func(t *await.T) {
		initial, err := s.clusters[1].AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: namespaceName, Execution: &commonpb.WorkflowExecution{WorkflowId: scheduleID, RunId: runID}, Archetype: chasm.FullyQualifiedName("scheduler2", "scheduler"),
		})
		require.NoError(t, err)
		require.Zero(t, initial.GetDatabaseMutableState().GetExecutionInfo().GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration())
		for _, recorded := range standbyRecorder.GetRecordedTasksByCategoryFiltered(tasks.CategoryTimer, filter) {
			if _, ok := recorded.Task.(*tasks.ChasmTaskPure); ok && recorded.Task.GetVisibilityTime().After(firstTick.Add(-time.Minute)) {
				originalFireTime = recorded.Task.GetVisibilityTime()
				originalTaskID = recorded.Task.GetTaskID()
				return
			}
		}
		require.Fail(t, "standby did not create the initial CHASM timer")
	}, replicationWaitTime, replicationCheckInterval)
	s.Require().WithinDuration(firstTick, originalFireTime, time.Minute)

	_, err = s.clusters[0].FrontendClient().PatchSchedule(chasmCtx, &workflowservice.PatchScheduleRequest{
		Namespace:  namespaceName,
		ScheduleId: scheduleID,
		Patch: &schedulepb.SchedulePatch{TimeSkippingConfig: &commonpb.TimeSkippingConfig{
			Enabled: true,
			FastForwardConfig: &commonpb.FastForwardConfig{
				Id:       uuid.NewString(),
				Duration: durationpb.New(2 * time.Hour),
			},
		}},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err)

	await.Require(ctx, s.T(), func(t *await.T) {
		desc, err := s.clusters[1].AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: namespaceName,
			Execution: &commonpb.WorkflowExecution{WorkflowId: scheduleID, RunId: runID},
			Archetype: chasm.FullyQualifiedName("scheduler2", "scheduler"),
		})
		require.NoError(t, err)
		archetype, err := chasm.ExecutionArchetypeID(desc.GetDatabaseMutableState())
		require.NoError(t, err)
		require.Equal(t, expectedArchetype, archetype)
		require.NotEqual(t, chasm.SchedulerArchetypeID, archetype)
		require.NotEqual(t, chasm.WorkflowArchetypeID, archetype)
		replicatedRun, err := chasm.ReadComponent(standbyContext, chasm.NewComponentRef[*scheduler2.Scheduler](chasm.ExecutionKey{NamespaceID: nsResp.GetNamespaceInfo().GetId(), BusinessID: scheduleID, RunID: runID}), func(component *scheduler2.Scheduler, cc chasm.Context, _ struct{}) (string, error) {
			require.Equal(t, cc.ExecutionKey().RunID, component.Incarnation)
			return component.Incarnation, nil
		}, struct{}{})
		require.NoError(t, err)
		require.Equal(t, runID, replicatedRun)
		skip := desc.GetDatabaseMutableState().GetExecutionInfo().GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
		require.Greater(t, skip, 90*time.Minute, "skip offset must replicate to standby")
		expectedFireTime := originalFireTime.Add(-skip)
		for _, recorded := range standbyRecorder.GetRecordedTasksByCategoryFiltered(tasks.CategoryTimer, filter) {
			if _, ok := recorded.Task.(*tasks.ChasmTaskPure); ok &&
				recorded.Task.GetTaskID() != originalTaskID && recorded.Task.GetVisibilityTime().Sub(expectedFireTime).Abs() < time.Minute {
				return
			}
		}
		require.Failf(t, "standby did not restamp the CHASM timer", "expected a physical timer near %v after offset %v", expectedFireTime, skip)
	}, replicationWaitTime, replicationCheckInterval)
}
