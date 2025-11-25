package history

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/deletemanager"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type timerQueueTaskExecutorBaseTestDeps struct {
	controller        *gomock.Controller
	mockDeleteManager *deletemanager.MockDeleteManager
	mockCache         *wcache.MockCache
	mockChasmEngine   *chasm.MockEngine

	testShardContext           *shard.ContextTest
	timerQueueTaskExecutorBase *timerQueueTaskExecutorBase
}

func setupTimerQueueTaskExecutorBaseTest(t *testing.T) *timerQueueTaskExecutorBaseTestDeps {
	d := &timerQueueTaskExecutorBaseTestDeps{}

	d.controller = gomock.NewController(t)
	d.mockDeleteManager = deletemanager.NewMockDeleteManager(d.controller)
	d.mockCache = wcache.NewMockCache(d.controller)
	d.mockChasmEngine = chasm.NewMockEngine(d.controller)

	config := tests.NewDynamicConfig()
	d.testShardContext = shard.NewTestContext(
		d.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		config,
	)
	d.testShardContext.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	d.testShardContext.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()

	d.timerQueueTaskExecutorBase = newTimerQueueTaskExecutorBase(
		d.testShardContext,
		d.mockCache,
		d.mockDeleteManager,
		d.testShardContext.Resource.MatchingClient,
		d.mockChasmEngine,
		d.testShardContext.GetLogger(),
		metrics.NoopMetricsHandler,
		config,
		true, // isActive (irelevant for test)
	)

	return d
}

func TestTimerQueueTaskExecutorBase_ExecuteDeleteHistoryEventTask_NoErr(t *testing.T) {
	d := setupTimerQueueTaskExecutorBaseTest(t)
	defer d.controller.Finish()

	task := &tasks.DeleteHistoryEventTask{
		WorkflowKey: definition.NewWorkflowKey(
			tests.NamespaceID.String(),
			tests.WorkflowID,
			tests.RunID,
		),
		Version:             123,
		TaskID:              12345,
		VisibilityTimestamp: time.Now().UTC(),
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := historyi.NewMockWorkflowContext(d.controller)
	mockMutableState := historyi.NewMockMutableState(d.controller)

	d.mockCache.EXPECT().GetOrCreateChasmExecution(gomock.Any(), d.testShardContext, tests.NamespaceID, we, chasm.ArchetypeAny, locks.PriorityLow).Return(mockWeCtx, wcache.NoopReleaseFn, nil)

	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), d.testShardContext).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetWorkflowKey().Return(task.WorkflowKey).AnyTimes()
	mockMutableState.EXPECT().GetCloseVersion().Return(int64(1), nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry)
	d.testShardContext.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED})

	stage := tasks.DeleteWorkflowExecutionStageNone
	d.mockDeleteManager.EXPECT().DeleteWorkflowExecutionByRetention(
		gomock.Any(),
		tests.NamespaceID,
		we,
		mockWeCtx,
		mockMutableState,
		&stage,
	).Return(nil)

	err := d.timerQueueTaskExecutorBase.executeDeleteHistoryEventTask(
		context.Background(),
		task)
	require.NoError(t, err)
}

func TestTimerQueueTaskExecutorBase_ArchiveHistory_DeleteFailed(t *testing.T) {
	d := setupTimerQueueTaskExecutorBaseTest(t)
	defer d.controller.Finish()

	task := &tasks.DeleteHistoryEventTask{
		WorkflowKey: definition.NewWorkflowKey(
			tests.NamespaceID.String(),
			tests.WorkflowID,
			tests.RunID,
		),
		Version:             123,
		TaskID:              12345,
		VisibilityTimestamp: time.Now().UTC(),
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := historyi.NewMockWorkflowContext(d.controller)
	mockMutableState := historyi.NewMockMutableState(d.controller)

	d.mockCache.EXPECT().GetOrCreateChasmExecution(gomock.Any(), d.testShardContext, tests.NamespaceID, we, chasm.ArchetypeAny, locks.PriorityLow).Return(mockWeCtx, wcache.NoopReleaseFn, nil)

	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), d.testShardContext).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetWorkflowKey().Return(task.WorkflowKey).AnyTimes()
	mockMutableState.EXPECT().GetCloseVersion().Return(int64(1), nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry)
	d.testShardContext.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED})

	stage := tasks.DeleteWorkflowExecutionStageNone
	d.mockDeleteManager.EXPECT().DeleteWorkflowExecutionByRetention(
		gomock.Any(),
		tests.NamespaceID,
		we,
		mockWeCtx,
		mockMutableState,
		&stage,
	).Return(serviceerror.NewInternal("test error"))

	err := d.timerQueueTaskExecutorBase.executeDeleteHistoryEventTask(
		context.Background(),
		task)
	require.Error(t, err)
}

func TestTimerQueueTaskExecutorBase_IsValidExecutionTimeoutTask(t *testing.T) {
	d := setupTimerQueueTaskExecutorBaseTest(t)
	defer d.controller.Finish()

	testCases := []struct {
		name            string
		firstRunIDMatch bool
		workflowRunning bool
		isValid         bool
	}{
		{
			name:            "different chain",
			firstRunIDMatch: false,
			isValid:         false,
		},
		{
			name:            "same chain, workflow running",
			firstRunIDMatch: true,
			workflowRunning: true,
			isValid:         true,
		},
		{
			name:            "same chain, workflow completed",
			firstRunIDMatch: true,
			workflowRunning: false,
			isValid:         false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			timerTask := &tasks.WorkflowExecutionTimeoutTask{
				NamespaceID:         tests.NamespaceID.String(),
				WorkflowID:          tests.WorkflowID,
				FirstRunID:          uuid.New(),
				VisibilityTimestamp: d.testShardContext.GetTimeSource().Now(),
				TaskID:              100,
			}
			mutableStateFirstRunID := timerTask.FirstRunID
			if !tc.firstRunIDMatch {
				mutableStateFirstRunID = uuid.New()
			}

			mockMutableState := historyi.NewMockMutableState(d.controller)
			mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				FirstExecutionRunId: mutableStateFirstRunID,
			}).AnyTimes()
			mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(tc.workflowRunning).AnyTimes()

			isValid := d.timerQueueTaskExecutorBase.isValidWorkflowExecutionTimeoutTask(mockMutableState, timerTask)
			require.Equal(t, tc.isValid, isValid)
		})
	}
}

func TestTimerQueueTaskExecutorBase_IsValidExecutionTimeouts(t *testing.T) {
	d := setupTimerQueueTaskExecutorBaseTest(t)
	defer d.controller.Finish()

	timeNow := d.testShardContext.GetTimeSource().Now()
	timeBefore := timeNow.Add(time.Duration(-15) * time.Second)
	timeAfter := timeNow.Add(time.Duration(15) * time.Second)

	timerTask := &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,
		FirstRunID:  uuid.New(),
		TaskID:      100,
	}
	mockMutableState := historyi.NewMockMutableState(d.controller)
	mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	testCases := []struct {
		name           string
		expirationTime time.Time
		isValid        bool
	}{
		{
			name:           "expiration set before now",
			expirationTime: timeBefore,
			isValid:        true,
		},
		{
			name:           "expiration set after now",
			expirationTime: timeAfter,
			isValid:        false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				FirstExecutionRunId:             timerTask.FirstRunID,
				WorkflowExecutionExpirationTime: timestamppb.New(tc.expirationTime),
			})
			isValid := d.timerQueueTaskExecutorBase.isValidWorkflowExecutionTimeoutTask(mockMutableState, timerTask)
			require.Equal(t, tc.isValid, isValid)
		})
	}
}
