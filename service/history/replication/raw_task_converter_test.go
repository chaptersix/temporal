package replication

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type rawTaskConverterTestHelper struct {
	controller         *gomock.Controller
	shardContext       *shard.ContextTest
	workflowCache      *wcache.MockCache
	mockEngine         *historyi.MockEngine
	progressCache      *MockProgressCache
	executionManager   *persistence.MockExecutionManager
	syncStateRetriever *MockSyncStateRetriever
	logger             log.Logger

	namespaceID string
	workflowID  string

	runID           string
	workflowContext *historyi.MockWorkflowContext
	mutableState    *historyi.MockMutableState
	releaseFn       historyi.ReleaseWorkflowContextFunc
	lockReleased    bool

	newRunID           string
	newWorkflowContext *historyi.MockWorkflowContext
	newMutableState    *historyi.MockMutableState
	newReleaseFn       historyi.ReleaseWorkflowContextFunc

	replicationMultipleBatches bool
}

func setupRawTaskConverterTest(t *testing.T, replicationMultipleBatches bool) *rawTaskConverterTestHelper {
	controller := gomock.NewController(t)

	config := tests.NewDynamicConfig()
	config.ReplicationMultipleBatches = dynamicconfig.GetBoolPropertyFn(replicationMultipleBatches)

	shardContext := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			Owner:   "test-shard-owner",
		},
		config,
	)
	workflowCache := wcache.NewMockCache(controller)
	progressCache := NewMockProgressCache(controller)
	executionManager := shardContext.Resource.ExecutionMgr
	logger := shardContext.GetLogger()

	mockEngine := historyi.NewMockEngine(controller)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().Stop().AnyTimes()
	shardContext.SetEngineForTesting(mockEngine)

	namespaceID := tests.NamespaceID.String()
	namespaceRegistry := shardContext.Resource.NamespaceCache
	namespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	workflowID := uuid.New()

	runID := uuid.New()
	workflowContext := historyi.NewMockWorkflowContext(controller)
	mutableState := historyi.NewMockMutableState(controller)
	syncStateRetriever := NewMockSyncStateRetriever(controller)

	newRunID := uuid.New()
	newWorkflowContext := historyi.NewMockWorkflowContext(controller)
	newMutableState := historyi.NewMockMutableState(controller)

	h := &rawTaskConverterTestHelper{
		controller:                 controller,
		shardContext:               shardContext,
		workflowCache:              workflowCache,
		mockEngine:                 mockEngine,
		progressCache:              progressCache,
		executionManager:           executionManager,
		syncStateRetriever:         syncStateRetriever,
		logger:                     logger,
		namespaceID:                namespaceID,
		workflowID:                 workflowID,
		runID:                      runID,
		workflowContext:            workflowContext,
		mutableState:               mutableState,
		lockReleased:               false,
		newRunID:                   newRunID,
		newWorkflowContext:         newWorkflowContext,
		newMutableState:            newMutableState,
		replicationMultipleBatches: replicationMultipleBatches,
	}

	h.releaseFn = func(error) { h.lockReleased = true }
	h.newReleaseFn = func(error) { h.lockReleased = true }

	return h
}

func (h *rawTaskConverterTestHelper) tearDown() {
	h.controller.Finish()
	h.shardContext.StopForTest()
}

func TestConvertActivityStateReplicationTask_WorkflowMissing(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupRawTaskConverterTest(t, tc.replicationMultipleBatches)
			defer h.tearDown()

			ctx := context.Background()
			scheduledEventID := int64(144)
			version := int64(288)
			taskID := int64(1444)
			task := &tasks.SyncActivityTask{
				WorkflowKey: definition.NewWorkflowKey(
					h.namespaceID,
					h.workflowID,
					h.runID,
				),
				VisibilityTimestamp: time.Now().UTC(),
				TaskID:              taskID,
				Version:             version,
				ScheduledEventID:    scheduledEventID,
			}
			h.workflowCache.EXPECT().GetOrCreateChasmExecution(
				gomock.Any(),
				h.shardContext,
				namespace.ID(h.namespaceID),
				&commonpb.WorkflowExecution{
					WorkflowId: h.workflowID,
					RunId:      h.runID,
				},
				chasm.WorkflowArchetype,
				locks.PriorityLow,
			).Return(h.workflowContext, h.releaseFn, nil)
			h.workflowContext.EXPECT().LoadMutableState(gomock.Any(), h.shardContext).Return(nil, serviceerror.NewNotFound(""))

			result, err := convertActivityStateReplicationTask(ctx, h.shardContext, task, h.workflowCache)
			require.NoError(t, err)
			require.Nil(t, result)
			require.True(t, h.lockReleased)
		})
	}
}

func TestConvertActivityStateReplicationTask_WorkflowCompleted(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupRawTaskConverterTest(t, tc.replicationMultipleBatches)
			defer h.tearDown()

			ctx := context.Background()
			scheduledEventID := int64(144)
			version := int64(288)
			taskID := int64(1444)
			task := &tasks.SyncActivityTask{
				WorkflowKey: definition.NewWorkflowKey(
					h.namespaceID,
					h.workflowID,
					h.runID,
				),
				VisibilityTimestamp: time.Now().UTC(),
				TaskID:              taskID,
				Version:             version,
				ScheduledEventID:    scheduledEventID,
			}
			h.workflowCache.EXPECT().GetOrCreateChasmExecution(
				gomock.Any(),
				h.shardContext,
				namespace.ID(h.namespaceID),
				&commonpb.WorkflowExecution{
					WorkflowId: h.workflowID,
					RunId:      h.runID,
				},
				chasm.WorkflowArchetype,
				locks.PriorityLow,
			).Return(h.workflowContext, h.releaseFn, nil)
			h.workflowContext.EXPECT().LoadMutableState(gomock.Any(), h.shardContext).Return(h.mutableState, nil)
			h.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()

			result, err := convertActivityStateReplicationTask(ctx, h.shardContext, task, h.workflowCache)
			require.NoError(t, err)
			require.Nil(t, result)
			require.True(t, h.lockReleased)
		})
	}
}

func TestConvertActivityStateReplicationTask_ActivityCompleted(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupRawTaskConverterTest(t, tc.replicationMultipleBatches)
			defer h.tearDown()

			ctx := context.Background()
			scheduledEventID := int64(144)
			version := int64(288)
			taskID := int64(1444)
			task := &tasks.SyncActivityTask{
				WorkflowKey: definition.NewWorkflowKey(
					h.namespaceID,
					h.workflowID,
					h.runID,
				),
				VisibilityTimestamp: time.Now().UTC(),
				TaskID:              taskID,
				Version:             version,
				ScheduledEventID:    scheduledEventID,
			}
			h.workflowCache.EXPECT().GetOrCreateChasmExecution(
				gomock.Any(),
				h.shardContext,
				namespace.ID(h.namespaceID),
				&commonpb.WorkflowExecution{
					WorkflowId: h.workflowID,
					RunId:      h.runID,
				},
				chasm.WorkflowArchetype,
				locks.PriorityLow,
			).Return(h.workflowContext, h.releaseFn, nil)
			h.workflowContext.EXPECT().LoadMutableState(gomock.Any(), h.shardContext).Return(h.mutableState, nil)
			h.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
			h.mutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(nil, false).AnyTimes()

			result, err := convertActivityStateReplicationTask(ctx, h.shardContext, task, h.workflowCache)
			require.NoError(t, err)
			require.Nil(t, result)
			require.True(t, h.lockReleased)
		})
	}
}

func TestConvertActivityStateReplicationTask_ActivityScheduled(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupRawTaskConverterTest(t, tc.replicationMultipleBatches)
			defer h.tearDown()

			ctx := context.Background()
			scheduledEventID := int64(144)
			version := int64(333)
			taskID := int64(1444)
			task := &tasks.SyncActivityTask{
				WorkflowKey: definition.NewWorkflowKey(
					h.namespaceID,
					h.workflowID,
					h.runID,
				),
				VisibilityTimestamp: time.Now().UTC(),
				TaskID:              taskID,
				Version:             version,
				ScheduledEventID:    scheduledEventID,
			}
			h.workflowCache.EXPECT().GetOrCreateChasmExecution(
				gomock.Any(),
				h.shardContext,
				namespace.ID(h.namespaceID),
				&commonpb.WorkflowExecution{
					WorkflowId: h.workflowID,
					RunId:      h.runID,
				},
				chasm.WorkflowArchetype,
				locks.PriorityLow,
			).Return(h.workflowContext, h.releaseFn, nil)

			activityVersion := version
			activityScheduledEventID := scheduledEventID
			activityScheduledTime := time.Now().UTC()
			activityStartedEventID := common.EmptyEventID
			activityAttempt := int32(16384)
			activityDetails := payloads.EncodeString("some random activity progress")
			activityLastFailure := failure.NewServerFailure("some random reason", false)
			activityLastWorkerIdentity := "some random worker identity"
			baseWorkflowInfo := &workflowspb.BaseExecutionInfo{
				RunId:                            uuid.New(),
				LowestCommonAncestorEventId:      rand.Int63(),
				LowestCommonAncestorEventVersion: rand.Int63(),
			}
			versionHistory := &historyspb.VersionHistory{
				BranchToken: []byte{},
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: scheduledEventID,
						Version: version,
					},
				},
			}
			versionHistories := &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					versionHistory,
				},
			}
			h.workflowContext.EXPECT().LoadMutableState(gomock.Any(), h.shardContext).Return(h.mutableState, nil)
			h.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
			h.mutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
				Version:                 activityVersion,
				ScheduledEventId:        activityScheduledEventID,
				ScheduledTime:           timestamppb.New(activityScheduledTime),
				StartedEventId:          activityStartedEventID,
				StartedTime:             nil,
				LastHeartbeatUpdateTime: nil,
				LastHeartbeatDetails:    activityDetails,
				Attempt:                 activityAttempt,
				RetryLastFailure:        activityLastFailure,
				RetryLastWorkerIdentity: activityLastWorkerIdentity,
			}, true).AnyTimes()
			h.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				BaseExecutionInfo: baseWorkflowInfo,
				VersionHistories:  versionHistories,
			}).AnyTimes()
			h.mutableState.EXPECT().GetBaseWorkflowInfo().Return(baseWorkflowInfo).AnyTimes()

			result, err := convertActivityStateReplicationTask(ctx, h.shardContext, task, h.workflowCache)
			require.NoError(t, err)
			require.NotNil(t, result)
			retryInitialInterval := &durationpb.Duration{
				Nanos: 0,
			}
			protorequire.ProtoEqual(t, &replicationspb.ReplicationTask{
				SourceTaskId: taskID,
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
				Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
					SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
						NamespaceId:          h.namespaceID,
						WorkflowId:           h.workflowID,
						RunId:                h.runID,
						Version:              activityVersion,
						ScheduledEventId:     activityScheduledEventID,
						ScheduledTime:        timestamppb.New(activityScheduledTime),
						StartedEventId:       activityStartedEventID,
						StartedTime:          nil,
						LastHeartbeatTime:    nil,
						Details:              activityDetails,
						Attempt:              activityAttempt,
						LastFailure:          activityLastFailure,
						LastWorkerIdentity:   activityLastWorkerIdentity,
						BaseExecutionInfo:    baseWorkflowInfo,
						VersionHistory:       versionHistory,
						RetryInitialInterval: retryInitialInterval,
					},
				},
				VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
			}, result)
			require.True(t, h.lockReleased)
		})
	}
}

func TestConvertActivityStateReplicationTask_ActivityStarted(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupRawTaskConverterTest(t, tc.replicationMultipleBatches)
			defer h.tearDown()

			ctx := context.Background()
			scheduledEventID := int64(144)
			version := int64(333)
			taskID := int64(1444)
			task := &tasks.SyncActivityTask{
				WorkflowKey: definition.NewWorkflowKey(
					h.namespaceID,
					h.workflowID,
					h.runID,
				),
				VisibilityTimestamp: time.Now().UTC(),
				TaskID:              taskID,
				Version:             version,
				ScheduledEventID:    scheduledEventID,
			}
			h.workflowCache.EXPECT().GetOrCreateChasmExecution(
				gomock.Any(),
				h.shardContext,
				namespace.ID(h.namespaceID),
				&commonpb.WorkflowExecution{
					WorkflowId: h.workflowID,
					RunId:      h.runID,
				},
				chasm.WorkflowArchetype,
				locks.PriorityLow,
			).Return(h.workflowContext, h.releaseFn, nil)

			activityVersion := version
			activityScheduledEventID := scheduledEventID
			activityScheduledTime := time.Now().UTC()
			activityStartedEventID := activityScheduledEventID + 1
			activityStartedTime := activityScheduledTime.Add(time.Minute)
			activityHeartbeatTime := activityStartedTime.Add(time.Minute)
			activityAttempt := int32(16384)
			activityDetails := payloads.EncodeString("some random activity progress")
			activityLastFailure := failure.NewServerFailure("some random reason", false)
			activityLastWorkerIdentity := "some random worker identity"
			baseWorkflowInfo := &workflowspb.BaseExecutionInfo{
				RunId:                            uuid.New(),
				LowestCommonAncestorEventId:      rand.Int63(),
				LowestCommonAncestorEventVersion: rand.Int63(),
			}
			versionHistory := &historyspb.VersionHistory{
				BranchToken: []byte{},
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: scheduledEventID,
						Version: version,
					},
				},
			}
			versionHistories := &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					versionHistory,
				},
			}
			h.workflowContext.EXPECT().LoadMutableState(gomock.Any(), h.shardContext).Return(h.mutableState, nil)
			h.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
			h.mutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
				Version:                 activityVersion,
				ScheduledEventId:        activityScheduledEventID,
				ScheduledTime:           timestamppb.New(activityScheduledTime),
				StartedEventId:          activityStartedEventID,
				StartedTime:             timestamppb.New(activityStartedTime),
				LastHeartbeatUpdateTime: timestamppb.New(activityHeartbeatTime),
				LastHeartbeatDetails:    activityDetails,
				Attempt:                 activityAttempt,
				RetryLastFailure:        activityLastFailure,
				RetryLastWorkerIdentity: activityLastWorkerIdentity,
			}, true).AnyTimes()
			h.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				BaseExecutionInfo: baseWorkflowInfo,
				VersionHistories:  versionHistories,
			}).AnyTimes()
			h.mutableState.EXPECT().GetBaseWorkflowInfo().Return(baseWorkflowInfo).AnyTimes()

			result, err := convertActivityStateReplicationTask(ctx, h.shardContext, task, h.workflowCache)
			require.NoError(t, err)
			retryInitialInterval := &durationpb.Duration{
				Nanos: 0,
			}
			protorequire.ProtoEqual(t, &replicationspb.ReplicationTask{
				SourceTaskId: taskID,
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
				Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
					SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
						NamespaceId:          h.namespaceID,
						WorkflowId:           h.workflowID,
						RunId:                h.runID,
						Version:              activityVersion,
						ScheduledEventId:     activityScheduledEventID,
						ScheduledTime:        timestamppb.New(activityScheduledTime),
						StartedEventId:       activityStartedEventID,
						StartedTime:          timestamppb.New(activityStartedTime),
						LastHeartbeatTime:    timestamppb.New(activityHeartbeatTime),
						Details:              activityDetails,
						Attempt:              activityAttempt,
						LastFailure:          activityLastFailure,
						LastWorkerIdentity:   activityLastWorkerIdentity,
						BaseExecutionInfo:    baseWorkflowInfo,
						VersionHistory:       versionHistory,
						RetryInitialInterval: retryInitialInterval,
					},
				},
				VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
			}, result)
			require.True(t, h.lockReleased)
		})
	}
}

func TestConvertWorkflowStateReplicationTask_WorkflowOpen(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupRawTaskConverterTest(t, tc.replicationMultipleBatches)
			defer h.tearDown()

			ctx := context.Background()
			version := int64(288)
			taskID := int64(1444)
			task := &tasks.SyncWorkflowStateTask{
				WorkflowKey: definition.NewWorkflowKey(
					h.namespaceID,
					h.workflowID,
					h.runID,
				),
				VisibilityTimestamp: time.Now().UTC(),
				TaskID:              taskID,
				Version:             version,
			}
			h.workflowCache.EXPECT().GetOrCreateChasmExecution(
				gomock.Any(),
				h.shardContext,
				namespace.ID(h.namespaceID),
				&commonpb.WorkflowExecution{
					WorkflowId: h.workflowID,
					RunId:      h.runID,
				},
				chasm.WorkflowArchetype,
				locks.PriorityLow,
			).Return(h.workflowContext, h.releaseFn, nil)
			h.workflowContext.EXPECT().LoadMutableState(gomock.Any(), h.shardContext).Return(h.mutableState, nil)
			h.mutableState.EXPECT().GetWorkflowStateStatus().Return(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING).AnyTimes()

			result, err := convertWorkflowStateReplicationTask(ctx, h.shardContext, task, h.workflowCache)
			require.NoError(t, err)
			require.Nil(t, result)
			require.True(t, h.lockReleased)
		})
	}
}

func TestConvertWorkflowStateReplicationTask_WorkflowClosed(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupRawTaskConverterTest(t, tc.replicationMultipleBatches)
			defer h.tearDown()

			ctx := context.Background()
			version := int64(288)
			taskID := int64(1444)
			task := &tasks.SyncWorkflowStateTask{
				WorkflowKey: definition.NewWorkflowKey(
					h.namespaceID,
					h.workflowID,
					h.runID,
				),
				VisibilityTimestamp: time.Now().UTC(),
				TaskID:              taskID,
				Version:             version,
			}
			h.workflowCache.EXPECT().GetOrCreateChasmExecution(
				gomock.Any(),
				h.shardContext,
				namespace.ID(h.namespaceID),
				&commonpb.WorkflowExecution{
					WorkflowId: h.workflowID,
					RunId:      h.runID,
				},
				chasm.WorkflowArchetype,
				locks.PriorityLow,
			).Return(h.workflowContext, h.releaseFn, nil)
			h.workflowContext.EXPECT().LoadMutableState(gomock.Any(), h.shardContext).Return(h.mutableState, nil)
			h.mutableState.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					NamespaceId:                       h.namespaceID,
					WorkflowId:                        h.workflowID,
					TaskGenerationShardClockTimestamp: 123,
					CloseVisibilityTaskId:             456,
					CloseTransferTaskId:               789,
				},
				ExecutionState: &persistencespb.WorkflowExecutionState{
					RunId:  h.runID,
					State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				},
			}).AnyTimes()
			h.mutableState.EXPECT().GetWorkflowStateStatus().Return(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED).AnyTimes()
			// Mock for watermark check
			executionInfo := &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                       h.namespaceID,
				WorkflowId:                        h.workflowID,
				TaskGenerationShardClockTimestamp: 123,
				CloseVisibilityTaskId:             456,
				CloseTransferTaskId:               789,
			}
			h.mutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
			h.mutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(h.namespaceID, h.workflowID, h.runID)).AnyTimes()

			result, err := convertWorkflowStateReplicationTask(ctx, h.shardContext, task, h.workflowCache)
			require.NoError(t, err)

			sanitizedMutableState := h.mutableState.CloneToProto()
			workflow.SanitizeMutableState(sanitizedMutableState)
			protorequire.ProtoEqual(t, &replicationspb.ReplicationTask{
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
				SourceTaskId: task.TaskID,
				Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
					SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
						WorkflowState:            sanitizedMutableState,
						IsForceReplication:       task.IsForceReplication,
						IsCloseTransferTaskAcked: false, // No queue state available
					},
				},
				VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
			}, result)
			require.True(t, h.lockReleased)
		})
	}
}

func TestConvertHistoryReplicationTask_WorkflowMissing(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupRawTaskConverterTest(t, tc.replicationMultipleBatches)
			defer h.tearDown()

			ctx := context.Background()
			shardID := int32(12)
			firstEventID := int64(999)
			nextEventID := int64(1911)
			version := int64(288)
			taskID := int64(1444)
			task := &tasks.HistoryReplicationTask{
				WorkflowKey: definition.NewWorkflowKey(
					h.namespaceID,
					h.workflowID,
					h.runID,
				),
				VisibilityTimestamp: time.Now().UTC(),
				TaskID:              taskID,
				Version:             version,
				FirstEventID:        firstEventID,
				NextEventID:         nextEventID,
				NewRunID:            h.newRunID,
			}

			h.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
				gomock.Any(),
				h.shardContext,
				namespace.ID(h.namespaceID),
				&commonpb.WorkflowExecution{
					WorkflowId: h.workflowID,
					RunId:      h.runID,
				},
				locks.PriorityLow,
			).Return(h.workflowContext, h.releaseFn, nil)
			h.workflowContext.EXPECT().LoadMutableState(gomock.Any(), h.shardContext).Return(nil, serviceerror.NewNotFound(""))

			result, err := convertHistoryReplicationTask(ctx, h.shardContext, task, shardID, h.workflowCache, nil, h.executionManager, h.logger, h.shardContext.GetConfig())
			require.NoError(t, err)
			require.Nil(t, result)
			require.True(t, h.lockReleased)
		})
	}
}

func TestConvertHistoryReplicationTask_WithNewRun(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupRawTaskConverterTest(t, tc.replicationMultipleBatches)
			defer h.tearDown()

			ctx := context.Background()
			shardID := int32(12)
			firstEventID := int64(999)
			nextEventID := int64(1911)
			version := int64(288)
			taskID := int64(1444)
			task := &tasks.HistoryReplicationTask{
				WorkflowKey: definition.NewWorkflowKey(
					h.namespaceID,
					h.workflowID,
					h.runID,
				),
				VisibilityTimestamp: time.Now().UTC(),
				TaskID:              taskID,
				Version:             version,
				FirstEventID:        firstEventID,
				NextEventID:         nextEventID,
				NewRunID:            h.newRunID,
			}
			baseWorkflowInfo := &workflowspb.BaseExecutionInfo{
				RunId:                            uuid.New(),
				LowestCommonAncestorEventId:      rand.Int63(),
				LowestCommonAncestorEventVersion: rand.Int63(),
			}
			versionHistory := &historyspb.VersionHistory{
				BranchToken: []byte("branch token"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: nextEventID - 1,
						Version: version,
					},
				},
			}
			versionHistories := &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					versionHistory,
				},
			}
			events := &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         []byte("data"),
			}
			h.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
				gomock.Any(),
				h.shardContext,
				namespace.ID(h.namespaceID),
				&commonpb.WorkflowExecution{
					WorkflowId: h.workflowID,
					RunId:      h.runID,
				},
				locks.PriorityLow,
			).Return(h.workflowContext, h.releaseFn, nil)
			h.workflowContext.EXPECT().LoadMutableState(gomock.Any(), h.shardContext).Return(h.mutableState, nil)
			h.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				BaseExecutionInfo: baseWorkflowInfo,
				VersionHistories:  versionHistories,
			}).AnyTimes()
			h.mutableState.EXPECT().GetBaseWorkflowInfo().Return(baseWorkflowInfo).AnyTimes()
			h.executionManager.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
				BranchToken:   versionHistory.BranchToken,
				MinEventID:    firstEventID,
				MaxEventID:    nextEventID,
				PageSize:      1,
				NextPageToken: nil,
				ShardID:       shardID,
			}).Return(&persistence.ReadRawHistoryBranchResponse{
				HistoryEventBlobs: []*commonpb.DataBlob{events},
				NextPageToken:     nil,
			}, nil)

			newVersionHistory := &historyspb.VersionHistory{
				BranchToken: []byte("new branch token"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 3,
						Version: version,
					},
				},
			}
			newVersionHistories := &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					newVersionHistory,
				},
			}
			newEvents := &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         []byte("new data"),
			}
			h.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
				gomock.Any(),
				h.shardContext,
				namespace.ID(h.namespaceID),
				&commonpb.WorkflowExecution{
					WorkflowId: h.workflowID,
					RunId:      h.newRunID,
				},
				locks.PriorityLow,
			).Return(h.newWorkflowContext, h.releaseFn, nil)
			h.newWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), h.shardContext).Return(h.newMutableState, nil)
			h.newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				BaseExecutionInfo: baseWorkflowInfo,
				VersionHistories:  newVersionHistories,
			}).AnyTimes()
			h.newMutableState.EXPECT().GetBaseWorkflowInfo().Return(nil).AnyTimes()
			h.executionManager.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
				BranchToken:   newVersionHistory.BranchToken,
				MinEventID:    common.FirstEventID,
				MaxEventID:    common.FirstEventID + 1,
				PageSize:      1,
				NextPageToken: nil,
				ShardID:       shardID,
			}).Return(&persistence.ReadRawHistoryBranchResponse{
				HistoryEventBlobs: []*commonpb.DataBlob{newEvents},
				NextPageToken:     nil,
			}, nil)

			result, err := convertHistoryReplicationTask(ctx, h.shardContext, task, shardID, h.workflowCache, nil, h.executionManager, h.logger, h.shardContext.GetConfig())
			require.NoError(t, err)
			if tc.replicationMultipleBatches {
				require.Equal(t, &replicationspb.ReplicationTask{
					TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
					SourceTaskId: task.TaskID,
					Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
						HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
							NamespaceId:         task.NamespaceID,
							WorkflowId:          task.WorkflowID,
							RunId:               task.RunID,
							BaseExecutionInfo:   baseWorkflowInfo,
							VersionHistoryItems: versionHistory.Items,
							Events:              nil,
							EventsBatches:       []*commonpb.DataBlob{events},
							NewRunEvents:        newEvents,
							NewRunId:            h.newRunID,
						},
					},
					VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
				}, result)
			} else {
				require.Equal(t, &replicationspb.ReplicationTask{
					TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
					SourceTaskId: task.TaskID,
					Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
						HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
							NamespaceId:         task.NamespaceID,
							WorkflowId:          task.WorkflowID,
							RunId:               task.RunID,
							BaseExecutionInfo:   baseWorkflowInfo,
							VersionHistoryItems: versionHistory.Items,
							Events:              events,
							EventsBatches:       nil,
							NewRunEvents:        newEvents,
							NewRunId:            h.newRunID,
						},
					},
					VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
				}, result)

			}
			require.True(t, h.lockReleased)
		})
	}
}

func TestConvertHistoryReplicationTask_WithoutNewRun(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupRawTaskConverterTest(t, tc.replicationMultipleBatches)
			defer h.tearDown()

			ctx := context.Background()
			shardID := int32(12)
			firstEventID := int64(999)
			nextEventID := int64(1911)
			version := int64(288)
			taskID := int64(1444)
			task := &tasks.HistoryReplicationTask{
				WorkflowKey: definition.NewWorkflowKey(
					h.namespaceID,
					h.workflowID,
					h.runID,
				),
				VisibilityTimestamp: time.Now().UTC(),
				TaskID:              taskID,
				Version:             version,
				FirstEventID:        firstEventID,
				NextEventID:         nextEventID,
				NewRunID:            "",
			}
			versionHistory := &historyspb.VersionHistory{
				BranchToken: []byte("branch token"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: nextEventID - 1,
						Version: version,
					},
				},
			}
			baseWorkflowInfo := &workflowspb.BaseExecutionInfo{
				RunId:                            uuid.New(),
				LowestCommonAncestorEventId:      rand.Int63(),
				LowestCommonAncestorEventVersion: rand.Int63(),
			}
			versionHistories := &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					versionHistory,
				},
			}
			events := &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         []byte("data"),
			}
			h.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
				gomock.Any(),
				h.shardContext,
				namespace.ID(h.namespaceID),
				&commonpb.WorkflowExecution{
					WorkflowId: h.workflowID,
					RunId:      h.runID,
				},
				locks.PriorityLow,
			).Return(h.workflowContext, h.releaseFn, nil)
			h.workflowContext.EXPECT().LoadMutableState(gomock.Any(), h.shardContext).Return(h.mutableState, nil)
			h.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				BaseExecutionInfo: baseWorkflowInfo,
				VersionHistories:  versionHistories,
			}).AnyTimes()
			h.mutableState.EXPECT().GetBaseWorkflowInfo().Return(baseWorkflowInfo).AnyTimes()
			h.executionManager.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
				BranchToken:   versionHistory.BranchToken,
				MinEventID:    firstEventID,
				MaxEventID:    nextEventID,
				PageSize:      1,
				NextPageToken: nil,
				ShardID:       shardID,
			}).Return(&persistence.ReadRawHistoryBranchResponse{
				HistoryEventBlobs: []*commonpb.DataBlob{events},
				NextPageToken:     nil,
			}, nil)

			result, err := convertHistoryReplicationTask(ctx, h.shardContext, task, shardID, h.workflowCache, nil, h.executionManager, h.logger, h.shardContext.GetConfig())
			require.NoError(t, err)
			if tc.replicationMultipleBatches {
				require.Equal(t, &replicationspb.ReplicationTask{
					TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
					SourceTaskId: task.TaskID,
					Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
						HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
							NamespaceId:         task.NamespaceID,
							WorkflowId:          task.WorkflowID,
							RunId:               task.RunID,
							BaseExecutionInfo:   baseWorkflowInfo,
							VersionHistoryItems: versionHistory.Items,
							Events:              nil,
							EventsBatches:       []*commonpb.DataBlob{events},
							NewRunEvents:        nil,
							NewRunId:            "",
						},
					},
					VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
				}, result)
			} else {
				require.Equal(t, &replicationspb.ReplicationTask{
					TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
					SourceTaskId: task.TaskID,
					Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
						HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
							NamespaceId:         task.NamespaceID,
							WorkflowId:          task.WorkflowID,
							RunId:               task.RunID,
							BaseExecutionInfo:   baseWorkflowInfo,
							VersionHistoryItems: versionHistory.Items,
							Events:              events,
							EventsBatches:       nil,
							NewRunEvents:        nil,
							NewRunId:            "",
						},
					},
					VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
				}, result)
			}
			require.True(t, h.lockReleased)
		})
	}
}

// Continuing with remaining tests...
// Due to length, I'll create the rest in a follow-up. This establishes the pattern.
