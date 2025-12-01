package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type executableVerifyVersionedTransitionTaskTestDeps struct {
	controller              *gomock.Controller
	clusterMetadata         *cluster.MockMetadata
	clientBean              *client.MockBean
	shardController         *shard.MockController
	namespaceCache          *namespace.MockRegistry
	metricsHandler          metrics.Handler
	logger                  log.Logger
	executableTask          *MockExecutableTask
	eagerNamespaceRefresher *MockEagerNamespaceRefresher
	wfcache                 *cache.MockCache
	eventSerializer         serialization.Serializer
	mockExecutionManager    *persistence.MockExecutionManager
	config                  *configs.Config
	sourceClusterName       string
	sourceShardKey          ClusterShardKey
	taskID                  int64
	namespaceID             string
	workflowID              string
	runID                   string
	task                    *ExecutableVerifyVersionedTransitionTask
	newRunID                string
	toolBox                 ProcessToolBox
}

func setupExecutableVerifyVersionedTransitionTaskTest(t *testing.T) *executableVerifyVersionedTransitionTaskTestDeps {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clientBean := client.NewMockBean(controller)
	shardController := shard.NewMockController(controller)
	namespaceCache := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NoopMetricsHandler
	logger := log.NewNoopLogger()
	executableTask := NewMockExecutableTask(controller)
	eventSerializer := serialization.NewSerializer()
	eagerNamespaceRefresher := NewMockEagerNamespaceRefresher(controller)
	wfcache := cache.NewMockCache(controller)
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := "old_run"
	newRunID := "new_run"

	taskID := rand.Int63()

	sourceClusterName := cluster.TestCurrentClusterName
	sourceShardKey := ClusterShardKey{
		ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}
	mockExecutionManager := persistence.NewMockExecutionManager(controller)
	config := tests.NewDynamicConfig()

	taskCreationTime := time.Unix(0, rand.Int63())
	toolBox := ProcessToolBox{
		ClusterMetadata:         clusterMetadata,
		ClientBean:              clientBean,
		ShardController:         shardController,
		NamespaceCache:          namespaceCache,
		MetricsHandler:          metricsHandler,
		Logger:                  logger,
		EventSerializer:         eventSerializer,
		EagerNamespaceRefresher: eagerNamespaceRefresher,
		DLQWriter:               NewExecutionManagerDLQWriter(mockExecutionManager),
		Config:                  config,
		WorkflowCache:           wfcache,
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	task := NewExecutableVerifyVersionedTransitionTask(
		toolBox,
		taskID,
		taskCreationTime,
		sourceClusterName,
		sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = executableTask
	executableTask.EXPECT().TaskID().Return(taskID).AnyTimes()
	executableTask.EXPECT().SourceClusterName().Return(sourceClusterName).AnyTimes()
	executableTask.EXPECT().TaskCreationTime().Return(taskCreationTime).AnyTimes()
	executableTask.EXPECT().GetPriority().Return(enumsspb.TASK_PRIORITY_HIGH).AnyTimes()

	return &executableVerifyVersionedTransitionTaskTestDeps{
		controller:              controller,
		clusterMetadata:         clusterMetadata,
		clientBean:              clientBean,
		shardController:         shardController,
		namespaceCache:          namespaceCache,
		metricsHandler:          metricsHandler,
		logger:                  logger,
		executableTask:          executableTask,
		eagerNamespaceRefresher: eagerNamespaceRefresher,
		wfcache:                 wfcache,
		eventSerializer:         eventSerializer,
		mockExecutionManager:    mockExecutionManager,
		config:                  config,
		sourceClusterName:       sourceClusterName,
		sourceShardKey:          sourceShardKey,
		taskID:                  taskID,
		namespaceID:             namespaceID,
		workflowID:              workflowID,
		runID:                   runID,
		task:                    task,
		newRunID:                newRunID,
		toolBox:                 toolBox,
	}
}

func mockGetMutableState(
	t *testing.T,
	deps *executableVerifyVersionedTransitionTaskTestDeps,
	namespaceId string,
	workflowId string,
	runId string,
	mutableState historyi.MutableState,
	err error,
) {
	shardContext := historyi.NewMockShardContext(deps.controller)
	deps.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(deps.task.NamespaceID),
		deps.task.WorkflowID,
	).Return(shardContext, nil)
	wfCtx := historyi.NewMockWorkflowContext(deps.controller)
	if err == nil {
		wfCtx.EXPECT().LoadMutableState(gomock.Any(), shardContext).Return(mutableState, err)
	}
	deps.wfcache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		shardContext,
		namespace.ID(namespaceId),
		&commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	).Return(wfCtx, func(err error) {}, err)
}

func TestExecute_CurrentBranch_VerifySuccess(t *testing.T) {
	deps := setupExecutableVerifyVersionedTransitionTaskTest(t)
	defer deps.controller.Finish()

	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: deps.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: deps.namespaceID,
				WorkflowId:  deps.workflowID,
				RunId:       deps.runID,
				NextEventId: taskNextEvent,
				NewRunId:    deps.newRunID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	deps.executableTask.EXPECT().TerminalState().Return(false)
	deps.executableTask.EXPECT().ReplicationTask().Times(1).Return(replicationTask)
	deps.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), deps.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	mu := historyi.NewMockMutableState(deps.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	mockGetMutableState(t, deps, deps.namespaceID, deps.workflowID, deps.runID, mu, nil)
	newRunMs := historyi.NewMockMutableState(deps.controller)
	newRunMs.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{}).AnyTimes()
	mockGetMutableState(t, deps, deps.namespaceID, deps.workflowID, deps.newRunID, newRunMs, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		deps.toolBox,
		deps.taskID,
		time.Now(),
		deps.sourceClusterName,
		deps.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = deps.executableTask

	err := task.Execute()
	require.NoError(t, err)
}

func TestExecute_CurrentBranch_NewRunNotFound(t *testing.T) {
	deps := setupExecutableVerifyVersionedTransitionTaskTest(t)
	defer deps.controller.Finish()

	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: deps.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: deps.namespaceID,
				WorkflowId:  deps.workflowID,
				RunId:       deps.runID,
				NextEventId: taskNextEvent,
				NewRunId:    deps.newRunID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	deps.executableTask.EXPECT().TerminalState().Return(false)
	deps.executableTask.EXPECT().ReplicationTask().Times(1).Return(replicationTask).AnyTimes()
	deps.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), deps.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(deps.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	mockGetMutableState(t, deps, deps.namespaceID, deps.workflowID, deps.runID, mu, nil)
	mockGetMutableState(t, deps, deps.namespaceID, deps.workflowID, deps.newRunID, nil, serviceerror.NewNotFound("workflow not found"))
	task := NewExecutableVerifyVersionedTransitionTask(
		deps.toolBox,
		deps.taskID,
		time.Now(),
		deps.sourceClusterName,
		deps.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = deps.executableTask

	err := task.Execute()
	require.IsType(t, &serviceerror.DataLoss{}, err)
}

func TestExecute_CurrentBranch_NotUpToDate(t *testing.T) {
	deps := setupExecutableVerifyVersionedTransitionTaskTest(t)
	defer deps.controller.Finish()

	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: deps.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: deps.namespaceID,
				WorkflowId:  deps.workflowID,
				RunId:       deps.runID,
				NextEventId: taskNextEvent,
				NewRunId:    deps.newRunID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          7,
		},
	}
	deps.executableTask.EXPECT().TerminalState().Return(false)
	deps.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	deps.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), deps.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(deps.controller)
	transitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 1, TransitionCount: 3},
		{NamespaceFailoverVersion: 3, TransitionCount: 6},
	}
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: transitionHistory,
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	mockGetMutableState(t, deps, deps.namespaceID, deps.workflowID, deps.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		deps.toolBox,
		deps.taskID,
		time.Now(),
		deps.sourceClusterName,
		deps.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = deps.executableTask

	err := task.Execute()
	require.IsType(t, &serviceerrors.SyncState{}, err)
	require.Equal(t, transitionHistory[1], err.(*serviceerrors.SyncState).VersionedTransition)
}

func TestExecute_MissingVersionedTransition(t *testing.T) {
	deps := setupExecutableVerifyVersionedTransitionTaskTest(t)
	defer deps.controller.Finish()

	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: deps.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: deps.namespaceID,
				WorkflowId:  deps.workflowID,
				RunId:       deps.runID,
				NextEventId: taskNextEvent,
				NewRunId:    deps.newRunID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          7,
		},
	}
	deps.executableTask.EXPECT().TerminalState().Return(false)
	deps.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	deps.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), deps.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(deps.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: nil,
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	mockGetMutableState(t, deps, deps.namespaceID, deps.workflowID, deps.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		deps.toolBox,
		deps.taskID,
		time.Now(),
		deps.sourceClusterName,
		deps.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = deps.executableTask

	err := task.Execute()
	require.IsType(t, &serviceerrors.SyncState{}, err)
	var expected *persistencespb.VersionedTransition
	require.Equal(t, expected, err.(*serviceerrors.SyncState).VersionedTransition)
}

func TestExecute_NonCurrentBranch_VerifySuccess(t *testing.T) {
	deps := setupExecutableVerifyVersionedTransitionTaskTest(t)
	defer deps.controller.Finish()

	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: deps.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: deps.namespaceID,
				WorkflowId:  deps.workflowID,
				RunId:       deps.runID,
				NextEventId: taskNextEvent,
				NewRunId:    deps.newRunID,
				EventVersionHistory: []*historyspb.VersionHistoryItem{
					{
						EventId: 9,
						Version: 1,
					},
				},
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          4,
		},
	}
	deps.executableTask.EXPECT().TerminalState().Return(false)
	deps.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	deps.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), deps.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(deps.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
				VersionHistories: &historyspb.VersionHistories{
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte{1, 2, 3},
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: 5,
									Version: 1,
								},
								{
									EventId: 10,
									Version: 3,
								},
							},
						},
						{
							BranchToken: []byte{1, 2, 3, 4},
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: 10,
									Version: 1,
								},
							},
						},
					},
				},
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	mockGetMutableState(t, deps, deps.namespaceID, deps.workflowID, deps.runID, mu, nil)
	newRunMs := historyi.NewMockMutableState(deps.controller)
	newRunMs.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{}).AnyTimes()
	mockGetMutableState(t, deps, deps.namespaceID, deps.workflowID, deps.newRunID, newRunMs, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		deps.toolBox,
		deps.taskID,
		time.Now(),
		deps.sourceClusterName,
		deps.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = deps.executableTask

	err := task.Execute()
	require.NoError(t, err)
}

func TestExecute_NonCurrentBranch_NotUpToDate(t *testing.T) {
	deps := setupExecutableVerifyVersionedTransitionTaskTest(t)
	defer deps.controller.Finish()

	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: deps.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: deps.namespaceID,
				WorkflowId:  deps.workflowID,
				RunId:       deps.runID,
				NextEventId: taskNextEvent,
				NewRunId:    deps.newRunID,
				EventVersionHistory: []*historyspb.VersionHistoryItem{
					{
						EventId: 9,
						Version: 1,
					},
				},
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          4,
		},
	}
	deps.executableTask.EXPECT().TerminalState().Return(false)
	deps.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	deps.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), deps.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(deps.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
				VersionHistories: &historyspb.VersionHistories{
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte{1, 2, 3},
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: 8,
									Version: 1,
								},
							},
						},
					},
				},
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	mockGetMutableState(t, deps, deps.namespaceID, deps.workflowID, deps.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		deps.toolBox,
		deps.taskID,
		time.Now(),
		deps.sourceClusterName,
		deps.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = deps.executableTask
	deps.executableTask.EXPECT().BackFillEvents(
		gomock.Any(),
		deps.sourceClusterName,
		deps.task.WorkflowKey,
		int64(9),
		int64(1),
		int64(9),
		int64(1),
		deps.newRunID,
	).Return(nil)

	err := task.Execute()
	require.NoError(t, err)
}

func TestVerifyVersionedTransitionExecute_Skip_TerminalState(t *testing.T) {
	deps := setupExecutableVerifyVersionedTransitionTaskTest(t)
	defer deps.controller.Finish()

	deps.executableTask.EXPECT().TerminalState().Return(true)

	err := deps.task.Execute()
	require.NoError(t, err)
}

func TestVerifyVersionedTransitionExecute_Skip_Namespace(t *testing.T) {
	deps := setupExecutableVerifyVersionedTransitionTaskTest(t)
	defer deps.controller.Finish()

	deps.executableTask.EXPECT().TerminalState().Return(false)
	deps.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), deps.task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := deps.task.Execute()
	require.NoError(t, err)
}

func TestVerifyVersionedTransitionExecute_Err(t *testing.T) {
	deps := setupExecutableVerifyVersionedTransitionTaskTest(t)
	defer deps.controller.Finish()

	deps.executableTask.EXPECT().TerminalState().Return(false)
	err := errors.New("OwO")
	deps.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), deps.task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

	require.Equal(t, err, deps.task.Execute())
}
