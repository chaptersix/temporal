package eventhandler

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.uber.org/mock/gomock"
)

type historyEventHandlerTestDeps struct {
	controller          *gomock.Controller
	clusterMetadata     *cluster.MockMetadata
	historyEventHandler *historyEventsHandlerImpl
	shardController     *shard.MockController
	eventImporter       *MockEventImporter
}

func setupHistoryEventHandlerTest(t *testing.T) *historyEventHandlerTestDeps {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	shardController := shard.NewMockController(controller)
	eventImporter := NewMockEventImporter(controller)
	historyEventHandler := &historyEventsHandlerImpl{
		clusterMetadata,
		eventImporter,
		shardController,
		log.NewNoopLogger(),
	}

	return &historyEventHandlerTestDeps{
		controller:          controller,
		clusterMetadata:     clusterMetadata,
		historyEventHandler: historyEventHandler,
		shardController:     shardController,
		eventImporter:       eventImporter,
	}
}

func TestHandleHistoryEvents_RemoteOnly(t *testing.T) {
	deps := setupHistoryEventHandlerTest(t)
	defer deps.controller.Finish()

	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	deps.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	deps.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{1, 0, 1},
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1},
			{EventId: 15, Version: 2},
		},
	}
	historyEvents1 := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	historyEvents2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
		},
		{
			EventId: 14,
		},
	}
	historyEvents := [][]*historypb.HistoryEvent{historyEvents1, historyEvents2}
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}
	shardContext := historyi.NewMockShardContext(deps.controller)
	engine := historyi.NewMockEngine(deps.controller)
	deps.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)
	engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		workflowKey,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
		"",
	).Times(1)

	err := deps.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		nil,
		versionHistory.Items,
		historyEvents,
		nil,
		"",
	)
	require.Nil(t, err)
}

func TestHandleHistoryEvents_LocalAndRemote_HandleLocalThenRemote(t *testing.T) {
	deps := setupHistoryEventHandlerTest(t)
	defer deps.controller.Finish()

	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	deps.clusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes() // current cluster ID is 1
	deps.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000)).AnyTimes()

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 5, Version: 3},
			{EventId: 10, Version: 1001},
			{EventId: 13, Version: 1002},
			{EventId: 15, Version: 1003},
		},
	}
	localHistoryEvents := [][]*historypb.HistoryEvent{
		{
			{EventId: 5, Version: 3},
		},
		{
			{EventId: 6, Version: 1001},
		},
		{
			{EventId: 7, Version: 1001},
			{EventId: 8, Version: 1001},
			{EventId: 9, Version: 1001},
			{EventId: 10, Version: 1001},
		},
	}
	remoteHistoryEvents := [][]*historypb.HistoryEvent{
		{
			{EventId: 11, Version: 2},
			{EventId: 12, Version: 2},
			{EventId: 13, Version: 2},
		},
		{
			{EventId: 14, Version: 1003},
			{EventId: 15, Version: 1003},
		},
	}
	initialHistoryEvents := append(localHistoryEvents, remoteHistoryEvents...)
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}
	shardContext := historyi.NewMockShardContext(deps.controller)
	engine := historyi.NewMockEngine(deps.controller)
	deps.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(2)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(2)
	engine.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
	}).Return(nil, serviceerror.NewNotFound("Mutable state not found")).Times(1)
	deps.eventImporter.EXPECT().ImportHistoryEventsFromBeginning(
		gomock.Any(),
		remoteCluster,
		workflowKey,
		int64(10),
		int64(1001),
	).Return(nil).Times(1)
	engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		workflowKey,
		nil,
		versionHistory.Items,
		remoteHistoryEvents,
		nil,
		"",
	).Times(1)

	err := deps.historyEventHandler.HandleHistoryEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		nil,
		versionHistory.Items,
		initialHistoryEvents,
		nil,
		"",
	)
	require.Nil(t, err)
}

func TestHandleLocalHistoryEvents_AlreadyExist(t *testing.T) {
	deps := setupHistoryEventHandlerTest(t)
	defer deps.controller.Finish()

	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()
	deps.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	deps.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 1},
			{EventId: 15, Version: 2},
		},
	}
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}
	shardContext := historyi.NewMockShardContext(deps.controller)
	engine := historyi.NewMockEngine(deps.controller)
	deps.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)
	mutableState := &historyservice.GetMutableStateResponse{
		VersionHistories: &historyspb.VersionHistories{Histories: []*historyspb.VersionHistory{{
			Items: []*historyspb.VersionHistoryItem{
				{EventId: 10, Version: 1},
			},
		}}},
	}

	engine.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
	}).Return(mutableState, nil).Times(1)

	err := deps.historyEventHandler.handleLocalGeneratedEvent(
		context.Background(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
	)
	require.Nil(t, err)
}

func TestHandleHistoryEvents_LocalOnly_ImportAllLocalAndCommit(t *testing.T) {
	deps := setupHistoryEventHandlerTest(t)
	defer deps.controller.Finish()

	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	deps.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	deps.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 2, Version: 3},
			{EventId: 5, Version: 5},
			{EventId: 7, Version: 1001},
		},
	}

	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}

	shardContext := historyi.NewMockShardContext(deps.controller)
	engine := historyi.NewMockEngine(deps.controller)
	deps.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)

	gomock.InOrder(
		engine.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{
			NamespaceId: namespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runId,
			},
		}).Return(nil, serviceerror.NewNotFound("Mutable state not found")).Times(1),
		deps.eventImporter.EXPECT().ImportHistoryEventsFromBeginning(
			gomock.Any(),
			remoteCluster,
			workflowKey,
			int64(7),
			int64(1001),
		).Return(nil).Times(1),
	)

	err := deps.historyEventHandler.handleLocalGeneratedEvent(
		context.Background(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
	)
	require.Nil(t, err)
}

func TestHandleHistoryEvents_LocalOnly_ExistButNotEnoughEvents_DataLose(t *testing.T) {
	deps := setupHistoryEventHandlerTest(t)
	defer deps.controller.Finish()

	remoteCluster := cluster.TestAlternativeClusterName
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()

	deps.clusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	deps.clusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 2, Version: 3},
			{EventId: 5, Version: 5},
			{EventId: 7, Version: 1001},
		},
	}

	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceId,
		WorkflowID:  workflowId,
		RunID:       runId,
	}

	shardContext := historyi.NewMockShardContext(deps.controller)
	engine := historyi.NewMockEngine(deps.controller)
	deps.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).Times(1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).Times(1)

	gomock.InOrder(
		engine.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{
			NamespaceId: namespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runId,
			},
		}).Return(&historyservice.GetMutableStateResponse{
			VersionHistories: &historyspb.VersionHistories{Histories: []*historyspb.VersionHistory{{
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 6, Version: 1001},
				},
			}}},
		}, nil).Times(1),
	)

	err := deps.historyEventHandler.handleLocalGeneratedEvent(
		context.Background(),
		remoteCluster,
		workflowKey,
		versionHistory.Items,
	)
	require.NotNil(t, err)
}
