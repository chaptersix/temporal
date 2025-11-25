package eventhandler

import (
	"context"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type resendHandlerTestDeps struct {
	controller          *gomock.Controller
	mockClusterMetadata *cluster.MockMetadata
	mockNamespaceCache  *namespace.MockRegistry
	mockClientBean      *client.MockBean
	mockAdminClient     *adminservicemock.MockAdminServiceClient
	mockHistoryClient   *historyservicemock.MockHistoryServiceClient
	namespaceID         namespace.ID
	namespace           namespace.Name
	serializer          serialization.Serializer
	logger              log.Logger
	config              *configs.Config
	resendHandler       ResendHandler
	engine              *historyi.MockEngine
	historyFetcher      *MockHistoryPaginatedFetcher
	importer            *MockEventImporter
}

func setupResendHandlerTest(t *testing.T) *resendHandlerTestDeps {
	controller := gomock.NewController(t)
	mockClusterMetadata := cluster.NewMockMetadata(controller)
	mockClientBean := client.NewMockBean(controller)
	mockAdminClient := adminservicemock.NewMockAdminServiceClient(controller)
	mockHistoryClient := historyservicemock.NewMockHistoryServiceClient(controller)
	mockNamespaceCache := namespace.NewMockRegistry(controller)

	mockClientBean.EXPECT().GetRemoteAdminClient(gomock.Any()).Return(mockAdminClient, nil).AnyTimes()

	logger := log.NewTestLogger()
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	namespaceID := namespace.ID(uuid.New())
	namespaceName := namespace.Name("some random namespace name")
	config := tests.NewDynamicConfig()
	historyFetcher := NewMockHistoryPaginatedFetcher(controller)
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)
	mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(namespaceName).Return(namespaceEntry, nil).AnyTimes()
	engine := historyi.NewMockEngine(controller)
	serializer := serialization.NewSerializer()
	importer := NewMockEventImporter(controller)
	resendHandler := NewResendHandler(
		mockNamespaceCache,
		mockClientBean,
		serializer,
		mockClusterMetadata,
		func(ctx context.Context, namespaceId namespace.ID, workflowId string) (historyi.Engine, error) {
			return engine, nil
		},
		historyFetcher,
		importer,
		logger,
		config,
	)

	return &resendHandlerTestDeps{
		controller:          controller,
		mockClusterMetadata: mockClusterMetadata,
		mockNamespaceCache:  mockNamespaceCache,
		mockClientBean:      mockClientBean,
		mockAdminClient:     mockAdminClient,
		mockHistoryClient:   mockHistoryClient,
		namespaceID:         namespaceID,
		namespace:           namespaceName,
		serializer:          serializer,
		logger:              logger,
		config:              config,
		resendHandler:       resendHandler,
		engine:              engine,
		historyFetcher:      historyFetcher,
		importer:            importer,
	}
}

type historyEventMatrixMatcher struct {
	expected [][]*historypb.HistoryEvent
}

func (m *historyEventMatrixMatcher) Matches(x interface{}) bool {
	actual, ok := x.([][]*historypb.HistoryEvent)
	if !ok {
		return false
	}
	if len(m.expected) != len(actual) {
		return false
	}
	for i := range m.expected {
		if len(m.expected[i]) != len(actual[i]) {
			return false
		}
		for j := range m.expected[i] {
			if m.expected[i][j].EventId != actual[i][j].EventId || m.expected[i][j].Version != actual[i][j].Version {
				return false
			}
		}
	}
	return true
}

func (m *historyEventMatrixMatcher) String() string {
	return "matches history event matrix"
}

// NewHistoryEventMatrixMatcher creates a gomock Matcher for [][]*historypb.HistoryEvent
func NewHistoryEventMatrixMatcher(expected [][]*historypb.HistoryEvent) gomock.Matcher {
	return &historyEventMatrixMatcher{expected: expected}
}

func TestResendHistoryEvents_NoRemoteEvents(t *testing.T) {
	deps := setupResendHandlerTest(t)
	defer deps.controller.Finish()

	workflowID := "some random workflow ID"
	runID := uuid.New()
	endEventID := int64(12)
	endEventVersion := int64(123)
	deps.config.ReplicationResendMaxBatchCount = dynamicconfig.GetIntPropertyFn(2)

	deps.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(123))
	deps.mockClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))
	eventBatch := []*historypb.HistoryEvent{
		{EventId: 1, Version: 123},
		{EventId: 2, Version: 123},
	}
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 123},
		},
	}
	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch), VersionHistory: versionHistory},
		}, nil, nil
	})
	deps.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		int64(12),
		int64(123),
	).Return(fetcher)
	err := deps.resendHandler.ResendHistoryEvents(
		context.Background(),
		cluster.TestAlternativeClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		endEventID,
		endEventVersion,
	)
	require.Error(t, err)
}

func TestSendSingleWorkflowHistory_AllRemoteEvents(t *testing.T) {
	deps := setupResendHandlerTest(t)
	defer deps.controller.Finish()

	workflowID := "some random workflow ID"
	runID := uuid.New()
	endEventID := int64(13)
	endEventVersion := int64(123)
	deps.config.ReplicationResendMaxBatchCount = dynamicconfig.GetIntPropertyFn(2)

	eventBatch0 := []*historypb.HistoryEvent{
		{EventId: 1, Version: 123},
		{EventId: 2, Version: 123},
	}
	eventBatch1 := []*historypb.HistoryEvent{
		{EventId: 3, Version: 123},
		{EventId: 4, Version: 123},
	}
	eventBatch2 := []*historypb.HistoryEvent{
		{EventId: 5, Version: 123},
		{EventId: 6, Version: 123},
	}
	eventBatch3 := []*historypb.HistoryEvent{
		{EventId: 7, Version: 123},
		{EventId: 8, Version: 123},
	}
	eventBatch4 := []*historypb.HistoryEvent{
		{EventId: 9, Version: 123},
		{EventId: 10, Version: 123},
	}
	eventBatch5 := []*historypb.HistoryEvent{
		{EventId: 11, Version: 123},
		{EventId: 12, Version: 123},
	}
	versionHistory0 := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 10, Version: 123},
		},
	}
	versionHistory1 := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 15, Version: 123},
		},
	}
	deps.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	deps.mockClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch0), VersionHistory: versionHistory0},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch1), VersionHistory: versionHistory0},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch2), VersionHistory: versionHistory0},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch3), VersionHistory: versionHistory1},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch4), VersionHistory: versionHistory1},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch5), VersionHistory: versionHistory1},
		}, nil, nil
	})

	deps.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		int64(13),
		int64(123),
	).Return(fetcher)

	workflowKey := definition.WorkflowKey{
		NamespaceID: deps.namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}

	gomock.InOrder(
		deps.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory0.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch0, eventBatch1}),
			nil,
			"",
		).Times(1),
		deps.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory0.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch2}),
			nil,
			"",
		).Times(1),
		deps.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory1.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch3, eventBatch4}),
			nil,
			"",
		).Times(1),
		deps.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory1.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch5}),
			nil,
			"",
		).Times(1),
	)

	err := deps.resendHandler.ResendHistoryEvents(
		context.Background(),
		cluster.TestAlternativeClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		endEventID,
		endEventVersion,
	)
	require.Nil(t, err)
}

func TestSendSingleWorkflowHistory_LocalAndRemoteEvents(t *testing.T) {
	deps := setupResendHandlerTest(t)
	defer deps.controller.Finish()

	workflowID := "some random workflow ID"
	runID := uuid.New()
	endEventID := int64(9)
	endEventVersion := int64(124)
	deps.config.ReplicationResendMaxBatchCount = dynamicconfig.GetIntPropertyFn(2)

	eventBatch0 := []*historypb.HistoryEvent{
		{EventId: 1, Version: 123},
		{EventId: 2, Version: 123},
	}
	eventBatch1 := []*historypb.HistoryEvent{
		{EventId: 3, Version: 123},
		{EventId: 4, Version: 123},
	}
	eventBatch2 := []*historypb.HistoryEvent{
		{EventId: 5, Version: 123},
		{EventId: 6, Version: 123},
	}
	eventBatch3 := []*historypb.HistoryEvent{
		{EventId: 7, Version: 124},
		{EventId: 8, Version: 124},
	}
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 6, Version: 123},
			{EventId: 10, Version: 124},
		},
	}
	deps.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(123))
	deps.mockClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	fetcher0 := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch0), VersionHistory: versionHistory},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch1), VersionHistory: versionHistory},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch2), VersionHistory: versionHistory},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch3), VersionHistory: versionHistory},
		}, nil, nil
	})

	fetcher1 := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch3), VersionHistory: versionHistory},
		}, nil, nil
	})

	deps.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		int64(9),
		int64(124),
	).Return(fetcher0).Times(1)

	deps.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		int64(6),
		int64(123),
		int64(9),
		int64(124),
	).Return(fetcher1).Times(1)

	workflowKey := definition.WorkflowKey{
		NamespaceID: deps.namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}

	deps.importer.EXPECT().ImportHistoryEventsFromBeginning(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		workflowKey,
		int64(6),
		int64(123),
	).Return(nil)
	deps.engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		workflowKey,
		nil,
		versionHistory.Items,
		NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch3}),
		nil,
		"",
	).Times(1)

	err := deps.resendHandler.ResendHistoryEvents(
		context.Background(),
		cluster.TestAlternativeClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		endEventID,
		endEventVersion,
	)
	require.Nil(t, err)
}

func TestSendSingleWorkflowHistory_MixedVersionHistory_RemoteEventsOnly(t *testing.T) {
	deps := setupResendHandlerTest(t)
	defer deps.controller.Finish()

	workflowID := "some random workflow ID"
	runID := uuid.New()
	endEventID := int64(9)
	endEventVersion := int64(124)
	deps.config.ReplicationResendMaxBatchCount = dynamicconfig.GetIntPropertyFn(2)

	eventBatch3 := []*historypb.HistoryEvent{
		{EventId: 7, Version: 124},
		{EventId: 8, Version: 124},
	}
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 6, Version: 123},
			{EventId: 10, Version: 124},
		},
	}
	deps.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(123))
	deps.mockClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	fetcher0 := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch3), VersionHistory: versionHistory},
		}, nil, nil
	})

	deps.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		int64(6),
		int64(123),
		int64(9),
		int64(124),
	).Return(fetcher0).Times(1)

	workflowKey := definition.WorkflowKey{
		NamespaceID: deps.namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}

	deps.engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		workflowKey,
		nil,
		versionHistory.Items,
		NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch3}),
		nil,
		"",
	).Times(1)

	err := deps.resendHandler.ResendHistoryEvents(
		context.Background(),
		cluster.TestAlternativeClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		int64(6),
		int64(123),
		endEventID,
		endEventVersion,
	)
	require.Nil(t, err)
}

func TestSendSingleWorkflowHistory_AllRemoteEvents_BatchTest(t *testing.T) {
	deps := setupResendHandlerTest(t)
	defer deps.controller.Finish()

	workflowID := "some random workflow ID"
	runID := uuid.New()
	endEventID := int64(13)
	endEventVersion := int64(123)
	deps.config.ReplicationResendMaxBatchCount = dynamicconfig.GetIntPropertyFn(10)

	eventBatch0 := []*historypb.HistoryEvent{
		{EventId: 1},
		{EventId: 2},
	}
	eventBatch1 := []*historypb.HistoryEvent{
		{EventId: 3, Version: 123},
		{EventId: 4, Version: 123},
	}
	eventBatch2 := []*historypb.HistoryEvent{
		{EventId: 5, Version: 123},
		{EventId: 6, Version: 123},
	}
	eventBatch3 := []*historypb.HistoryEvent{
		{EventId: 7, Version: 124},
		{EventId: 8, Version: 124},
	}
	eventBatch4 := []*historypb.HistoryEvent{
		{EventId: 9, Version: 124},
		{EventId: 10, Version: 124},
	}
	eventBatch5 := []*historypb.HistoryEvent{
		{EventId: 11, Version: 127},
		{EventId: 12, Version: 127},
	}
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 2},
			{EventId: 6, Version: 123},
			{EventId: 10, Version: 124},
			{EventId: 12, Version: 127},
		},
	}
	deps.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1))
	deps.mockClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1000))

	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]*HistoryBatch, []byte, error) {
		return []*HistoryBatch{
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch0), VersionHistory: versionHistory},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch1), VersionHistory: versionHistory},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch2), VersionHistory: versionHistory},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch3), VersionHistory: versionHistory},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch4), VersionHistory: versionHistory},
			{RawEventBatch: serializeEventsForResend(t, deps.serializer, eventBatch5), VersionHistory: versionHistory},
		}, nil, nil
	})

	deps.historyFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		cluster.TestAlternativeClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		int64(13),
		int64(123),
	).Return(fetcher)

	workflowKey := definition.WorkflowKey{
		NamespaceID: deps.namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}

	gomock.InOrder(
		deps.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch0}),
			nil,
			"",
		).Times(1),
		deps.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch1, eventBatch2}),
			nil,
			"",
		).Times(1),
		deps.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch3, eventBatch4}),
			nil,
			"",
		).Times(1),
		deps.engine.EXPECT().ReplicateHistoryEvents(
			gomock.Any(),
			workflowKey,
			nil,
			versionHistory.Items,
			NewHistoryEventMatrixMatcher([][]*historypb.HistoryEvent{eventBatch5}),
			nil,
			"",
		).Times(1),
	)

	err := deps.resendHandler.ResendHistoryEvents(
		context.Background(),
		cluster.TestAlternativeClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		endEventID,
		endEventVersion,
	)
	require.Nil(t, err)
}

func serializeEventsForResend(t *testing.T, serializer serialization.Serializer, events []*historypb.HistoryEvent) *commonpb.DataBlob {
	blob, err := serializer.SerializeEvents(events)
	require.Nil(t, err)
	return &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         blob.Data,
	}
}
