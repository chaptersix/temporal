package eventhandler

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type historyPaginatedFetcherTestDeps struct {
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
	fetcher             *HistoryPaginatedFetcherImpl
}

func setupHistoryPaginatedFetcherTest(t *testing.T) *historyPaginatedFetcherTestDeps {
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
	serializer := serialization.NewSerializer()
	fetcher := &HistoryPaginatedFetcherImpl{
		NamespaceRegistry: mockNamespaceCache,
		ClientBean:        mockClientBean,
		Serializer:        serialization.NewSerializer(),
		Logger:            logger,
	}

	return &historyPaginatedFetcherTestDeps{
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
		fetcher:             fetcher,
	}
}

func TestGetSingleWorkflowHistoryIterator(t *testing.T) {
	deps := setupHistoryPaginatedFetcherTest(t)
	defer deps.controller.Finish()

	workflowID := "some random workflow ID"
	runID := uuid.New()
	startEventID := int64(123)
	startEventVersion := int64(100)
	token := []byte{1}
	pageSize := defaultPageSize
	eventBatch := []*historypb.HistoryEvent{
		{
			EventId:   2,
			Version:   123,
			EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		},
		{
			EventId:   3,
			Version:   123,
			EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		},
	}
	blob := serializeEventsForFetcher(t, deps.serializer, eventBatch)
	versionHistoryItems := []*historyspb.VersionHistoryItem{
		{
			EventId: 1,
			Version: 1,
		},
	}

	deps.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: deps.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			StartEventId:      startEventID,
			StartEventVersion: startEventVersion,
			EndEventId:        common.EmptyEventID,
			EndEventVersion:   common.EmptyVersion,
			MaximumPageSize:   pageSize,
			NextPageToken:     nil,
		}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{blob},
		NextPageToken:  token,
		VersionHistory: &historyspb.VersionHistory{
			Items: versionHistoryItems,
		},
	}, nil)

	deps.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(
		gomock.Any(),
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: deps.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			StartEventId:      startEventID,
			StartEventVersion: startEventVersion,
			EndEventId:        common.EmptyEventID,
			EndEventVersion:   common.EmptyVersion,
			MaximumPageSize:   pageSize,
			NextPageToken:     token,
		}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{blob},
		NextPageToken:  nil,
		VersionHistory: &historyspb.VersionHistory{
			Items: versionHistoryItems,
		},
	}, nil)

	fetcher := deps.fetcher.GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		context.Background(),
		cluster.TestCurrentClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		startEventID,
		startEventVersion,
		common.EmptyEventID,
		common.EmptyVersion,
	)
	require.True(t, fetcher.HasNext())
	batch, err := fetcher.Next()
	require.Nil(t, err)
	require.Equal(t, blob, batch.RawEventBatch)

	require.True(t, fetcher.HasNext())
	batch, err = fetcher.Next()
	require.Nil(t, err)
	require.Equal(t, blob, batch.RawEventBatch)

	require.False(t, fetcher.HasNext())
}

func TestGetHistory(t *testing.T) {
	deps := setupHistoryPaginatedFetcherTest(t)
	defer deps.controller.Finish()

	workflowID := "some random workflow ID"
	runID := uuid.New()
	startEventID := int64(123)
	endEventID := int64(345)
	version := int64(20)
	nextTokenIn := []byte("some random next token in")
	nextTokenOut := []byte("some random next token out")
	pageSize := int32(59)
	blob := []byte("some random events blob")

	response := &adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         blob,
		}},
		NextPageToken: nextTokenOut,
	}
	deps.mockAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: deps.namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		StartEventId:      startEventID,
		StartEventVersion: version,
		EndEventId:        endEventID,
		EndEventVersion:   version,
		MaximumPageSize:   pageSize,
		NextPageToken:     nextTokenIn,
	}).Return(response, nil)

	out, token, err := deps.fetcher.getHistory(
		context.Background(),
		cluster.TestCurrentClusterName,
		deps.namespaceID,
		workflowID,
		runID,
		startEventID,
		version,
		endEventID,
		version,
		nextTokenIn,
		pageSize,
		false,
	)
	require.Nil(t, err)
	require.Equal(t, nextTokenOut, token)
	require.Equal(t, blob, out[0].RawEventBatch.Data)
}

func serializeEventsForFetcher(t *testing.T, serializer serialization.Serializer, events []*historypb.HistoryEvent) *commonpb.DataBlob {
	blob, err := serializer.SerializeEvents(events)
	require.Nil(t, err)
	return &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         blob.Data,
	}
}
