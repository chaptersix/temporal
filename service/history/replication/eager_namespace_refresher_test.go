package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
)

const mockCurrentCuster = "current_cluster_1"

func setupEagerNamespaceRefresher(t *testing.T) (*gomock.Controller, *persistence.MockMetadataManager, *namespace.MockRegistry, EagerNamespaceRefresher, log.Logger, *client.MockBean, *nsreplication.MockTaskExecutor, metrics.Handler, *adminservicemock.MockAdminServiceClient) {
	controller := gomock.NewController(t)
	logger := log.NewTestLogger()
	mockMetadataManager := persistence.NewMockMetadataManager(controller)
	mockNamespaceRegistry := namespace.NewMockRegistry(controller)
	clientBean := client.NewMockBean(controller)
	remoteAdminClient := adminservicemock.NewMockAdminServiceClient(controller)
	clientBean.EXPECT().GetRemoteAdminClient(gomock.Any()).Return(remoteAdminClient, nil).AnyTimes()
	scope := tally.NewTestScope("test", nil)
	mockReplicationTaskExecutor := nsreplication.NewMockTaskExecutor(controller)
	mockMetricsHandler := metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope).WithTags(
		metrics.ServiceNameTag("serviceName"))
	eagerNamespaceRefresher := NewEagerNamespaceRefresher(
		mockMetadataManager,
		mockNamespaceRegistry,
		logger,
		clientBean,
		mockReplicationTaskExecutor,
		mockCurrentCuster,
		mockMetricsHandler,
	)

	return controller, mockMetadataManager, mockNamespaceRegistry, eagerNamespaceRefresher, logger, clientBean, mockReplicationTaskExecutor, mockMetricsHandler, remoteAdminClient
}

func TestSyncNamespaceFromSourceCluster_CreateSuccess(t *testing.T) {
	controller, _, mockNamespaceRegistry, eagerNamespaceRefresher, _, _, mockReplicationTaskExecutor, _, remoteAdminClient := setupEagerNamespaceRefresher(t)
	defer controller.Finish()
	namespaceId := namespace.ID("abc")
	nsName := "another-random-namespace-name"
	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespaceId.String(),
			Name:  nsName,
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: mockCurrentCuster},
				{ClusterName: "not_current_cluster_1"},
			},
		},
		IsGlobalNamespace: true,
	}
	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_CREATE,
		Id:                 nsResponse.GetInfo().Id,
		Info:               nsResponse.GetInfo(),
		Config:             nsResponse.GetConfig(),
		ReplicationConfig:  nsResponse.GetReplicationConfig(),
		ConfigVersion:      nsResponse.GetConfigVersion(),
		FailoverVersion:    nsResponse.GetFailoverVersion(),
		FailoverHistory:    nsResponse.GetFailoverHistory(),
	}
	remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil)
	mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task).Return(nil).Times(1)
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespaceId).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).Times(1)
	mockNamespaceRegistry.EXPECT().RefreshNamespaceById(namespaceId).Return(fromAdminClientApiResponse(nsResponse), nil).Times(1)
	ns, err := eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	require.Nil(t, err)
	require.Equal(t, namespaceId, ns.ID())
}

func TestSyncNamespaceFromSourceCluster_UpdateSuccess(t *testing.T) {
	controller, _, mockNamespaceRegistry, eagerNamespaceRefresher, _, _, mockReplicationTaskExecutor, _, remoteAdminClient := setupEagerNamespaceRefresher(t)
	defer controller.Finish()
	namespaceId := namespace.ID("abc")
	nsName := "another-random-namespace-name"
	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespaceId.String(),
			Name:  nsName,
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: mockCurrentCuster},
				{ClusterName: "not_current_cluster_1"},
			},
		},
		IsGlobalNamespace: true,
	}
	remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil)
	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE,
		Id:                 nsResponse.GetInfo().Id,
		Info:               nsResponse.GetInfo(),
		Config:             nsResponse.GetConfig(),
		ReplicationConfig:  nsResponse.GetReplicationConfig(),
		ConfigVersion:      nsResponse.GetConfigVersion(),
		FailoverVersion:    nsResponse.GetFailoverVersion(),
		FailoverHistory:    nsResponse.GetFailoverHistory(),
	}
	mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task).Return(nil).Times(1)
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespaceId).Return(nil, nil).Times(1)
	mockNamespaceRegistry.EXPECT().RefreshNamespaceById(namespaceId).Return(fromAdminClientApiResponse(nsResponse), nil).Times(1)
	ns, err := eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	require.Nil(t, err)
	require.Equal(t, namespaceId, ns.ID())
}

func TestSyncNamespaceFromSourceCluster_NamespaceNotBelongsToCurrentCluster(t *testing.T) {
	controller, _, _, eagerNamespaceRefresher, _, _, _, _, remoteAdminClient := setupEagerNamespaceRefresher(t)
	defer controller.Finish()
	namespaceId := namespace.ID("abc")

	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: "not_current_cluster_1"},
				{ClusterName: "not_current_cluster_2"},
			},
		},
		IsGlobalNamespace: true,
	}
	remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil).Times(1)

	_, err := eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	require.Error(t, err)
	require.IsType(t, &serviceerror.FailedPrecondition{}, err)
}

func TestSyncNamespaceFromSourceCluster_ExecutorReturnsError(t *testing.T) {
	controller, _, mockNamespaceRegistry, eagerNamespaceRefresher, _, _, mockReplicationTaskExecutor, _, remoteAdminClient := setupEagerNamespaceRefresher(t)
	defer controller.Finish()
	namespaceId := namespace.ID("abc")

	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: mockCurrentCuster},
				{ClusterName: "not_current_cluster_2"},
			},
		},
		IsGlobalNamespace: true,
	}
	remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil).Times(1)
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespaceId).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).Times(1)

	expectedError := errors.New("some error")
	mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(expectedError)
	_, err := eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	require.Error(t, err)
	require.Equal(t, expectedError, err)
}

func TestSyncNamespaceFromSourceCluster_NamespaceIsNotGlobalNamespace(t *testing.T) {
	controller, _, _, eagerNamespaceRefresher, _, _, _, _, remoteAdminClient := setupEagerNamespaceRefresher(t)
	defer controller.Finish()
	namespaceId := namespace.ID("abc")

	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		IsGlobalNamespace: false,
	}
	remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil).Times(1)

	_, err := eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	require.Error(t, err)
	require.IsType(t, &serviceerror.FailedPrecondition{}, err)
}

func fromAdminClientApiResponse(response *adminservice.GetNamespaceResponse) *namespace.Namespace {
	info := &persistencespb.NamespaceInfo{
		Id:          response.GetInfo().GetId(),
		Name:        response.GetInfo().GetName(),
		State:       response.GetInfo().GetState(),
		Description: response.GetInfo().GetDescription(),
		Owner:       response.GetInfo().GetOwnerEmail(),
		Data:        response.GetInfo().GetData(),
	}
	config := &persistencespb.NamespaceConfig{
		Retention:                    response.GetConfig().GetWorkflowExecutionRetentionTtl(),
		HistoryArchivalState:         response.GetConfig().GetHistoryArchivalState(),
		HistoryArchivalUri:           response.GetConfig().GetHistoryArchivalUri(),
		VisibilityArchivalState:      response.GetConfig().GetVisibilityArchivalState(),
		VisibilityArchivalUri:        response.GetConfig().GetVisibilityArchivalUri(),
		CustomSearchAttributeAliases: response.GetConfig().GetCustomSearchAttributeAliases(),
	}
	replicationConfig := &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: response.GetReplicationConfig().GetActiveClusterName(),
		State:             response.GetReplicationConfig().GetState(),
		Clusters:          nsreplication.ConvertClusterReplicationConfigFromProto(response.GetReplicationConfig().GetClusters()),
		FailoverHistory:   nsreplication.ConvertFailoverHistoryToPersistenceProto(response.GetFailoverHistory()),
	}

	return namespace.FromPersistentState(
		&persistencespb.NamespaceDetail{
			Info:              info,
			Config:            config,
			ReplicationConfig: replicationConfig,
			ConfigVersion:     response.ConfigVersion,
			FailoverVersion:   response.GetFailoverVersion(),
		},
		namespace.WithGlobalFlag(response.IsGlobalNamespace))
}
