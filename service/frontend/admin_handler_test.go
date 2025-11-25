package frontend

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	clientmocks "go.temporal.io/server/client"
	historyclient "go.temporal.io/server/client/history"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/common/searchattribute"
	serviceerror2 "go.temporal.io/server/common/serviceerror"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/worker/dlq"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/metadata"
)

type (
	adminHandlerTest struct {
		historyrequire.HistoryRequire

		controller         *gomock.Controller
		mockResource       *resourcetest.Test
		mockHistoryClient  *historyservicemock.MockHistoryServiceClient
		mockNamespaceCache *namespace.MockRegistry

		mockExecutionMgr           *persistence.MockExecutionManager
		mockVisibilityMgr          *manager.MockVisibilityManager
		mockClusterMetadataManager *persistence.MockClusterMetadataManager
		mockClientFactory          *clientmocks.MockFactory
		mockAdminClient            *adminservicemock.MockAdminServiceClient
		mockMetadata               *cluster.MockMetadata
		mockProducer               *persistence.MockNamespaceReplicationQueue
		mockMatchingClient         *matchingservicemock.MockMatchingServiceClient
		mockSaMapper               *searchattribute.MockMapper

		namespace      namespace.Name
		namespaceID    namespace.ID
		namespaceEntry *namespace.Namespace

		handler *AdminHandler
	}
)

func setupAdminHandlerTest(t *testing.T) *adminHandlerTest {
	s := &adminHandlerTest{}
	s.HistoryRequire = historyrequire.New(t)

	s.namespace = "some random namespace name"
	s.namespaceID = "deadd0d0-c001-face-d00d-000000000000"
	s.namespaceEntry = namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: s.namespace.String(),
			Id:   s.namespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)

	s.controller = gomock.NewController(t)
	s.mockResource = resourcetest.NewTest(s.controller, primitives.FrontendService)
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockHistoryClient = s.mockResource.HistoryClient
	s.mockExecutionMgr = s.mockResource.ExecutionMgr
	s.mockClusterMetadataManager = s.mockResource.ClusterMetadataMgr
	s.mockClientFactory = s.mockResource.ClientFactory
	s.mockAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)
	s.mockMetadata = s.mockResource.ClusterMetadata
	s.mockVisibilityMgr = s.mockResource.VisibilityManager
	s.mockProducer = persistence.NewMockNamespaceReplicationQueue(s.controller)
	s.mockMatchingClient = s.mockResource.MatchingClient

	mockSaMapperProvider := searchattribute.NewMockMapperProvider(s.controller)
	s.mockSaMapper = searchattribute.NewMockMapper(s.controller)
	mockSaMapperProvider.EXPECT().GetMapper(s.namespace).Return(s.mockSaMapper, nil).AnyTimes()

	persistenceConfig := &config.Persistence{
		NumHistoryShards: 1,
	}

	cfg := &Config{
		NumHistoryShards: 4,

		SearchAttributesNumberOfKeysLimit:     dynamicconfig.GetIntPropertyFnFilteredByNamespace(10),
		SearchAttributesSizeOfValueLimit:      dynamicconfig.GetIntPropertyFnFilteredByNamespace(10),
		SearchAttributesTotalSizeLimit:        dynamicconfig.GetIntPropertyFnFilteredByNamespace(10),
		VisibilityAllowList:                   dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		SuppressErrorSetSystemSearchAttribute: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
	}
	args := NewAdminHandlerArgs{
		persistenceConfig,
		cfg,
		s.mockResource.GetNamespaceReplicationQueue(),
		s.mockProducer,
		s.mockVisibilityMgr,
		s.mockResource.GetLogger(),
		s.mockResource.GetTaskManager(),
		s.mockResource.GetTaskManager(),
		s.mockResource.GetExecutionManager(),
		s.mockResource.GetClusterMetadataManager(),
		s.mockResource.GetMetadataManager(),
		s.mockResource.GetClientFactory(),
		s.mockResource.GetClientBean(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetSDKClientFactory(),
		s.mockResource.GetMembershipMonitor(),
		s.mockResource.GetHostInfoProvider(),
		s.mockResource.GetMetricsHandler(),
		s.mockResource.GetNamespaceRegistry(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetSearchAttributesManager(),
		mockSaMapperProvider,
		s.mockMetadata,
		health.NewServer(),
		serialization.NewSerializer(),
		clock.NewRealTimeSource(),
		tasks.NewDefaultTaskCategoryRegistry(),
		s.mockResource.GetMatchingClient(),
	}
	s.mockMetadata.EXPECT().GetCurrentClusterName().Return(uuid.New()).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetName().Return("mock-execution-manager").AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetStoreNames().Return([]string{"mock-vis-store"})
	s.handler = NewAdminHandler(args)
	s.handler.Start()
	t.Cleanup(func() { s.controller.Finish(); s.handler.Stop() })
	return s
}

func TestAdmin_AddSearchAttributes(t *testing.T) {
	s := setupAdminHandlerTest(t)
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *adminservice.AddSearchAttributesRequest
		Expected error
	}
	// request validation tests
	testCases1 := []test{
		{
			Name:     "nil request",
			Request:  nil,
			Expected: &serviceerror.InvalidArgument{Message: "Request is nil."},
		},
		{
			Name:     "empty request",
			Request:  &adminservice.AddSearchAttributesRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "SearchAttributes are not set on request."},
		},
	}
	for _, testCase := range testCases1 {
		t.Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			require.Equal(t, testCase.Expected, err)
			require.Nil(t, resp)
		})
	}

	// Elasticsearch is not configured
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("").AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestEsNameTypeMap(), nil).AnyTimes()
	testCases3 := []test{
		{
			Name: "reserved key (empty index)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute WorkflowId is reserved by system."},
		},
		{
			Name: "key already whitelisted (empty index)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomTextField": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute CustomTextField already exists."},
		},
	}
	for _, testCase := range testCases3 {
		t.Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			require.Equal(t, testCase.Expected, err)
			require.Nil(t, resp)
		})
	}

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("random-index-name").AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestEsNameTypeMap(), nil).AnyTimes()
	testCases2 := []test{
		{
			Name: "reserved key (ES configured)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute WorkflowId is reserved by system."},
		},
		{
			Name: "key already whitelisted (ES configured)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomTextField": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute CustomTextField already exists."},
		},
	}
	for _, testCase := range testCases2 {
		t.Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			require.Equal(t, testCase.Expected, err)
			require.Nil(t, resp)
		})
	}

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()

	// Start workflow failed.
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-add-search-attributes-workflow", gomock.Any()).Return(nil, errors.New("start failed"))
	resp, err := handler.AddSearchAttributes(ctx, &adminservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	require.Error(t, err)
	require.Equal(t, "Unable to start temporal-sys-add-search-attributes-workflow workflow: start failed.", err.Error())
	require.Nil(t, resp)

	// Workflow failed.
	mockRun := mocksdk.NewMockWorkflowRun(s.controller)
	mockRun.EXPECT().Get(gomock.Any(), nil).Return(errors.New("workflow failed"))
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-add-search-attributes-workflow", gomock.Any()).Return(mockRun, nil)
	resp, err = handler.AddSearchAttributes(ctx, &adminservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	require.Error(t, err)
	require.Equal(t, "Workflow temporal-sys-add-search-attributes-workflow returned an error: workflow failed.", err.Error())
	require.Nil(t, resp)

	// Success case.
	mockRun.EXPECT().Get(gomock.Any(), nil).Return(nil)
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-add-search-attributes-workflow", gomock.Any()).Return(mockRun, nil)

	resp, err = handler.AddSearchAttributes(ctx, &adminservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestAdmin_GetSearchAttributes_EmptyIndexName(t *testing.T) {
	s := setupAdminHandlerTest(t)
	handler := s.handler
	ctx := context.Background()

	resp, err := handler.GetSearchAttributes(ctx, nil)
	require.Error(t, err)
	require.Equal(t, &serviceerror.InvalidArgument{Message: "Request is nil."}, err)
	require.Nil(t, resp)

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(s.namespaceEntry, nil).AnyTimes()

	// Elasticsearch is not configured
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("").AnyTimes()
	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestEsNameTypeMap(), nil).AnyTimes()

	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{Namespace: s.namespace.String()})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestAdmin_GetSearchAttributes_NonEmptyIndexName(t *testing.T) {
	s := setupAdminHandlerTest(t)
	handler := s.handler
	ctx := context.Background()

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("random-index-name").AnyTimes()

	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestEsNameTypeMap(), nil).AnyTimes()
	resp, err := handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)

	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("another-index-name", true).Return(searchattribute.TestEsNameTypeMap(), nil).AnyTimes()
	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{IndexName: "another-index-name"})
	require.NoError(t, err)
	require.NotNil(t, resp)

	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		nil, errors.New("random error"))
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestEsNameTypeMap(), nil).AnyTimes()
	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{Namespace: s.namespace.String()})
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestAdmin_RemoveSearchAttributes_EmptyIndexName(t *testing.T) {
	s := setupAdminHandlerTest(t)
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *adminservice.RemoveSearchAttributesRequest
		Expected error
	}
	// request validation tests
	testCases1 := []test{
		{
			Name:     "nil request",
			Request:  nil,
			Expected: &serviceerror.InvalidArgument{Message: "Request is nil."},
		},
		{
			Name:     "empty request",
			Request:  &adminservice.RemoveSearchAttributesRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "SearchAttributes are not set on request."},
		},
	}
	for _, testCase := range testCases1 {
		t.Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			require.Equal(t, testCase.Expected, err)
			require.Nil(t, resp)
		})
	}

	// Elasticsearch is not configured
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("").AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap(), nil).AnyTimes()
	testCases2 := []test{
		{
			Name: "reserved search attribute (empty index)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"WorkflowId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to remove non-custom search attributes: WorkflowId."},
		},
		{
			Name: "search attribute doesn't exist (empty index)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"ProductId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute ProductId doesn't exist."},
		},
	}
	for _, testCase := range testCases2 {
		t.Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			require.Equal(t, testCase.Expected, err)
			require.Nil(t, resp)
		})
	}
}

func TestAdmin_RemoveSearchAttributes_NonEmptyIndexName(t *testing.T) {
	s := setupAdminHandlerTest(t)
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *adminservice.RemoveSearchAttributesRequest
		Expected error
	}
	testCases := []test{
		{
			Name: "reserved search attribute (ES configured)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"WorkflowId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to remove non-custom search attributes: WorkflowId."},
		},
		{
			Name: "search attribute doesn't exist (ES configured)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"ProductId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute ProductId doesn't exist."},
		},
	}

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("random-index-name").AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestEsNameTypeMap(), nil).AnyTimes()
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			require.Equal(t, testCase.Expected, err)
			require.Nil(t, resp)
		})
	}

	// Success case.
	s.mockResource.SearchAttributesManager.EXPECT().SaveSearchAttributes(gomock.Any(), "random-index-name", gomock.Any()).Return(nil)

	resp, err := handler.RemoveSearchAttributes(ctx, &adminservice.RemoveSearchAttributesRequest{
		SearchAttributes: []string{
			"CustomKeywordField",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestAdmin_RemoveRemoteCluster_Success(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var clusterName = "cluster"
	s.mockClusterMetadataManager.EXPECT().DeleteClusterMetadata(
		gomock.Any(),
		&persistence.DeleteClusterMetadataRequest{ClusterName: clusterName},
	).Return(nil)

	_, err := s.handler.RemoveRemoteCluster(context.Background(), &adminservice.RemoveRemoteClusterRequest{ClusterName: clusterName})
	require.NoError(t, err)
}

func TestAdmin_RemoveRemoteCluster_Error(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var clusterName = "cluster"
	s.mockClusterMetadataManager.EXPECT().DeleteClusterMetadata(
		gomock.Any(),
		&persistence.DeleteClusterMetadataRequest{ClusterName: clusterName},
	).Return(fmt.Errorf("test error"))

	_, err := s.handler.RemoveRemoteCluster(context.Background(), &adminservice.RemoveRemoteClusterRequest{ClusterName: clusterName})
	require.Error(t, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_RecordFound_Success(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()
	var FrontendHttpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()
	var recordVersion int64 = 5

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			Version: recordVersion,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	require.NoError(t, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_RecordNotFound_Success(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()
	var FrontendHttpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			HttpAddress:              FrontendHttpAddress,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	require.NoError(t, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_ValidationError_ClusterNameConflict(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()
	var clusterId = uuid.New()

	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              s.mockMetadata.GetCurrentClusterName(),
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_ValidationError_FailoverVersionIncrementMismatch(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_ValidationError_ShardCount_Invalid(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        5,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_ShardCount_Multiple(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()
	var recordVersion int64 = 5

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        16,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			Version: recordVersion,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        16,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	require.NoError(t, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_ValidationError_GlobalNamespaceDisabled(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: false,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_ValidationError_InitialFailoverVersionConflict(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		uuid.New(): {InitialFailoverVersion: 0},
	})
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_DescribeCluster_Error(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()

	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		nil,
		fmt.Errorf("test error"),
	)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	require.Error(t, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_GetClusterMetadata_Error(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		fmt.Errorf("test error"),
	)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	require.Error(t, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_SaveClusterMetadata_Error(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()
	var FrontendHttpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, fmt.Errorf("test error"))
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	require.Error(t, err)
}

func TestAdmin_AddOrUpdateRemoteCluster_SaveClusterMetadata_NotApplied_Error(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var rpcAddress = uuid.New()
	var FrontendHttpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestAdmin_DescribeCluster_CurrentCluster_Success(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var clusterId = uuid.New()
	clusterName := s.mockMetadata.GetCurrentClusterName()
	s.mockResource.HostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("test"))
	s.mockResource.MembershipMonitor.EXPECT().GetReachableMembers().Return(nil, nil)
	s.mockResource.HistoryServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.HistoryServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.FrontendServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.FrontendServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.MatchingServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.MatchingServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.WorkerServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.WorkerServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockVisibilityMgr.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName})
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			ClusterMetadata: &persistencespb.ClusterMetadata{
				ClusterName:              clusterName,
				HistoryShardCount:        0,
				ClusterId:                clusterId,
				FailoverVersionIncrement: 0,
				InitialFailoverVersion:   0,
				IsGlobalNamespaceEnabled: true,
			},
			Version: 1,
		}, nil)

	resp, err := s.handler.DescribeCluster(context.Background(), &adminservice.DescribeClusterRequest{})
	require.NoError(t, err)
	require.Equal(t, resp.GetClusterName(), clusterName)
	require.Equal(t, resp.GetClusterId(), clusterId)
	require.Equal(t, resp.GetHistoryShardCount(), int32(0))
	require.Equal(t, resp.GetFailoverVersionIncrement(), int64(0))
	require.Equal(t, resp.GetInitialFailoverVersion(), int64(0))
	require.True(t, resp.GetIsGlobalNamespaceEnabled())
}

func TestAdmin_DescribeCluster_NonCurrentCluster_Success(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.HostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("test"))
	s.mockResource.MembershipMonitor.EXPECT().GetReachableMembers().Return(nil, nil)
	s.mockResource.HistoryServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.HistoryServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.FrontendServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.FrontendServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.MatchingServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.MatchingServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.WorkerServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.WorkerServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockVisibilityMgr.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName})
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			ClusterMetadata: &persistencespb.ClusterMetadata{
				ClusterName:              clusterName,
				HistoryShardCount:        0,
				ClusterId:                clusterId,
				FailoverVersionIncrement: 0,
				InitialFailoverVersion:   0,
				IsGlobalNamespaceEnabled: true,
			},
			Version: 1,
		}, nil)

	resp, err := s.handler.DescribeCluster(context.Background(), &adminservice.DescribeClusterRequest{ClusterName: clusterName})
	require.NoError(t, err)
	require.Equal(t, resp.GetClusterName(), clusterName)
	require.Equal(t, resp.GetClusterId(), clusterId)
	require.Equal(t, resp.GetHistoryShardCount(), int32(0))
	require.Equal(t, resp.GetFailoverVersionIncrement(), int64(0))
	require.Equal(t, resp.GetInitialFailoverVersion(), int64(0))
	require.True(t, resp.GetIsGlobalNamespaceEnabled())
}

func TestAdmin_ListClusters_Success(t *testing.T) {
	s := setupAdminHandlerTest(t)
	var pageSize int32 = 1

	s.mockClusterMetadataManager.EXPECT().ListClusterMetadata(gomock.Any(), &persistence.ListClusterMetadataRequest{
		PageSize: int(pageSize),
	}).Return(
		&persistence.ListClusterMetadataResponse{
			ClusterMetadata: []*persistence.GetClusterMetadataResponse{
				{
					ClusterMetadata: &persistencespb.ClusterMetadata{ClusterName: "test"},
				},
			}}, nil)

	resp, err := s.handler.ListClusters(context.Background(), &adminservice.ListClustersRequest{
		PageSize: pageSize,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.Clusters))
	require.Equal(t, 0, len(resp.GetNextPageToken()))
}

func TestStreamWorkflowReplicationMessages_ClientToServerBroken(t *testing.T) {
	s := setupAdminHandlerTest(t)
	clientClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	serverClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	clusterShardMD := historyclient.EncodeClusterShardMD(
		clientClusterShardID,
		serverClusterShardID,
	)
	ctx := metadata.NewIncomingContext(context.Background(), clusterShardMD)
	clientCluster := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesServer(s.controller)
	clientCluster.EXPECT().Context().Return(ctx).AnyTimes()
	serverCluster := historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesClient(s.controller)
	s.mockHistoryClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(serverCluster, nil)
	serverCluster.EXPECT().CloseSend().AnyTimes()

	waitGroupStart := sync.WaitGroup{}
	waitGroupStart.Add(2)
	waitGroupEnd := sync.WaitGroup{}
	waitGroupEnd.Add(2)
	channel := make(chan struct{})

	clientCluster.EXPECT().Recv().DoAndReturn(func() (*adminservice.StreamWorkflowReplicationMessagesRequest, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		return nil, serviceerror.NewUnavailable("random error")
	})
	serverCluster.EXPECT().Recv().DoAndReturn(func() (*historyservice.StreamWorkflowReplicationMessagesResponse, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		<-channel
		return nil, serviceerror.NewInternal("random error")
	})
	_ = s.handler.StreamWorkflowReplicationMessages(clientCluster)
	close(channel)
	waitGroupEnd.Wait()
}

func TestStreamWorkflowReplicationMessages_ServerToClientBroken(t *testing.T) {
	s := setupAdminHandlerTest(t)
	clientClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	serverClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	clusterShardMD := historyclient.EncodeClusterShardMD(
		clientClusterShardID,
		serverClusterShardID,
	)
	ctx := metadata.NewIncomingContext(context.Background(), clusterShardMD)
	clientCluster := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesServer(s.controller)
	clientCluster.EXPECT().Context().Return(ctx).AnyTimes()
	serverCluster := historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesClient(s.controller)
	s.mockHistoryClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(serverCluster, nil)
	serverCluster.EXPECT().CloseSend().AnyTimes()

	waitGroupStart := sync.WaitGroup{}
	waitGroupStart.Add(2)
	waitGroupEnd := sync.WaitGroup{}
	waitGroupEnd.Add(2)
	channel := make(chan struct{})

	clientCluster.EXPECT().Recv().DoAndReturn(func() (*adminservice.StreamWorkflowReplicationMessagesRequest, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		<-channel
		return nil, serviceerror.NewUnavailable("random error")
	})

	s.mockHistoryClient.EXPECT().DescribeHistoryHost(gomock.Any(), &historyservice.DescribeHistoryHostRequest{ShardId: serverClusterShardID.ShardID}).Return(&historyservice.DescribeHistoryHostResponse{}, nil)
	serverCluster.EXPECT().Recv().DoAndReturn(func() (*historyservice.StreamWorkflowReplicationMessagesResponse, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		return nil, serviceerror2.NewShardOwnershipLost("host1", "host2")
	})
	_ = s.handler.StreamWorkflowReplicationMessages(clientCluster)
	close(channel)
	waitGroupEnd.Wait()
}

func TestGetNamespace_WithIDSuccess(t *testing.T) {
	s := setupAdminHandlerTest(t)
	namespaceID := "someId"
	nsResponse := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			FailoverVersion: 1,
			Info: &persistencespb.NamespaceInfo{
				Id:    namespaceID,
				Name:  "another random namespace name",
				State: enumspb.NAMESPACE_STATE_DELETED,
				Data:  make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
	}
	s.mockResource.MetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: namespaceID,
	}).Return(nsResponse, nil)
	resp, err := s.handler.GetNamespace(context.Background(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceID,
		},
	})
	require.NoError(t, err)
	require.Equal(t, namespaceID, resp.GetInfo().GetId())
}

func TestGetNamespace_WithNameSuccess(t *testing.T) {
	s := setupAdminHandlerTest(t)
	namespaceName := "some name"
	namespaceId := "some id"
	nsResponse := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			FailoverVersion: 1,
			Info: &persistencespb.NamespaceInfo{
				Id:    namespaceId,
				Name:  namespaceName,
				State: enumspb.NAMESPACE_STATE_DELETED,
				Data:  make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
	}
	s.mockResource.MetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: namespaceName,
	}).Return(nsResponse, nil)
	resp, err := s.handler.GetNamespace(context.Background(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Namespace{
			Namespace: namespaceName,
		},
	})
	require.NoError(t, err)
	require.Equal(t, namespaceId, resp.GetInfo().GetId())
	require.Equal(t, namespaceName, resp.GetInfo().GetName())
	require.Equal(t, cluster.TestAlternativeClusterName, resp.GetReplicationConfig().GetActiveClusterName())
}

func TestGetNamespace_EmptyRequest(t *testing.T) {
	s := setupAdminHandlerTest(t)
	v := &adminservice.GetNamespaceRequest{}
	_, err := s.handler.GetNamespace(context.Background(), v)
	require.Equal(t, errRequestNotSet, err)
}

func TestGetDLQTasks(t *testing.T) {
	s := setupAdminHandlerTest(t)
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "success",
			err:  nil,
		},
		{
			name: "failed to get dlq tasks",
			err:  serviceerror.NewNotFound("failed to get dlq tasks"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blob := &commonpb.DataBlob{}
			expectation := s.mockHistoryClient.EXPECT().GetDLQTasks(gomock.Any(), &historyservice.GetDLQTasksRequest{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  int32(tasks.CategoryTransfer.ID()),
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				PageSize:      1,
				NextPageToken: []byte{13},
			})
			if tc.err != nil {
				expectation.Return(nil, tc.err)
			} else {
				expectation.Return(&historyservice.GetDLQTasksResponse{
					DlqTasks: []*commonspb.HistoryDLQTask{
						{
							Metadata: &commonspb.HistoryDLQTaskMetadata{
								MessageId: 21,
							},
							Payload: &commonspb.HistoryTask{
								ShardId: 34,
								Blob:    blob,
							},
						},
					},
					NextPageToken: []byte{55},
				}, nil)
			}
			response, err := s.handler.GetDLQTasks(context.Background(), &adminservice.GetDLQTasksRequest{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  int32(tasks.CategoryTransfer.ID()),
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				PageSize:      1,
				NextPageToken: []byte{13},
			})
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, &adminservice.GetDLQTasksResponse{
				DlqTasks: []*commonspb.HistoryDLQTask{
					{
						Metadata: &commonspb.HistoryDLQTaskMetadata{
							MessageId: 21,
						},
						Payload: &commonspb.HistoryTask{
							ShardId: 34,
							Blob:    blob,
						},
					},
				},
				NextPageToken: []byte{55},
			}, response)
		})
	}
}

func TestPurgeDLQTasks(t *testing.T) {
	s := setupAdminHandlerTest(t)
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "Success",
			err:  nil,
		},
		{
			name: "WorkflowExecutionFailed",
			err:  serviceerror.NewNotFound("example sdk workflow start failure"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockSdkClient := mocksdk.NewMockClient(s.controller)
			s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient)
			expectation := mockSdkClient.EXPECT().ExecuteWorkflow(
				gomock.Any(),
				gomock.Any(),
				dlq.WorkflowName,
				dlq.WorkflowParams{
					WorkflowType: dlq.WorkflowTypeDelete,
					DeleteParams: dlq.DeleteParams{
						Key: dlq.Key{
							TaskCategoryID: tasks.CategoryTransfer.ID(),
							SourceCluster:  "test-source-cluster",
							TargetCluster:  "test-target-cluster",
						},
						MaxMessageID: 42,
					},
				},
			)
			if tc.err != nil {
				expectation.Return(nil, tc.err)
			} else {
				run := mocksdk.NewMockWorkflowRun(s.controller)
				run.EXPECT().GetRunID().Return("test-run-id")
				expectation.Return(run, nil)
			}
			response, err := s.handler.PurgeDLQTasks(context.Background(), &adminservice.PurgeDLQTasksRequest{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  int32(tasks.CategoryTransfer.ID()),
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
					MessageId: 42,
				},
			})
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, response)
			var token adminservice.DLQJobToken
			err = token.Unmarshal(response.JobToken)
			require.NoError(t, err)
			require.Equal(t, "manage-dlq-tasks-1_test-source-cluster_test-target-cluster_aG2oua8T", token.WorkflowId)
			require.Equal(t, "test-run-id", token.RunId)
		})
	}
}

func TestPurgeDLQTasks_ClusterNotSet(t *testing.T) {
	s := setupAdminHandlerTest(t)
	_, err := s.handler.PurgeDLQTasks(context.Background(), &adminservice.PurgeDLQTasksRequest{
		DlqKey: &commonspb.HistoryDLQKey{
			TaskCategory:  1,
			SourceCluster: "",
			TargetCluster: "test-target-cluster",
		},
		InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
			MessageId: 42,
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	require.ErrorContains(t, err, errSourceClusterNotSet.Error())
}

func TestDescribeDLQJob(t *testing.T) {
	s := setupAdminHandlerTest(t)
	workflowID := "test-workflow-id"
	runID := "test-run-id"
	defaultMergeQueryResponse := dlq.ProgressQueryResponse{
		MaxMessageIDToProcess:  0,
		LastProcessedMessageID: 0,
		WorkflowType:           dlq.WorkflowTypeMerge,
		DlqKey: dlq.Key{
			TaskCategoryID: 1,
			SourceCluster:  "test-source-cluster",
			TargetCluster:  "test-target-cluster",
		},
	}
	defaultPurgeQueryResponse := dlq.ProgressQueryResponse{
		MaxMessageIDToProcess:  0,
		LastProcessedMessageID: 0,
		WorkflowType:           dlq.WorkflowTypeDelete,
		DlqKey: dlq.Key{
			TaskCategoryID: 1,
			SourceCluster:  "test-source-cluster",
			TargetCluster:  "test-target-cluster",
		},
	}
	defaultWorkflowExecution := workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}
	for _, tc := range []struct {
		name                  string
		err                   error
		progressQueryResponse dlq.ProgressQueryResponse
		workflowExecution     *workflowservice.DescribeWorkflowExecutionResponse
		expectedResponse      *adminservice.DescribeDLQJobResponse
	}{
		{
			name:                  "MergeRunning",
			err:                   nil,
			progressQueryResponse: defaultMergeQueryResponse,
			workflowExecution:     &defaultWorkflowExecution,
			expectedResponse: &adminservice.DescribeDLQJobResponse{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  1,
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				OperationType:          enumsspb.DLQ_OPERATION_TYPE_MERGE,
				OperationState:         enumsspb.DLQ_OPERATION_STATE_RUNNING,
				MaxMessageId:           0,
				LastProcessedMessageId: 0,
			},
		},
		{
			name:                  "MergeFinished",
			err:                   nil,
			progressQueryResponse: defaultMergeQueryResponse,
			workflowExecution: &workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				},
			},
			expectedResponse: &adminservice.DescribeDLQJobResponse{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  1,
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				OperationType:          enumsspb.DLQ_OPERATION_TYPE_MERGE,
				OperationState:         enumsspb.DLQ_OPERATION_STATE_COMPLETED,
				MaxMessageId:           0,
				LastProcessedMessageId: 0,
			},
		},
		{
			name:                  "MergeFailed",
			err:                   nil,
			progressQueryResponse: defaultMergeQueryResponse,
			workflowExecution: &workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
				},
			},
			expectedResponse: &adminservice.DescribeDLQJobResponse{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  1,
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				OperationType:          enumsspb.DLQ_OPERATION_TYPE_MERGE,
				OperationState:         enumsspb.DLQ_OPERATION_STATE_FAILED,
				MaxMessageId:           0,
				LastProcessedMessageId: 0,
			},
		},
		{
			name:                  "DeleteRunning",
			err:                   nil,
			progressQueryResponse: defaultPurgeQueryResponse,
			workflowExecution:     &defaultWorkflowExecution,
			expectedResponse: &adminservice.DescribeDLQJobResponse{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  1,
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				OperationType:          enumsspb.DLQ_OPERATION_TYPE_PURGE,
				OperationState:         enumsspb.DLQ_OPERATION_STATE_RUNNING,
				MaxMessageId:           0,
				LastProcessedMessageId: 0,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jobToken := adminservice.DLQJobToken{
				WorkflowId: workflowID,
				RunId:      runID,
			}
			mockSdkClient := mocksdk.NewMockClient(s.controller)
			s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient)
			describeExpectation := mockSdkClient.EXPECT().DescribeWorkflowExecution(
				gomock.Any(),
				workflowID,
				runID,
			)
			queryExpectation := mockSdkClient.EXPECT().QueryWorkflow(
				gomock.Any(),
				workflowID,
				runID,
				dlq.QueryTypeProgress,
			)
			mockValue := mocksdk.NewMockEncodedValue(s.controller)
			mockValue.EXPECT().Get(gomock.Any()).Do(func(result interface{}) {
				*(result.(*dlq.ProgressQueryResponse)) = tc.progressQueryResponse
			})
			queryExpectation.Return(mockValue, nil)
			if tc.err != nil {
				describeExpectation.Return(nil, tc.err)
			} else {
				describeExpectation.Return(tc.workflowExecution, nil)
			}
			jobTokenBytes, _ := jobToken.Marshal()
			response, err := s.handler.DescribeDLQJob(context.Background(), &adminservice.DescribeDLQJobRequest{
				JobToken: jobTokenBytes,
			})
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, response)
			protoassert.ProtoEqual(t, tc.expectedResponse, response)
		})
	}
}

func TestDescribeDLQJob_InvalidJobToken(t *testing.T) {
	s := setupAdminHandlerTest(t)
	_, err := s.handler.DescribeDLQJob(context.Background(), &adminservice.DescribeDLQJobRequest{JobToken: []byte("invalid_token")})
	require.Error(t, err)
	require.ErrorContains(t, err, "Invalid DLQ job token")

}

func TestCancelDLQJob(t *testing.T) {
	s := setupAdminHandlerTest(t)
	for _, tc := range []struct {
		name              string
		terminateErr      error
		describeErr       error
		workflowExecution *workflowservice.DescribeWorkflowExecutionResponse
		terminateCalls    int
		expectedCancelled bool
	}{
		{
			name:         "SuccessForRunningWorkflow",
			terminateErr: nil,
			describeErr:  nil,
			workflowExecution: &workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
			terminateCalls:    1,
			expectedCancelled: true,
		},
		{
			name:         "SuccessForCompletedWorkflow",
			terminateErr: nil,
			describeErr:  nil,
			workflowExecution: &workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				},
			},
			terminateCalls:    0,
			expectedCancelled: false,
		},
		{
			name:         "TerminateWorkflowFailed",
			terminateErr: serviceerror.NewNotFound("example sdk terminate workflow failure"),
			describeErr:  nil,
			workflowExecution: &workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
			terminateCalls:    1,
			expectedCancelled: false,
		},
		{
			name:         "DescribeWorkflowFailed",
			terminateErr: nil,
			describeErr:  serviceerror.NewNotFound("example sdk describe workflow failure"),
			workflowExecution: &workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
			terminateCalls:    0,
			expectedCancelled: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workflowID := "test-workflow-id"
			runID := "test-run-id"
			jobToken := adminservice.DLQJobToken{
				WorkflowId: workflowID,
				RunId:      runID,
			}
			mockSdkClient := mocksdk.NewMockClient(s.controller)
			s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient)
			describeExpectation := mockSdkClient.EXPECT().DescribeWorkflowExecution(
				gomock.Any(),
				workflowID,
				runID,
			)
			terminateExpectation := mockSdkClient.EXPECT().TerminateWorkflow(
				gomock.Any(),
				workflowID,
				runID,
				"test-reason",
			)
			terminateExpectation.Return(tc.terminateErr).Times(tc.terminateCalls)
			if tc.describeErr != nil {
				describeExpectation.Return(nil, tc.describeErr)
			} else {
				describeExpectation.Return(tc.workflowExecution, nil)
			}
			jobTokenBytes, _ := jobToken.Marshal()
			response, err := s.handler.CancelDLQJob(context.Background(), &adminservice.CancelDLQJobRequest{
				JobToken: jobTokenBytes,
				Reason:   "test-reason",
			})
			if tc.describeErr != nil {
				require.ErrorIs(t, err, tc.describeErr)
				return
			}
			if tc.terminateErr != nil {
				require.ErrorIs(t, err, tc.terminateErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, response)
			require.Equal(t, tc.expectedCancelled, response.Canceled)
		})
	}
}

func TestCancelDLQJob_InvalidJobToken(t *testing.T) {
	s := setupAdminHandlerTest(t)
	_, err := s.handler.CancelDLQJob(context.Background(), &adminservice.CancelDLQJobRequest{JobToken: []byte("invalid_token"), Reason: "test-reason"})
	require.Error(t, err)
	require.ErrorContains(t, err, "Invalid DLQ job token")
}

func TestAddDLQTasks_Ok(t *testing.T) {
	s := setupAdminHandlerTest(t)
	s.mockHistoryClient.EXPECT().AddTasks(gomock.Any(), &historyservice.AddTasksRequest{
		ShardId: 13,
		Tasks: []*historyservice.AddTasksRequest_Task{
			{
				CategoryId: 21,
				Blob: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         []byte("test-data"),
				},
			},
		},
	}).Return(nil, nil)
	_, err := s.handler.AddTasks(context.Background(), &adminservice.AddTasksRequest{
		ShardId: 13,
		Tasks: []*adminservice.AddTasksRequest_Task{
			{
				CategoryId: 21,
				Blob: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         []byte("test-data"),
				},
			},
		},
	})
	require.NoError(t, err)
}

func TestAddDLQTasks_Err(t *testing.T) {
	s := setupAdminHandlerTest(t)
	errTest := errors.New("some error")
	s.mockHistoryClient.EXPECT().AddTasks(gomock.Any(), gomock.Any()).Return(nil, errTest)
	_, err := s.handler.AddTasks(context.Background(), &adminservice.AddTasksRequest{})
	require.ErrorIs(t, err, errTest)
}

func TestListQueues_Ok(t *testing.T) {
	s := setupAdminHandlerTest(t)
	s.mockHistoryClient.EXPECT().ListQueues(gomock.Any(), &historyservice.ListQueuesRequest{
		QueueType:     int32(persistence.QueueTypeHistoryDLQ),
		PageSize:      0,
		NextPageToken: nil,
	}).Return(&historyservice.ListQueuesResponse{
		Queues: []*historyservice.ListQueuesResponse_QueueInfo{
			{
				QueueName:    "testQueue",
				MessageCount: 100,
			},
		},
	}, nil)
	resp, err := s.handler.ListQueues(context.Background(), &adminservice.ListQueuesRequest{
		QueueType:     int32(persistence.QueueTypeHistoryDLQ),
		PageSize:      0,
		NextPageToken: nil,
	})
	require.NoError(t, err)
	require.Equal(t, "testQueue", resp.Queues[0].QueueName)
	require.Equal(t, int64(100), resp.Queues[0].MessageCount)

}

func TestListQueues_Err(t *testing.T) {
	s := setupAdminHandlerTest(t)
	errTest := errors.New("some error")
	s.mockHistoryClient.EXPECT().ListQueues(gomock.Any(), gomock.Any()).Return(nil, errTest)
	_, err := s.handler.ListQueues(context.Background(), &adminservice.ListQueuesRequest{})
	require.ErrorIs(t, err, errTest)
}

func TestForceUnloadTaskQueuePartition(t *testing.T) {
	s := setupAdminHandlerTest(t)
	handler := s.handler
	ctx := context.Background()
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(s.namespaceID, nil).AnyTimes()

	type test struct {
		Name     string
		Request  *adminservice.ForceUnloadTaskQueuePartitionRequest
		Expected error
	}
	// request validation tests
	errorCases := []test{
		{
			Name:     "nil request",
			Request:  nil,
			Expected: &serviceerror.InvalidArgument{Message: "Request is nil."},
		},
		{
			Name:     "empty request",
			Request:  &adminservice.ForceUnloadTaskQueuePartitionRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "Namespace is not set on request."},
		},
	}
	for _, test := range errorCases {
		t.Run(test.Name, func(t *testing.T) {
			resp, err := handler.ForceUnloadTaskQueuePartition(ctx, test.Request)
			require.Equal(t, test.Expected, err)
			require.Nil(t, resp)
		})
	}

	// valid request
	tqPartitionRequest := &taskqueuespb.TaskQueuePartition{
		TaskQueue:     "hello-world",
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: 0},
	}

	// request-response structures for mocking matching
	matchingMockRequest := &matchingservice.ForceUnloadTaskQueuePartitionRequest{
		NamespaceId:        s.namespaceID.String(),
		TaskQueuePartition: tqPartitionRequest,
	}
	matchingMockResponse := &matchingservice.ForceUnloadTaskQueuePartitionResponse{
		WasLoaded: true,
	}
	s.mockMatchingClient.EXPECT().ForceUnloadTaskQueuePartition(ctx, matchingMockRequest).Return(matchingMockResponse, nil).Times(1)

	resp, err := handler.ForceUnloadTaskQueuePartition(ctx, &adminservice.ForceUnloadTaskQueuePartitionRequest{
		Namespace:          s.namespace.String(),
		TaskQueuePartition: tqPartitionRequest,
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.WasLoaded)
}

func TestDescribeTaskQueuePartition(t *testing.T) {
	s := setupAdminHandlerTest(t)
	handler := s.handler
	ctx := context.Background()
	unversioned := " "
	buildID := "blx"

	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(s.namespaceID, nil).AnyTimes()

	type test struct {
		Name     string
		Request  *adminservice.DescribeTaskQueuePartitionRequest
		Expected error
	}
	// request validation tests
	errorCases := []test{
		{
			Name:     "nil request",
			Request:  nil,
			Expected: &serviceerror.InvalidArgument{Message: "Request is nil."},
		},
		{
			Name:     "empty request",
			Request:  &adminservice.DescribeTaskQueuePartitionRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "Namespace is not set on request."},
		},
	}
	for _, test := range errorCases {
		t.Run(test.Name, func(t *testing.T) {
			resp, err := handler.DescribeTaskQueuePartition(ctx, test.Request)
			require.Equal(t, test.Expected, err)
			require.Nil(t, resp)
		})
	}

	// request on a partition with buildIds
	tqPartitionRequest := &taskqueuespb.TaskQueuePartition{
		TaskQueue:     "hello-world",
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: 0},
	}
	buildIdRequest := &taskqueuepb.TaskQueueVersionSelection{
		BuildIds:    []string{unversioned, buildID},
		Unversioned: true,
		AllActive:   true,
	}

	// request-response structures for mocking matching
	matchingMockRequest := &matchingservice.DescribeTaskQueuePartitionRequest{
		NamespaceId:                   s.namespaceID.String(),
		TaskQueuePartition:            tqPartitionRequest,
		Versions:                      buildIdRequest,
		ReportStats:                   true,
		ReportPollers:                 true,
		ReportInternalTaskQueueStatus: true,
	}
	unversionedPhysicalTaskQueueInfo := &taskqueuespb.PhysicalTaskQueueInfo{
		Pollers: []*taskqueuepb.PollerInfo(nil),
		TaskQueueStats: &taskqueuepb.TaskQueueStats{
			ApproximateBacklogCount: 0,
			ApproximateBacklogAge:   nil,
			TasksAddRate:            0,
			TasksDispatchRate:       0,
		},
		InternalTaskQueueStatus: []*taskqueuespb.InternalTaskQueueStatus{&taskqueuespb.InternalTaskQueueStatus{
			ReadLevel: 0,
			AckLevel:  0,
			TaskIdBlock: &taskqueuepb.TaskIdBlock{
				StartId: 0,
				EndId:   0,
			},
			LoadedTasks: 0,
		}},
	}
	versionedPhysicalTaskQueueInfo := &taskqueuespb.PhysicalTaskQueueInfo{
		Pollers: []*taskqueuepb.PollerInfo(nil),
		TaskQueueStats: &taskqueuepb.TaskQueueStats{
			ApproximateBacklogCount: 100,
			ApproximateBacklogAge:   nil,
			TasksAddRate:            10.21,
			TasksDispatchRate:       10.50,
		},
		InternalTaskQueueStatus: []*taskqueuespb.InternalTaskQueueStatus{&taskqueuespb.InternalTaskQueueStatus{
			ReadLevel: 1,
			AckLevel:  1,
			TaskIdBlock: &taskqueuepb.TaskIdBlock{
				StartId: 1,
				EndId:   1000,
			},
			LoadedTasks: 10,
		}},
	}

	matchingMockResponse := &matchingservice.DescribeTaskQueuePartitionResponse{
		VersionsInfoInternal: map[string]*taskqueuespb.TaskQueueVersionInfoInternal{
			unversioned: {
				PhysicalTaskQueueInfo: unversionedPhysicalTaskQueueInfo,
			},
			buildID: {
				PhysicalTaskQueueInfo: versionedPhysicalTaskQueueInfo,
			},
		},
	}
	s.mockMatchingClient.EXPECT().DescribeTaskQueuePartition(ctx, matchingMockRequest).Return(matchingMockResponse, nil).Times(1)

	resp, err := handler.DescribeTaskQueuePartition(ctx, &adminservice.DescribeTaskQueuePartitionRequest{
		Namespace:          s.namespace.String(),
		TaskQueuePartition: tqPartitionRequest,
		BuildIds:           buildIdRequest,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 2, len(resp.VersionsInfoInternal))

	s.validatePhysicalTaskQueueInfo(t, unversionedPhysicalTaskQueueInfo, resp.VersionsInfoInternal[unversioned].GetPhysicalTaskQueueInfo())
	s.validatePhysicalTaskQueueInfo(t, versionedPhysicalTaskQueueInfo, resp.VersionsInfoInternal[buildID].GetPhysicalTaskQueueInfo())
}

func TestImportWorkflowExecution_NoSearchAttributes(t *testing.T) {
	s := setupAdminHandlerTest(t)
	tv := testvars.New(t).WithNamespaceName(s.namespace).WithNamespaceID(s.namespaceID)

	serializer := serialization.NewSerializer()
	generator := test.InitializeHistoryEventGenerator(tv.NamespaceName(), tv.NamespaceID(), tv.Any().Int64())

	// Generate random history.
	var historyBatches []*commonpb.DataBlob
	for generator.HasNextVertex() {
		events := generator.GetNextVertices()
		var historyEvents []*historypb.HistoryEvent
		for _, event := range events {
			historyEvent := event.GetData().(*historypb.HistoryEvent)
			historyEvents = append(historyEvents, historyEvent)
		}
		historyBatch, err := serializer.SerializeEvents(historyEvents)
		require.NoError(t, err)
		historyBatches = append(historyBatches, historyBatch)
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(tv.NamespaceName()).Return(tv.NamespaceID(), nil)

	s.mockHistoryClient.EXPECT().ImportWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *historyservice.ImportWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.ImportWorkflowExecutionResponse, error) {
		require.Equal(t, tv.NamespaceID().String(), request.NamespaceId)
		require.Equal(t, historyBatches, request.HistoryBatches, "history batches shouldn't be reserialized because there is no search attributes")
		return &historyservice.ImportWorkflowExecutionResponse{}, nil
	})
	_, err := s.handler.ImportWorkflowExecution(context.Background(), &adminservice.ImportWorkflowExecutionRequest{
		Namespace:      tv.NamespaceName().String(),
		Execution:      tv.WorkflowExecution(),
		HistoryBatches: historyBatches,
		VersionHistory: nil,
		Token:          nil,
	})
	require.NoError(t, err)
}

func TestImportWorkflowExecution_WithAliasedSearchAttributes(t *testing.T) {
	s := setupAdminHandlerTest(t)
	tv := testvars.New(t).WithNamespaceName(s.namespace).WithNamespaceID(s.namespaceID)

	serializer := serialization.NewSerializer()

	subTests := []struct {
		Name        string
		SaName      string
		ExpectedErr error
	}{
		{
			Name:        "valid SA alias",
			SaName:      "AliasOfKeyword01",
			ExpectedErr: nil,
		},
		{
			Name:        "invalid SA alias",
			SaName:      "InvalidAlias",
			ExpectedErr: &serviceerror.InvalidArgument{},
		},
		{
			Name:        "invalid SA field",
			SaName:      "AliasOfInvalidField",
			ExpectedErr: &serviceerror.InvalidArgument{},
		},
	}
	for _, subTest := range subTests {
		t.Run(subTest.Name, func(t *testing.T) {
			generator := test.InitializeHistoryEventGenerator(tv.NamespaceName(), tv.NamespaceID(), tv.Any().Int64())
			saValue := tv.Any().Payload()
			aliasedSas := &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				subTest.SaName: saValue,
			}}

			// Generate random history and set search attributes for all events that have search_attributes field.
			var historyBatches []*commonpb.DataBlob
			eventsWithSasCount := 0
			for generator.HasNextVertex() {
				events := generator.GetNextVertices()
				var historyEvents []*historypb.HistoryEvent
				for _, event := range events {
					historyEvent := event.GetData().(*historypb.HistoryEvent)
					eventHasSas := searchattribute.SetToEvent(historyEvent, aliasedSas)
					if eventHasSas {
						eventsWithSasCount++
					}
					historyEvents = append(historyEvents, historyEvent)
				}
				historyBatch, err := serializer.SerializeEvents(historyEvents)
				require.NoError(t, err)
				historyBatches = append(historyBatches, historyBatch)
			}
			if subTest.ExpectedErr != nil {
				// Import will fail fast on first event and won't check other events.
				eventsWithSasCount = 1
			}

			s.mockNamespaceCache.EXPECT().GetNamespaceID(tv.NamespaceName()).Return(tv.NamespaceID(), nil)
			s.mockVisibilityMgr.EXPECT().GetIndexName().Return(tv.IndexName()).Times(eventsWithSasCount)

			// Mock mapper remove alias from alias name.
			s.mockSaMapper.EXPECT().GetFieldName(gomock.Any(), tv.NamespaceName().String()).DoAndReturn(func(alias string, nsName string) (string, error) {
				if strings.HasPrefix(alias, "AliasOf") {
					return strings.TrimPrefix(alias, "AliasOf"), nil
				}
				return "", serviceerror.NewInvalidArgument("unknown alias")
			}).Times(eventsWithSasCount)

			s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes(tv.IndexName(), gomock.Any()).Return(searchattribute.TestNameTypeMap(), nil).Times(eventsWithSasCount)

			if subTest.ExpectedErr != nil {
				s.mockSaMapper.EXPECT().GetAlias(gomock.Any(), tv.NamespaceName().String()).Return("", serviceerror.NewInvalidArgument(""))
			} else {
				s.mockVisibilityMgr.EXPECT().ValidateCustomSearchAttributes(gomock.Any()).Return(nil, nil).Times(eventsWithSasCount)
				s.mockHistoryClient.EXPECT().ImportWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *historyservice.ImportWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.ImportWorkflowExecutionResponse, error) {
					require.Equal(t, tv.NamespaceID().String(), request.NamespaceId)
					for _, historyBatch := range request.HistoryBatches {
						events, err := serializer.DeserializeEvents(historyBatch)
						require.NoError(t, err)
						for _, event := range events {
							unaliasedSas, eventHasSas := searchattribute.GetFromEvent(event)
							if eventHasSas {
								require.NotNil(t, unaliasedSas, "search attributes must be set on every event with search_attributes field")
								require.Len(t, unaliasedSas.GetIndexedFields(), 1, "only 1 search attribute must be set")
								protoassert.ProtoEqual(t, saValue, unaliasedSas.GetIndexedFields()["Keyword01"])
							}
						}
					}
					return &historyservice.ImportWorkflowExecutionResponse{}, nil
				})
			}
			_, err := s.handler.ImportWorkflowExecution(context.Background(), &adminservice.ImportWorkflowExecutionRequest{
				Namespace:      tv.NamespaceName().String(),
				Execution:      tv.WorkflowExecution(),
				HistoryBatches: historyBatches,
				VersionHistory: nil,
				Token:          nil,
			})
			if subTest.ExpectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.ErrorAs(t, err, &subTest.ExpectedErr)
			}
		})
	}
}

func TestImportWorkflowExecution_WithNonAliasedSearchAttributes(t *testing.T) {
	s := setupAdminHandlerTest(t)
	tv := testvars.New(t).WithNamespaceName(s.namespace).WithNamespaceID(s.namespaceID)

	serializer := serialization.NewSerializer()
	subTests := []struct {
		Name        string
		SaName      string
		ExpectedErr error
	}{
		{
			Name:        "valid SA field",
			SaName:      "CustomKeywordField",
			ExpectedErr: nil,
		},
		{
			Name:        "invalid SA field",
			SaName:      "InvalidField",
			ExpectedErr: &serviceerror.InvalidArgument{},
		},
	}
	for _, subTest := range subTests {
		t.Run(subTest.Name, func(t *testing.T) {
			generator := test.InitializeHistoryEventGenerator(tv.NamespaceName(), tv.NamespaceID(), tv.Any().Int64())
			saValue := tv.Any().Payload()
			aliasedSas := &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				subTest.SaName: saValue,
			}}

			// Generate random history and set search attributes for all events that have search_attributes field.
			var historyBatches []*commonpb.DataBlob
			eventsWithSasCount := 0
			for generator.HasNextVertex() {
				events := generator.GetNextVertices()
				var historyEvents []*historypb.HistoryEvent
				for _, event := range events {
					historyEvent := event.GetData().(*historypb.HistoryEvent)
					eventHasSas := searchattribute.SetToEvent(historyEvent, aliasedSas)
					if eventHasSas {
						eventsWithSasCount++
					}
					historyEvents = append(historyEvents, historyEvent)
				}
				historyBatch, err := serializer.SerializeEvents(historyEvents)
				require.NoError(t, err)
				historyBatches = append(historyBatches, historyBatch)
			}
			if subTest.ExpectedErr != nil {
				// Import will fail fast on first event and won't check other events.
				eventsWithSasCount = 1
			}

			s.mockNamespaceCache.EXPECT().GetNamespaceID(tv.NamespaceName()).Return(tv.NamespaceID(), nil)
			s.mockVisibilityMgr.EXPECT().GetIndexName().Return(tv.IndexName()).Times(eventsWithSasCount)

			s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes(tv.IndexName(), gomock.Any()).Return(searchattribute.TestEsNameTypeMap(), nil).Times(eventsWithSasCount)

			// Mock mapper returns error because field name is not an alias.
			s.mockSaMapper.EXPECT().GetFieldName(gomock.Any(), tv.NamespaceName().String()).DoAndReturn(func(alias string, nsName string) (string, error) {
				return "", serviceerror.NewInvalidArgument("unknown alias")
			}).Times(eventsWithSasCount)

			if subTest.ExpectedErr != nil {
				s.mockSaMapper.EXPECT().GetAlias(gomock.Any(), tv.NamespaceName().String()).Return("", serviceerror.NewInvalidArgument(""))
			} else {
				s.mockVisibilityMgr.EXPECT().ValidateCustomSearchAttributes(gomock.Any()).Return(nil, nil).Times(eventsWithSasCount)
				s.mockHistoryClient.EXPECT().ImportWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *historyservice.ImportWorkflowExecutionRequest, opts ...grpc.CallOption) (*historyservice.ImportWorkflowExecutionResponse, error) {
					require.Equal(t, tv.NamespaceID().String(), request.NamespaceId)
					for _, historyBatch := range request.HistoryBatches {
						events, err := serializer.DeserializeEvents(historyBatch)
						require.NoError(t, err)
						for _, event := range events {
							unaliasedSas, eventHasSas := searchattribute.GetFromEvent(event)
							if eventHasSas {
								require.NotNil(t, unaliasedSas, "search attributes must be set on every event with search_attributes field")
								require.Len(t, unaliasedSas.GetIndexedFields(), 1, "only 1 search attribute must be set")
								protoassert.ProtoEqual(t, saValue, unaliasedSas.GetIndexedFields()["CustomKeywordField"])
							}
						}
					}
					return &historyservice.ImportWorkflowExecutionResponse{}, nil
				})
			}

			_, err := s.handler.ImportWorkflowExecution(context.Background(), &adminservice.ImportWorkflowExecutionRequest{
				Namespace:      tv.NamespaceName().String(),
				Execution:      tv.WorkflowExecution(),
				HistoryBatches: historyBatches,
				VersionHistory: nil,
				Token:          nil,
			})
			if subTest.ExpectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.ErrorAs(t, err, &subTest.ExpectedErr)
			}
		})
	}
}

func (s *adminHandlerTest) validatePhysicalTaskQueueInfo(t *testing.T, expectedPhysicalTaskQueueInfo *taskqueuespb.PhysicalTaskQueueInfo,
	responsePhysicalTaskQueueInfo *taskqueuespb.PhysicalTaskQueueInfo) {

	require.Equal(t, expectedPhysicalTaskQueueInfo.GetPollers(), responsePhysicalTaskQueueInfo.GetPollers())
	require.Equal(t, expectedPhysicalTaskQueueInfo.GetTaskQueueStats(), responsePhysicalTaskQueueInfo.GetTaskQueueStats())
	require.Equal(t, expectedPhysicalTaskQueueInfo.GetInternalTaskQueueStatus(), responsePhysicalTaskQueueInfo.GetInternalTaskQueueStatus())
}
