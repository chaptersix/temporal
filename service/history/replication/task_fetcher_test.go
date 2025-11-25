package replication

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type taskFetcherTestDeps struct {
	controller             *gomock.Controller
	mockResource           *resourcetest.Test
	frontendClient         *adminservicemock.MockAdminServiceClient
	config                 *configs.Config
	logger                 log.Logger
	replicationTaskFetcher *taskFetcherImpl
}

func setupTaskFetcherTest(t *testing.T) *taskFetcherTestDeps {
	controller := gomock.NewController(t)

	mockResource := resourcetest.NewTest(controller, primitives.HistoryService)
	frontendClient := mockResource.RemoteAdminClient
	logger := log.NewNoopLogger()
	config := tests.NewDynamicConfig()
	config.ReplicationTaskFetcherParallelism = dynamicconfig.GetIntPropertyFn(1)

	replicationTaskFetcher := newReplicationTaskFetcher(
		logger,
		cluster.TestAlternativeClusterName,
		cluster.TestCurrentClusterName,
		config,
		mockResource.ClientBean,
	)

	return &taskFetcherTestDeps{
		controller:             controller,
		mockResource:           mockResource,
		frontendClient:         frontendClient,
		config:                 config,
		logger:                 logger,
		replicationTaskFetcher: replicationTaskFetcher,
	}
}

type getReplicationMessagesRequestMatcher struct {
	clusterName string
	tokens      map[int32]*replicationspb.ReplicationToken
}

func newGetReplicationMessagesRequestMatcher(
	req *adminservice.GetReplicationMessagesRequest,
) *getReplicationMessagesRequestMatcher {
	tokens := make(map[int32]*replicationspb.ReplicationToken)
	for _, token := range req.Tokens {
		tokens[token.ShardId] = token
	}
	return &getReplicationMessagesRequestMatcher{
		clusterName: req.ClusterName,
		tokens:      tokens,
	}
}

func (m *getReplicationMessagesRequestMatcher) Matches(x interface{}) bool {
	req, ok := x.(*adminservice.GetReplicationMessagesRequest)
	if !ok {
		return false
	}
	return reflect.DeepEqual(m, newGetReplicationMessagesRequestMatcher(req))
}

func (m *getReplicationMessagesRequestMatcher) String() string {
	// noop, not used
	return ""
}

func TestBufferRequests_NoDuplicate(t *testing.T) {
	s := setupTaskFetcherTest(t)
	defer s.controller.Finish()

	shardID := int32(1)

	respChan := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan,
	}

	s.replicationTaskFetcher.workers[0].bufferRequests(shardRequest)

	select {
	case <-respChan:
		require.Fail(t, "new request channel should not be closed")
	default:
		// noop
	}

	require.Equal(t, map[int32]*replicationTaskRequest{
		shardID: shardRequest,
	}, s.replicationTaskFetcher.workers[0].requestByShard)
}

func TestBufferRequests_Duplicate(t *testing.T) {
	s := setupTaskFetcherTest(t)
	defer s.controller.Finish()

	shardID := int32(1)

	respChan1 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest1 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan1,
	}

	respChan2 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest2 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan2,
	}

	s.replicationTaskFetcher.workers[0].bufferRequests(shardRequest1)
	s.replicationTaskFetcher.workers[0].bufferRequests(shardRequest2)

	_, ok := <-respChan1
	require.False(t, ok)

	select {
	case <-respChan2:
		require.Fail(t, "new request channel should not be closed")
	default:
		// noop
	}

	require.Equal(t, map[int32]*replicationTaskRequest{
		shardID: shardRequest2,
	}, s.replicationTaskFetcher.workers[0].requestByShard)
}

func TestGetMessages_All(t *testing.T) {
	s := setupTaskFetcherTest(t)
	defer s.controller.Finish()

	shardID := int32(1)
	respChan := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan,
	}
	requestByShard := map[int32]*replicationTaskRequest{
		shardID: shardRequest,
	}

	replicationMessageRequest := &adminservice.GetReplicationMessagesRequest{
		Tokens: []*replicationspb.ReplicationToken{
			shardRequest.token,
		},
		ClusterName: cluster.TestCurrentClusterName,
	}
	responseByShard := map[int32]*replicationspb.ReplicationMessages{
		shardID: {},
	}
	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		newGetReplicationMessagesRequestMatcher(replicationMessageRequest),
	).Return(&adminservice.GetReplicationMessagesResponse{ShardMessages: responseByShard}, nil)
	s.replicationTaskFetcher.workers[0].requestByShard = requestByShard
	err := s.replicationTaskFetcher.workers[0].getMessages()
	require.NoError(t, err)
	require.Equal(t, responseByShard[shardID], <-respChan)
}

func TestGetMessages_Partial(t *testing.T) {
	s := setupTaskFetcherTest(t)
	defer s.controller.Finish()

	shardID1 := int32(1)
	respChan1 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest1 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID1,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan1,
	}
	shardID2 := int32(2)
	respChan2 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest2 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID2,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan2,
	}
	requestByShard := map[int32]*replicationTaskRequest{
		shardID1: shardRequest1,
		shardID2: shardRequest2,
	}

	replicationMessageRequest := &adminservice.GetReplicationMessagesRequest{
		Tokens: []*replicationspb.ReplicationToken{
			shardRequest1.token,
			shardRequest2.token,
		},
		ClusterName: cluster.TestCurrentClusterName,
	}
	responseByShard := map[int32]*replicationspb.ReplicationMessages{
		shardID1: {},
	}
	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		newGetReplicationMessagesRequestMatcher(replicationMessageRequest),
	).Return(&adminservice.GetReplicationMessagesResponse{ShardMessages: responseByShard}, nil)
	s.replicationTaskFetcher.workers[0].requestByShard = requestByShard
	err := s.replicationTaskFetcher.workers[0].getMessages()
	require.NoError(t, err)
	require.Equal(t, responseByShard[shardID1], <-respChan1)
	require.Equal(t, (*replicationspb.ReplicationMessages)(nil), <-respChan2)
}

func TestGetMessages_Error(t *testing.T) {
	s := setupTaskFetcherTest(t)
	defer s.controller.Finish()

	shardID1 := int32(1)
	respChan1 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest1 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID1,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan1,
	}
	shardID2 := int32(2)
	respChan2 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest2 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID2,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan2,
	}
	requestByShard := map[int32]*replicationTaskRequest{
		shardID1: shardRequest1,
		shardID2: shardRequest2,
	}

	replicationMessageRequest := &adminservice.GetReplicationMessagesRequest{
		Tokens: []*replicationspb.ReplicationToken{
			shardRequest1.token,
			shardRequest2.token,
		},
		ClusterName: cluster.TestCurrentClusterName,
	}
	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		newGetReplicationMessagesRequestMatcher(replicationMessageRequest),
	).Return(nil, errors.New("random error"))
	s.replicationTaskFetcher.workers[0].requestByShard = requestByShard
	err := s.replicationTaskFetcher.workers[0].getMessages()
	require.Error(t, err)
	require.Equal(t, (*replicationspb.ReplicationMessages)(nil), <-respChan1)
	require.Equal(t, (*replicationspb.ReplicationMessages)(nil), <-respChan2)
}

func TestConcurrentFetchAndProcess_Success(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	numShards := 1024

	mockResource := resourcetest.NewTest(controller, primitives.HistoryService)
	frontendClient := mockResource.RemoteAdminClient
	logger := log.NewNoopLogger()
	config := tests.NewDynamicConfig()
	config.ReplicationTaskFetcherParallelism = dynamicconfig.GetIntPropertyFn(8)

	replicationTaskFetcher := newReplicationTaskFetcher(
		logger,
		cluster.TestAlternativeClusterName,
		cluster.TestCurrentClusterName,
		config,
		mockResource.ClientBean,
	)

	frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		gomock.Any(),
	).Return(&adminservice.GetReplicationMessagesResponse{}, nil).AnyTimes()

	replicationTaskFetcher.Start()
	defer replicationTaskFetcher.Stop()

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(numShards)
	for i := 0; i < numShards; i++ {
		shardID := int32(i)
		go func() {
			defer waitGroup.Done()
			respChan := make(chan *replicationspb.ReplicationMessages, 1)
			shardRequest := &replicationTaskRequest{
				token: &replicationspb.ReplicationToken{
					ShardId:                shardID,
					LastProcessedMessageId: 1,
					LastRetrievedMessageId: 2,
				},
				respChan: respChan,
			}

			replicationTaskFetcher.getRequestChan() <- shardRequest
			<-respChan
		}()
	}
	waitGroup.Wait()
}

func TestConcurrentFetchAndProcess_Error(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	numShards := 1024

	mockResource := resourcetest.NewTest(controller, primitives.HistoryService)
	frontendClient := mockResource.RemoteAdminClient
	logger := log.NewNoopLogger()
	config := tests.NewDynamicConfig()
	config.ReplicationTaskFetcherParallelism = dynamicconfig.GetIntPropertyFn(8)

	replicationTaskFetcher := newReplicationTaskFetcher(
		logger,
		cluster.TestAlternativeClusterName,
		cluster.TestCurrentClusterName,
		config,
		mockResource.ClientBean,
	)

	frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		gomock.Any(),
	).Return(nil, errors.New("random error")).AnyTimes()

	replicationTaskFetcher.Start()
	defer replicationTaskFetcher.Stop()

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(numShards)
	for i := 0; i < numShards; i++ {
		shardID := int32(i)
		go func() {
			defer waitGroup.Done()
			respChan := make(chan *replicationspb.ReplicationMessages, 1)
			shardRequest := &replicationTaskRequest{
				token: &replicationspb.ReplicationToken{
					ShardId:                shardID,
					LastProcessedMessageId: 1,
					LastRetrievedMessageId: 2,
				},
				respChan: respChan,
			}

			replicationTaskFetcher.getRequestChan() <- shardRequest
			<-respChan
		}()
	}
	waitGroup.Wait()
}
