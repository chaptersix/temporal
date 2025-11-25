package queues

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestImmediateQueue_PaginationFnProvider_ShardOwnershipLost(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			Owner:   "test-shard-owner",
		},
		tests.NewDynamicConfig(),
	)
	mockExecutionManager := mockShard.Resource.ExecutionMgr
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	immediateQueue := NewImmediateQueue(
		mockShard,
		tasks.CategoryTransfer,
		nil, // scheduler
		nil, // rescheduler
		&testQueueOptions,
		NewReaderPriorityRateLimiter(
			func() float64 { return 10 },
			1,
		),
		GrouperNamespaceID{},
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		nil, // execuable factory
	)

	paginationFnProvider := immediateQueue.paginationFnProvider

	mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(nil, &persistence.ShardOwnershipLostError{
		ShardID: mockShard.GetShardID(),
	}).Times(1)

	paginationFn := paginationFnProvider(NewRandomRange())
	_, _, err := paginationFn(nil)
	require.True(t, shard.IsShardOwnershipLostError(err))

	// make sure shard is also marked as invalid
	require.False(t, mockShard.IsValid())
}
