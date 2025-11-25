package shard

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestReplicationReaderIDConversion(t *testing.T) {
	expectedClusterID := int64(rand.Int31())
	expectedShardID := rand.Int31()

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	require.Equal(t, expectedClusterID, actualClusterID)
	require.Equal(t, expectedShardID, actualShardID)
}

func TestReplicationReaderIDConversion_1(t *testing.T) {
	expectedClusterID := int64(1)
	expectedShardID := int32(1)

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	require.Equal(t, expectedClusterID, actualClusterID)
	require.Equal(t, expectedShardID, actualShardID)
}

func TestReplicationReaderIDConversion_Int32Max(t *testing.T) {
	expectedClusterID := int64(math.MaxInt32)
	expectedShardID := int32(math.MaxInt32)

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	require.Equal(t, expectedClusterID, actualClusterID)
	require.Equal(t, expectedShardID, actualShardID)
}

func newMockContext(t *testing.T) *historyi.MockShardContext {
	controller := gomock.NewController(t)
	mockContext := historyi.NewMockShardContext(controller)
	mockContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	mockContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	return mockContext
}
