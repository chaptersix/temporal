package replication

import (
	"math/rand"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func setupProgressCache(t *testing.T) (*gomock.Controller, *shard.ContextTest, ProgressCache, string, string, string) {
	controller := gomock.NewController(t)
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()

	shardContext := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			Owner:   "test-shard-owner",
		},
		tests.NewDynamicConfig(),
	)
	progressCache := NewProgressCache(shardContext.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler)
	namespaceID := tests.NamespaceID.String()
	workflowID := uuid.New()
	runID := uuid.New()

	return controller, mockShard, progressCache, namespaceID, workflowID, runID
}

func TestProgressCache(t *testing.T) {
	controller, mockShard, progressCache, _, _, runID := setupProgressCache(t)
	defer controller.Finish()
	defer mockShard.StopForTest()

	protoAssert := protorequire.New(t)
	targetClusterID := rand.Int31()
	firstEventID := int64(999)
	versionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 80,
		TransitionCount:          10,
	}
	versionedTransitions := []*persistencespb.VersionedTransition{versionedTransition}
	versionHistoryItems := []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(firstEventID, versionedTransition.NamespaceFailoverVersion),
	}
	expected := &ReplicationProgress{
		versionedTransitions:       [][]*persistencespb.VersionedTransition{versionedTransitions},
		eventVersionHistoryItems:   [][]*historyspb.VersionHistoryItem{versionHistoryItems},
		lastVersionTransitionIndex: 0,
	}

	// get non-existing progress
	cachedProgress := progressCache.Get(runID, targetClusterID)
	require.Nil(t, cachedProgress)

	err := progressCache.Update(runID, targetClusterID, versionedTransitions, versionHistoryItems)
	require.Nil(t, err)

	// get existing progress
	cachedProgress = progressCache.Get(runID, targetClusterID)
	protoAssert.DeepEqual(expected, cachedProgress)

	// update existing versioned transition and version history
	versionedTransitions2 := []*persistencespb.VersionedTransition{
		{
			NamespaceFailoverVersion: 80,
			TransitionCount:          20,
		},
	}
	versionHistoryItems2 := []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(firstEventID+1, versionedTransition.NamespaceFailoverVersion),
	}
	err = progressCache.Update(runID, targetClusterID, versionedTransitions2, versionHistoryItems2)
	require.Nil(t, err)

	expected2 := &ReplicationProgress{
		versionedTransitions:         [][]*persistencespb.VersionedTransition{versionedTransitions2},
		eventVersionHistoryItems:     [][]*historyspb.VersionHistoryItem{versionHistoryItems2},
		lastVersionTransitionIndex:   0,
		lastEventVersionHistoryIndex: 0,
	}
	cachedProgress = progressCache.Get(runID, targetClusterID)
	protoAssert.DeepEqual(expected2, cachedProgress)

	// add new versioned transition and version history
	versionedTransitions3 := []*persistencespb.VersionedTransition{
		{
			NamespaceFailoverVersion: 90,
			TransitionCount:          15,
		},
	}
	versionHistoryItems3 := []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(firstEventID, versionedTransition.NamespaceFailoverVersion),
		versionhistory.NewVersionHistoryItem(firstEventID+1, versionedTransition.NamespaceFailoverVersion+1),
	}
	err = progressCache.Update(runID, targetClusterID, versionedTransitions3, versionHistoryItems3)
	require.Nil(t, err)

	expected3 := &ReplicationProgress{
		versionedTransitions:         [][]*persistencespb.VersionedTransition{versionedTransitions2, versionedTransitions3},
		eventVersionHistoryItems:     [][]*historyspb.VersionHistoryItem{versionHistoryItems2, versionHistoryItems3},
		lastVersionTransitionIndex:   1,
		lastEventVersionHistoryIndex: 1,
	}
	cachedProgress = progressCache.Get(runID, targetClusterID)
	protoAssert.DeepEqual(expected3, cachedProgress)

	// noop update: versioned transition and version history are already included in the existing progress
	err = progressCache.Update(runID, targetClusterID, versionedTransitions, versionHistoryItems)
	require.Nil(t, err)

	cachedProgress = progressCache.Get(runID, targetClusterID)
	protoAssert.DeepEqual(expected3, cachedProgress)
}
