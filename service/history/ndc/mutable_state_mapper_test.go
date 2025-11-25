package ndc

import (
	"context"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

type mutableStateMapperTestDeps struct {
	controller         *gomock.Controller
	logger             log.Logger
	shardContext       historyi.MockShardContext
	branchMgrProvider  branchMgrProvider
	mockBranchMgr      *MockBranchMgr
	mutableStateMapper *MutableStateMapperImpl
	clusterMetadata    *cluster.MockMetadata
}

func setupMutableStateMapperTest(t *testing.T) *mutableStateMapperTestDeps {
	t.Helper()

	controller := gomock.NewController(t)

	mockBranchMgr := NewMockBranchMgr(controller)
	branchMgrProvider := func(
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
		logger log.Logger) BranchMgr {
		return mockBranchMgr
	}
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).Return("some random cluster name").AnyTimes()
	mutableStateMapper := NewMutableStateMapping(
		nil,
		nil,
		branchMgrProvider,
		nil,
		nil)

	t.Cleanup(func() {
		controller.Finish()
	})

	return &mutableStateMapperTestDeps{
		controller:         controller,
		mockBranchMgr:      mockBranchMgr,
		branchMgrProvider:  branchMgrProvider,
		clusterMetadata:    clusterMetadata,
		mutableStateMapper: mutableStateMapper,
	}
}

func TestGetOrCreateHistoryBranch_ValidEventBatch_NoDedupe(t *testing.T) {
	t.Parallel()
	deps := setupMutableStateMapperTest(t)

	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	eventSlices := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 11,
			},
			{
				EventId: 12,
			},
		},
		{
			{
				EventId: 13,
			},
			{
				EventId: 14,
			},
		},
	}
	task, _ := newReplicationTask(
		deps.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		eventSlices,
		nil,
		"",
		nil,
		false,
	)
	deps.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(11), gomock.Any()).Return(true, int32(0), nil).Times(1)

	_, out, err := deps.mutableStateMapper.GetOrCreateHistoryBranch(context.Background(), nil, nil, task)
	require.NoError(t, err)
	require.Equal(t, int32(0), out.BranchIndex)
	require.Equal(t, 0, out.EventsApplyIndex)
	require.True(t, out.DoContinue)
}

func TestGetOrCreateHistoryBranch_ValidEventBatch_FirstBatchDedupe(t *testing.T) {
	t.Parallel()
	deps := setupMutableStateMapperTest(t)

	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	eventSlices := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 11,
			},
			{
				EventId: 12,
			},
		},
		{
			{
				EventId: 13,
			},
			{
				EventId: 14,
			},
		},
	}
	task, _ := newReplicationTask(
		deps.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		eventSlices,
		nil,
		"",
		nil,
		false,
	)
	deps.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(11), gomock.Any()).Return(false, int32(0), nil).Times(1)
	deps.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(13), gomock.Any()).Return(true, int32(0), nil).Times(1)
	_, out, err := deps.mutableStateMapper.GetOrCreateHistoryBranch(context.Background(), nil, nil, task)
	require.NoError(t, err)
	require.Equal(t, int32(0), out.BranchIndex)
	require.Equal(t, 1, out.EventsApplyIndex)
	require.True(t, out.DoContinue)
}

func TestGetOrCreateHistoryBranch_ValidEventBatch_AllBatchDedupe(t *testing.T) {
	t.Parallel()
	deps := setupMutableStateMapperTest(t)

	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	eventSlices := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 11,
			},
			{
				EventId: 12,
			},
		},
		{
			{
				EventId: 13,
			},
			{
				EventId: 14,
			},
		},
	}
	task, _ := newReplicationTask(
		deps.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		eventSlices,
		nil,
		"",
		nil,
		false,
	)
	deps.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(11), gomock.Any()).Return(false, int32(0), nil).Times(1)
	deps.mockBranchMgr.EXPECT().
		GetOrCreate(gomock.Any(), gomock.Any(), int64(13), gomock.Any()).Return(false, int32(0), nil).Times(1)
	_, out, err := deps.mutableStateMapper.GetOrCreateHistoryBranch(context.Background(), nil, nil, task)
	require.NoError(t, err)
	require.Equal(t, int32(0), out.BranchIndex)
	require.Equal(t, 0, out.EventsApplyIndex)
	require.False(t, out.DoContinue)
}
