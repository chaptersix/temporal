package api

import (
	"context"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type workflowConsistencyCheckerTestDeps struct {
	controller    *gomock.Controller
	shardContext  *historyi.MockShardContext
	workflowCache *wcache.MockCache
	config        *configs.Config
	shardID       int32
	namespaceID   string
	workflowID    string
	currentRunID  string
	checker       *WorkflowConsistencyCheckerImpl
}

func setupWorkflowConsistencyCheckerTest(t *testing.T) *workflowConsistencyCheckerTestDeps {
	controller := gomock.NewController(t)
	shardContext := historyi.NewMockShardContext(controller)
	workflowCache := wcache.NewMockCache(controller)
	config := tests.NewDynamicConfig()

	shardID := rand.Int31()
	namespaceID := uuid.New().String()
	workflowID := uuid.New().String()
	currentRunID := uuid.New().String()

	shardContext.EXPECT().GetShardID().Return(shardID).AnyTimes()
	shardContext.EXPECT().GetConfig().Return(config).AnyTimes()

	checker := NewWorkflowConsistencyChecker(shardContext, workflowCache)

	t.Cleanup(func() {
		controller.Finish()
	})

	return &workflowConsistencyCheckerTestDeps{
		controller:    controller,
		shardContext:  shardContext,
		workflowCache: workflowCache,
		config:        config,
		shardID:       shardID,
		namespaceID:   namespaceID,
		workflowID:    workflowID,
		currentRunID:  currentRunID,
		checker:       checker,
	}
}

func TestGetWorkflowContextValidatedByCheck_Success_PassCheck(t *testing.T) {
	deps := setupWorkflowConsistencyCheckerTest(t)
	ctx := context.Background()

	wfContext := historyi.NewMockWorkflowContext(deps.controller)
	mutableState := historyi.NewMockMutableState(deps.controller)
	released := false
	releaseFn := func(err error) { released = true }

	deps.workflowCache.EXPECT().GetOrCreateChasmExecution(
		ctx,
		deps.shardContext,
		namespace.ID(deps.namespaceID),
		protomock.Eq(&commonpb.WorkflowExecution{
			WorkflowId: deps.workflowID,
			RunId:      deps.currentRunID,
		}),
		chasm.WorkflowArchetype,
		locks.PriorityHigh,
	).Return(wfContext, releaseFn, nil)
	wfContext.EXPECT().LoadMutableState(ctx, deps.shardContext).Return(mutableState, nil)

	workflowLease, err := deps.checker.GetWorkflowLease(
		ctx, nil,
		definition.NewWorkflowKey(deps.namespaceID, deps.workflowID, deps.currentRunID),
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.Equal(t, mutableState, workflowLease.GetMutableState())
	require.False(t, released)
}

func TestGetCurrentRunID_Success(t *testing.T) {
	deps := setupWorkflowConsistencyCheckerTest(t)
	ctx := context.Background()

	released := false
	releaseFn := func(err error) { released = true }

	deps.workflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(
		ctx,
		deps.shardContext,
		namespace.ID(deps.namespaceID),
		deps.workflowID,
		locks.PriorityHigh,
	).Return(releaseFn, nil)
	deps.shardContext.EXPECT().GetCurrentExecution(
		ctx,
		&persistence.GetCurrentExecutionRequest{
			ShardID:     deps.shardContext.GetShardID(),
			NamespaceID: deps.namespaceID,
			WorkflowID:  deps.workflowID,
		},
	).Return(&persistence.GetCurrentExecutionResponse{RunID: deps.currentRunID}, nil)

	runID, err := deps.checker.GetCurrentRunID(ctx, deps.namespaceID, deps.workflowID, locks.PriorityHigh)
	require.NoError(t, err)
	require.Equal(t, deps.currentRunID, runID)
	require.True(t, released)
}

func TestGetCurrentRunID_Error(t *testing.T) {
	deps := setupWorkflowConsistencyCheckerTest(t)
	ctx := context.Background()

	released := false
	releaseFn := func(err error) { released = true }

	deps.workflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(
		ctx,
		deps.shardContext,
		namespace.ID(deps.namespaceID),
		deps.workflowID,
		locks.PriorityHigh,
	).Return(releaseFn, nil)
	deps.shardContext.EXPECT().GetCurrentExecution(
		ctx,
		&persistence.GetCurrentExecutionRequest{
			ShardID:     deps.shardContext.GetShardID(),
			NamespaceID: deps.namespaceID,
			WorkflowID:  deps.workflowID,
		},
	).Return(nil, serviceerror.NewUnavailable(""))

	runID, err := deps.checker.GetCurrentRunID(ctx, deps.namespaceID, deps.workflowID, locks.PriorityHigh)
	require.IsType(t, &serviceerror.Unavailable{}, err)
	require.Empty(t, runID)
	require.True(t, released)
}

func Test_clockConsistencyCheck(t *testing.T) {
	deps := setupWorkflowConsistencyCheckerTest(t)

	err := deps.checker.clockConsistencyCheck(nil)
	require.NoError(t, err)

	reqClock := &clockspb.VectorClock{
		ShardId:   1,
		Clock:     10,
		ClusterId: 1,
	}

	// not compatible - different shard id
	differentShardClock := &clockspb.VectorClock{
		ShardId:   2,
		Clock:     1,
		ClusterId: 1,
	}
	deps.shardContext.EXPECT().CurrentVectorClock().Return(differentShardClock)
	err = deps.checker.clockConsistencyCheck(reqClock)
	require.NoError(t, err)

	// not compatible - different cluster id
	differentClusterClock := &clockspb.VectorClock{
		ShardId:   1,
		Clock:     1,
		ClusterId: 2,
	}
	deps.shardContext.EXPECT().CurrentVectorClock().Return(differentClusterClock)
	err = deps.checker.clockConsistencyCheck(reqClock)
	require.NoError(t, err)

	// not compatible - shard context clock is missing
	deps.shardContext.EXPECT().CurrentVectorClock().Return(nil)
	err = deps.checker.clockConsistencyCheck(reqClock)
	require.NoError(t, err)

	// shard clock ahead
	shardClock := &clockspb.VectorClock{
		ShardId:   1,
		Clock:     20,
		ClusterId: 1,
	}
	deps.shardContext.EXPECT().CurrentVectorClock().Return(shardClock)
	err = deps.checker.clockConsistencyCheck(reqClock)
	require.NoError(t, err)

	// shard clock behind
	shardClock = &clockspb.VectorClock{
		ShardId:   1,
		Clock:     1,
		ClusterId: 1,
	}
	deps.shardContext.EXPECT().CurrentVectorClock().Return(shardClock)
	deps.shardContext.EXPECT().UnloadForOwnershipLost()
	err = deps.checker.clockConsistencyCheck(reqClock)
	require.Error(t, err)
}
