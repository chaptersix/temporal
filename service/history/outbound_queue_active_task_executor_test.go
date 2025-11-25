package history

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type outboundQueueActiveTaskExecutorTestDeps struct {
	controller            *gomock.Controller
	mockShard             *shard.ContextTest
	mockWorkflowCache     *cache.MockCache
	mockChasmEngine       *chasm.MockEngine
	mockNamespaceRegistry *namespace.MockRegistry
	hsmRegistry           *hsm.Registry
	mockWorkflowContext   *historyi.MockWorkflowContext
	mockMutableState      *historyi.MockMutableState
	mockExecutable        *queues.MockExecutable
	mockChasmTree         *historyi.MockChasmTree

	logger         log.Logger
	metricsHandler metrics.Handler

	namespaceID    namespace.ID
	namespaceEntry *namespace.Namespace

	executor *outboundQueueActiveTaskExecutor
}

func setupOutboundQueueActiveTaskExecutorTest(t *testing.T) *outboundQueueActiveTaskExecutorTestDeps {
	d := &outboundQueueActiveTaskExecutorTestDeps{}

	d.namespaceID = tests.NamespaceID
	d.namespaceEntry = tests.GlobalNamespaceEntry

	// Setup controller and mocks
	d.controller = gomock.NewController(t)
	d.mockShard = shard.NewTestContext(
		d.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	d.mockWorkflowCache = cache.NewMockCache(d.controller)
	d.mockChasmEngine = chasm.NewMockEngine(d.controller)
	d.mockNamespaceRegistry = namespace.NewMockRegistry(d.controller)
	d.hsmRegistry = hsm.NewRegistry()
	d.mockWorkflowContext = historyi.NewMockWorkflowContext(d.controller)
	d.mockMutableState = historyi.NewMockMutableState(d.controller)
	d.mockExecutable = queues.NewMockExecutable(d.controller)
	d.mockChasmTree = historyi.NewMockChasmTree(d.controller)

	d.logger = d.mockShard.GetLogger()
	d.metricsHandler = d.mockShard.GetMetricsHandler()

	ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{
		Name: d.namespaceEntry.Name().String(),
		Id:   string(d.namespaceID),
	}, nil, "")
	d.mockNamespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
	d.mockNamespaceRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(ns.Name(), nil).AnyTimes()

	d.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(d.mockShard.GetShardID())).AnyTimes()
	d.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(d.namespaceEntry, nil).AnyTimes()
	d.mockShard.SetStateMachineRegistry(d.hsmRegistry)
	d.mockShard.Resource.NamespaceCache.EXPECT().
		GetNamespaceByID(gomock.Any()).
		Return(d.namespaceEntry, nil).
		AnyTimes()

	d.mockWorkflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Return(cache.NoopReleaseFn, nil).AnyTimes()

	d.mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	d.mockMutableState.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes()
	d.mockMutableState.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()

	d.executor = newOutboundQueueActiveTaskExecutor(
		d.mockShard,
		d.mockWorkflowCache,
		d.logger,
		d.metricsHandler,
		d.mockChasmEngine,
	)

	return d
}

func (d *outboundQueueActiveTaskExecutorTestDeps) mustGenerateTaskID(t *testing.T) int64 {
	taskID, err := d.mockShard.GenerateTaskID()
	require.NoError(t, err)
	return taskID
}

func TestOutboundQueueActiveTaskExecutor_Execute_ChasmTask(t *testing.T) {
	d := setupOutboundQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()

	tv := testvars.New(t)
	ctx := context.Background()

	testCases := []struct {
		name                string
		setupMocks          func(*tasks.ChasmTask)
		expectHandlerCalled bool
		expectedError       string
	}{
		{
			name: "success",
			setupMocks: func(task *tasks.ChasmTask) {
				// Setup successful workflow context loading and CHASM execution

				d.mockWorkflowCache.EXPECT().
					GetOrCreateChasmExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), chasm.ArchetypeAny, gomock.Any()).
					Return(d.mockWorkflowContext, func(error) {}, nil)

				d.mockWorkflowContext.EXPECT().
					LoadMutableState(gomock.Any(), gomock.Any()).
					Return(d.mockMutableState, nil)

				d.mockMutableState.EXPECT().
					ChasmTree().
					Return(d.mockChasmTree)

				d.mockChasmTree.EXPECT().
					ExecuteSideEffectTask(
						gomock.Any(),
						gomock.Any(),
						gomock.Any(),
						gomock.Any(),
						gomock.Any(),
					)
			},
			expectHandlerCalled: true,
		},
		{
			name: "mutable state failure",
			setupMocks: func(task *tasks.ChasmTask) {
				// Workflow context loads but mutable state fails
				d.mockWorkflowCache.EXPECT().
					GetOrCreateChasmExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), chasm.ArchetypeAny, gomock.Any()).
					Return(d.mockWorkflowContext, func(error) {}, nil)

				d.mockWorkflowContext.EXPECT().
					LoadMutableState(gomock.Any(), gomock.Any()).
					Return(nil, errors.New("mutable state failed to load"))
			},
			expectHandlerCalled: false,
			expectedError:       "mutable state failed to load",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a CHASM task
			task := &tasks.ChasmTask{
				WorkflowKey: tv.Any().WorkflowKey(),
				TaskID:      d.mustGenerateTaskID(t),
				Category:    tasks.CategoryOutbound,
				Destination: tv.Any().String(),
				Info: &persistencespb.ChasmTaskInfo{
					TypeId: tv.Any().UInt32(),
				},
			}

			tc.setupMocks(task)
			d.mockExecutable.EXPECT().GetTask().Return(task).AnyTimes()

			result := d.executor.Execute(ctx, d.mockExecutable)

			if tc.expectedError != "" {
				require.Error(t, result.ExecutionErr)
				require.Contains(t, result.ExecutionErr.Error(), tc.expectedError)
			} else {
				require.NoError(t, result.ExecutionErr)
			}
			require.True(t, result.ExecutedAsActive)
			require.NotEmpty(t, result.ExecutionMetricTags)
		})
	}
}

func TestOutboundQueueActiveTaskExecutor_Execute_PreValidationFails(t *testing.T) {
	d := setupOutboundQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()

	tv := testvars.New(t)
	ctx := context.Background()

	testCases := []struct {
		name          string
		setupTask     func() tasks.Task
		setupMocks    func(tasks.Task)
		expectedError string
	}{
		{
			name: "invalid task type",
			setupTask: func() tasks.Task {
				// Create a task type that's NOT StateMachineOutboundTask or ChasmTask
				return &tasks.ActivityTask{
					WorkflowKey: tv.Any().WorkflowKey(),
					TaskID:      d.mustGenerateTaskID(t),
					TaskQueue:   tv.Any().String(),
				}
			},
			setupMocks:    func(task tasks.Task) {},
			expectedError: "unknown task type",
		},
		{
			name: "clock validation failure",
			setupTask: func() tasks.Task {
				return &tasks.ChasmTask{
					Destination: tv.Any().String(),
					TaskID:      math.MaxInt64,
					Info:        &persistencespb.ChasmTaskInfo{},
				}
			},
			setupMocks:    func(task tasks.Task) {},
			expectedError: "task clock validation failed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			task := tc.setupTask()
			tc.setupMocks(task)
			d.mockExecutable.EXPECT().GetTask().Return(task)

			result := d.executor.Execute(ctx, d.mockExecutable)

			require.Error(t, result.ExecutionErr)
			require.Contains(t, result.ExecutionErr.Error(), tc.expectedError)
			require.True(t, result.ExecutedAsActive)
			require.NotEmpty(t, result.ExecutionMetricTags)
		})
	}
}
