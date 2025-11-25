package queues_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	executableTestDeps struct {
		controller            *gomock.Controller
		mockExecutor          *queues.MockExecutor
		mockScheduler         *queues.MockScheduler
		mockRescheduler       *queues.MockRescheduler
		mockNamespaceRegistry *namespace.MockRegistry
		mockClusterMetadata   *cluster.MockMetadata
		chasmRegistry         *chasm.Registry
		metricsHandler        *metricstest.CaptureHandler
		timeSource            *clock.EventTimeSource
	}
	executableParams struct {
		dlqWriter                  *queues.DLQWriter
		dlqEnabled                 dynamicconfig.BoolPropertyFn
		priorityAssigner           queues.PriorityAssigner
		maxUnexpectedErrorAttempts dynamicconfig.IntPropertyFn
		dlqInternalErrors          dynamicconfig.BoolPropertyFn
		dlqErrorPattern            dynamicconfig.StringPropertyFn
	}
	executableOption func(*executableParams)
)

func setupExecutableTest(t *testing.T) *executableTestDeps {
	controller := gomock.NewController(t)
	mockExecutor := queues.NewMockExecutor(controller)
	mockScheduler := queues.NewMockScheduler(controller)
	mockRescheduler := queues.NewMockRescheduler(controller)
	mockNamespaceRegistry := namespace.NewMockRegistry(controller)
	mockClusterMetadata := cluster.NewMockMetadata(controller)
	metricsHandler := metricstest.NewCaptureHandler()

	mockNamespaceRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			ShardCount: 1,
		},
	}).AnyTimes()

	timeSource := clock.NewEventTimeSource()
	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())

	return &executableTestDeps{
		controller:            controller,
		mockExecutor:          mockExecutor,
		mockScheduler:         mockScheduler,
		mockRescheduler:       mockRescheduler,
		mockNamespaceRegistry: mockNamespaceRegistry,
		mockClusterMetadata:   mockClusterMetadata,
		chasmRegistry:         chasmRegistry,
		metricsHandler:        metricsHandler,
		timeSource:            timeSource,
	}
}

func (d *executableTestDeps) newTestExecutable(opts ...executableOption) queues.Executable {
	p := executableParams{
		dlqWriter: nil,
		dlqEnabled: func() bool {
			return false
		},
		dlqInternalErrors: func() bool {
			return false
		},
		priorityAssigner: queues.NewNoopPriorityAssigner(),
		maxUnexpectedErrorAttempts: func() int {
			return math.MaxInt
		},
		dlqErrorPattern: func() string {
			return ""
		},
	}
	for _, opt := range opts {
		opt(&p)
	}
	return queues.NewExecutable(
		queues.DefaultReaderId,
		tasks.NewFakeTask(
			definition.NewWorkflowKey(
				tests.NamespaceID.String(),
				tests.WorkflowID,
				tests.RunID,
			),
			tasks.CategoryTransfer,
			d.timeSource.Now(),
		),
		d.mockExecutor,
		d.mockScheduler,
		d.mockRescheduler,
		p.priorityAssigner,
		d.timeSource,
		d.mockNamespaceRegistry,
		d.mockClusterMetadata,
		d.chasmRegistry,
		queues.GetTaskTypeTagValue,
		log.NewTestLogger(),
		d.metricsHandler,
		telemetry.NoopTracer,
		func(params *queues.ExecutableParams) {
			params.DLQEnabled = p.dlqEnabled
			params.DLQWriter = p.dlqWriter
			params.MaxUnexpectedErrorAttempts = p.maxUnexpectedErrorAttempts
			params.DLQInternalErrors = p.dlqInternalErrors
			params.DLQErrorPattern = p.dlqErrorPattern
		},
	)
}

func accessInternalState(executable queues.Executable) {
	_ = fmt.Sprintf("%v", executable)
}

func TestExecute_TaskExecuted(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    true,
		ExecutionErr:        errors.New("some random error"),
	})
	require.Error(t, executable.Execute())

	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    true,
		ExecutionErr:        nil,
	})
	require.NoError(t, executable.Execute())
}

func TestExecute_InMemoryNoUserLatency_SingleAttempt(t *testing.T) {
	t.Parallel()
	scheduleLatency := 100 * time.Millisecond
	userLatency := 500 * time.Millisecond
	attemptLatency := time.Second
	attemptNoUserLatency := scheduleLatency + attemptLatency - userLatency

	testCases := []struct {
		name                         string
		taskErr                      error
		expectError                  bool
		expectedAttemptNoUserLatency time.Duration
		expectBackoff                bool
	}{
		{
			name:                         "NoError",
			taskErr:                      nil,
			expectError:                  false,
			expectedAttemptNoUserLatency: attemptNoUserLatency,
			expectBackoff:                false,
		},
		{
			name:                         "UnavailableError",
			taskErr:                      serviceerror.NewUnavailable("some random error"),
			expectError:                  true,
			expectedAttemptNoUserLatency: attemptNoUserLatency,
			expectBackoff:                true,
		},
		{
			name:                         "NotFoundError",
			taskErr:                      serviceerror.NewNotFound("not found error"),
			expectError:                  false,
			expectedAttemptNoUserLatency: 0,
			expectBackoff:                false,
		},
		{
			name:                         "NotFoundErrorWrapped",
			taskErr:                      fmt.Errorf("%w: some reason", consts.ErrWorkflowCompleted),
			expectError:                  false,
			expectedAttemptNoUserLatency: 0,
			expectBackoff:                false,
		},
		{
			name:                         "BusyWorkflowError",
			taskErr:                      consts.ErrResourceExhaustedBusyWorkflow,
			expectError:                  true,
			expectedAttemptNoUserLatency: 0,
			expectBackoff:                false,
		},
		{
			name:                         "APSLimitError",
			taskErr:                      consts.ErrResourceExhaustedBusyWorkflow,
			expectError:                  true,
			expectedAttemptNoUserLatency: 0,
			expectBackoff:                false,
		},
		{
			name: "OPSLimitError",
			taskErr: &serviceerror.ResourceExhausted{
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_OPS_LIMIT,
				Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
				Message: "Namespace Max OPS Limit Reached.",
			},
			expectError:                  true,
			expectedAttemptNoUserLatency: 0,
			expectBackoff:                false,
		},
		{
			name:                         "PersistenceNamespaceLimitExceeded",
			taskErr:                      persistence.ErrPersistenceNamespaceLimitExceeded,
			expectError:                  true,
			expectedAttemptNoUserLatency: 0,
			expectBackoff:                false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deps := setupExecutableTest(t)
			executable := deps.newTestExecutable()

			now := time.Now()
			deps.timeSource.Update(now)
			executable.SetScheduledTime(now)

			now = now.Add(scheduleLatency)
			deps.timeSource.Update(now)

			deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Do(func(ctx context.Context, taskInfo interface{}) {
				metrics.ContextCounterAdd(
					ctx,
					metrics.HistoryWorkflowExecutionCacheLatency.Name(),
					int64(userLatency),
				)

				now = now.Add(attemptLatency)
				deps.timeSource.Update(now)
			}).Return(queues.ExecuteResponse{
				ExecutionMetricTags: nil,
				ExecutedAsActive:    true,
				ExecutionErr:        tc.taskErr,
			})

			err := executable.Execute()
			if err != nil {
				err = executable.HandleErr(err)
			}

			if tc.expectError {
				require.Error(t, err)
				deps.mockScheduler.EXPECT().TrySubmit(executable).Return(false)
				deps.mockRescheduler.EXPECT().Add(executable, gomock.Any())
				executable.Nack(err)
				return
			}

			require.NoError(t, err)
			capture := deps.metricsHandler.StartCapture()
			executable.Ack()
			snapshot := capture.Snapshot()
			recordings := snapshot[metrics.TaskLatency.Name()]
			if tc.expectedAttemptNoUserLatency == 0 {
				// invalid task, no noUserLatency will be recorded.
				require.Empty(t, recordings)
				return
			}

			require.Len(t, recordings, 1)
			actualAttemptNoUserLatency, ok := recordings[0].Value.(time.Duration)
			require.True(t, ok)
			if tc.expectBackoff {
				// the backoff duration is random, so we can't compare the exact value
				require.Less(t, tc.expectedAttemptNoUserLatency, actualAttemptNoUserLatency)
			} else {
				require.Equal(t, tc.expectedAttemptNoUserLatency, actualAttemptNoUserLatency)
			}
		})
	}
}

func TestExecute_InMemoryNoUserLatency_MultipleAttempts(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)

	numAttempts := 3
	scheduleLatencies := []time.Duration{100 * time.Millisecond, 150 * time.Millisecond, 200 * time.Millisecond}
	userLatencies := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond}
	attemptLatencies := []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}
	taskErrors := []error{
		serviceerror.NewUnavailable("test unavailable error"),
		consts.ErrResourceExhaustedBusyWorkflow,
		nil,
	}
	expectedInMemoryNoUserLatency := scheduleLatencies[0] + attemptLatencies[0] - userLatencies[0] +
		scheduleLatencies[2] + attemptLatencies[2] - userLatencies[2]
	_ = expectedInMemoryNoUserLatency

	executable := deps.newTestExecutable()

	now := time.Now()
	deps.timeSource.Update(now)
	executable.SetScheduledTime(now)

	for i := 0; i != numAttempts; i++ {
		now = now.Add(scheduleLatencies[i])
		deps.timeSource.Update(now)

		deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Do(func(ctx context.Context, taskInfo interface{}) {
			metrics.ContextCounterAdd(
				ctx,
				metrics.HistoryWorkflowExecutionCacheLatency.Name(),
				int64(userLatencies[i]),
			)

			now = now.Add(attemptLatencies[i])
			deps.timeSource.Update(now)
		}).Return(queues.ExecuteResponse{
			ExecutionMetricTags: nil,
			ExecutedAsActive:    true,
			ExecutionErr:        taskErrors[i],
		})

		err := executable.Execute()
		if err != nil {
			err = executable.HandleErr(err)
		}

		if taskErrors[i] != nil {
			require.Error(t, err)
			deps.mockScheduler.EXPECT().TrySubmit(executable).Return(true)
			executable.Nack(err)
		} else {
			require.NoError(t, err)
			capture := deps.metricsHandler.StartCapture()
			executable.Ack()
			snapshot := capture.Snapshot()
			recordings := snapshot[metrics.TaskLatency.Name()]
			require.Len(t, recordings, 1)
			actualInMemoryNoUserLatency, ok := recordings[0].Value.(time.Duration)
			require.True(t, ok)
			require.Equal(t, expectedInMemoryNoUserLatency, actualInMemoryNoUserLatency)
		}
	}
}

func TestExecute_CapturePanic(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(_ context.Context, _ queues.Executable) queues.ExecuteResponse {
			panic("test panic during execution")
		},
	)
	require.Error(t, executable.Execute())
}

func TestExecute_CallerInfo(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(ctx context.Context, _ queues.Executable) queues.ExecuteResponse {
			require.Equal(t, headers.CallerTypeBackgroundHigh, headers.GetCallerInfo(ctx).CallerType)
			return queues.ExecuteResponse{
				ExecutionMetricTags: nil,
				ExecutedAsActive:    true,
				ExecutionErr:        nil,
			}
		},
	)
	require.NoError(t, executable.Execute())

	executable = deps.newTestExecutable(func(p *executableParams) {
		p.priorityAssigner = queues.NewStaticPriorityAssigner(ctasks.PriorityLow)
	})
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(ctx context.Context, _ queues.Executable) queues.ExecuteResponse {
			require.Equal(t, headers.CallerTypeBackgroundLow, headers.GetCallerInfo(ctx).CallerType)
			return queues.ExecuteResponse{
				ExecutionMetricTags: nil,
				ExecutedAsActive:    true,
				ExecutionErr:        nil,
			}
		},
	)
	require.NoError(t, executable.Execute())

	executable = deps.newTestExecutable(func(p *executableParams) {
		p.priorityAssigner = queues.NewStaticPriorityAssigner(ctasks.PriorityPreemptable)
	})
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(ctx context.Context, _ queues.Executable) queues.ExecuteResponse {
			require.Equal(t, headers.CallerTypePreemptable, headers.GetCallerInfo(ctx).CallerType)
			return queues.ExecuteResponse{
				ExecutionMetricTags: nil,
				ExecutedAsActive:    true,
				ExecutionErr:        nil,
			}
		},
	)
	require.NoError(t, executable.Execute())
}

func TestExecuteHandleErr_ResetAttempt(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    true,
		ExecutionErr:        errors.New("some random error"),
	})
	err := executable.Execute()
	require.Error(t, err)
	require.Error(t, executable.HandleErr(err))
	require.Equal(t, 2, executable.Attempt())

	// isActive changed to false, should reset attempt
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        nil,
	})
	require.NoError(t, executable.Execute())
	require.Equal(t, 1, executable.Attempt())
}

func TestExecuteHandleErr_Corrupted(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return false
		}
	})

	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(_ context.Context, _ queues.Executable) queues.ExecuteResponse {
			panic(serialization.NewUnknownEncodingTypeError("unknownEncoding", enumspb.ENCODING_TYPE_PROTO3))
		},
	).Times(2)
	err := executable.Execute()
	require.Error(t, err)
	require.NoError(t, executable.HandleErr(err))
	require.Error(t, executable.Execute())
}

func TestExecute_SendToDLQAfterMaxAttempts(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.maxUnexpectedErrorAttempts = func() int {
			return 2
		}
	})
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        errors.New("some random error"),
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	require.Error(t, executable.HandleErr(err))

	// Attempt 2
	err = executable.Execute()
	err2 := executable.HandleErr(err)
	require.ErrorIs(t, err2, queues.ErrTerminalTaskFailure)
	require.NoError(t, executable.Execute())
	require.Len(t, queueWriter.EnqueueTaskRequests, 1)
}

func TestExecute_DontSendToDLQAfterMaxAttemptsDLQDisabled(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return false
		}
		p.maxUnexpectedErrorAttempts = func() int {
			return 1
		}
	})

	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        errors.New("some random error"),
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	require.Error(t, executable.HandleErr(err))

	// Attempt 2
	require.Error(t, executable.Execute())
	require.Error(t, executable.HandleErr(err))
	require.Empty(t, queueWriter.EnqueueTaskRequests)
}

func TestExecute_DontSendToDLQAfterMaxAttemptsExpectedError(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.maxUnexpectedErrorAttempts = func() int {
			return 1
		}
	})

	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr: &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "test",
		},
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	require.Error(t, executable.HandleErr(err))

	// Attempt 2
	require.Error(t, executable.Execute())
	require.Error(t, executable.HandleErr(err))
	require.Empty(t, queueWriter.EnqueueTaskRequests)
}

func TestExecute_SendToDLQAfterMaxAttemptsThenDisableDropCorruption(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	dlqEnabled := true
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return dlqEnabled
		}
		p.maxUnexpectedErrorAttempts = func() int {
			return 1
		}
	})
	execError := serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, errors.New("random error"))
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        execError,
	}).Times(1)

	err := executable.Execute()
	err2 := executable.HandleErr(err)

	require.ErrorIs(t, err2, queues.ErrTerminalTaskFailure)
	require.Contains(t, err2.Error(), execError.Error())

	dlqEnabled = false
	require.NoError(t, executable.Execute())
	require.Empty(t, queueWriter.EnqueueTaskRequests)
}

func TestExecute_SendToDLQAfterMaxAttemptsThenDisable(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	dlqEnabled := true
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return dlqEnabled
		}
		p.maxUnexpectedErrorAttempts = func() int {
			return 1
		}
	})
	execError := errors.New("some random error")
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        execError,
	}).Times(1)

	err := executable.Execute()
	err2 := executable.HandleErr(err)

	require.ErrorIs(t, err2, queues.ErrTerminalTaskFailure)

	dlqEnabled = false

	// Make sure task is retried this time
	execError2 := errors.New("some other random error")
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        execError2,
	}).Times(1)
	require.ErrorIs(t, executable.Execute(), execError2)
	require.Empty(t, queueWriter.EnqueueTaskRequests)
}

func TestExecute_SendsInternalErrorsToDLQ_WhenEnabled(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqInternalErrors = func() bool {
			return true
		}
	})

	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        serviceerror.NewInternal("injected error"),
	}).Times(1)

	err := executable.HandleErr(executable.Execute())
	require.ErrorIs(t, err, queues.ErrTerminalTaskFailure)
	require.NoError(t, executable.Execute())
	require.Len(t, queueWriter.EnqueueTaskRequests, 1)
}

func TestExecute_DoesntSendInternalErrorsToDLQ_WhenDisabled(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqInternalErrors = func() bool {
			return false
		}
	})

	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        serviceerror.NewInternal("injected error"),
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	require.Error(t, executable.HandleErr(err))

	// Attempt 2
	err = executable.Execute()
	require.Error(t, err)
	require.Error(t, executable.HandleErr(err))
	require.Empty(t, queueWriter.EnqueueTaskRequests)
}

func TestExecute_SendInternalErrorsToDLQ_ThenDisable(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	dlqEnabled := true
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return dlqEnabled
		}
		p.dlqInternalErrors = func() bool {
			return true
		}
	})

	injectedErr := serviceerror.NewInternal("injected error")
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        injectedErr,
	}).Times(2)

	require.ErrorIs(t, executable.HandleErr(executable.Execute()), queues.ErrTerminalTaskFailure)

	// The task should be dropped but not sent to DLQ
	dlqEnabled = false
	require.ErrorIs(t, executable.Execute(), injectedErr)
	require.Empty(t, queueWriter.EnqueueTaskRequests)
}

func TestExecute_DLQ(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
	})

	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(_ context.Context, _ queues.Executable) queues.ExecuteResponse {
			panic(serialization.NewUnknownEncodingTypeError("unknownEncoding", enumspb.ENCODING_TYPE_PROTO3))
		},
	)
	err := executable.Execute()
	require.Error(t, err)
	require.Error(t, executable.HandleErr(err))
	require.NoError(t, executable.Execute())
	require.Len(t, queueWriter.EnqueueTaskRequests, 1)
}

func TestExecute_DLQThenDisable(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	dlqEnabled := true
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return dlqEnabled
		}
	})

	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(_ context.Context, _ queues.Executable) queues.ExecuteResponse {
			panic(serialization.NewUnknownEncodingTypeError("unknownEncoding", enumspb.ENCODING_TYPE_PROTO3))
		},
	)
	err := executable.Execute()
	require.Error(t, err)
	require.Error(t, executable.HandleErr(err))
	dlqEnabled = false
	require.NoError(t, executable.Execute())
	require.Empty(t, queueWriter.EnqueueTaskRequests)
}

func TestExecute_DLQFailThenRetry(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
	})

	capture := deps.metricsHandler.StartCapture()
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(_ context.Context, _ queues.Executable) queues.ExecuteResponse {
			panic(serialization.NewUnknownEncodingTypeError("unknownEncoding", enumspb.ENCODING_TYPE_PROTO3))
		},
	)
	err := executable.Execute()
	require.Error(t, err)
	require.Error(t, executable.HandleErr(err))
	queueWriter.EnqueueTaskErr = errors.New("some random error")
	err = executable.Execute()
	require.Error(t, err)
	require.Error(t, executable.HandleErr(err))
	queueWriter.EnqueueTaskErr = nil
	err = executable.Execute()
	require.NoError(t, err)
	snapshot := capture.Snapshot()
	require.Len(t, snapshot[metrics.TaskTerminalFailures.Name()], 1)
	require.Len(t, snapshot[metrics.TaskDLQFailures.Name()], 1)
	require.Len(t, snapshot[metrics.TaskDLQSendLatency.Name()], 2)
}

func TestHandleErr_EntityNotExists(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	require.NoError(t, executable.HandleErr(serviceerror.NewNotFound("")))
}

func TestHandleErr_ErrTaskRetry(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	require.Equal(t, consts.ErrTaskRetry, executable.HandleErr(consts.ErrTaskRetry))
}

func TestHandleErr_ErrDeleteOpenExecution(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	require.Equal(t, consts.ErrDependencyTaskNotCompleted, executable.HandleErr(consts.ErrDependencyTaskNotCompleted))
}

func TestHandleErr_ErrTaskDiscarded(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	require.NoError(t, executable.HandleErr(consts.ErrTaskDiscarded))
}

func TestHandleErr_ErrTaskVersionMismatch(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	require.NoError(t, executable.HandleErr(consts.ErrTaskVersionMismatch))
}

func TestHandleErr_NamespaceNotActiveError(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	err := serviceerror.NewNamespaceNotActive("", "", "")

	require.Equal(t, err, deps.newTestExecutable().HandleErr(err))
}

func TestHandleErr_RandomErr(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	require.Error(t, executable.HandleErr(errors.New("random error")))
}

func TestTaskAck_ValidTask_NoRetry(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	require.Equal(t, ctasks.TaskStatePending, executable.State())

	capture := deps.metricsHandler.StartCapture()

	executable.Ack()
	require.Equal(t, ctasks.TaskStateAcked, executable.State())

	snapshot := capture.Snapshot()
	require.Len(t, snapshot[metrics.TaskAttempt.Name()], 1)
	require.Len(t, snapshot[metrics.TaskLatency.Name()], 1)
	require.Len(t, snapshot[metrics.TaskQueueLatency.Name()], 1)
}

func TestTaskAck_ValidTask_WithRetry(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	require.Equal(t, ctasks.TaskStatePending, executable.State())

	// For retried tasks, they are not considered invalid even
	// if their last attempt completed with a invalid task error.
	_ = executable.HandleErr(context.DeadlineExceeded)
	_ = executable.HandleErr(consts.ErrActivityNotFound)

	capture := deps.metricsHandler.StartCapture()

	executable.Ack()
	require.Equal(t, ctasks.TaskStateAcked, executable.State())

	snapshot := capture.Snapshot()
	require.Len(t, snapshot[metrics.TaskAttempt.Name()], 1)
	require.Len(t, snapshot[metrics.TaskLatency.Name()], 1)
	require.Len(t, snapshot[metrics.TaskQueueLatency.Name()], 1)
}

func TestTaskAck_InvalidTask(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	require.Equal(t, ctasks.TaskStatePending, executable.State())

	// This will mark the task as invalid
	_ = executable.HandleErr(consts.ErrActivityNotFound)

	capture := deps.metricsHandler.StartCapture()

	executable.Ack()
	require.Equal(t, ctasks.TaskStateAcked, executable.State())

	snapshot := capture.Snapshot()
	require.Empty(t, snapshot[metrics.TaskAttempt.Name()])
	require.Empty(t, snapshot[metrics.TaskLatency.Name()])
	require.Empty(t, snapshot[metrics.TaskQueueLatency.Name()])
}

func TestTaskNack_Resubmit_Success(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	deps.mockScheduler.EXPECT().TrySubmit(executable).DoAndReturn(func(_ queues.Executable) bool {
		require.Equal(t, ctasks.TaskStatePending, executable.State())

		go func() {
			// access internal state in a separate goroutine to check if there's any race condition
			// between reschdule and nack.
			accessInternalState(executable)
		}()
		return true
	})

	executable.Nack(errors.New("some random error"))
}

func TestTaskNack_Resubmit_Fail(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	deps.mockScheduler.EXPECT().TrySubmit(executable).Return(false)
	deps.mockRescheduler.EXPECT().Add(executable, gomock.AssignableToTypeOf(time.Now())).Do(func(_ queues.Executable, _ time.Time) {
		require.Equal(t, ctasks.TaskStatePending, executable.State())

		go func() {
			// access internal state in a separate goroutine to check if there's any race condition
			// between reschdule and nack.
			accessInternalState(executable)
		}()
	}).Times(1)

	executable.Nack(errors.New("some random error"))
}

func TestTaskNack_Reschedule(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		taskErr error
	}{
		{
			name:    "ErrTaskRetry",
			taskErr: consts.ErrTaskRetry, // this error won't trigger re-submit
		},
		{
			name:    "ErrDeleteOpenExecErr",
			taskErr: consts.ErrDependencyTaskNotCompleted, // this error won't trigger re-submit
		},
		{
			name:    "ErrNamespaceHandover",
			taskErr: consts.ErrNamespaceHandover, // this error won't trigger re-submit
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deps := setupExecutableTest(t)
			executable := deps.newTestExecutable()

			deps.mockRescheduler.EXPECT().Add(executable, gomock.AssignableToTypeOf(time.Now())).Do(func(_ queues.Executable, _ time.Time) {
				require.Equal(t, ctasks.TaskStatePending, executable.State())

				go func() {
					// access internal state in a separate goroutine to check if there's any race condition
					// between reschdule and nack.
					accessInternalState(executable)
				}()
			}).Times(1)

			executable.Nack(tc.taskErr)
		})
	}
}

func TestTaskAbort(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	executable.Abort()

	require.NoError(t, executable.Execute()) // should be no-op and won't invoke executor

	executable.Ack() // should be no-op
	require.Equal(t, ctasks.TaskStateAborted, executable.State())

	executable.Nack(errors.New("some random error")) // should be no-op and won't invoke scheduler or rescheduler

	executable.Reschedule() // should be no-op and won't invoke rescheduler

	// all error should be treated as non-retryable to break retry loop
	require.False(t, executable.IsRetryableError(errors.New("some random error")))
}

func TestTaskCancellation(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	executable := deps.newTestExecutable()

	executable.Cancel()

	require.NoError(t, executable.Execute()) // should be no-op and won't invoke executor

	executable.Ack() // should be no-op
	require.Equal(t, ctasks.TaskStateCancelled, executable.State())

	executable.Nack(errors.New("some random error")) // should be no-op and won't invoke scheduler or rescheduler

	executable.Reschedule() // should be no-op and won't invoke rescheduler

	// all error should be treated as non-retryable to break retry loop
	require.False(t, executable.IsRetryableError(errors.New("some random error")))
}

func TestExecute_SendToDLQErrPatternDoesNotMatch(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqErrorPattern = func() string {
			return "testpattern"
		}
	})
	executionError := errors.New("some random error")
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        executionError,
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	err2 := executable.HandleErr(err)
	require.Error(t, err2)
	require.NotErrorIs(t, err2, queues.ErrTerminalTaskFailure)
	require.Contains(t, err2.Error(), executionError.Error())

	// Attempt 2
	require.Error(t, executable.Execute())
	require.Error(t, executable.HandleErr(err))
	require.Empty(t, queueWriter.EnqueueTaskRequests)
}

func TestExecute_SendToDLQErrPatternEmptyString(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqErrorPattern = func() string {
			return ""
		}
	})
	executionError := errors.New("some random error")
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        executionError,
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	err2 := executable.HandleErr(err)
	require.Error(t, err2)
	require.NotErrorIs(t, err2, queues.ErrTerminalTaskFailure)
	require.Contains(t, err2.Error(), executionError.Error())

	// Attempt 2
	require.Error(t, executable.Execute())
	require.Error(t, executable.HandleErr(err))
	require.Empty(t, queueWriter.EnqueueTaskRequests)
}

func TestExecute_SendToDLQErrPatternMatchesMultiple(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable1 := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqErrorPattern = func() string {
			return "test substring 1|test substring 2"
		}
	})
	executionError1 := errors.New("some random error with test substring 1")
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable1).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        executionError1,
	}).Times(1)

	executable2 := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqErrorPattern = func() string {
			return "test substring 1|test substring 2"
		}
	})
	executionError2 := errors.New("some random error with test substring 2")
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable2).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        executionError2,
	}).Times(1)

	// Attempt 1
	err := executable1.Execute()
	err2 := executable1.HandleErr(err)
	require.Error(t, err2)
	require.ErrorIs(t, err2, queues.ErrTerminalTaskFailure)
	require.Contains(t, err2.Error(), executionError1.Error())

	err = executable2.Execute()
	err2 = executable2.HandleErr(err)
	require.Error(t, err2)
	require.ErrorIs(t, err2, queues.ErrTerminalTaskFailure)
	require.Contains(t, err2.Error(), executionError2.Error())

	// Attempt 2
	require.NoError(t, executable1.Execute())
	require.NoError(t, executable2.Execute())
	require.Len(t, queueWriter.EnqueueTaskRequests, 2)
}

func TestExecute_ErrPatternIfDLQDisabled(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return false
		}
		p.dlqErrorPattern = func() string {
			return "test substring"
		}
	})
	executionError := errors.New("some random error with test substring")
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        executionError,
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	err2 := executable.HandleErr(err)
	require.Error(t, err2)
	require.ErrorIs(t, err2, queues.ErrTerminalTaskFailure)
	require.Contains(t, err2.Error(), executionError.Error())

	// Attempt 2
	require.Error(t, executable.Execute())
	require.Error(t, executable.HandleErr(err))
	require.Empty(t, queueWriter.EnqueueTaskRequests)
}

func TestExecute_ErrorErrPatternThenDisableDLQ(t *testing.T) {
	t.Parallel()
	deps := setupExecutableTest(t)
	queueWriter := &queuestest.FakeQueueWriter{}
	dlqEnabled := true
	executable := deps.newTestExecutable(func(p *executableParams) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), deps.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return dlqEnabled
		}
		p.dlqErrorPattern = func() string {
			return "test substring"
		}
	})
	execError := errors.New("some random error with test substring")
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        execError,
	}).Times(1)

	err := executable.Execute()
	err2 := executable.HandleErr(err)

	require.ErrorIs(t, err2, queues.ErrTerminalTaskFailure)

	dlqEnabled = false

	// Make sure task is retried this time
	execError2 := errors.New("some other random error")
	deps.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        execError2,
	}).Times(1)
	require.ErrorIs(t, executable.Execute(), execError2)
	require.Empty(t, queueWriter.EnqueueTaskRequests)
}
