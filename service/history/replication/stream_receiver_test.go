package replication

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	streamReceiverTestDeps struct {
		controller              *gomock.Controller
		clusterMetadata         *cluster.MockMetadata
		highPriorityTaskTracker *MockExecutableTaskTracker
		lowPriorityTaskTracker  *MockExecutableTaskTracker
		stream                  *mockStream
		taskScheduler           *mockScheduler
		streamReceiver          *StreamReceiverImpl
		receiverFlowController  *MockReceiverFlowController
	}

	mockStream struct {
		requests []*adminservice.StreamWorkflowReplicationMessagesRequest
		respChan chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]
		closed   bool
	}
	mockScheduler struct {
		tasks []TrackableExecutableTask
	}
)

func setupStreamReceiverTest(t *testing.T) *streamReceiverTestDeps {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	highPriorityTaskTracker := NewMockExecutableTaskTracker(controller)
	lowPriorityTaskTracker := NewMockExecutableTaskTracker(controller)
	stream := &mockStream{
		requests: nil,
		respChan: make(chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse], 100),
	}
	taskScheduler := &mockScheduler{
		tasks: nil,
	}

	processToolBox := ProcessToolBox{
		ClusterMetadata:           clusterMetadata,
		HighPriorityTaskScheduler: taskScheduler,
		LowPriorityTaskScheduler:  taskScheduler,
		MetricsHandler:            metrics.NoopMetricsHandler,
		Logger:                    log.NewTestLogger(),
		DLQWriter:                 NoopDLQWriter{},
	}
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, gomock.Any()).Return("some-cluster-name").AnyTimes()
	streamReceiver := NewStreamReceiver(
		processToolBox,
		NewExecutableTaskConverter(processToolBox),
		NewClusterShardKey(rand.Int31(), rand.Int31()),
		NewClusterShardKey(rand.Int31(), rand.Int31()),
	)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(
		map[string]cluster.ClusterInformation{
			uuid.New().String(): {
				Enabled:                true,
				InitialFailoverVersion: int64(streamReceiver.clientShardKey.ClusterID),
			},
			uuid.New().String(): {
				Enabled:                true,
				InitialFailoverVersion: int64(streamReceiver.serverShardKey.ClusterID),
			},
		},
	).AnyTimes()
	streamReceiver.highPriorityTaskTracker = highPriorityTaskTracker
	streamReceiver.lowPriorityTaskTracker = lowPriorityTaskTracker
	stream.requests = []*adminservice.StreamWorkflowReplicationMessagesRequest{}
	receiverFlowController := NewMockReceiverFlowController(controller)
	streamReceiver.flowController = receiverFlowController

	return &streamReceiverTestDeps{
		controller:              controller,
		clusterMetadata:         clusterMetadata,
		highPriorityTaskTracker: highPriorityTaskTracker,
		lowPriorityTaskTracker:  lowPriorityTaskTracker,
		stream:                  stream,
		taskScheduler:           taskScheduler,
		streamReceiver:          streamReceiver,
		receiverFlowController:  receiverFlowController,
	}
}

func TestAckMessage_Noop(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	deps.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	deps.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	deps.highPriorityTaskTracker.EXPECT().Size().Return(0)
	deps.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	deps.streamReceiver.ackMessage(deps.stream)

	require.Equal(t, 0, len(deps.stream.requests))
}

func TestAckMessage_SyncStatus_ReceiverModeUnset(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	deps.streamReceiver.receiverMode = ReceiverModeUnset // when stream receiver is in unset mode, means no task received yet, so no ACK should be sent
	deps.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	deps.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	deps.highPriorityTaskTracker.EXPECT().Size().Return(0)
	deps.lowPriorityTaskTracker.EXPECT().Size().Return(0)
	_, err := deps.streamReceiver.ackMessage(deps.stream)
	require.Equal(t, 0, len(deps.stream.requests))
	require.NoError(t, err)
}

func TestAckMessage_SyncStatus_ReceiverModeSingleStack(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}

	deps.streamReceiver.receiverMode = ReceiverModeSingleStack
	deps.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	deps.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	deps.highPriorityTaskTracker.EXPECT().Size().Return(0)
	deps.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	_, err := deps.streamReceiver.ackMessage(deps.stream)
	require.NoError(t, err)
	require.Equal(t, []*adminservice.StreamWorkflowReplicationMessagesRequest{{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &replicationspb.SyncReplicationState{
				InclusiveLowWatermark:     watermarkInfo.Watermark,
				InclusiveLowWatermarkTime: timestamppb.New(watermarkInfo.Timestamp),
			},
		},
	},
	}, deps.stream.requests)
}

func TestAckMessage_SyncStatus_ReceiverModeSingleStack_NoHighPriorityWatermark(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}

	deps.streamReceiver.receiverMode = ReceiverModeSingleStack
	deps.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	deps.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	deps.highPriorityTaskTracker.EXPECT().Size().Return(0)
	deps.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	_, err := deps.streamReceiver.ackMessage(deps.stream)
	require.Error(t, err)
	require.Equal(t, 0, len(deps.stream.requests))
}

func TestAckMessage_SyncStatus_ReceiverModeSingleStack_HasBothWatermark(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}

	deps.streamReceiver.receiverMode = ReceiverModeSingleStack
	deps.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	deps.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	deps.highPriorityTaskTracker.EXPECT().Size().Return(0)
	deps.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	_, err := deps.streamReceiver.ackMessage(deps.stream)
	require.Error(t, err)
	require.Equal(t, 0, len(deps.stream.requests))
}

func TestAckMessage_SyncStatus_ReceiverModeTieredStack_NoHighPriorityWatermark(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	deps.streamReceiver.receiverMode = ReceiverModeTieredStack
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}
	deps.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	deps.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	deps.highPriorityTaskTracker.EXPECT().Size().Return(0)
	deps.lowPriorityTaskTracker.EXPECT().Size().Return(0)
	_, err := deps.streamReceiver.ackMessage(deps.stream)
	require.Equal(t, 0, len(deps.stream.requests))
	require.NoError(t, err)
}

func TestAckMessage_SyncStatus_ReceiverModeTieredStack_NoLowPriorityWatermark(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	deps.streamReceiver.receiverMode = ReceiverModeTieredStack
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}
	deps.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	deps.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	deps.highPriorityTaskTracker.EXPECT().Size().Return(0)
	deps.lowPriorityTaskTracker.EXPECT().Size().Return(0)
	_, err := deps.streamReceiver.ackMessage(deps.stream)
	require.Equal(t, 0, len(deps.stream.requests))
	require.NoError(t, err)
}

func TestAckMessage_SyncStatus_ReceiverModeTieredStack(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	deps.streamReceiver.receiverMode = ReceiverModeTieredStack
	highWatermarkInfo := &WatermarkInfo{
		Watermark: 10,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	lowWatermarkInfo := &WatermarkInfo{
		Watermark: 11,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	deps.highPriorityTaskTracker.EXPECT().LowWatermark().Return(highWatermarkInfo)
	deps.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(lowWatermarkInfo)
	deps.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_HIGH).Return(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME)
	deps.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW).Return(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE)
	deps.highPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()
	deps.lowPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()
	_, err := deps.streamReceiver.ackMessage(deps.stream)
	require.NoError(t, err)
	require.Equal(t, []*adminservice.StreamWorkflowReplicationMessagesRequest{{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &replicationspb.SyncReplicationState{
				InclusiveLowWatermark:     highWatermarkInfo.Watermark,
				InclusiveLowWatermarkTime: timestamppb.New(highWatermarkInfo.Timestamp),
				HighPriorityState: &replicationspb.ReplicationState{
					InclusiveLowWatermark:     highWatermarkInfo.Watermark,
					InclusiveLowWatermarkTime: timestamppb.New(highWatermarkInfo.Timestamp),
					FlowControlCommand:        enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME,
				},
				LowPriorityState: &replicationspb.ReplicationState{
					InclusiveLowWatermark:     lowWatermarkInfo.Watermark,
					InclusiveLowWatermarkTime: timestamppb.New(lowWatermarkInfo.Timestamp),
					FlowControlCommand:        enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
				},
			},
		},
	},
	}, deps.stream.requests)
}

func TestProcessMessage_TrackSubmit_SingleStack(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_LOW,
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
				},
			},
		},
		Err: nil,
	}
	deps.stream.respChan <- streamResp
	close(deps.stream.respChan)

	deps.highPriorityTaskTracker.EXPECT().TrackTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask {
			require.Equal(t, streamResp.Resp.GetMessages().ExclusiveHighWatermark, highWatermarkInfo.Watermark)
			require.Equal(t, streamResp.Resp.GetMessages().ExclusiveHighWatermarkTime.AsTime(), highWatermarkInfo.Timestamp)
			require.Equal(t, 1, len(tasks))
			require.IsType(t, &ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)

	err := deps.streamReceiver.processMessages(deps.stream)
	require.NoError(t, err)
	require.Equal(t, 1, len(deps.taskScheduler.tasks))
	require.IsType(t, &ExecutableUnknownTask{}, deps.taskScheduler.tasks[0])
	require.Equal(t, ReceiverModeSingleStack, deps.streamReceiver.receiverMode)
}

func TestProcessMessage_TrackSubmit_SingleStack_ReceivedPrioritizedTask(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	deps.streamReceiver.receiverMode = ReceiverModeSingleStack
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
				},
			},
		},
		Err: nil,
	}
	deps.stream.respChan <- streamResp

	// no TrackTasks call should be made
	err := deps.streamReceiver.processMessages(deps.stream)
	require.IsType(t, &StreamError{}, err)
	require.Equal(t, 0, len(deps.taskScheduler.tasks))
}

func TestProcessMessage_TrackSubmit_TieredStack_ReceivedNonPrioritizedTask(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	deps.streamReceiver.receiverMode = ReceiverModeTieredStack
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
				},
			},
		},
		Err: nil,
	}
	deps.stream.respChan <- streamResp

	// no TrackTasks call should be made
	err := deps.streamReceiver.processMessages(deps.stream)
	require.IsType(t, &StreamError{}, err)
	require.Equal(t, 0, len(deps.taskScheduler.tasks))
}

func TestProcessMessage_TrackSubmit_TieredStack(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	streamResp1 := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
				},
			},
		},
		Err: nil,
	}
	streamResp2 := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks: []*replicationspb.ReplicationTask{
						{
							TaskType:       enumsspb.ReplicationTaskType(-1),
							SourceTaskId:   rand.Int63(),
							VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
							Priority:       enumsspb.TASK_PRIORITY_LOW,
						},
					},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
					Priority:                   enumsspb.TASK_PRIORITY_LOW,
				},
			},
		},
		Err: nil,
	}
	deps.stream.respChan <- streamResp1
	deps.stream.respChan <- streamResp2
	close(deps.stream.respChan)

	deps.highPriorityTaskTracker.EXPECT().TrackTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask {
			require.Equal(t, streamResp1.Resp.GetMessages().ExclusiveHighWatermark, highWatermarkInfo.Watermark)
			require.Equal(t, streamResp1.Resp.GetMessages().ExclusiveHighWatermarkTime.AsTime(), highWatermarkInfo.Timestamp)
			require.Equal(t, 1, len(tasks))
			require.IsType(t, &ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)
	deps.lowPriorityTaskTracker.EXPECT().TrackTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask {
			require.Equal(t, streamResp2.Resp.GetMessages().ExclusiveHighWatermark, highWatermarkInfo.Watermark)
			require.Equal(t, streamResp2.Resp.GetMessages().ExclusiveHighWatermarkTime.AsTime(), highWatermarkInfo.Timestamp)
			require.Equal(t, 1, len(tasks))
			require.IsType(t, &ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)

	err := deps.streamReceiver.processMessages(deps.stream)
	require.NoError(t, err)
	require.Equal(t, 2, len(deps.taskScheduler.tasks))
	require.Equal(t, ReceiverModeTieredStack, deps.streamReceiver.receiverMode)
}

func TestGetTaskScheduler(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	high := &mockScheduler{}
	low := &mockScheduler{}
	deps.streamReceiver.ProcessToolBox.HighPriorityTaskScheduler = high
	deps.streamReceiver.ProcessToolBox.LowPriorityTaskScheduler = low

	tests := []struct {
		name         string
		priority     enumsspb.TaskPriority
		task         TrackableExecutableTask
		expected     ctasks.Scheduler[TrackableExecutableTask]
		expectErr    bool
		errorMessage string
	}{
		{
			name:     "Unspecified priority with ExecutableWorkflowStateTask",
			priority: enumsspb.TASK_PRIORITY_UNSPECIFIED,
			task:     &ExecutableWorkflowStateTask{},
			expected: low,
		},
		{
			name:     "Unspecified priority with other task",
			priority: enumsspb.TASK_PRIORITY_UNSPECIFIED,
			task:     &ExecutableHistoryTask{},
			expected: high,
		},
		{
			name:     "High priority",
			priority: enumsspb.TASK_PRIORITY_HIGH,
			task:     &ExecutableHistoryTask{},
			expected: high,
		},
		{
			name:     "Low priority",
			priority: enumsspb.TASK_PRIORITY_LOW,
			task:     &ExecutableWorkflowStateTask{},
			expected: low,
		},
		{
			name:         "Invalid priority",
			priority:     enumsspb.TaskPriority(999),
			task:         &ExecutableHistoryTask{},
			expectErr:    true,
			errorMessage: "InvalidArgument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheduler, err := deps.streamReceiver.getTaskScheduler(tt.priority, tt.task)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.True(t, scheduler == tt.expected, "Expected scheduler to match")
			}
		})
	}
}

func TestProcessMessage_Err(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: nil,
		Err:  serviceerror.NewUnavailable("random recv error"),
	}
	deps.stream.respChan <- streamResp
	close(deps.stream.respChan)

	err := deps.streamReceiver.processMessages(deps.stream)
	require.Error(t, err)
}

func TestSendEventLoop_Panic_Captured(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	deps.streamReceiver.sendEventLoop() // should not cause panic
}

func TestRecvEventLoop_Panic_Captured(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	deps.streamReceiver.recvEventLoop() // should not cause panic
}

func TestLivenessMonitor(t *testing.T) {
	deps := setupStreamReceiverTest(t)
	defer deps.controller.Finish()

	livenessMonitor(
		deps.streamReceiver.recvSignalChan,
		dynamicconfig.GetDurationPropertyFn(time.Second),
		dynamicconfig.GetIntPropertyFn(1),
		deps.streamReceiver.shutdownChan,
		deps.streamReceiver.Stop,
		deps.streamReceiver.logger,
	)
	require.False(t, deps.streamReceiver.IsValid())
}

func (s *mockStream) Send(
	req *adminservice.StreamWorkflowReplicationMessagesRequest,
) error {
	s.requests = append(s.requests, req)
	return nil
}

func (s *mockStream) Recv() (<-chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse], error) {
	return s.respChan, nil
}

func (s *mockStream) Close() {
	s.closed = true
}

func (s *mockStream) IsValid() bool {
	return !s.closed
}

func (s *mockScheduler) Submit(task TrackableExecutableTask) {
	s.tasks = append(s.tasks, task)
}

func (s *mockScheduler) TrySubmit(task TrackableExecutableTask) bool {
	s.tasks = append(s.tasks, task)
	return true
}

func (s *mockScheduler) Start() {}
func (s *mockScheduler) Stop()  {}
