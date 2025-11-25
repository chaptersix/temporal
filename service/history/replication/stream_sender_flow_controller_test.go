package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/mock/gomock"
)

func setupSenderFlowController(t *testing.T) (*gomock.Controller, *SenderFlowControllerImpl, *quotas.MockRateLimiter) {
	controller := gomock.NewController(t)
	logger := log.NewTestLogger()
	config := &configs.Config{
		ReplicationStreamSenderHighPriorityQPS: func() int { return 10 },
		ReplicationStreamSenderLowPriorityQPS:  func() int { return 5 },
	}
	senderFlowCtrlImpl := NewSenderFlowController(config, logger)
	mockRateLimiter := quotas.NewMockRateLimiter(controller)
	return controller, senderFlowCtrlImpl, mockRateLimiter
}

func TestWait_HighPriority(t *testing.T) {
	controller, senderFlowCtrlImpl, mockRateLimiter := setupSenderFlowController(t)
	defer controller.Finish()

	state := senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_HIGH]
	state.rateLimiter = mockRateLimiter

	mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_HIGH)
		require.NoError(t, err)
	}()

	wg.Wait()
}

func TestWait_Error(t *testing.T) {
	controller, senderFlowCtrlImpl, mockRateLimiter := setupSenderFlowController(t)
	defer controller.Finish()

	state := senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_HIGH]
	state.rateLimiter = mockRateLimiter

	mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(context.Canceled)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_HIGH)
		require.Error(t, err)
	}()

	wg.Wait()
}

func TestWait_LowPriority(t *testing.T) {
	controller, senderFlowCtrlImpl, mockRateLimiter := setupSenderFlowController(t)
	defer controller.Finish()

	state := senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_LOW]
	state.rateLimiter = mockRateLimiter

	mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_LOW)
		require.NoError(t, err)
	}()

	wg.Wait()
}

func TestWait_DefaultPriority(t *testing.T) {
	controller, senderFlowCtrlImpl, mockRateLimiter := setupSenderFlowController(t)
	defer controller.Finish()

	senderFlowCtrlImpl.defaultRateLimiter = mockRateLimiter

	mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_UNSPECIFIED)
		require.NoError(t, err)
	}()

	wg.Wait()
}

func TestRefreshReceiverFlowControlInfo(t *testing.T) {
	controller, _, _ := setupSenderFlowController(t)
	defer controller.Finish()

	logger := log.NewTestLogger()
	config := &configs.Config{
		ReplicationStreamSenderHighPriorityQPS: func() int { return 10 },
		ReplicationStreamSenderLowPriorityQPS:  func() int { return 5 },
	}
	senderFlowCtrlImpl := NewSenderFlowController(config, logger)
	state := &replicationspb.SyncReplicationState{
		HighPriorityState: &replicationspb.ReplicationState{
			FlowControlCommand: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME,
		},
		LowPriorityState: &replicationspb.ReplicationState{
			FlowControlCommand: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
		},
	}

	senderFlowCtrlImpl.RefreshReceiverFlowControlInfo(state)

	require.True(t, senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_HIGH].resume)
	require.False(t, senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_LOW].resume)
}

func TestPauseToResume(t *testing.T) {
	controller, senderFlowCtrlImpl, mockRateLimiter := setupSenderFlowController(t)
	defer controller.Finish()

	state := senderFlowCtrlImpl.flowControlStates[enumsspb.TASK_PRIORITY_HIGH]
	state.rateLimiter = mockRateLimiter

	// Set initial state to paused
	state.mu.Lock()
	state.resume = false
	state.mu.Unlock()
	mockRateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := senderFlowCtrlImpl.Wait(context.Background(), enumsspb.TASK_PRIORITY_HIGH)
		require.NoError(t, err)
	}()

	// Ensure the goroutine has time to start and block
	assert.Eventually(t, func() bool {
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.waiters == 1
	}, 1*time.Second, 100*time.Millisecond)

	require.Equal(t, 1, state.waiters)

	// Transition from paused to resumed
	senderFlowCtrlImpl.setState(state, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME)
	wg.Wait()

	require.Equal(t, 0, state.waiters)
	require.True(t, state.resume)
}
