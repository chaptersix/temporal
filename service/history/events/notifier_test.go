package events

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
)

type notifierTestDeps struct {
	notifier *NotifierImpl
}

func setupNotifierTest(t *testing.T) *notifierTestDeps {
	notifier := NewNotifier(
		clock.NewRealTimeSource(),
		metrics.NoopMetricsHandler,
		func(namespaceID namespace.ID, workflowID string) int32 {
			key := namespaceID.String() + "_" + workflowID
			return int32(len(key))
		},
	)
	notifier.Start()

	t.Cleanup(func() {
		notifier.Stop()
	})

	return &notifierTestDeps{
		notifier: notifier,
	}
}

func TestSingleSubscriberWatchingEvents(t *testing.T) {
	deps := setupNotifierTest(t)

	namespaceID := "namespace ID"
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "workflow ID",
		RunId:      "run ID",
	}
	lastFirstEventID := int64(3)
	lastFirstEventTxnID := int64(398)
	previousStartedEventID := int64(5)
	nextEventID := int64(18)
	workflowState := enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	workflowStatus := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	branchToken := make([]byte, 0)
	versionHistoryItem := versionhistory.NewVersionHistoryItem(nextEventID-1, 1)
	currentVersionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{versionHistoryItem})
	versionHistories := versionhistory.NewVersionHistories(currentVersionHistory)
	transitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 1234, TransitionCount: 1024},
		{TransitionCount: 1025},
	}
	historyEvent := NewNotification(namespaceID, execution, lastFirstEventID, lastFirstEventTxnID, nextEventID, previousStartedEventID, workflowState, workflowStatus, versionHistories, transitionHistory)
	timerChan := time.NewTimer(time.Second * 2).C

	subscriberID, channel, err := deps.notifier.WatchHistoryEvent(definition.NewWorkflowKey(namespaceID, execution.GetWorkflowId(), execution.GetRunId()))
	require.Nil(t, err)

	go func() {
		<-timerChan
		deps.notifier.NotifyNewHistoryEvent(historyEvent)
	}()

	msg := <-channel
	require.Equal(t, historyEvent, msg)

	err = deps.notifier.UnwatchHistoryEvent(definition.NewWorkflowKey(namespaceID, execution.GetWorkflowId(), execution.GetRunId()), subscriberID)
	require.Nil(t, err)
}

func TestMultipleSubscriberWatchingEvents(t *testing.T) {
	deps := setupNotifierTest(t)

	namespaceID := "namespace ID"
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "workflow ID",
		RunId:      "run ID",
	}

	lastFirstEventID := int64(3)
	lastFirstEventTxnID := int64(3980)
	previousStartedEventID := int64(5)
	nextEventID := int64(18)
	workflowState := enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	workflowStatus := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	branchToken := make([]byte, 0)
	versionHistoryItem := versionhistory.NewVersionHistoryItem(nextEventID-1, 1)
	currentVersionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{versionHistoryItem})
	versionHistories := versionhistory.NewVersionHistories(currentVersionHistory)
	transitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 1234, TransitionCount: 1024},
		{TransitionCount: 1025},
	}
	historyEvent := NewNotification(namespaceID, execution, lastFirstEventID, lastFirstEventTxnID, nextEventID, previousStartedEventID, workflowState, workflowStatus, versionHistories, transitionHistory)
	timerChan := time.NewTimer(time.Second * 5).C

	subscriberCount := 100
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(subscriberCount)

	watchFunc := func() {
		subscriberID, channel, err := deps.notifier.WatchHistoryEvent(definition.NewWorkflowKey(namespaceID, execution.GetWorkflowId(), execution.GetRunId()))
		require.Nil(t, err)

		timeourChan := time.NewTimer(time.Second * 10).C

		select {
		case msg := <-channel:
			require.Equal(t, historyEvent, msg)
		case <-timeourChan:
			require.Fail(t, "subscribe to new events timeout")
		}
		err = deps.notifier.UnwatchHistoryEvent(definition.NewWorkflowKey(namespaceID, execution.GetWorkflowId(), execution.GetRunId()), subscriberID)
		require.Nil(t, err)
		waitGroup.Done()
	}

	for count := 0; count < subscriberCount; count++ {
		go watchFunc()
	}

	<-timerChan
	deps.notifier.NotifyNewHistoryEvent(historyEvent)
	waitGroup.Wait()
}
