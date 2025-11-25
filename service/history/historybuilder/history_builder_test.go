package historybuilder

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultNamespace = "default"

type historyBuilderTestDeps struct {
	now            time.Time
	version        int64
	nextEventID    int64
	nextTaskID     int64
	mockTimeSource *clock.EventTimeSource
	historyBuilder *HistoryBuilder
}

func setupHistoryBuilderTest(t *testing.T) *historyBuilderTestDeps {
	now := time.Now().UTC()
	version := rand.Int63()
	nextEventID := rand.Int63()
	nextTaskID := rand.Int63()
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(now)

	deps := &historyBuilderTestDeps{
		now:            now,
		version:        version,
		nextEventID:    nextEventID,
		nextTaskID:     nextTaskID,
		mockTimeSource: mockTimeSource,
	}

	taskIDGen := func(number int) ([]int64, error) {
		return taskIDGenerator(deps, number)
	}

	deps.historyBuilder = New(
		mockTimeSource,
		taskIDGen,
		version,
		nextEventID,
		nil,
		metrics.NoopMetricsHandler,
	)

	return deps
}

var (
	testNamespaceID   = namespace.ID(uuid.New())
	testNamespaceName = namespace.Name("test namespace")
	testWorkflowID    = "test workflow ID"
	testRunID         = uuid.New()

	testParentNamespaceID      = uuid.New()
	testParentNamespaceName    = "test parent namespace"
	testParentWorkflowID       = "test parent workflow ID"
	testParentRunID            = uuid.New()
	testParentInitiatedID      = rand.Int63()
	testParentInitiatedVersion = rand.Int63()

	testRootWorkflowID = "test root workflow ID"
	testRootRunID      = uuid.New()

	testIdentity  = "test identity"
	testRequestID = uuid.New()

	testPayload = &commonpb.Payload{
		Metadata: map[string][]byte{
			"random metadata key": []byte("random metadata value"),
		},
		Data: []byte("random data"),
	}
	testPayloads     = &commonpb.Payloads{Payloads: []*commonpb.Payload{testPayload}}
	testWorkflowType = &commonpb.WorkflowType{
		Name: "test workflow type",
	}
	testActivityType = &commonpb.ActivityType{
		Name: "test activity type",
	}
	testTaskQueue = &taskqueuepb.TaskQueue{
		Name: "test task queue",
		Kind: enumspb.TaskQueueKind(rand.Int31n(int32(len(enumspb.TaskQueueKind_name)))),
	}
	testRetryPolicy = &commonpb.RetryPolicy{
		InitialInterval:        durationpb.New(time.Duration(rand.Int63())),
		BackoffCoefficient:     rand.Float64(),
		MaximumAttempts:        rand.Int31(),
		MaximumInterval:        durationpb.New(time.Duration(rand.Int63())),
		NonRetryableErrorTypes: []string{"test non retryable error type"},
	}
	testCronSchedule = "12 * * * *"
	testMemo         = &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"random memo key": testPayload,
		},
	}
	testSearchAttributes = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"random search attribute key": testPayload,
		},
	}
	testHeader = &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"random header key": testPayload,
		},
	}
	testLink = &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  "handler-ns",
				WorkflowId: "handler-wf-id",
				RunId:      "handler-run-id",
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
		},
	}
	testFailure       = &failurepb.Failure{}
	testRequestReason = "test request reason"
)

/* workflow */
func TestWorkflowExecutionStarted(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	attempt := rand.Int31()
	workflowExecutionExpirationTime := timestamppb.New(time.Unix(0, rand.Int63()))
	continueAsNewInitiator := enumspb.ContinueAsNewInitiator(rand.Int31n(int32(len(enumspb.ContinueAsNewInitiator_name))))
	firstWorkflowTaskBackoff := durationpb.New(time.Duration(rand.Int63()))

	workflowExecutionTimeout := durationpb.New(time.Duration(rand.Int63()))
	workflowRunTimeout := durationpb.New(time.Duration(rand.Int63()))
	workflowTaskStartToCloseTimeout := durationpb.New(time.Duration(rand.Int63()))

	resetPoints := &workflowpb.ResetPoints{}
	prevRunID := uuid.New()
	firstRunID := uuid.New()
	originalRunID := uuid.New()

	request := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId: testNamespaceID.String(),
		ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
			NamespaceId: testParentNamespaceID,
			Namespace:   testParentNamespaceName,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: testParentWorkflowID,
				RunId:      testParentRunID,
			},
			InitiatedId:      testParentInitiatedID,
			InitiatedVersion: testParentInitiatedVersion,
		},
		Attempt:                         attempt,
		WorkflowExecutionExpirationTime: workflowExecutionExpirationTime,
		ContinueAsNewInitiator:          continueAsNewInitiator,
		ContinuedFailure:                testFailure,
		LastCompletionResult:            testPayloads,
		FirstWorkflowTaskBackoff:        firstWorkflowTaskBackoff,
		RootExecutionInfo: &workflowspb.RootExecutionInfo{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: testRootWorkflowID,
				RunId:      testRootRunID,
			},
		},

		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                testNamespaceName.String(),
			WorkflowId:               testWorkflowID,
			WorkflowType:             testWorkflowType,
			TaskQueue:                testTaskQueue,
			Input:                    testPayloads,
			WorkflowExecutionTimeout: workflowExecutionTimeout,
			WorkflowRunTimeout:       workflowRunTimeout,
			WorkflowTaskTimeout:      workflowTaskStartToCloseTimeout,
			Identity:                 testIdentity,
			RequestId:                testRequestID,
			// WorkflowIdReusePolicy: not used for event generation
			RetryPolicy:      testRetryPolicy,
			CronSchedule:     testCronSchedule,
			Memo:             testMemo,
			SearchAttributes: testSearchAttributes,
			Header:           testHeader,
			Links:            []*commonpb.Link{testLink},
		},
	}

	event := deps.historyBuilder.AddWorkflowExecutionStartedEvent(
		deps.now,
		request,
		resetPoints,
		prevRunID,
		firstRunID,
		originalRunID,
	)
	require.Equal(t, event, flush(t, deps))
	protorequire.ProtoEqual(
		t,
		&historypb.HistoryEvent{
			EventId:   deps.nextEventID,
			TaskId:    deps.nextTaskID,
			EventTime: timestamppb.New(deps.now),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Version:   deps.version,
			Links:     []*commonpb.Link{testLink},
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType:                    testWorkflowType,
					TaskQueue:                       testTaskQueue,
					Header:                          testHeader,
					Input:                           testPayloads,
					WorkflowRunTimeout:              workflowRunTimeout,
					WorkflowExecutionTimeout:        workflowExecutionTimeout,
					WorkflowTaskTimeout:             workflowTaskStartToCloseTimeout,
					ContinuedExecutionRunId:         prevRunID,
					PrevAutoResetPoints:             resetPoints,
					Identity:                        testIdentity,
					RetryPolicy:                     testRetryPolicy,
					Attempt:                         attempt,
					WorkflowExecutionExpirationTime: workflowExecutionExpirationTime,
					CronSchedule:                    testCronSchedule,
					LastCompletionResult:            testPayloads,
					ContinuedFailure:                testFailure,
					Initiator:                       continueAsNewInitiator,
					FirstWorkflowTaskBackoff:        firstWorkflowTaskBackoff,
					FirstExecutionRunId:             firstRunID,
					OriginalExecutionRunId:          originalRunID,
					Memo:                            testMemo,
					SearchAttributes:                testSearchAttributes,
					WorkflowId:                      testWorkflowID,

					ParentWorkflowNamespace:   testParentNamespaceName,
					ParentWorkflowNamespaceId: testParentNamespaceID,
					ParentWorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: testParentWorkflowID,
						RunId:      testParentRunID,
					},
					ParentInitiatedEventId:      testParentInitiatedID,
					ParentInitiatedEventVersion: testParentInitiatedVersion,

					RootWorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: testRootWorkflowID,
						RunId:      testRootRunID,
					},
				},
			},
		},
		event,
	)
}

func TestWorkflowExecutionCancelRequested(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	initiatedEventID := rand.Int63()
	request := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			// Namespace: not used for test
			// WorkflowExecution: not used for test
			// FirstExecutionRunId: not used for test
			Identity:  testIdentity,
			RequestId: testRequestID,
			Reason:    testRequestReason,
		},
		ExternalInitiatedEventId: initiatedEventID,
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: testParentWorkflowID,
			RunId:      testParentRunID,
		},
		// ChildWorkflowOnly: not used for test
	}

	event := deps.historyBuilder.AddWorkflowExecutionCancelRequestedEvent(
		request,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
			WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
				Cause:                    testRequestReason,
				Identity:                 testIdentity,
				ExternalInitiatedEventId: initiatedEventID,
				ExternalWorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testParentWorkflowID,
					RunId:      testParentRunID,
				},
			},
		},
	}, event)
}

func TestWorkflowExecutionSignaled(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	signalName := "random signal name"
	event := deps.historyBuilder.AddWorkflowExecutionSignaledEvent(
		signalName, testPayloads, testIdentity, testHeader, nil, nil,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: signalName,
				Input:      testPayloads,
				Identity:   testIdentity,
				Header:     testHeader,
			},
		},
	}, event)
}

func TestWorkflowExecutionMarkerRecord(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.RecordMarkerCommandAttributes{
		MarkerName: "random marker name",
		Details: map[string]*commonpb.Payloads{
			"random marker details key": testPayloads,
		},
		Header:  testHeader,
		Failure: testFailure,
	}
	event := deps.historyBuilder.AddMarkerRecordedEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{
			MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				MarkerName:                   attributes.MarkerName,
				Details:                      attributes.Details,
				Header:                       attributes.Header,
				Failure:                      attributes.Failure,
			},
		},
	}, event)
}

func TestWorkflowExecutionSearchAttribute(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{
		SearchAttributes: testSearchAttributes,
	}
	event := deps.historyBuilder.AddUpsertWorkflowSearchAttributesEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{
			UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				SearchAttributes:             attributes.SearchAttributes,
			},
		},
	}, event)
}

func TestWorkflowExecutionMemo(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.ModifyWorkflowPropertiesCommandAttributes{
		UpsertedMemo: testMemo,
	}
	event := deps.historyBuilder.AddWorkflowPropertiesModifiedEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowPropertiesModifiedEventAttributes{
			WorkflowPropertiesModifiedEventAttributes: &historypb.WorkflowPropertiesModifiedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				UpsertedMemo:                 attributes.UpsertedMemo,
			},
		},
	}, event)
}

func TestWorkflowExecutionCompleted(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.CompleteWorkflowExecutionCommandAttributes{
		Result: testPayloads,
	}
	event := deps.historyBuilder.AddCompletedWorkflowEvent(
		workflowTaskCompletionEventID,
		attributes,
		"",
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Result:                       attributes.Result,
			},
		},
	}, event)
}

func TestWorkflowExecutionFailed(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	attributes := &commandpb.FailWorkflowExecutionCommandAttributes{
		Failure: testFailure,
	}
	event, batchID := deps.historyBuilder.AddFailWorkflowEvent(
		workflowTaskCompletionEventID,
		retryState,
		attributes,
		"",
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, batchID, event.EventId)
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Failure:                      attributes.Failure,
				RetryState:                   retryState,
			},
		},
	}, event)
}

func TestWorkflowExecutionTimeout(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := deps.historyBuilder.AddTimeoutWorkflowEvent(
		retryState,
		"",
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
			WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
				RetryState: retryState,
			},
		},
	}, event)
}

func TestWorkflowExecutionCancelled(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.CancelWorkflowExecutionCommandAttributes{
		Details: testPayloads,
	}
	event := deps.historyBuilder.AddWorkflowExecutionCanceledEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
			WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Details:                      attributes.Details,
			},
		},
	}, event)
}

func TestWorkflowExecutionTerminated(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	reason := "random reason"
	event := deps.historyBuilder.AddWorkflowExecutionTerminatedEvent(
		reason,
		testPayloads,
		testIdentity,
		nil,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Reason:   reason,
				Details:  testPayloads,
				Identity: testIdentity,
			},
		},
	}, event)
}

func TestWorkflowExecutionContinueAsNew(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	initiator := enumspb.ContinueAsNewInitiator(rand.Int31n(int32(len(enumspb.ContinueAsNewInitiator_name))))
	firstWorkflowTaskBackoff := durationpb.New(time.Duration(rand.Int63()))
	workflowRunTimeout := durationpb.New(time.Duration(rand.Int63()))
	workflowTaskStartToCloseTimeout := durationpb.New(time.Duration(rand.Int63()))

	attributes := &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
		WorkflowType:         testWorkflowType,
		TaskQueue:            testTaskQueue,
		Input:                testPayloads,
		WorkflowRunTimeout:   workflowRunTimeout,
		WorkflowTaskTimeout:  workflowTaskStartToCloseTimeout,
		BackoffStartInterval: firstWorkflowTaskBackoff,
		RetryPolicy:          testRetryPolicy,
		Initiator:            initiator,
		Failure:              testFailure,
		LastCompletionResult: testPayloads,
		CronSchedule:         testCronSchedule,
		Header:               testHeader,
		Memo:                 testMemo,
		SearchAttributes:     testSearchAttributes,
	}
	event := deps.historyBuilder.AddContinuedAsNewEvent(
		workflowTaskCompletionEventID,
		testRunID,
		attributes,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
			WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				NewExecutionRunId:            testRunID,
				WorkflowType:                 testWorkflowType,
				TaskQueue:                    testTaskQueue,
				Header:                       testHeader,
				Input:                        testPayloads,
				WorkflowRunTimeout:           workflowRunTimeout,
				WorkflowTaskTimeout:          workflowTaskStartToCloseTimeout,
				BackoffStartInterval:         firstWorkflowTaskBackoff,
				Initiator:                    initiator,
				Failure:                      testFailure,
				LastCompletionResult:         testPayloads,
				Memo:                         testMemo,
				SearchAttributes:             testSearchAttributes,
			},
		},
	}, event)
}

/* workflow */

/* workflow tasks */
func TestWorkflowTaskScheduled(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	startToCloseTimeout := time.Duration(rand.Int31()) * time.Second
	attempt := rand.Int31()
	event := deps.historyBuilder.AddWorkflowTaskScheduledEvent(
		testTaskQueue,
		durationpb.New(startToCloseTimeout),
		attempt,
		deps.now,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue:           testTaskQueue,
				StartToCloseTimeout: durationpb.New(startToCloseTimeout),
				Attempt:             attempt,
			},
		},
	}, event)
}

func TestWorkflowTaskStarted(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	event := deps.historyBuilder.AddWorkflowTaskStartedEvent(
		scheduledEventID,
		testRequestID,
		testIdentity,
		deps.now,
		false,
		123678,
		nil,
		int64(0),
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId:     scheduledEventID,
				Identity:             testIdentity,
				RequestId:            testRequestID,
				SuggestContinueAsNew: false,
				HistorySizeBytes:     123678,
			},
		},
	}, event)
}

func TestWorkflowTaskCompleted(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	checksum := "random checksum"
	sdkMetadata := &sdkpb.WorkflowTaskCompletedMetadata{CoreUsedFlags: []uint32{1, 2, 3}, LangUsedFlags: []uint32{4, 5, 6}}
	meteringMeta := &commonpb.MeteringMetadata{NonfirstLocalActivityExecutionAttempts: 42}
	event := deps.historyBuilder.AddWorkflowTaskCompletedEvent(
		scheduledEventID,
		startedEventID,
		testIdentity,
		checksum,
		&commonpb.WorkerVersionStamp{BuildId: "build_id_9"},
		sdkMetadata,
		meteringMeta,
		"",
		nil,
		enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Identity:         testIdentity,
				BinaryChecksum:   checksum,
				WorkerVersion:    &commonpb.WorkerVersionStamp{BuildId: "build_id_9"},
				SdkMetadata:      sdkMetadata,
				MeteringMetadata: meteringMeta,
			},
		},
	}, event)
}

func TestWorkflowTaskFailed(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	cause := enumspb.WorkflowTaskFailedCause(rand.Int31n(int32(len(enumspb.WorkflowTaskFailedCause_name))))
	baseRunID := uuid.New()
	newRunID := uuid.New()
	forkEventVersion := rand.Int63()
	checksum := "random checksum"
	event := deps.historyBuilder.AddWorkflowTaskFailedEvent(
		scheduledEventID,
		startedEventID,
		cause,
		testFailure,
		testIdentity,
		baseRunID,
		newRunID,
		forkEventVersion,
		checksum,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
			WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Cause:            cause,
				Failure:          testFailure,
				Identity:         testIdentity,
				BaseRunId:        baseRunID,
				NewRunId:         newRunID,
				ForkEventVersion: forkEventVersion,
				BinaryChecksum:   checksum,
			},
		},
	}, event)
}

func TestWorkflowTaskTimeout(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	timeoutType := enumspb.TimeoutType(rand.Int31n(int32(len(enumspb.TimeoutType_name))))
	event := deps.historyBuilder.AddWorkflowTaskTimedOutEvent(
		scheduledEventID,
		startedEventID,
		timeoutType,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{
			WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				TimeoutType:      timeoutType,
			},
		},
	}, event)
}

/* workflow tasks */

/* activity tasks */
func TestActivityTaskScheduled(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	activityID := "random activity ID"
	scheduleToCloseTimeout := durationpb.New(time.Duration(rand.Int63()))
	scheduleToStartTimeout := durationpb.New(time.Duration(rand.Int63()))
	startToCloseTimeout := durationpb.New(time.Duration(rand.Int63()))
	heartbeatTimeout := durationpb.New(time.Duration(rand.Int63()))
	attributes := &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId:             activityID,
		ActivityType:           testActivityType,
		TaskQueue:              testTaskQueue,
		Header:                 testHeader,
		Input:                  testPayloads,
		RetryPolicy:            testRetryPolicy,
		ScheduleToCloseTimeout: scheduleToCloseTimeout,
		ScheduleToStartTimeout: scheduleToStartTimeout,
		StartToCloseTimeout:    startToCloseTimeout,
		HeartbeatTimeout:       heartbeatTimeout,
	}
	event := deps.historyBuilder.AddActivityTaskScheduledEvent(
		workflowTaskCompletionEventID,
		attributes,
		defaultNamespace,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				ActivityId:                   activityID,
				ActivityType:                 testActivityType,
				TaskQueue:                    testTaskQueue,
				Header:                       testHeader,
				Input:                        testPayloads,
				RetryPolicy:                  testRetryPolicy,
				ScheduleToCloseTimeout:       scheduleToCloseTimeout,
				ScheduleToStartTimeout:       scheduleToStartTimeout,
				StartToCloseTimeout:          startToCloseTimeout,
				HeartbeatTimeout:             heartbeatTimeout,
			},
		},
	}, event)
}

func TestActivityTaskStarted(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	attempt := rand.Int31()
	stamp := &commonpb.WorkerVersionStamp{BuildId: "bld", UseVersioning: false}
	event := deps.historyBuilder.AddActivityTaskStartedEvent(
		scheduledEventID,
		attempt,
		testRequestID,
		testIdentity,
		testFailure,
		stamp,
		int64(0),
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: scheduledEventID,
				Attempt:          attempt,
				Identity:         testIdentity,
				RequestId:        testRequestID,
				LastFailure:      testFailure,
				WorkerVersion:    stamp,
			},
		},
	}, event)
}

func TestActivityTaskCancelRequested(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	scheduledEventID := rand.Int63()
	event := deps.historyBuilder.AddActivityTaskCancelRequestedEvent(
		workflowTaskCompletionEventID,
		scheduledEventID,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{
			ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				ScheduledEventId:             scheduledEventID,
			},
		},
	}, event)
}

func TestActivityTaskCompleted(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	event := deps.historyBuilder.AddActivityTaskCompletedEvent(
		scheduledEventID,
		startedEventID,
		testIdentity,
		testPayloads,
		defaultNamespace,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Result:           testPayloads,
				Identity:         testIdentity,
			},
		},
	}, event)
}

func TestActivityTaskFailed(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := deps.historyBuilder.AddActivityTaskFailedEvent(
		scheduledEventID,
		startedEventID,
		testFailure,
		retryState,
		testIdentity,
		defaultNamespace,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
			ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Failure:          testFailure,
				RetryState:       retryState,
				Identity:         testIdentity,
			},
		},
	}, event)
}

func TestActivityTaskTimeout(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := deps.historyBuilder.AddActivityTaskTimedOutEvent(
		scheduledEventID,
		startedEventID,
		testFailure,
		retryState,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
			ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Failure:          testFailure,
				RetryState:       retryState,
			},
		},
	}, event)
}

func TestActivityTaskCancelled(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	cancelRequestedEventID := rand.Int63()
	event := deps.historyBuilder.AddActivityTaskCanceledEvent(
		scheduledEventID,
		startedEventID,
		cancelRequestedEventID,
		testPayloads,
		testIdentity,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
			ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
				ScheduledEventId:             scheduledEventID,
				StartedEventId:               startedEventID,
				LatestCancelRequestedEventId: cancelRequestedEventID,
				Details:                      testPayloads,
				Identity:                     testIdentity,
			},
		},
	}, event)
}

/* activity tasks */

/* timer */
func TestTimerStarted(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	timerID := "random timer ID"
	startToFireTimeout := durationpb.New(time.Duration(rand.Int63()))
	attributes := &commandpb.StartTimerCommandAttributes{
		TimerId:            timerID,
		StartToFireTimeout: startToFireTimeout,
	}
	event := deps.historyBuilder.AddTimerStartedEvent(
		workflowTaskCompletionEventID,
		attributes,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{
			TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				TimerId:                      timerID,
				StartToFireTimeout:           startToFireTimeout,
			},
		},
	}, event)
}

func TestTimerFired(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	startedEventID := rand.Int63()
	timerID := "random timer ID"
	event := deps.historyBuilder.AddTimerFiredEvent(
		startedEventID,
		timerID,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{
			TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
				TimerId:        timerID,
				StartedEventId: startedEventID,
			},
		},
	}, event)
}

func TestTimerCancelled(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	startedEventID := rand.Int63()
	timerID := "random timer ID"
	event := deps.historyBuilder.AddTimerCanceledEvent(
		workflowTaskCompletionEventID,
		startedEventID,
		timerID,
		testIdentity,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_TIMER_CANCELED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_TimerCanceledEventAttributes{
			TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				StartedEventId:               startedEventID,
				TimerId:                      timerID,
				Identity:                     testIdentity,
			},
		},
	}, event)
}

/* timer */

/* cancellation of external workflow */
func TestRequestCancelExternalWorkflowExecutionInitiated(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	control := "random control"
	childWorkflowOnly := rand.Int31()%2 == 0
	attributes := &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
		Namespace:         testNamespaceName.String(),
		WorkflowId:        testWorkflowID,
		RunId:             testRunID,
		Control:           control,
		ChildWorkflowOnly: childWorkflowOnly,
	}
	event := deps.historyBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletionEventID,
		attributes,
		testNamespaceID,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				Control:           control,
				ChildWorkflowOnly: childWorkflowOnly,
			},
		},
	}, event)
}

func TestRequestCancelExternalWorkflowExecutionSuccess(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	event := deps.historyBuilder.AddExternalWorkflowExecutionCancelRequested(
		scheduledEventID,
		testNamespaceName,
		testNamespaceID,
		testWorkflowID,
		testRunID,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{
			ExternalWorkflowExecutionCancelRequestedEventAttributes: &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{
				InitiatedEventId: scheduledEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
			},
		},
	}, event)
}

func TestRequestCancelExternalWorkflowExecutionFailed(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	scheduledEventID := rand.Int63()
	cause := enumspb.CancelExternalWorkflowExecutionFailedCause(rand.Int31n(int32(len(enumspb.CancelExternalWorkflowExecutionFailedCause_name))))
	event := deps.historyBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
		workflowTaskCompletionEventID,
		scheduledEventID,
		testNamespaceName,
		testNamespaceID,
		testWorkflowID,
		testRunID,
		cause,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			RequestCancelExternalWorkflowExecutionFailedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				InitiatedEventId:             scheduledEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				Cause: cause,
			},
		},
	}, event)
}

/* cancellation of external workflow */

/* signal to external workflow */
func TestSignalExternalWorkflowExecutionInitiated(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	signalName := "random signal name"
	control := "random control"
	childWorkflowOnly := rand.Int31()%2 == 0
	attributes := &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
		Namespace: testNamespaceName.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		SignalName:        signalName,
		Input:             testPayloads,
		Control:           control,
		ChildWorkflowOnly: childWorkflowOnly,
		Header:            testHeader,
	}
	event := deps.historyBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletionEventID,
		attributes,
		testNamespaceID,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
			SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				SignalName:        signalName,
				Input:             testPayloads,
				Control:           control,
				ChildWorkflowOnly: childWorkflowOnly,
				Header:            testHeader,
			},
		},
	}, event)
}

func TestSignalExternalWorkflowExecutionSuccess(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	control := "random control"
	event := deps.historyBuilder.AddExternalWorkflowExecutionSignaled(
		scheduledEventID,
		testNamespaceName,
		testNamespaceID,
		testWorkflowID,
		testRunID,
		control,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{
			ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{
				InitiatedEventId: scheduledEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				Control: control,
			},
		},
	}, event)
}

func TestSignalExternalWorkflowExecutionFailed(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	scheduledEventID := rand.Int63()
	control := "random control"
	cause := enumspb.SignalExternalWorkflowExecutionFailedCause(rand.Int31n(int32(len(enumspb.SignalExternalWorkflowExecutionFailedCause_name))))
	event := deps.historyBuilder.AddSignalExternalWorkflowExecutionFailedEvent(
		workflowTaskCompletionEventID,
		scheduledEventID,
		testNamespaceName,
		testNamespaceID,
		testWorkflowID,
		testRunID,
		control,
		cause,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{
			SignalExternalWorkflowExecutionFailedEventAttributes: &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				InitiatedEventId:             scheduledEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				Control: control,
				Cause:   cause,
			},
		},
	}, event)
}

/* signal to external workflow */

/* child workflow */
func TestStartChildWorkflowExecutionInitiated(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	workflowExecutionTimeout := durationpb.New(time.Duration(rand.Int63()))
	workflowRunTimeout := durationpb.New(time.Duration(rand.Int63()))
	workflowTaskStartToCloseTimeout := durationpb.New(time.Duration(rand.Int63()))
	parentClosePolicy := enumspb.ParentClosePolicy(rand.Int31n(int32(len(enumspb.ParentClosePolicy_name))))
	workflowIdReusePolicy := enumspb.WorkflowIdReusePolicy(rand.Int31n(int32(len(enumspb.WorkflowIdReusePolicy_name))))
	control := "random control"

	attributes := &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:                testNamespaceName.String(),
		WorkflowId:               testWorkflowID,
		WorkflowType:             testWorkflowType,
		TaskQueue:                testTaskQueue,
		Input:                    testPayloads,
		WorkflowExecutionTimeout: workflowExecutionTimeout,
		WorkflowRunTimeout:       workflowRunTimeout,
		WorkflowTaskTimeout:      workflowTaskStartToCloseTimeout,
		ParentClosePolicy:        parentClosePolicy,
		Control:                  control,
		WorkflowIdReusePolicy:    workflowIdReusePolicy,
		RetryPolicy:              testRetryPolicy,
		CronSchedule:             testCronSchedule,
		Memo:                     testMemo,
		SearchAttributes:         testSearchAttributes,
		Header:                   testHeader,
	}
	event := deps.historyBuilder.AddStartChildWorkflowExecutionInitiatedEvent(
		workflowTaskCompletionEventID,
		attributes,
		testNamespaceID,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   deps.nextEventID,
		TaskId:    deps.nextTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
			StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowId:                   testWorkflowID,
				WorkflowType:                 testWorkflowType,
				TaskQueue:                    testTaskQueue,
				Input:                        testPayloads,
				WorkflowExecutionTimeout:     workflowExecutionTimeout,
				WorkflowRunTimeout:           workflowRunTimeout,
				WorkflowTaskTimeout:          workflowTaskStartToCloseTimeout,
				ParentClosePolicy:            parentClosePolicy,
				Control:                      control,
				WorkflowIdReusePolicy:        workflowIdReusePolicy,
				RetryPolicy:                  testRetryPolicy,
				CronSchedule:                 testCronSchedule,
				Memo:                         testMemo,
				SearchAttributes:             testSearchAttributes,
				Header:                       testHeader,
			},
		},
	}, event)
}

func TestStartChildWorkflowExecutionSuccess(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	event := deps.historyBuilder.AddChildWorkflowExecutionStartedEvent(
		scheduledEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
		testHeader,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{
			ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
				Namespace:   testNamespaceName.String(),
				NamespaceId: testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType:     testWorkflowType,
				InitiatedEventId: scheduledEventID,
				Header:           testHeader,
			},
		},
	}, event)
}

func TestStartChildWorkflowExecutionFailed(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	workflowTaskCompletionEventID := rand.Int63()
	scheduledEventID := rand.Int63()
	control := "random control"
	cause := enumspb.StartChildWorkflowExecutionFailedCause(rand.Int31n(int32(len(enumspb.StartChildWorkflowExecutionFailedCause_name))))
	event := deps.historyBuilder.AddStartChildWorkflowExecutionFailedEvent(
		workflowTaskCompletionEventID,
		scheduledEventID,
		cause,
		testNamespaceName,
		testNamespaceID,
		testWorkflowID,
		testWorkflowType,
		control,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{
			StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletionEventID,
				Namespace:                    testNamespaceName.String(),
				NamespaceId:                  testNamespaceID.String(),
				WorkflowId:                   testWorkflowID,
				WorkflowType:                 testWorkflowType,
				InitiatedEventId:             scheduledEventID,
				Control:                      control,
				Cause:                        cause,
			},
		},
	}, event)
}

func TestChildWorkflowExecutionCompleted(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()

	event := deps.historyBuilder.AddChildWorkflowExecutionCompletedEvent(
		scheduledEventID,
		startedEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
		testPayloads,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
			ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
				InitiatedEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType: testWorkflowType,
				Result:       testPayloads,
			},
		},
	}, event)
}

func TestChildWorkflowExecutionFailed(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := deps.historyBuilder.AddChildWorkflowExecutionFailedEvent(
		scheduledEventID,
		startedEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
		testFailure,
		retryState,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
			ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{
				InitiatedEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType: testWorkflowType,
				Failure:      testFailure,
				RetryState:   retryState,
			},
		},
	}, event)
}

func TestChildWorkflowExecutionTimeout(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	retryState := enumspb.RetryState(rand.Int31n(int32(len(enumspb.RetryState_name))))
	event := deps.historyBuilder.AddChildWorkflowExecutionTimedOutEvent(
		scheduledEventID,
		startedEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
		retryState,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{
			ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
				InitiatedEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType: testWorkflowType,
				RetryState:   retryState,
			},
		},
	}, event)
}

func TestChildWorkflowExecutionCancelled(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	event := deps.historyBuilder.AddChildWorkflowExecutionCanceledEvent(
		scheduledEventID,
		startedEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
		testPayloads,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{
			ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{
				InitiatedEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType: testWorkflowType,
				Details:      testPayloads,
			},
		},
	}, event)
}

func TestChildWorkflowExecutionTerminated(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startedEventID := rand.Int63()
	event := deps.historyBuilder.AddChildWorkflowExecutionTerminatedEvent(
		scheduledEventID,
		startedEventID,
		testNamespaceName,
		testNamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		testWorkflowType,
	)
	require.Equal(t, event, flush(t, deps))
	require.Equal(t, &historypb.HistoryEvent{
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		EventTime: timestamppb.New(deps.now),
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		Version:   deps.version,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{
			ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
				InitiatedEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Namespace:        testNamespaceName.String(),
				NamespaceId:      testNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: testWorkflowID,
					RunId:      testRunID,
				},
				WorkflowType: testWorkflowType,
			},
		},
	}, event)
}

/* child workflow */

func TestAppendFlushFinishEvent_WithoutBuffer_SingleBatch_WithoutFlushBuffer(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	testAppendFlushFinishEventWithoutBufferSingleBatch(t, deps, false)
}

func TestAppendFlushFinishEvent_WithoutBuffer_SingleBatch_WithFlushBuffer(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	testAppendFlushFinishEventWithoutBufferSingleBatch(t, deps, true)
}

func testAppendFlushFinishEventWithoutBufferSingleBatch(t *testing.T, deps *historyBuilderTestDeps, flushBuffer bool) {
	deps.historyBuilder.dbBufferBatch = nil
	deps.historyBuilder.memEventsBatches = nil
	deps.historyBuilder.memLatestBatch = nil
	deps.historyBuilder.memBufferBatch = nil

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}

	deps.historyBuilder.add(event1)
	deps.historyBuilder.add(event2)
	historyMutation, err := deps.historyBuilder.Finish(flushBuffer)
	require.NoError(t, err)
	assertEventIDTaskID(t, historyMutation)

	require.Equal(t, &HistoryMutation{
		DBEventsBatches:        [][]*historypb.HistoryEvent{{event1, event2}},
		DBClearBuffer:          false,
		DBBufferBatch:          nil,
		MemBufferBatch:         nil,
		ScheduledIDToStartedID: make(map[int64]int64),
		RequestIDToEventID:     make(map[string]int64),
	}, historyMutation)
}

func TestAppendFlushFinishEvent_WithoutBuffer_MultiBatch_WithoutFlushBuffer(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	testAppendFlushFinishEventWithoutBufferMultiBatch(t, deps, false)
}

func TestAppendFlushFinishEvent_WithoutBuffer_MultiBatch_WithFlushBuffer(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	testAppendFlushFinishEventWithoutBufferMultiBatch(t, deps, true)
}

func testAppendFlushFinishEventWithoutBufferMultiBatch(t *testing.T, deps *historyBuilderTestDeps, flushBuffer bool) {
	deps.historyBuilder.dbBufferBatch = nil
	deps.historyBuilder.memEventsBatches = nil
	deps.historyBuilder.memLatestBatch = nil
	deps.historyBuilder.memBufferBatch = nil

	event11 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event12 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event21 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event22 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event31 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}
	event32 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		EventId:   rand.Int63(),
		TaskId:    common.EmptyEventTaskID,
	}

	// 1st batch
	deps.historyBuilder.add(event11)
	deps.historyBuilder.add(event12)
	deps.historyBuilder.FlushAndCreateNewBatch()

	// 2nd batch
	deps.historyBuilder.add(event21)
	deps.historyBuilder.add(event22)
	deps.historyBuilder.FlushAndCreateNewBatch()

	// 3rd batch
	deps.historyBuilder.add(event31)
	deps.historyBuilder.add(event32)
	deps.historyBuilder.FlushAndCreateNewBatch()

	historyMutation, err := deps.historyBuilder.Finish(flushBuffer)
	require.NoError(t, err)
	assertEventIDTaskID(t, historyMutation)

	require.Equal(t, &HistoryMutation{
		DBEventsBatches: [][]*historypb.HistoryEvent{
			{event11, event12},
			{event21, event22},
			{event31, event32},
		},
		DBClearBuffer:          false,
		DBBufferBatch:          nil,
		MemBufferBatch:         nil,
		ScheduledIDToStartedID: make(map[int64]int64),
		RequestIDToEventID:     make(map[string]int64),
	}, historyMutation)
}

func TestAppendFlushFinishEvent_WithBuffer_WithoutDBBuffer_WithoutFlushBuffer(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	deps.historyBuilder.dbBufferBatch = nil
	deps.historyBuilder.memEventsBatches = nil
	deps.historyBuilder.memLatestBatch = nil
	deps.historyBuilder.memBufferBatch = nil

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	deps.historyBuilder.add(event1)
	deps.historyBuilder.add(event2)
	historyMutation, err := deps.historyBuilder.Finish(false)
	require.NoError(t, err)
	assertEventIDTaskID(t, historyMutation)

	require.Equal(t, &HistoryMutation{
		DBEventsBatches:        nil,
		DBClearBuffer:          false,
		DBBufferBatch:          []*historypb.HistoryEvent{event1, event2},
		MemBufferBatch:         []*historypb.HistoryEvent{event1, event2},
		ScheduledIDToStartedID: make(map[int64]int64),
		RequestIDToEventID:     make(map[string]int64),
	}, historyMutation)
}

func TestAppendFlushFinishEvent_WithBuffer_WithoutDBBuffer_WithFlushBuffer(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	deps.historyBuilder.dbBufferBatch = nil
	deps.historyBuilder.memEventsBatches = nil
	deps.historyBuilder.memLatestBatch = nil
	deps.historyBuilder.memBufferBatch = nil

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	deps.historyBuilder.add(event1)
	deps.historyBuilder.add(event2)
	historyMutation, err := deps.historyBuilder.Finish(true)
	require.NoError(t, err)
	assertEventIDTaskID(t, historyMutation)

	require.Equal(t, &HistoryMutation{
		DBEventsBatches:        [][]*historypb.HistoryEvent{{event1, event2}},
		DBClearBuffer:          false,
		DBBufferBatch:          nil,
		MemBufferBatch:         nil,
		ScheduledIDToStartedID: make(map[int64]int64),
		RequestIDToEventID:     make(map[string]int64),
	}, historyMutation)
}

func TestAppendFlushFinishEvent_WithoutBuffer_WithDBBuffer_WithoutFlushBuffer(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	deps.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{event1, event2}
	deps.historyBuilder.memEventsBatches = nil
	deps.historyBuilder.memLatestBatch = nil
	deps.historyBuilder.memBufferBatch = nil

	historyMutation, err := deps.historyBuilder.Finish(false)
	require.NoError(t, err)
	assertEventIDTaskID(t, historyMutation)

	require.Equal(t, &HistoryMutation{
		DBEventsBatches:        nil,
		DBClearBuffer:          false,
		DBBufferBatch:          nil,
		MemBufferBatch:         []*historypb.HistoryEvent{event1, event2},
		ScheduledIDToStartedID: make(map[int64]int64),
		RequestIDToEventID:     make(map[string]int64),
	}, historyMutation)
}

func TestAppendFlushFinishEvent_WithoutBuffer_WithDBBuffer_WithFlushBuffer(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	deps.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{event1, event2}
	deps.historyBuilder.memEventsBatches = nil
	deps.historyBuilder.memLatestBatch = nil
	deps.historyBuilder.memBufferBatch = nil

	historyMutation, err := deps.historyBuilder.Finish(true)
	require.NoError(t, err)
	assertEventIDTaskID(t, historyMutation)

	require.Equal(t, &HistoryMutation{
		DBEventsBatches:        [][]*historypb.HistoryEvent{{event1, event2}},
		DBClearBuffer:          true,
		DBBufferBatch:          nil,
		MemBufferBatch:         nil,
		ScheduledIDToStartedID: make(map[int64]int64),
		RequestIDToEventID:     make(map[string]int64),
	}, historyMutation)
}

func TestAppendFlushFinishEvent_WithBuffer_WithDBBuffer_WithoutFlushBuffer(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	event0 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	deps.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{event0}
	deps.historyBuilder.memEventsBatches = nil
	deps.historyBuilder.memLatestBatch = nil
	deps.historyBuilder.memBufferBatch = nil

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	deps.historyBuilder.add(event1)
	deps.historyBuilder.add(event2)
	historyMutation, err := deps.historyBuilder.Finish(false)
	require.NoError(t, err)
	assertEventIDTaskID(t, historyMutation)

	require.Equal(t, &HistoryMutation{
		DBEventsBatches:        nil,
		DBClearBuffer:          false,
		DBBufferBatch:          []*historypb.HistoryEvent{event1, event2},
		MemBufferBatch:         []*historypb.HistoryEvent{event0, event1, event2},
		ScheduledIDToStartedID: make(map[int64]int64),
		RequestIDToEventID:     make(map[string]int64),
	}, historyMutation)
}

func TestAppendFlushFinishEvent_WithBuffer_WithDBBuffer_WithFlushBuffer(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	event0 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	deps.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{event0}
	deps.historyBuilder.memEventsBatches = nil
	deps.historyBuilder.memLatestBatch = nil
	deps.historyBuilder.memBufferBatch = nil

	event1 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}
	event2 := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}

	deps.historyBuilder.add(event1)
	deps.historyBuilder.add(event2)
	historyMutation, err := deps.historyBuilder.Finish(true)
	require.NoError(t, err)
	assertEventIDTaskID(t, historyMutation)

	require.Equal(t, &HistoryMutation{
		DBEventsBatches:        [][]*historypb.HistoryEvent{{event0, event1, event2}},
		DBClearBuffer:          true,
		DBBufferBatch:          nil,
		MemBufferBatch:         nil,
		ScheduledIDToStartedID: make(map[int64]int64),
		RequestIDToEventID:     make(map[string]int64),
	}, historyMutation)
}

func TestWireEventIDs_Activity(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	scheduledEventID := rand.Int63()
	startEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}
	completeEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}
	failedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
			ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}
	timeoutEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
			ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}
	cancelEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
			ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}

	testWireEventIDs(t, deps, scheduledEventID, startEvent, completeEvent)
	testWireEventIDs(t, deps, scheduledEventID, startEvent, failedEvent)
	testWireEventIDs(t, deps, scheduledEventID, startEvent, timeoutEvent)
	testWireEventIDs(t, deps, scheduledEventID, startEvent, cancelEvent)
}

func TestWireEventIDs_ChildWorkflow(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	initiatedEventID := rand.Int63()
	startEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{
			ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}
	completeEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
			ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}
	failedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
			ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}
	timeoutEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{
			ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}
	cancelEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{
			ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}
	terminatedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{
			ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
				InitiatedEventId: initiatedEventID,
			},
		},
	}

	testWireEventIDs(t, deps, initiatedEventID, startEvent, completeEvent)
	testWireEventIDs(t, deps, initiatedEventID, startEvent, failedEvent)
	testWireEventIDs(t, deps, initiatedEventID, startEvent, timeoutEvent)
	testWireEventIDs(t, deps, initiatedEventID, startEvent, cancelEvent)
	testWireEventIDs(t, deps, initiatedEventID, startEvent, terminatedEvent)
}

func testWireEventIDs(t *testing.T, deps *historyBuilderTestDeps, scheduledEventID int64, startEvent *historypb.HistoryEvent, finishEvent *historypb.HistoryEvent) {
	deps.historyBuilder = New(
		deps.mockTimeSource,
		s.taskIDGenerator,
		deps.version,
		deps.nextEventID,
		nil,
		metrics.NoopMetricsHandler,
	)
	deps.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{startEvent}
	deps.historyBuilder.memEventsBatches = nil
	deps.historyBuilder.memLatestBatch = nil
	deps.historyBuilder.memBufferBatch = []*historypb.HistoryEvent{finishEvent}
	deps.historyBuilder.FlushBufferToCurrentBatch()

	require.Empty(t, deps.historyBuilder.dbBufferBatch)
	require.Empty(t, deps.historyBuilder.memEventsBatches)
	require.Equal(t, []*historypb.HistoryEvent{startEvent, finishEvent}, deps.historyBuilder.memLatestBatch)
	require.Empty(t, deps.historyBuilder.memBufferBatch)

	require.Equal(t, map[int64]int64{
		scheduledEventID: startEvent.GetEventId(),
	}, deps.historyBuilder.scheduledIDToStartedID)

	switch finishEvent.GetEventType() {
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		require.Equal(t, startEvent.GetEventId(), finishEvent.GetActivityTaskCompletedEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		require.Equal(t, startEvent.GetEventId(), finishEvent.GetActivityTaskFailedEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		require.Equal(t, startEvent.GetEventId(), finishEvent.GetActivityTaskTimedOutEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		require.Equal(t, startEvent.GetEventId(), finishEvent.GetActivityTaskCanceledEventAttributes().GetStartedEventId())

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
		require.Equal(t, startEvent.GetEventId(), finishEvent.GetChildWorkflowExecutionCompletedEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
		require.Equal(t, startEvent.GetEventId(), finishEvent.GetChildWorkflowExecutionFailedEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
		require.Equal(t, startEvent.GetEventId(), finishEvent.GetChildWorkflowExecutionTimedOutEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
		require.Equal(t, startEvent.GetEventId(), finishEvent.GetChildWorkflowExecutionCanceledEventAttributes().GetStartedEventId())
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
		require.Equal(t, startEvent.GetEventId(), finishEvent.GetChildWorkflowExecutionTerminatedEventAttributes().GetStartedEventId())
	}
}

func TestHasBufferEvent(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	historyBuilder := New(
		deps.mockTimeSource,
		s.taskIDGenerator,
		deps.version,
		deps.nextEventID,
		nil,
		metrics.NoopMetricsHandler,
	)
	historyBuilder.dbBufferBatch = nil
	historyBuilder.memEventsBatches = nil
	historyBuilder.memLatestBatch = nil
	historyBuilder.memBufferBatch = nil
	require.False(t, historyBuilder.HasBufferEvents())

	historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
	}}
	historyBuilder.memEventsBatches = nil
	historyBuilder.memLatestBatch = nil
	historyBuilder.memBufferBatch = nil
	require.True(t, historyBuilder.HasBufferEvents())

	historyBuilder.dbBufferBatch = nil
	historyBuilder.memEventsBatches = nil
	historyBuilder.memLatestBatch = nil
	historyBuilder.memBufferBatch = []*historypb.HistoryEvent{{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
	}}
	require.True(t, historyBuilder.HasBufferEvents())

	historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{{
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
	}}
	historyBuilder.memEventsBatches = nil
	historyBuilder.memLatestBatch = nil
	historyBuilder.memBufferBatch = []*historypb.HistoryEvent{{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
	}}
	require.True(t, historyBuilder.HasBufferEvents())
}

func TestBufferEvent(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	// workflow status events will be assign event ID immediately
	workflowEvents := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:          true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:        true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:           true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:        true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:       true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW: true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:         true,
	}

	// workflow task events will be assign event ID immediately
	workflowTaskEvents := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED: true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:   true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED: true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:    true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT: true,
	}

	// events corresponding to commands from client will be assigned an event ID immediately
	commandEvents := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:                         true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:                            true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:                          true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:                  true,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:                              true,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:                       true,
		enumspb.EVENT_TYPE_TIMER_STARTED:                                        true,
		enumspb.EVENT_TYPE_TIMER_CANCELED:                                       true,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED: true,
		enumspb.EVENT_TYPE_MARKER_RECORDED:                                      true,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:             true,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:         true,
		enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:                    true,
		enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED:                         true,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED:                            true,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED:                     true,
	}

	// events corresponding to message from client will be assigned an event ID immediately
	messageEvents := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:  true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED: true,
	}

	// other events will not be assigned an event ID immediately (created automatically)
	otherEvents := map[enumspb.EventType]bool{}
	for _, eventType := range enumspb.EventType_value {
		if _, ok := workflowEvents[enumspb.EventType(eventType)]; ok {
			continue
		}
		if _, ok := workflowTaskEvents[enumspb.EventType(eventType)]; ok {
			continue
		}
		if _, ok := commandEvents[enumspb.EventType(eventType)]; ok {
			continue
		}
		if _, ok := messageEvents[enumspb.EventType(eventType)]; ok {
			continue
		}
		otherEvents[enumspb.EventType(eventType)] = true
	}

	// test workflowEvents, workflowTaskEvents, commandEvents will return true
	for eventType := range workflowEvents {
		require.False(t, deps.historyBuilder.bufferEvent(eventType))
	}
	for eventType := range workflowTaskEvents {
		require.False(t, deps.historyBuilder.bufferEvent(eventType))
	}
	for eventType := range commandEvents {
		require.False(t, deps.historyBuilder.bufferEvent(eventType))
	}
	for eventType := range messageEvents {
		require.False(t, deps.historyBuilder.bufferEvent(eventType))
	}
	// other events will return false
	for eventType := range otherEvents {
		require.True(t, deps.historyBuilder.bufferEvent(eventType))
	}

	commandsWithEventsCount := 0
	for ct := range enumspb.CommandType_name {
		commandType := enumspb.CommandType(ct)
		// Unspecified is not counted.
		// ProtocolMessage command doesn't have corresponding event.
		if commandType == enumspb.COMMAND_TYPE_UNSPECIFIED || commandType == enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE {
			continue
		}
		commandsWithEventsCount++
	}
	require.Equal(t, 
		commandsWithEventsCount,
		len(commandEvents),
		"This assertion is broken when a new command is added and no corresponding logic for corresponding command event is added to HistoryBuilder.bufferEvent",
	)
}

func TestReorder(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	// Only completion events are reordered.
	reorderEventTypes := map[enumspb.EventType]struct{}{
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:             {},
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:                {},
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:             {},
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:              {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:  {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:     {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:  {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:   {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED: {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:           {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED:              {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:            {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:           {},
	}
	var reorderEvents []*historypb.HistoryEvent
	for eventType := range reorderEventTypes {
		reorderEvents = append(reorderEvents, &historypb.HistoryEvent{
			EventType: eventType,
		})
	}

	var nonReorderEvents []*historypb.HistoryEvent
	for eventTypeValue := range enumspb.EventType_name {
		eventType := enumspb.EventType(eventTypeValue)
		if _, ok := reorderEventTypes[eventType]; ok || eventType == enumspb.EVENT_TYPE_UNSPECIFIED {
			continue
		}

		nonReorderEvents = append(nonReorderEvents, &historypb.HistoryEvent{
			EventType: eventType,
		})
	}

	require.Equal(t, 
		append(nonReorderEvents, reorderEvents...),
		deps.historyBuilder.reorderBuffer(append(reorderEvents, nonReorderEvents...)),
	)
}

func TestBufferSize_Memory(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	require.Zero(t, deps.historyBuilder.NumBufferedEvents())
	require.Zero(t, deps.historyBuilder.SizeInBytesOfBufferedEvents())
	deps.historyBuilder.AddWorkflowExecutionSignaledEvent(
		"signal-name",
		&commonpb.Payloads{},
		"identity",
		&commonpb.Header{},
		nil,
		nil,
	)
	require.Equal(t, 1, deps.historyBuilder.NumBufferedEvents())
	// the size of the proto  is non-deterministic, so just assert that it's non-zero, and it isn't really high
	require.Greater(t, deps.historyBuilder.SizeInBytesOfBufferedEvents(), 0)
	require.Less(t, deps.historyBuilder.SizeInBytesOfBufferedEvents(), 100)
	flush(t, deps)
	require.Zero(t, deps.historyBuilder.NumBufferedEvents())
	require.Zero(t, deps.historyBuilder.SizeInBytesOfBufferedEvents())
}

func TestBufferSize_DB(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	require.Zero(t, deps.historyBuilder.NumBufferedEvents())
	require.Zero(t, deps.historyBuilder.SizeInBytesOfBufferedEvents())
	deps.historyBuilder.dbBufferBatch = []*historypb.HistoryEvent{{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		EventId:   common.BufferedEventID,
		TaskId:    common.EmptyEventTaskID,
	}}
	require.Equal(t, 1, deps.historyBuilder.NumBufferedEvents())
	// the size of the proto  is non-deterministic, so just assert that it's non-zero, and it isn't really high
	require.Greater(t, deps.historyBuilder.SizeInBytesOfBufferedEvents(), 0)
	require.Less(t, deps.historyBuilder.SizeInBytesOfBufferedEvents(), 100)
	flush(t, deps)
	require.Zero(t, deps.historyBuilder.NumBufferedEvents())
	require.Zero(t, deps.historyBuilder.SizeInBytesOfBufferedEvents())
}

func TestLastEventVersion(t *testing.T) {
	deps := setupHistoryBuilderTest(t)

	_, ok := deps.historyBuilder.LastEventVersion()
	require.False(t, ok)

	deps.historyBuilder.AddWorkflowExecutionStartedEvent(
		time.Now(),
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{},
		},
		nil,
		"",
		"",
		"",
	)
	version, ok := deps.historyBuilder.LastEventVersion()
	require.True(t, ok)
	require.Equal(t, deps.version, version)

	deps.historyBuilder.FlushAndCreateNewBatch()
	version, ok = deps.historyBuilder.LastEventVersion()
	require.True(t, ok)
	require.Equal(t, deps.version, version)

	_, err := deps.historyBuilder.Finish(true)
	require.NoError(t, err)
	_, ok = deps.historyBuilder.LastEventVersion()
	require.False(t, ok)

}

func assertEventIDTaskID(t *testing.T, historyMutation *HistoryMutation) {

	for _, event := range historyMutation.DBBufferBatch {
		require.Equal(t, common.BufferedEventID, event.EventId)
		require.Equal(t, common.EmptyEventTaskID, event.TaskId)
	}

	for _, event := range historyMutation.MemBufferBatch {
		require.Equal(t, common.BufferedEventID, event.EventId)
		require.Equal(t, common.EmptyEventTaskID, event.TaskId)
	}

	for _, eventBatch := range historyMutation.DBEventsBatches {
		for _, event := range eventBatch {
			require.NotEqual(t, common.BufferedEventID, event.EventId)
			require.NotEqual(t, common.EmptyEventTaskID, event.TaskId)
		}
	}
}

func flush(t *testing.T, deps *historyBuilderTestDeps) *historypb.HistoryEvent {
	hasBufferEvents := deps.historyBuilder.HasBufferEvents()
	historyMutation, err := deps.historyBuilder.Finish(false)
	require.NoError(t, err)
	assertEventIDTaskID(t, historyMutation)
	require.Equal(t, make(map[int64]int64), historyMutation.ScheduledIDToStartedID)

	if !hasBufferEvents {
		require.Equal(t, 1, len(historyMutation.DBEventsBatches))
		require.Equal(t, 1, len(historyMutation.DBEventsBatches[0]))
		return historyMutation.DBEventsBatches[0][0]
	}

	if len(historyMutation.MemBufferBatch) > 0 {
		require.Equal(t, 1, len(historyMutation.MemBufferBatch))
		return historyMutation.MemBufferBatch[0]
	}

	require.Fail(t, "expect one and only event")
	return nil
}

func taskIDGenerator(deps *historyBuilderTestDeps, number int) ([]int64, error) {
	nextTaskID := deps.nextTaskID
	result := make([]int64, number)
	for i := 0; i < number; i++ {
		result[i] = nextTaskID
		nextTaskID++
	}
	return result, nil
}

