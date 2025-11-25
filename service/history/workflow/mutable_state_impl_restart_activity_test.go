package workflow

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	commonclock "go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	snapshot struct {
		mutableStateApproximateSize int
		activitySize                int
	}

	retryActivityTestDeps struct {
		controller       *gomock.Controller
		mockConfig       *configs.Config
		mockShard        *shard.ContextTest
		mockEventsCache  *events.MockCache
		onActivityCreate *snapshot

		mutableState    *MutableStateImpl
		nextBackoffStub *nextBackoffIntervalStub
		logger          log.Logger
		testScope       tally.TestScope
		activity        *persistencespb.ActivityInfo
		failure         *failurepb.Failure
		timeSource      *commonclock.EventTimeSource
	}
)

const nextBackoffIntervalParametersFormat = "now(%v):currentAttempt(%v):maxAttempts(%v):initInterval(%v):maxInterval(%v):expirationTime(%v):backoffCoefficient(%v)"

type nextBackoffIntervalStub struct {
	expected string
	recorded string
	duration time.Duration
	state    enumspb.RetryState
}

func (nbis *nextBackoffIntervalStub) nextBackoffInterval(
	now time.Time,
	currentAttempt int32,
	maxAttempts int32,
	initInterval *durationpb.Duration,
	maxInterval *durationpb.Duration,
	expirationTime *timestamppb.Timestamp,
	backoffCoefficient float64,
	_ BackoffCalculatorAlgorithmFunc,
) (time.Duration, enumspb.RetryState) {
	nbis.recorded = fmt.Sprintf(
		nextBackoffIntervalParametersFormat,
		now,
		currentAttempt,
		maxAttempts,
		initInterval,
		maxInterval,
		expirationTime,
		backoffCoefficient,
	)
	return nbis.duration, nbis.state
}

func (nbis *nextBackoffIntervalStub) onNextCallExpect(
	now time.Time,
	currentAttempt int32,
	maxAttempts int32,
	initInterval *durationpb.Duration,
	maxInterval *durationpb.Duration,
	expirationTime *timestamppb.Timestamp,
	backoffCoefficient float64,
) {
	nbis.expected = fmt.Sprintf(
		nextBackoffIntervalParametersFormat,
		now,
		currentAttempt,
		maxAttempts,
		initInterval,
		maxInterval,
		expirationTime,
		backoffCoefficient,
	)
}

func (nbis *nextBackoffIntervalStub) onNextCallReturn(duration time.Duration, state enumspb.RetryState) {
	nbis.duration = duration
	nbis.state = state
}

func setupRetryActivityTest(t *testing.T) *retryActivityTestDeps {
	mockConfig := tests.NewDynamicConfig()
	// set the checksum probabilities to 100% for exercising during test
	mockConfig.MutableStateChecksumGenProbability = func(namespace string) int { return 100 }
	mockConfig.MutableStateChecksumVerifyProbability = func(namespace string) int { return 100 }
	mockConfig.MutableStateActivityFailureSizeLimitWarn = func(namespace string) int { return 1 * 1024 }
	mockConfig.MutableStateActivityFailureSizeLimitError = func(namespace string) int { return 2 * 1024 }

	timeSource := commonclock.NewEventTimeSource()

	controller := gomock.NewController(t)
	mockEventsCache := events.NewMockCache(controller)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		mockConfig,
	)
	mockShard.SetEventsCacheForTesting(mockEventsCache)
	mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

	reg := hsm.NewRegistry()
	err := RegisterStateMachine(reg)
	require.NoError(t, err)
	mockShard.SetStateMachineRegistry(reg)

	testScope := mockShard.Resource.MetricsScope.(tally.TestScope)
	logger := mockShard.GetLogger()

	mutableState := NewMutableState(
		mockShard,
		mockEventsCache,
		logger,
		tests.LocalNamespaceEntry,
		tests.WorkflowID,
		tests.RunID,
		time.Now().UTC())

	nextBackoffStub := &nextBackoffIntervalStub{}

	t.Cleanup(func() {
		mockShard.StopForTest()
	})

	deps := &retryActivityTestDeps{
		controller:       controller,
		mockConfig:       mockConfig,
		mockShard:        mockShard,
		mockEventsCache:  mockEventsCache,
		mutableState:     mutableState,
		nextBackoffStub:  nextBackoffStub,
		logger:           logger,
		testScope:        testScope,
		timeSource:       timeSource,
	}

	// Make activity and put it in failing state
	deps.activity = makeActivityAndPutIntoFailingState(t, deps)
	deps.failure = activityFailure(t, deps)
	deps.nextBackoffStub.onNextCallExpect(
		timeSource.Now(),
		deps.activity.Attempt,
		deps.activity.RetryMaximumAttempts,
		deps.activity.RetryInitialInterval,
		deps.activity.RetryMaximumInterval,
		deps.activity.RetryExpirationTime,
		deps.activity.RetryBackoffCoefficient,
	)

	return deps
}

func makeActivityAndPutIntoFailingState(t *testing.T, deps *retryActivityTestDeps) *persistencespb.ActivityInfo {
	deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	workflowTaskCompletedEventID := int64(4)
	_, activityInfo, err := deps.mutableState.AddActivityTaskScheduledEvent(
		workflowTaskCompletedEventID,
		&commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:             "5",
			ActivityType:           &commonpb.ActivityType{Name: "activity-type"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "task-queue"},
			ScheduleToStartTimeout: durationpb.New(100 * time.Millisecond),
			ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
			StartToCloseTimeout:    durationpb.New(3 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: timestamp.DurationFromSeconds(1),
			},
		},
		false,
	)
	require.NoError(t, err)

	_, err = deps.mutableState.AddActivityTaskStartedEvent(
		activityInfo,
		activityInfo.ScheduledEventId,
		uuid.New(),
		"worker-identity",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	delete(deps.mutableState.syncActivityTasks, activityInfo.ScheduledEventId)
	delete(deps.mutableState.updateActivityInfos, activityInfo.ScheduledEventId)
	deps.onActivityCreate = &snapshot{
		mutableStateApproximateSize: deps.mutableState.approximateSize,
		activitySize:                activityInfo.Size(),
	}
	return activityInfo
}

func activityFailure(t *testing.T, deps *retryActivityTestDeps) *failurepb.Failure {
	failureSizeErrorLimit := deps.mockConfig.MutableStateActivityFailureSizeLimitError(
		deps.mutableState.namespaceEntry.Name().String(),
	)

	failure := &failurepb.Failure{
		Message: "activity failure with large details",
		Source:  "application",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         "application-failure-type",
			NonRetryable: false,
			Details: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{
					{
						Data: make([]byte, failureSizeErrorLimit*2),
					},
				},
			},
		}},
	}
	require.Greater(t, failure.Size(), failureSizeErrorLimit)
	return failure
}

func assertActivityWasNotScheduled(t *testing.T, deps *retryActivityTestDeps, ai *persistencespb.ActivityInfo, kind string) {
	t.Helper()
	require.Equal(t, deps.onActivityCreate.mutableStateApproximateSize, deps.mutableState.approximateSize, "mutable state size should not change when activity not restarted")
	require.NotContains(t, deps.mutableState.syncActivityTasks, ai.ScheduledEventId, "activity %s was scheduled", kind)
	require.NotContains(t, deps.mutableState.updateActivityInfos, ai.ScheduledEventId, "activity with no restart policy was marked for update")
}

func assertNoChange(t *testing.T, deps *retryActivityTestDeps, ai *persistencespb.ActivityInfo, msg string) {
	t.Helper()
	require.Equal(t, deps.onActivityCreate.activitySize, ai.Size(), msg)
}

func assertTruncateFailureCalled(t *testing.T, deps *retryActivityTestDeps) {
	t.Helper()
	require.IsType(t, &failurepb.Failure{}, deps.failure, "original failure should be of type Failure")
	require.IsType(t, &failurepb.Failure_ServerFailureInfo{}, deps.activity.RetryLastFailure.FailureInfo, "after truncation failure should be of type Failure_ServerFailureInfo")
}

func moveClockBeyondActivityExpirationTime(deps *retryActivityTestDeps) {
	expireAfter := deps.activity.StartToCloseTimeout
	if expireAfter != nil {
		deps.timeSource.Advance(deps.activity.StartedTime.AsTime().Sub(deps.timeSource.Now()) + expireAfter.AsDuration() + 1*time.Second)
	}
}

func TestRetryActivity_when_activity_has_no_retry_policy_should_fail(t *testing.T) {
	deps := setupRetryActivityTest(t)
	deps.activity.HasRetryPolicy = false
	deps.onActivityCreate.activitySize = deps.activity.Size()

	state, err := deps.mutableState.RetryActivity(deps.activity, deps.failure)

	require.NoError(t, err, "activity which has no retry policy should not be retried but it failed")
	require.Equal(t, enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET, state)
	assertActivityWasNotScheduled(t, deps, deps.activity, "with no retry policy")
	assertNoChange(t, deps, deps.activity, "activity should not change if it is not restarted")
}

func TestRetryActivity_when_activity_has_pending_cancel_request_should_fail(t *testing.T) {
	deps := setupRetryActivityTest(t)
	deps.activity.CancelRequested = true
	deps.onActivityCreate.activitySize = deps.activity.Size()

	state, err := deps.mutableState.RetryActivity(deps.activity, deps.failure)

	require.NoError(t, err, "activity which has no retry policy should not be retried but it failed")
	require.Equal(t, enumspb.RETRY_STATE_CANCEL_REQUESTED, state)
	assertActivityWasNotScheduled(t, deps, deps.activity, "with pending cancellation")
	assertNoChange(t, deps, deps.activity, "activity should not change if it is not restarted")
}

func TestRetryActivity_should_be_scheduled_when_next_backoff_interval_can_be_calculated(t *testing.T) {
	deps := setupRetryActivityTest(t)
	deps.mutableState.timeSource = deps.timeSource
	taskGeneratorMock := NewMockTaskGenerator(deps.controller)
	nextAttempt := deps.activity.Attempt + 1
	scheduledTime := deps.timeSource.Now().Add(1 * time.Second).UTC()
	taskGeneratorMock.EXPECT().GenerateActivityRetryTasks(deps.activity)
	deps.mutableState.taskGenerator = taskGeneratorMock

	// second := time.Second
	_, err := deps.mutableState.RetryActivity(deps.activity, deps.failure)
	require.NoError(t, err)
	require.Equal(t, deps.onActivityCreate.mutableStateApproximateSize-deps.onActivityCreate.activitySize+deps.activity.Size(), deps.mutableState.approximateSize)
	require.Equal(t, deps.activity.Version, deps.mutableState.currentVersion)
	require.Equal(t, deps.activity.Attempt, nextAttempt)

	require.Equal(t, scheduledTime, deps.activity.ScheduledTime.AsTime(), "Activity scheduled time is incorrect")
	// require.Equal(t, deps.nextBackoffStub.expected, deps.nextBackoffStub.recorded)
	assertTruncateFailureCalled(t, deps)
}

// TestRetryActivity_should_be_scheduled_when_next_retry_delay_is_set asserts that the activity is retried after NextRetryDelay period specified in the application failure.
func TestRetryActivity_should_be_scheduled_when_next_retry_delay_is_set(t *testing.T) {
	deps := setupRetryActivityTest(t)
	deps.mutableState.timeSource = deps.timeSource
	taskGeneratorMock := NewMockTaskGenerator(deps.controller)
	nextAttempt := deps.activity.Attempt + 1
	expectedScheduledTime := deps.timeSource.Now().Add(time.Minute).UTC()
	taskGeneratorMock.EXPECT().GenerateActivityRetryTasks(deps.activity)
	deps.mutableState.taskGenerator = taskGeneratorMock

	deps.failure.GetApplicationFailureInfo().NextRetryDelay = durationpb.New(time.Minute)
	_, err := deps.mutableState.RetryActivity(deps.activity, deps.failure)
	require.NoError(t, err)
	require.Equal(t, deps.onActivityCreate.mutableStateApproximateSize-deps.onActivityCreate.activitySize+deps.activity.Size(), deps.mutableState.approximateSize)
	require.Equal(t, deps.activity.Version, deps.mutableState.currentVersion)
	require.Equal(t, deps.activity.Attempt, nextAttempt)

	require.Equal(t, expectedScheduledTime, deps.activity.ScheduledTime.AsTime(), "Activity scheduled time is incorrect")
	assertTruncateFailureCalled(t, deps)
}

func TestRetryActivity_next_retry_delay_should_override_max_interval(t *testing.T) {
	deps := setupRetryActivityTest(t)
	deps.mutableState.timeSource = deps.timeSource
	taskGeneratorMock := NewMockTaskGenerator(deps.controller)
	nextAttempt := deps.activity.Attempt + 1
	expectedScheduledTime := deps.timeSource.Now().Add(3 * time.Minute).UTC()
	taskGeneratorMock.EXPECT().GenerateActivityRetryTasks(deps.activity)
	deps.mutableState.taskGenerator = taskGeneratorMock

	deps.failure.GetApplicationFailureInfo().NextRetryDelay = durationpb.New(3 * time.Minute)
	deps.activity.RetryMaximumInterval = durationpb.New(2 * time.Minute) // set retry max interval to be less than next retry delay duration.
	_, err := deps.mutableState.RetryActivity(deps.activity, deps.failure)
	require.NoError(t, err)
	require.Equal(t, deps.activity.Attempt, nextAttempt)

	require.Equal(t, expectedScheduledTime, deps.activity.ScheduledTime.AsTime(), "Activity scheduled time is incorrect")
	assertTruncateFailureCalled(t, deps)
}

func TestRetryActivity_when_no_next_backoff_interval_should_fail(t *testing.T) {
	deps := setupRetryActivityTest(t)
	taskGeneratorMock := NewMockTaskGenerator(deps.controller)
	deps.mutableState.taskGenerator = taskGeneratorMock
	deps.mutableState.timeSource = deps.timeSource
	moveClockBeyondActivityExpirationTime(deps)

	state, err := deps.mutableState.RetryActivity(deps.activity, deps.failure)

	require.NoError(t, err)
	require.Equal(t, enumspb.RETRY_STATE_TIMEOUT, state, "wrong state")
	assertActivityWasNotScheduled(t, deps, deps.activity, "which retries for too long")
	assertNoChange(t, deps, deps.activity, "activity should not change if it is not restarted")
}

func TestRetryActivity_when_task_can_not_be_generated_should_fail(t *testing.T) {
	deps := setupRetryActivityTest(t)
	e := errors.New("can't generate task")
	taskGeneratorMock := NewMockTaskGenerator(deps.controller)
	taskGeneratorMock.EXPECT().GenerateActivityRetryTasks(deps.activity).Return(e)
	deps.mutableState.taskGenerator = taskGeneratorMock

	deps.nextBackoffStub.onNextCallReturn(time.Second, enumspb.RETRY_STATE_IN_PROGRESS)
	state, err := deps.mutableState.RetryActivity(deps.activity, deps.failure)
	require.Error(t, err, e.Error())
	require.Equal(t,
		enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR,
		state,
		"failure to generate task should produce RETRY_STATE_INTERNAL_SERVER_ERROR got %v",
		state,
	)
}

func TestRetryActivity_when_workflow_is_not_mutable_should_fail(t *testing.T) {
	deps := setupRetryActivityTest(t)
	deps.mutableState.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED

	state, err := deps.mutableState.RetryActivity(deps.activity, deps.failure)

	require.Error(t, ErrWorkflowFinished, err.Error(), "when workflow finished should get error stating it")
	require.Equal(t, enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, state)
	assertActivityWasNotScheduled(t, deps, deps.activity, "for non-mutable workflow")
	assertNoChange(t, deps, deps.activity, "activity should not change if it is not restarted")
}

func TestRetryActivity_when_failure_in_list_of_not_retryable_should_fail(t *testing.T) {
	deps := setupRetryActivityTest(t)
	taskGeneratorMock := NewMockTaskGenerator(deps.controller)
	deps.mutableState.taskGenerator = taskGeneratorMock

	deps.activity.RetryNonRetryableErrorTypes = []string{"application-failure-type"}
	deps.onActivityCreate.activitySize = deps.activity.Size()

	state, err := deps.mutableState.RetryActivity(deps.activity, deps.failure)

	require.NoError(t, err)
	require.Equal(t, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, state, "wrong state want NON_RETRYABLE_FAILURE got %v", state)
	assertActivityWasNotScheduled(t, deps, deps.activity, "which retries for too long")
	assertNoChange(t, deps, deps.activity, "activity should not change if it is not restarted")
}
