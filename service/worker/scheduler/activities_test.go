package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	migrationTestNamespace   = "test-namespace"
	migrationTestNamespaceID = "test-namespace-id"
	migrationTestScheduleID  = "test-schedule-id"
)

func testSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: &durationpb.Duration{Seconds: 3600}}, // 1 hour
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "test-workflow-id",
					WorkflowType: &commonpb.WorkflowType{Name: "test-workflow"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "test-queue"},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}
}

func testMigrateScheduleRequest() *schedulespb.MigrateScheduleRequest {
	now := time.Now().UTC()
	return &schedulespb.MigrateScheduleRequest{
		Schedule: testSchedule(),
		Info: &schedulepb.ScheduleInfo{
			ActionCount: 10,
		},
		State: &schedulespb.InternalState{
			Namespace:         migrationTestNamespace,
			NamespaceId:       migrationTestNamespaceID,
			ScheduleId:        migrationTestScheduleID,
			ConflictToken:     42,
			LastProcessedTime: timestamppb.New(now),
			BufferedStarts: []*schedulespb.BufferedStart{
				{
					NominalTime:   timestamppb.New(now),
					ActualTime:    timestamppb.New(now),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
				},
			},
		},
		SearchAttributes: map[string]*commonpb.Payload{
			"CustomAttr": {Data: []byte("attr-value")},
		},
		Memo: map[string]*commonpb.Payload{
			"MemoKey": {Data: []byte("memo-value")},
		},
	}
}

func TestMigrateScheduleActivity_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)

	mockSchedulerClient := schedulerpb.NewMockSchedulerServiceClient(ctrl)

	// Expect ImportSchedule to be called with correct parameters
	mockSchedulerClient.EXPECT().
		ImportSchedule(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *schedulerpb.ImportScheduleRequest, _ ...interface{}) (*schedulerpb.ImportScheduleResponse, error) {
			// Verify the request has the right data
			require.Equal(t, migrationTestNamespaceID, req.NamespaceId)
			require.Equal(t, migrationTestNamespace, req.Namespace)
			require.Equal(t, migrationTestScheduleID, req.ScheduleId)
			require.Equal(t, int64(42), req.SchedulerState.ConflictToken)
			require.NotNil(t, req.SchedulerState.Schedule)
			require.NotNil(t, req.GeneratorState)
			require.NotNil(t, req.InvokerState)
			return &schedulerpb.ImportScheduleResponse{}, nil
		})

	a := &activities{
		activityDeps: activityDeps{
			Logger:          logger,
			SchedulerClient: mockSchedulerClient,
		},
	}

	req := testMigrateScheduleRequest()
	err := a.MigrateSchedule(context.Background(), req)
	require.NoError(t, err)
}

func TestMigrateScheduleActivity_AlreadyExistsIsSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)

	mockSchedulerClient := schedulerpb.NewMockSchedulerServiceClient(ctrl)

	// ImportSchedule returns WorkflowExecutionAlreadyStarted error
	mockSchedulerClient.EXPECT().
		ImportSchedule(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("", "", ""))

	a := &activities{
		activityDeps: activityDeps{
			Logger:          logger,
			SchedulerClient: mockSchedulerClient,
		},
	}

	req := testMigrateScheduleRequest()
	err := a.MigrateSchedule(context.Background(), req)
	// Should succeed - already exists is treated as success (idempotency)
	require.NoError(t, err)
}

func TestMigrateScheduleActivity_ImportError(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)

	mockSchedulerClient := schedulerpb.NewMockSchedulerServiceClient(ctrl)

	// ImportSchedule returns a transient error
	mockSchedulerClient.EXPECT().
		ImportSchedule(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("transient error"))

	a := &activities{
		activityDeps: activityDeps{
			Logger:          logger,
			SchedulerClient: mockSchedulerClient,
		},
	}

	req := testMigrateScheduleRequest()
	err := a.MigrateSchedule(context.Background(), req)
	// Should return the error so workflow can retry
	require.Error(t, err)
	require.Contains(t, err.Error(), "transient error")
}

func TestMigrateScheduleActivity_PreservesConflictToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)

	mockSchedulerClient := schedulerpb.NewMockSchedulerServiceClient(ctrl)

	expectedConflictToken := int64(99)

	// Verify the conflict token is preserved
	mockSchedulerClient.EXPECT().
		ImportSchedule(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *schedulerpb.ImportScheduleRequest, _ ...interface{}) (*schedulerpb.ImportScheduleResponse, error) {
			require.Equal(t, expectedConflictToken, req.SchedulerState.ConflictToken)
			return &schedulerpb.ImportScheduleResponse{}, nil
		})

	a := &activities{
		activityDeps: activityDeps{
			Logger:          logger,
			SchedulerClient: mockSchedulerClient,
		},
	}

	req := testMigrateScheduleRequest()
	req.State.ConflictToken = expectedConflictToken
	err := a.MigrateSchedule(context.Background(), req)
	require.NoError(t, err)
}

func TestMigrateScheduleActivity_TransfersSearchAttributesAndMemo(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)

	mockSchedulerClient := schedulerpb.NewMockSchedulerServiceClient(ctrl)

	// Verify search attributes and memo are passed through
	mockSchedulerClient.EXPECT().
		ImportSchedule(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *schedulerpb.ImportScheduleRequest, _ ...interface{}) (*schedulerpb.ImportScheduleResponse, error) {
			require.Contains(t, req.SearchAttributes, "CustomAttr")
			require.Contains(t, req.Memo, "MemoKey")
			return &schedulerpb.ImportScheduleResponse{}, nil
		})

	a := &activities{
		activityDeps: activityDeps{
			Logger:          logger,
			SchedulerClient: mockSchedulerClient,
		},
	}

	req := testMigrateScheduleRequest()
	err := a.MigrateSchedule(context.Background(), req)
	require.NoError(t, err)
}
