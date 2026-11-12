package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type migrationCloseFaultEngine struct {
	chasm.Engine
	afterCommit bool
	armed       bool
}

func (e *migrationCloseFaultEngine) UpdateComponent(
	ctx context.Context, ref chasm.ComponentRef, update func(chasm.MutableContext, chasm.Component) error, opts ...chasm.TransitionOption,
) ([]byte, error) {
	if !e.armed {
		return e.Engine.UpdateComponent(ctx, ref, update, opts...)
	}
	e.armed = false
	if e.afterCommit {
		if _, err := e.Engine.UpdateComponent(ctx, ref, update, opts...); err != nil {
			return nil, err
		}
	}
	return nil, serviceerror.NewUnavailable("injected source-close response loss")
}

func newRollbackScenario(t *testing.T) (*schedulerTestEngine, *scheduler.SchedulerMigrateToWorkflowTaskHandler, *historyservicemock.MockHistoryServiceClient) {
	t.Helper()
	ts := clock.NewEventTimeSource().Update(time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC))
	sched := defaultSchedule()
	sched.State.Paused = true
	sched.State.Notes = "user maintenance"
	e := newSchedulerTestEngine(t, sched, withEngineTimeSource(ts))
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		_, err := s.Patch(ctx, &schedulerpb.PatchScheduleRequest{FrontendRequest: &workflowservice.PatchScheduleRequest{
			RequestId: "trigger-request", Patch: &schedulepb.SchedulePatch{
				TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
			},
		}})
		if err != nil {
			return err
		}
		s.Visibility.Get(ctx).MergeCustomMemo(ctx, map[string]*commonpb.Payload{"custom": payload.EncodeString("memo")})
		if err := s.Visibility.Get(ctx).MergeCustomSearchAttributes(ctx, map[string]*commonpb.Payload{"CustomKeywordField": payload.EncodeString("attribute")}); err != nil {
			return err
		}
		_, err = s.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{RequestId: "rollback"})
		return err
	}))
	historyClient := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
	handler := scheduler.NewSchedulerMigrateToWorkflowTaskHandler(scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{
		Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: e.logger,
		HistoryClient: historyClient, SaMapperProvider: searchattribute.NewTestMapperProvider(nil),
	})
	return e, handler, historyClient
}

func executeRollbackTask(t *testing.T, e *schedulerTestEngine, handler *scheduler.SchedulerMigrateToWorkflowTaskHandler, engine chasm.Engine) (bool, error) {
	t.Helper()
	var valid bool
	err := e.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		var err error
		valid, err = handler.Validate(ctx, s, chasm.TaskInvocation{}, &schedulerpb.SchedulerMigrateToWorkflowTask{})
		return err
	})
	if err != nil || !valid {
		return !valid, err
	}
	return false, handler.Execute(chasm.NewEngineContext(context.Background(), engine), e.rootRef, chasm.TaskAttributes{}, &schedulerpb.SchedulerMigrateToWorkflowTask{})
}

func TestMigrationScenario_RollbackRetryBoundaries(t *testing.T) {
	for _, boundary := range []string{"success", "before_destination", "after_destination", "before_source_close", "after_source_close"} {
		t.Run(boundary, func(t *testing.T) {
			e, handler, historyClient := newRollbackScenario(t)
			faultEngine := &migrationCloseFaultEngine{Engine: e.engine,
				armed: boundary == "before_source_close" || boundary == "after_source_close", afterCommit: boundary == "after_source_close"}
			var destination *schedulespb.StartScheduleArgs
			var firstRequestID string
			attempts, creations := 0, 0
			historyClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
				func(_ context.Context, req *historyservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
					attempts++
					if attempts == 1 {
						firstRequestID = req.StartRequest.RequestId
					}
					if attempts == 1 && boundary == "before_destination" {
						return nil, serviceerror.NewUnavailable("injected before destination commit")
					}
					if destination != nil {
						return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("retry", firstRequestID, "destination-run")
					}
					destination = &schedulespb.StartScheduleArgs{}
					require.NoError(t, sdk.PreferProtoDataConverter.FromPayloads(req.StartRequest.Input, destination))
					protorequire.ProtoEqual(t, payload.EncodeString("memo"), req.StartRequest.Memo.Fields["custom"])
					protorequire.ProtoEqual(t, payload.EncodeString("attribute"), req.StartRequest.SearchAttributes.IndexedFields["CustomKeywordField"])
					creations++
					if attempts == 1 && boundary == "after_destination" {
						return nil, serviceerror.NewUnavailable("injected after destination commit")
					}
					return &historyservice.StartWorkflowExecutionResponse{RunId: "destination-run"}, nil
				})
			dropped, err := executeRollbackTask(t, e, handler, faultEngine)
			require.False(t, dropped)
			if boundary == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				_, err = executeRollbackTask(t, e, handler, faultEngine)
				require.NoError(t, err)
			}
			require.Equal(t, 1, creations)
			require.NotNil(t, destination)
			require.True(t, destination.Schedule.State.Paused)
			require.Equal(t, "user maintenance", destination.Schedule.State.Notes)
			require.Len(t, destination.State.BufferedStarts, 1)
			require.True(t, destination.State.BufferedStarts[0].Manual)
			require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
				require.True(t, s.Closed)
				require.Nil(t, s.WorkflowMigration)
				return nil
			}))
			dropped, err = executeRollbackTask(t, e, handler, faultEngine)
			require.NoError(t, err)
			require.True(t, dropped, "duplicate task must not create another destination")
		})
	}
}

func TestMigrationRollbackDestinationCollision(t *testing.T) {
	e, handler, historyClient := newRollbackScenario(t)
	historyClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil,
		serviceerror.NewWorkflowExecutionAlreadyStarted("unrelated scheduler owns this ID", "unrelated-create-request", "unrelated-run"))
	historyClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(
		&historyservice.GetMutableStateResponse{FirstExecutionRunId: "unrelated-run"}, nil)
	historyClient.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).Return(
		&historyservice.DescribeMutableStateResponse{DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "unrelated-run", CreateRequestId: "unrelated-create-request"},
		}}, nil)
	_, err := executeRollbackTask(t, e, handler, e.engine)
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.False(t, s.Closed, "rollback must retain the source and its acknowledged trigger when a different workflow owns the destination ID")
		return nil
	}))
	require.ErrorAs(t, err, new(*serviceerror.AlreadyExists), "an unrelated destination is a conflict, not a successful handoff")
}

func TestMigrationRollbackSendsStoredRequestID(t *testing.T) {
	e, handler, historyClient := newRollbackScenario(t)
	var requestIDs []string
	historyClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Times(2).DoAndReturn(
		func(_ context.Context, req *historyservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
			requestIDs = append(requestIDs, req.StartRequest.RequestId)
			if len(requestIDs) == 1 {
				return nil, serviceerror.NewUnavailable("injected")
			}
			return &historyservice.StartWorkflowExecutionResponse{RunId: "destination-run"}, nil
		})
	_, err := executeRollbackTask(t, e, handler, e.engine)
	require.Error(t, err)
	_, err = executeRollbackTask(t, e, handler, e.engine)
	require.NoError(t, err)
	require.Equal(t, []string{"rollback", "rollback"}, requestIDs)
}

func TestMigrationRollbackAssignsRequestIDToLegacyMigration(t *testing.T) {
	e, handler, historyClient := newRollbackScenario(t)
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, _ chasm.MutableContext) error {
		s.WorkflowMigration.RequestId = ""
		return nil
	}))
	var requestID string
	historyClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *historyservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
			requestID = req.StartRequest.RequestId
			return nil, serviceerror.NewUnavailable("injected")
		})
	_, err := executeRollbackTask(t, e, handler, e.engine)
	require.Error(t, err)
	require.NotEmpty(t, requestID)
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.Equal(t, requestID, s.WorkflowMigration.GetRequestId(), "the ID must be persisted before the first start attempt")
		require.False(t, s.Closed)
		return nil
	}))
}

func TestMigrationRollbackLegacyMigrationRejectsUnprovenDestination(t *testing.T) {
	e, handler, historyClient := newRollbackScenario(t)
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, _ chasm.MutableContext) error {
		s.WorkflowMigration.RequestId = ""
		return nil
	}))
	// An attempt made before the upgrade may have started the workflow under a random ID.
	historyClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil,
		serviceerror.NewWorkflowExecutionAlreadyStarted("old attempt", "random-pre-upgrade-id", "old-run"))
	historyClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(
		&historyservice.GetMutableStateResponse{FirstExecutionRunId: "old-run"}, nil)
	historyClient.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).Return(
		&historyservice.DescribeMutableStateResponse{DatabaseMutableState: &persistencespb.WorkflowMutableState{
			ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "old-run", CreateRequestId: "random-pre-upgrade-id"},
		}}, nil)
	_, err := executeRollbackTask(t, e, handler, e.engine)
	require.ErrorAs(t, err, new(*serviceerror.AlreadyExists))
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.False(t, s.Closed)
		return nil
	}))
}

func TestMigrateToWorkflowGeneratesRequestID(t *testing.T) {
	s, ctx, _ := setupSchedulerForTest(t)
	_, err := s.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, s.WorkflowMigration.GetRequestId())
	stored := s.WorkflowMigration.GetRequestId()

	// A repeated request does not replace the stored ID.
	_, err = s.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{RequestId: "other"})
	require.NoError(t, err)
	require.Equal(t, stored, s.WorkflowMigration.GetRequestId())
}
