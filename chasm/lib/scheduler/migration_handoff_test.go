package scheduler_test

import (
	"context"
	"os"
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
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type migrationCloseFaultEngine struct {
	chasm.Engine
	afterCommit bool
	armed       bool
	skip        int
}

func requireMigrationCounterexamples(t *testing.T) {
	t.Helper()
	if os.Getenv("TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES") == "" {
		t.Skip("set TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1 to run known failing migration repros")
	}
}

func (e *migrationCloseFaultEngine) UpdateComponent(
	ctx context.Context, ref chasm.ComponentRef, update func(chasm.MutableContext, chasm.Component) error, opts ...chasm.TransitionOption,
) ([]byte, error) {
	if e.skip > 0 {
		e.skip--
		return e.Engine.UpdateComponent(ctx, ref, update, opts...)
	}
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
		s.Visibility.Get(ctx).MergeCustomSearchAttributes(ctx, map[string]*commonpb.Payload{"CustomKeywordField": payload.EncodeString("attribute")})
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
			faultEngine := &migrationCloseFaultEngine{Engine: e.engine, skip: 1,
				armed: boundary == "before_source_close" || boundary == "after_source_close", afterCommit: boundary == "after_source_close"}
			var destination *schedulespb.StartScheduleArgs
			var destinationRequestID string
			attempts, creations := 0, 0
			historyClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
				func(_ context.Context, req *historyservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
					attempts++
					if attempts == 1 && boundary == "before_destination" {
						return nil, serviceerror.NewUnavailable("injected before destination commit")
					}
					if destination != nil {
						require.Equal(t, destinationRequestID, req.StartRequest.RequestId)
						return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("retry", destinationRequestID, "destination-run")
					}
					destinationRequestID = req.StartRequest.RequestId
					require.Equal(t, "rollback", destinationRequestID)
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
			historyClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
				func(context.Context, *historyservice.GetMutableStateRequest, ...grpc.CallOption) (*historyservice.GetMutableStateResponse, error) {
					if destination == nil {
						return nil, serviceerror.NewNotFound("no destination")
					}
					return &historyservice.GetMutableStateResponse{FirstExecutionRunId: "destination-run"}, nil
				})
			historyClient.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
				func(context.Context, *historyservice.DescribeMutableStateRequest, ...grpc.CallOption) (*historyservice.DescribeMutableStateResponse, error) {
					return &historyservice.DescribeMutableStateResponse{DatabaseMutableState: &persistencespb.WorkflowMutableState{ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "destination-run", CreateRequestId: destinationRequestID}}}, nil
				})
			dropped, err := executeRollbackTask(t, e, handler, faultEngine)
			if boundary == "before_destination" {
				require.Error(t, err)
				_, err = executeRollbackTask(t, e, handler, faultEngine)
				require.Error(t, err)
				require.Zero(t, creations)
				return
			}

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

func TestMigrationCounterexample_RollbackDestinationCollision(t *testing.T) {
	requireMigrationCounterexamples(t)
	e, handler, historyClient := newRollbackScenario(t)
	historyClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil,
		serviceerror.NewWorkflowExecutionAlreadyStarted("unrelated scheduler owns this ID", "unrelated-create-request", "unrelated-run"))
	_, err := executeRollbackTask(t, e, handler, e.engine)
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.False(t, s.Closed, "rollback must retain the source and its acknowledged trigger when a different workflow owns the destination ID")
		return nil
	}))
	require.Error(t, err, "an unrelated destination is a conflict, not a successful handoff")
}

func TestMigrationScenario_RollbackWithoutDurableRequestIDFailsClosed(t *testing.T) {
	e, handler, _ := newRollbackScenario(t)
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, _ chasm.MutableContext) error {
		s.WorkflowMigration.RequestId = ""
		return nil
	}))

	_, err := executeRollbackTask(t, e, handler, e.engine)

	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPreconditionErr)
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.False(t, s.Closed)
		require.NotNil(t, s.WorkflowMigration)
		return nil
	}))
}

func TestMigrationScenario_NativeBackfillResumesAfterWatermark(t *testing.T) {
	checkForwardBackfillWatermark(t, false)
}

func TestMigrationCounterexample_ForwardBackfillReplaysWatermark(t *testing.T) {
	requireMigrationCounterexamples(t)
	checkForwardBackfillWatermark(t, true)
}

func checkForwardBackfillWatermark(t *testing.T, migrate bool) {
	t.Helper()
	env := newTestEnv(t)
	when := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	env.TimeSource.Update(when)
	watermark := timestamppb.New(when.Add(-time.Minute))
	backfill := &schedulepb.BackfillRequest{
		StartTime: watermark, EndTime: timestamppb.New(when), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	state := &schedulerpb.BackfillerState{
		Request:           &schedulerpb.BackfillerState_BackfillRequest{BackfillRequest: backfill},
		LastProcessedTime: watermark, Attempt: 1,
	}
	if migrate {
		imported := migration.LegacyToCreateFromMigrationStateRequest(env.Scheduler.Schedule,
			&schedulepb.ScheduleInfo{ActionCount: 1}, &schedulespb.InternalState{
				Namespace: namespace, NamespaceId: namespaceID, ScheduleId: scheduleID,
				OngoingBackfills: []*schedulepb.BackfillRequest{backfill},
			}, nil, nil, when)
		require.Len(t, imported.State.Backfillers, 1)
		for _, converted := range imported.State.Backfillers {
			state = converted
		}
	}
	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: env.Logger, SpecProcessor: env.SpecProcessor,
	})
	result, err := handler.ProcessBackfill(env.Scheduler, &scheduler.Backfiller{BackfillerState: state}, 10)
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.Len(t, result.BufferedStarts, 1, "V1's incremental cursor is exclusive: the action at its watermark was already accounted for")
	protorequire.ProtoEqual(t, timestamppb.New(when), result.BufferedStarts[0].ActualTime)
}
