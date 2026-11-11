package scheduler_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMigrationBackfill_NativeResumesAfterWatermark(t *testing.T) {
	checkMigrationBackfillWatermark(t, false)
}

func TestMigrationBackfill_MigratedResumesAfterWatermark(t *testing.T) {
	if os.Getenv("TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES") == "" {
		t.Skip("set TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1 to run known failing migration repros")
	}
	checkMigrationBackfillWatermark(t, true)
}

func TestMigrationBackfill_FreshInclusiveBoundary(t *testing.T) {
	env := newTestEnv(t)
	when := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	env.TimeSource.Update(when)
	firstAction := when.Add(-time.Minute)
	backfill := &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(firstAction.Add(-time.Millisecond)),
		EndTime:       timestamppb.New(when),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	state := migratedBackfillerState(t, env, backfill, when)

	complete, starts := processMigrationBackfill(t, env, state, 10)

	require.True(t, complete)
	require.Len(t, starts, 2)
	protorequire.ProtoEqual(t, timestamppb.New(firstAction), starts[0].ActualTime)
	protorequire.ProtoEqual(t, timestamppb.New(when), starts[1].ActualTime)
}

func TestMigrationBackfill_UnixEpochCursor(t *testing.T) {
	env := newTestEnv(t)
	cursor := time.Unix(0, 0).UTC()
	end := cursor.Add(time.Minute)
	backfill := &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(cursor),
		EndTime:       timestamppb.New(end),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	state := migratedBackfillerState(t, env, backfill, end)

	complete, starts := processMigrationBackfill(t, env, state, 10)

	require.True(t, complete)
	require.Len(t, starts, 1)
	protorequire.ProtoEqual(t, timestamppb.New(end), starts[0].ActualTime)
}

func TestMigrationBackfill_CapacityStallPreservesBoundary(t *testing.T) {
	env := newTestEnv(t)
	when := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	env.TimeSource.Update(when)
	firstAction := when.Add(-time.Minute)
	backfill := &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(firstAction.Add(-time.Millisecond)),
		EndTime:       timestamppb.New(when),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	state := migratedBackfillerState(t, env, backfill, when)

	state.Attempt++
	protorequire.ProtoEqual(t, backfill.StartTime, state.LastProcessedTime)

	complete, starts := processMigrationBackfill(t, env, state, 10)
	require.True(t, complete)
	require.Len(t, starts, 2)
	protorequire.ProtoEqual(t, timestamppb.New(firstAction), starts[0].ActualTime)
}

func TestMigrationBackfill_Empty(t *testing.T) {
	env := newTestEnv(t)
	when := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	imported := migration.LegacyToCreateFromMigrationStateRequest(
		env.Scheduler.Schedule,
		&schedulepb.ScheduleInfo{},
		&schedulespb.InternalState{
			Namespace:   namespace,
			NamespaceId: namespaceID,
			ScheduleId:  scheduleID,
		},
		nil,
		nil,
		when,
	)
	require.Empty(t, imported.State.Backfillers)
}

func TestMigrationBackfill_CreatedTaskExecutesRemainingRange(t *testing.T) {
	boundary := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	timeSource := clock.NewEventTimeSource().Update(boundary.Add(time.Hour))
	engine, engineCtx := newTestEngineContext(t, logger, withEngineTimeSource(timeSource))
	schedule := defaultSchedule()
	schedule.State.Paused = true
	request := migration.LegacyToCreateFromMigrationStateRequest(
		schedule,
		&schedulepb.ScheduleInfo{},
		&schedulespb.InternalState{
			Namespace:         namespace,
			NamespaceId:       namespaceID,
			ScheduleId:        scheduleID,
			LastProcessedTime: timestamppb.New(timeSource.Now()),
			OngoingBackfills: []*schedulepb.BackfillRequest{{
				StartTime:     timestamppb.New(boundary),
				EndTime:       timestamppb.New(boundary.Add(time.Minute)),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			}},
		},
		nil,
		nil,
		timeSource.Now(),
	)

	_, err := scheduler.NewTestHandler(logger).TestCreateFromMigrationState(engineCtx, request)
	require.NoError(t, err)
	ref := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})
	_, err = chasm.ReadComponent(
		engineCtx,
		ref,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			require.Empty(t, s.Backfillers)
			starts := s.Invoker.Get(ctx).BufferedStarts
			require.Len(t, starts, 1)
			protorequire.ProtoEqual(t, timestamppb.New(boundary.Add(time.Minute)), starts[0].ActualTime)
			return struct{}{}, nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	_, err = engine.FirePureTasks(ref, timeSource.Now())
	require.NoError(t, err)
}

func TestMigrationBackfill_WaitsForCallbackReconciliation(t *testing.T) {
	boundary := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	timeSource := clock.NewEventTimeSource().Update(boundary.Add(time.Hour))
	engine, engineCtx := newTestEngineContext(t, logger, withEngineTimeSource(timeSource))
	schedule := defaultSchedule()
	schedule.State.Paused = true
	schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	request := migration.LegacyToCreateFromMigrationStateRequest(
		schedule,
		&schedulepb.ScheduleInfo{RunningWorkflows: []*commonpb.WorkflowExecution{{
			WorkflowId: "stale-workflow",
			RunId:      "stale-run",
		}}},
		&schedulespb.InternalState{
			Namespace:         namespace,
			NamespaceId:       namespaceID,
			ScheduleId:        scheduleID,
			LastProcessedTime: timestamppb.New(timeSource.Now()),
			BufferedStarts: []*schedulespb.BufferedStart{{
				NominalTime: timestamppb.New(boundary),
				ActualTime:  timestamppb.New(boundary),
				Manual:      true,
				RequestId:   "pending-request",
				WorkflowId:  "pending-workflow",
			}},
			OngoingBackfills: []*schedulepb.BackfillRequest{{
				StartTime: timestamppb.New(boundary),
				EndTime:   timestamppb.New(boundary.Add(time.Minute)),
			}},
		},
		nil,
		nil,
		timeSource.Now(),
	)

	_, err := scheduler.NewTestHandler(logger).TestCreateFromMigrationState(engineCtx, request)
	require.NoError(t, err)
	ref := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})
	_, err = chasm.ReadComponent(
		engineCtx,
		ref,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			require.Len(t, s.Backfillers, 1)
			require.Zero(t, s.Info.OverlapSkipped)
			starts := s.Invoker.Get(ctx).BufferedStarts
			require.Len(t, starts, 2)
			require.Equal(t, "pending-request", starts[0].RequestId)
			require.False(t, starts[1].HasCallback)
			return struct{}{}, nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	historyClient := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
	historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&historyservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				CloseTime: timestamppb.New(boundary.Add(time.Minute)),
			},
		},
		nil,
	)
	callbackHandler := scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
		Config:        defaultConfig(),
		HistoryClient: historyClient,
	})
	require.NoError(t, callbackHandler.Execute(
		engineCtx,
		ref,
		chasm.TaskAttributes{},
		&schedulerpb.SchedulerCallbacksTask{},
	))
	_, err = engine.FirePureTasks(ref, timeSource.Now())
	require.NoError(t, err)
	_, err = chasm.ReadComponent(
		engineCtx,
		ref,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			require.Empty(t, s.Backfillers)
			require.LessOrEqual(t, s.Info.OverlapSkipped, int64(1))
			for _, start := range s.Invoker.Get(ctx).BufferedStarts {
				if start.GetRunId() == "stale-run" {
					require.NotNil(t, start.Completed)
				}
			}
			return struct{}{}, nil
		},
		struct{}{},
	)
	require.NoError(t, err)
}

func checkMigrationBackfillWatermark(t *testing.T, migrate bool) {
	t.Helper()
	env := newTestEnv(t)
	when := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	env.TimeSource.Update(when)
	watermark := timestamppb.New(when.Add(-time.Minute))
	backfill := &schedulepb.BackfillRequest{
		StartTime:     watermark,
		EndTime:       timestamppb.New(when),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	state := &schedulerpb.BackfillerState{
		Request:           &schedulerpb.BackfillerState_BackfillRequest{BackfillRequest: backfill},
		LastProcessedTime: watermark,
		Attempt:           1,
	}
	if migrate {
		state = migratedBackfillerState(t, env, backfill, when)
	}
	complete, starts := processMigrationBackfill(t, env, state, 10)
	require.True(t, complete)
	require.Len(t, starts, 1, "V1's incremental cursor is exclusive")
	protorequire.ProtoEqual(t, timestamppb.New(when), starts[0].ActualTime)
}

func migratedBackfillerState(
	t *testing.T,
	env *testEnv,
	backfill *schedulepb.BackfillRequest,
	when time.Time,
) *schedulerpb.BackfillerState {
	t.Helper()
	imported := migration.LegacyToCreateFromMigrationStateRequest(
		env.Scheduler.Schedule,
		&schedulepb.ScheduleInfo{ActionCount: 1},
		&schedulespb.InternalState{
			Namespace:        namespace,
			NamespaceId:      namespaceID,
			ScheduleId:       scheduleID,
			OngoingBackfills: []*schedulepb.BackfillRequest{backfill},
		},
		nil,
		nil,
		when,
	)
	require.Len(t, imported.State.Backfillers, 1)
	for _, converted := range imported.State.Backfillers {
		protorequire.ProtoEqual(t, backfill.StartTime, converted.LastProcessedTime)
		require.Zero(t, converted.Attempt)
		require.True(t, converted.HasRecordedProgress)
		protorequire.ProtoEqual(t, backfill, converted.GetBackfillRequest())
		return converted
	}
	return nil
}

func processMigrationBackfill(
	t *testing.T,
	env *testEnv,
	state *schedulerpb.BackfillerState,
	limit int,
) (bool, []*schedulespb.BufferedStart) {
	t.Helper()
	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})
	result, err := handler.ProcessBackfill(env.Scheduler, &scheduler.Backfiller{BackfillerState: state}, limit)
	require.NoError(t, err)
	return result.Complete, result.BufferedStarts
}
