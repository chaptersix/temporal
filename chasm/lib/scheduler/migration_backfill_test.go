package scheduler_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/protorequire"
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
			state = converted
		}
	}
	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})
	result, err := handler.ProcessBackfill(env.Scheduler, &scheduler.Backfiller{BackfillerState: state}, 10)
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.Len(t, result.BufferedStarts, 1, "V1's incremental cursor is exclusive")
	protorequire.ProtoEqual(t, timestamppb.New(when), result.BufferedStarts[0].ActualTime)
}
