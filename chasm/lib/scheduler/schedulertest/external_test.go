package schedulertest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStartAcceptanceCallbackAndRecording(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
	} {
		for _, position := range []string{"before_response", "before_record", "after_record"} {
			t.Run(policy.String()+"/"+position, func(t *testing.T) {
				t.Parallel()
				start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
				schedule := oneShotSchedule(start, time.Minute)
				schedule.Policies.OverlapPolicy = policy
				d := schedulertest.NewDriver(t, schedulertest.WithSchedule(schedule), schedulertest.WithControlledStarts())
				require.True(t, d.StepPure())
				require.True(t, d.BeginSideEffect())
				call := d.NextStart()
				accepted, err := call.Accept()
				require.NoError(t, err)
				before := d.Snapshot()
				require.Len(t, before.AcceptedStarts, 1)
				require.Empty(t, before.RecordedStarts)
				require.Zero(t, before.ActionCount)
				if position == "before_response" {
					require.NoError(t, d.DeliverCompletion(call.Request.GetRequestId()))
					require.NoError(t, d.DeliverCompletion(call.Request.GetRequestId()))
				}
				call.Respond(accepted, nil)
				record := d.NextRecord()
				require.Zero(t, d.Snapshot().ActionCount, "a successful response is not yet a recorded start")
				if position == "before_record" {
					require.NoError(t, d.DeliverCompletion(call.Request.GetRequestId()))
					require.NoError(t, d.DeliverCompletion(call.Request.GetRequestId()))
				}
				require.NoError(t, record.Record())
				d.FinishSideEffect()
				if position == "after_record" {
					require.NoError(t, d.DeliverCompletion(call.Request.GetRequestId()))
					require.NoError(t, d.DeliverCompletion(call.Request.GetRequestId()))
				}
				cur := d.Snapshot()
				require.Empty(t, schedulertest.CheckInvariants(&before, cur))
				require.Equal(t, int64(1), cur.ActionCount)
				require.Equal(t, accepted, cur.RecordedStarts[call.Request.GetRequestId()])
			})
		}
	}
}

func TestLostStartResponseDeduplicatesRetry(t *testing.T) {
	t.Parallel()
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	schedule := oneShotSchedule(start, time.Minute)
	schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	d := schedulertest.NewDriver(t, schedulertest.WithSchedule(schedule), schedulertest.WithControlledStarts())
	require.True(t, d.StepPure())
	require.True(t, d.BeginSideEffect())
	first := d.NextStart()
	accepted, err := first.Accept()
	require.NoError(t, err)
	first.Respond(accepted, serviceerror.NewUnavailable("accepted but response lost"))
	d.FinishSideEffect()
	pending := d.Snapshot()
	require.Len(t, pending.AcceptedStarts, 1)
	require.Len(t, pending.PendingStarts, 1)
	require.Zero(t, pending.ActionCount)
	require.Empty(t, schedulertest.CheckInvariants(nil, pending))
	require.True(t, d.StepPure())
	require.True(t, d.BeginSideEffect())
	retry := d.NextStart()
	require.Equal(t, first.Request.GetRequestId(), retry.Request.GetRequestId())
	require.Equal(t, first.Request.GetWorkflowId(), retry.Request.GetWorkflowId())
	again, err := retry.Accept()
	require.NoError(t, err)
	require.Equal(t, accepted, again)
	retry.Respond(again, nil)
	d.FinishSideEffect()
	cur := d.Snapshot()
	require.Len(t, cur.AcceptedStarts, 1)
	require.Equal(t, int64(1), cur.ActionCount)
	require.Empty(t, schedulertest.CheckInvariants(&pending, cur))
}

func TestAcceptedStartAccountingDetector(t *testing.T) {
	t.Parallel()
	start := schedulertest.ExternalStart{WorkflowID: "workflow", RunID: "run"}
	cases := []struct {
		name     string
		snapshot schedulertest.Snapshot
		want     string
	}{
		{"lost after RPC", schedulertest.Snapshot{AcceptedStarts: map[string]schedulertest.ExternalStart{"a": start}}, "accepted-start-lost"},
		{"double counted", schedulertest.Snapshot{AcceptedStarts: map[string]schedulertest.ExternalStart{"a": start}, RecordedStarts: map[string]schedulertest.ExternalStart{"a": start}, ActionCount: 2}, "start-accounting"},
		{"wrong run", schedulertest.Snapshot{AcceptedStarts: map[string]schedulertest.ExternalStart{"a": start}, RecordedStarts: map[string]schedulertest.ExternalStart{"a": {WorkflowID: "workflow", RunID: "wrong"}}, ActionCount: 1}, "start-identity"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.snapshot.HasPendingTask = true
			require.Contains(t, violationNames(schedulertest.CheckInvariants(nil, tc.snapshot)), tc.want)
		})
	}
}

func TestExpiredSelectionReleasesRemainingBuffer(t *testing.T) {
	t.Parallel()
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	schedule := schedulertest.DefaultSchedule()
	schedule.Spec.Interval[0].Interval = durationpb.New(10 * time.Second)
	schedule.Spec.EndTime = timestamppb.New(start.Add(31 * time.Second))
	schedule.Policies.CatchupWindow = durationpb.New(time.Minute)
	schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	d := schedulertest.NewDriver(t, schedulertest.WithSchedule(schedule),
		schedulertest.WithMaxRunDuration(5*time.Minute),
		schedulertest.WithConfig(configWith(func(tw *scheduler.Tweakables) { tw.IdleTime = 10 * time.Minute })),
		schedulertest.WithAfterStep(schedulertest.CheckInvariantsHook(t)))
	for range 3 {
		require.True(t, d.Step())
	}
	require.True(t, d.StepPure())
	require.Empty(t, d.Snapshot().PendingStarts)
}
