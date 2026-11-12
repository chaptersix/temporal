package tests

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSchedule2MirroredIncludeExcludeSpec(t *testing.T) {
	t.Parallel()
	// A tiny compute bound trips the mirrored spec near-instantly; the default (~1.2M candidate
	// scans per GetNextTime) makes this test burn seconds of CPU on every scheduler code path.
	opts := []testcore.TestOption{testcore.WithDynamicConfig(dynamicconfig.SchedulerSpecMaxIterations, 1000)}
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend}, opts...)

	sid := testcore.RandomizeStr("sched-cancelling-spec")
	wid := testcore.RandomizeStr("sched-cancelling-spec-wf")
	wt := testcore.RandomizeStr("sched-cancelling-spec-wt")

	everySecond := &schedulepb.CalendarSpec{Second: "*", Minute: "*", Hour: "*"}

	ctx, cancel := context.WithTimeout(schedule2Backend.context(t.Context()), 10*time.Second)
	defer cancel()
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Calendar:        []*schedulepb.CalendarSpec{everySecond},
			ExcludeCalendar: []*schedulepb.CalendarSpec{everySecond}, //nolint:staticcheck // Exercise supported legacy exclusion syntax at public ingress.
		},
		Action: startWorkflowAction(s, wid, wt),
	})

	// ListMatchingTimes must surface the compute limit as an error rather than hang.
	_, lmErr := s.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		StartTime:  timestamppb.New(time.Now().UTC()),
		EndTime:    timestamppb.New(time.Now().UTC().Add(time.Hour)),
	})
	require.Error(t, lmErr, "ListMatchingTimes should error for a mirrored include/exclude spec")
}

func TestSchedule2MirroredIncludeExcludeSpecOnUpdate(t *testing.T) {
	t.Parallel()
	// A tiny compute bound trips the mirrored spec near-instantly (see TestMirroredIncludeExcludeSpec).
	opts := []testcore.TestOption{testcore.WithDynamicConfig(dynamicconfig.SchedulerSpecMaxIterations, 1000)}
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend}, opts...)

	sid := testcore.RandomizeStr("sched-cancelling-update")
	wid := testcore.RandomizeStr("sched-cancelling-update-wf")
	wt := testcore.RandomizeStr("sched-cancelling-update-wt")

	ctx := schedule2Backend.context(t.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(1 * time.Hour),
		Action: startWorkflowAction(s, wid, wt),
	})

	everySecond := &schedulepb.CalendarSpec{Second: "*", Minute: "*", Hour: "*"}
	updateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := s.FrontendClient().UpdateSchedule(updateCtx, &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Calendar:        []*schedulepb.CalendarSpec{everySecond},
				ExcludeCalendar: []*schedulepb.CalendarSpec{everySecond}, //nolint:staticcheck // Exercise supported legacy exclusion syntax at public ingress.
			},
			Action: startWorkflowAction(s, wid, wt),
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	require.NoError(t, err, "UpdateSchedule to a mirrored include/exclude spec should not hang")

	// Once the mirrored spec is applied, ListMatchingTimes must surface the compute limit.
	await.RequireTruef(t, func() bool {
		_, lmErr := s.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			StartTime:  timestamppb.New(time.Now().UTC()),
			EndTime:    timestamppb.New(time.Now().UTC().Add(time.Hour)),
		})
		return lmErr != nil
	}, awaitTimeout, pollInterval, "ListMatchingTimes should error once the mirrored spec is applied")
}

func TestSchedule2ScheduleFarFutureActionTimes(t *testing.T) {
	t.Parallel()
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend})

	sid := testcore.RandomizeStr("sched-far-future")
	wid := testcore.RandomizeStr("sched-far-future-wf")
	wt := testcore.RandomizeStr("sched-far-future-wt")

	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	warn := time.Duration(scheduler.DefaultWarnIterations) * time.Second
	interval := 10 * warn

	ctx := schedule2Backend.context(t.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(interval),
		Action: startWorkflowAction(s, wid, wt),
	})

	var future []*timestamppb.Timestamp
	await.RequireTruef(t, func() bool {
		resp, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		if err != nil || len(resp.GetInfo().GetFutureActionTimes()) < 10 {
			return false
		}
		future = resp.GetInfo().GetFutureActionTimes()
		return true
	}, awaitTimeout, pollInterval, "FutureActionTimes should fill to 10 far-future entries")

	for i := 1; i < len(future); i++ {
		require.Equal(t, interval, future[i].AsTime().Sub(future[i-1].AsTime()),
			"consecutive future action times should be exactly one interval apart")
	}
	require.Greater(t, future[len(future)-1].AsTime().Sub(time.Now().UTC()), warn,
		"future action times should extend well past the two-week compute horizon")

	base := time.Now().UTC()
	resp, err := s.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		StartTime:  timestamppb.New(base),
		EndTime:    timestamppb.New(base.Add(11 * interval)),
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(resp.GetStartTime()), 10,
		"ListMatchingTimes should enumerate the far-future series")
}

func TestSchedule2ScheduleManyCalendars(t *testing.T) {
	t.Parallel()
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend})

	sid := testcore.RandomizeStr("sched-many-calendars")
	wid := testcore.RandomizeStr("sched-many-calendars-wf")
	wt := testcore.RandomizeStr("sched-many-calendars-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	// Calendars match seconds 0..49 every minute so the schedule fires often. The excludes cancel
	// every fire in one upcoming minute (computed from now) so they actually take effect, while
	// the schedule keeps firing every other minute.
	const n = 50
	excludeMinute := time.Now().UTC().Truncate(time.Minute).Add(2 * time.Minute)
	var calendars, excludes []*schedulepb.CalendarSpec
	for i := range n {
		calendars = append(calendars, &schedulepb.CalendarSpec{
			Second: fmt.Sprintf("%d", i), Minute: "*", Hour: "*",
		})
		excludes = append(excludes, calendarSpec(excludeMinute.Add(time.Duration(i)*time.Second)))
	}

	ctx := schedule2Backend.context(t.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Calendar:        calendars,
			ExcludeCalendar: excludes, //nolint:staticcheck // Exercise supported legacy exclusion syntax at public ingress.
		},
		Action: startWorkflowAction(s, wid, wt),
	})

	await.RequireTruef(t, func() bool { return runs.Load() >= 3 },
		awaitTimeout, pollInterval, "schedule with many calendars should keep dispatching actions")

	describe := func() *schedulepb.ScheduleInfo {
		resp, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		require.NoError(t, err)
		return resp.GetInfo()
	}

	var baseRecent, baseFuture time.Time
	await.RequireTruef(t, func() bool {
		info := describe()
		if len(info.GetRecentActions()) == 0 || len(info.GetFutureActionTimes()) == 0 {
			return false
		}
		baseRecent = info.GetRecentActions()[len(info.GetRecentActions())-1].GetActualTime().AsTime()
		baseFuture = info.GetFutureActionTimes()[0].AsTime()
		return true
	}, awaitTimeout, pollInterval, "RecentActions and FutureActionTimes should be populated")

	await.RequireTruef(t, func() bool {
		info := describe()
		if len(info.GetRecentActions()) == 0 || len(info.GetFutureActionTimes()) == 0 {
			return false
		}
		newerRecent := info.GetRecentActions()[len(info.GetRecentActions())-1].GetActualTime().AsTime().After(baseRecent)
		advancedFuture := info.GetFutureActionTimes()[0].AsTime().After(baseFuture)
		return newerRecent && advancedFuture
	}, awaitTimeout, pollInterval, "RecentActions and FutureActionTimes should keep updating as actions fire")

	// The excludes cancel every fire in excludeMinute, so ListMatchingTimes over it is empty
	// (the schedule keeps firing in other minutes, asserted above).
	resp, err := s.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		StartTime:  timestamppb.New(excludeMinute.Add(-time.Second)),
		EndTime:    timestamppb.New(excludeMinute.Add(50 * time.Second)),
	})
	require.NoError(t, err)
	require.Empty(t, resp.GetStartTime(), "the excluded minute must have no matches")
}
