//go:build test_dep

package scheduler2

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPreparedPublicReadSharesResolverAndDetachedSource(t *testing.T) {
	t.Parallel()
	service, wall, reads, data := preparationServiceFixture(t)
	spec := &schedulepb.ScheduleSpec{TimezoneName: "Fixture/Zone", StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 0}}, Hour: []*schedulepb.Range{{Start: 1}}, Minute: []*schedulepb.Range{{Start: 30}}, DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}}, Month: []*schedulepb.Range{{Start: 1, End: 12}}, DayOfWeek: []*schedulepb.Range{{End: 6}}}}, Jitter: durationpb.New(20 * time.Second)}
	stage, _, err := service.PrepareDefinition(spec, "seed", 1)
	require.NoError(t, err)
	captured := preparedReadSnapshot{source: common.CloneProto(stage.source), revision: stage.revision, owner: stage.owner}
	calls := 0
	frontend := &publicTestFrontend{preparation: service, computeLimits: func() (int, int) { calls++; return 0, 0 }}
	after := time.Date(2024, 11, 2, 0, 0, 0, 0, time.UTC)
	got, err := frontend.evaluatePreparedRead(t.Context(), captured, after, nil, 10)
	require.NoError(t, err)
	require.Len(t, got, 10)
	require.Equal(t, 10, calls)
	require.Equal(t, 1, *reads)
	reference := common.CloneProto(spec)
	reference.TimezoneData = data
	compiled, err := legacy.NewSpecBuilder(func() int { return 0 }, func() int { return 0 }).NewCompiledSpec(reference)
	require.NoError(t, err)
	for _, at := range got {
		want, err := compiled.GetNextTime("seed", after)
		require.NoError(t, err)
		require.Equal(t, want.Next, at.AsTime())
		after = want.Next
	}
	wall.Advance(24*time.Hour + time.Nanosecond)
	_, err = frontend.evaluatePreparedRead(t.Context(), captured, after, nil, 1)
	require.NoError(t, err)
	require.Equal(t, 2, *reads, "read evaluator uses shared source TTL")
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = frontend.evaluatePreparedRead(canceled, captured, after, nil, 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPreparedPublicReadCaptureBeforeInstallation(t *testing.T) {
	t.Parallel()
	h, service, reads, _ := stagedPublicIntegrationTree(t)
	s, ctx := h.root(false)
	before := *reads
	captured, err := capturePreparedDescription(s, ctx, struct{}{})
	require.NoError(t, err)
	require.Nil(t, captured.description.Info.FutureActionTimes)
	require.Equal(t, before, *reads, "capturing a read must not resolve source")
	require.True(t, s.sourcePreparationPending(ctx))
	h.reload()
	f := &publicTestFrontend{preparation: service, computeLimits: func() (int, int) { return 0, 0 }}
	got, err := f.evaluatePreparedRead(t.Context(), captured, captured.after, nil, 10)
	require.NoError(t, err)
	require.Len(t, got, 10)
	require.Equal(t, before, *reads)
}

func TestPreparedPublicReadComputeLimitAndQueryEnd(t *testing.T) {
	t.Parallel()
	service, _, _, _ := preparationServiceFixture(t)
	spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}, ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 2}}, Minute: []*schedulepb.Range{{End: 59}}, Hour: []*schedulepb.Range{{End: 23}}, DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}}, Month: []*schedulepb.Range{{Start: 1, End: 12}}, DayOfWeek: []*schedulepb.Range{{End: 6}}}}}
	stage, _, err := service.PrepareDefinition(spec, "", 1)
	require.NoError(t, err)
	snapshot := preparedReadSnapshot{source: stage.source, revision: 1, owner: stage.owner}
	f := &publicTestFrontend{preparation: service, computeLimits: func() (int, int) { return 0, 1 }}
	after := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err := f.evaluatePreparedRead(t.Context(), snapshot, after, nil, 10)
	require.ErrorIs(t, err, legacy.ErrComputeLimitExceeded)
	require.Len(t, got, 1)
	f.computeLimits = func() (int, int) { return 0, 0 }
	end := after.Add(time.Second)
	got, err = f.evaluatePreparedRead(t.Context(), snapshot, after, &end, 1000)
	require.NoError(t, err)
	require.Len(t, got, 1)
}
