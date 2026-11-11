package scheduler2

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBackfillPublicDedupAndPublication(t *testing.T) {
	t.Parallel()
	e := newManualCommandEngine(t)
	base := generationStart()
	request := &schedulepb.BackfillRequest{StartTime: timestamppb.New(base), EndTime: timestamppb.New(base), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}
	require.Error(t, Backfill(e.ctx, e.ref, "range", nil))
	used, err := e.h.node.HasRequestID("range", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
	e.failPublication = true
	require.ErrorIs(t, Backfill(e.ctx, e.ref, "range", request), errManualPublication)
	s, _ := e.h.root(false)
	require.Zero(t, s.BackfillSequence)
	require.Empty(t, s.Backfills)
	used, err = e.h.node.HasRequestID("range", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
	e.failPublication = false
	require.NoError(t, Backfill(e.ctx, e.ref, "range", request))
	request.StartTime.Seconds++
	request.EndTime.Seconds++
	require.NoError(t, Backfill(e.ctx, e.ref, "range", nil), "successful request ID wins even with a changed invalid body")
	s, _ = e.h.root(false)
	require.Equal(t, uint64(1), s.BackfillSequence)
	require.Len(t, s.Backfills, 1)
	require.Equal(t, base.UnixNano(), s.Backfills[0].Start)
	require.Equal(t, base.UnixNano(), s.Backfills[0].End)
	for i := 0; i < 3; i++ {
		require.NoError(t, Backfill(e.ctx, e.ref, "", request))
	}
	require.Error(t, Backfill(e.ctx, e.ref, "overflow", request))
	used, err = e.h.node.HasRequestID("overflow", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
	s, _ = e.h.root(false)
	require.Len(t, s.Backfills, 4)
	require.Equal(t, uint64(4), s.BackfillSequence)
	coordinatorStep(e.h, true) // The empty definition completes one range.
	require.NoError(t, Backfill(e.ctx, e.ref, "overflow", request))
	used, err = e.h.node.HasRequestID("overflow", e.maxAge)
	require.NoError(t, err)
	require.True(t, used)
	s, _ = e.h.root(false)
	require.Len(t, s.Backfills, 4)
	require.Equal(t, uint64(5), s.BackfillSequence)
	require.NoError(t, Backfill(e.ctx, e.ref, "range", nil), "completed request retains framework dedup within retention")
	s, _ = e.h.root(false)
	require.Len(t, s.Backfills, 4)
	require.Equal(t, uint64(5), s.BackfillSequence)
}

func TestBackfillRejectedRequestDoesNotPublish(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		change       func(*schedulepb.BackfillRequest)
		unconfigured bool
		exhausted    bool
	}{
		{name: "missing start", change: func(r *schedulepb.BackfillRequest) { r.StartTime = nil }},
		{name: "missing end", change: func(r *schedulepb.BackfillRequest) { r.EndTime = nil }},
		{name: "bad nanos", change: func(r *schedulepb.BackfillRequest) { r.StartTime.Nanos = 1_000_000_000 }},
		{name: "reversed", change: func(r *schedulepb.BackfillRequest) { r.StartTime.Seconds++ }},
		{name: "before minimum", change: func(r *schedulepb.BackfillRequest) {
			r.StartTime = timestamppb.New(time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC))
		}},
		{name: "end at maximum", change: func(r *schedulepb.BackfillRequest) {
			r.EndTime = timestamppb.New(time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC))
		}},
		{name: "unknown policy", change: func(r *schedulepb.BackfillRequest) { r.OverlapPolicy = enumspb.ScheduleOverlapPolicy(999) }},
		{name: "unconfigured", unconfigured: true},
		{name: "sequence exhausted", exhausted: true},
		{name: "nil request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := manualHarness(t)
			s, _ := h.root(true)
			if tc.unconfigured {
				s.HasGenerator = false
			}
			if tc.exhausted {
				s.BackfillSequence = math.MaxUint64
			}
			commitGeneration(h, true)
			before := cloneNodes(h.durable)
			request := &schedulepb.BackfillRequest{StartTime: timestamppb.New(generationStart()), EndTime: timestamppb.New(generationStart())}
			if tc.change != nil {
				tc.change(request)
			}
			if tc.name == "nil request" {
				request = nil
			}
			s, ctx := h.root(true)
			require.Error(t, s.AcceptBackfill(ctx.(chasm.MutableContext), request))
			timers := commitGeneration(h, true)
			require.Empty(t, timers)
			// Mutable access advances the framework transition stamp even without
			// semantic writes. Compare every data/task/identity field separately
			// from that bookkeeping stamp.
			after := cloneNodes(h.durable)
			for _, nodes := range []map[string]*persistencespb.ChasmNode{before, after} {
				for _, node := range nodes {
					node.Metadata.LastUpdateVersionedTransition = nil
				}
			}
			require.Equal(t, before, after, "committing a rejected call must preserve scheduler state and tasks")
		})
	}
}

func TestCoordinatorRejectsAdmissionBypass(t *testing.T) {
	t.Parallel()
	for _, kind := range []decision.Kind{decision.Due, decision.Drain, decision.DispatchManual, decision.DispatchBackfill} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			t.Parallel()
			h := manualHarness(t)
			s, ctx := h.root(true)
			before := common.CloneProto(s.RootState)
			require.ErrorContains(t, s.Apply(ctx.(chasm.MutableContext), decision.Event{Kind: kind, Incarnation: s.Incarnation}), "coordinator ownership")
			require.Empty(t, commitGeneration(h, true))
			s, _ = h.root(false)
			protorequire.ProtoEqual(t, before, s.RootState)
		})
	}
}

func TestCoordinatorLegacyTasksCannotAdmit(t *testing.T) {
	t.Parallel()
	h := manualHarness(t)
	acceptManual(h, &schedulepb.TriggerImmediatelyRequest{}, true)
	s, ctx := h.root(true)
	before := common.CloneProto(s.RootState)
	attrs := chasm.TaskAttributes{ScheduledTime: time.Unix(0, h.now)}
	invocation := chasm.TaskInvocation{TaskAttributes: attrs}
	manualTask := &scheduler2pb.ManualTask{Token: s.ManualToken}
	drainTask := &scheduler2pb.DrainTask{Token: s.DrainToken}
	g := s.Generator.Get(ctx)
	generationTask := &scheduler2pb.GenerateTask{Token: g.Token}
	valid, err := (&ManualHandler{}).Validate(ctx, s, invocation, manualTask)
	require.NoError(t, err)
	require.False(t, valid)
	require.NoError(t, (&ManualHandler{}).Execute(ctx.(chasm.MutableContext), s, attrs, manualTask))
	valid, err = (&DrainHandler{}).Validate(ctx, s, invocation, drainTask)
	require.NoError(t, err)
	require.False(t, valid)
	require.NoError(t, (&DrainHandler{}).Execute(ctx.(chasm.MutableContext), s, attrs, drainTask))
	valid, err = (&GenerateHandler{}).Validate(ctx, g, invocation, generationTask)
	require.NoError(t, err)
	require.False(t, valid)
	require.NoError(t, (&GenerateHandler{}).Execute(ctx.(chasm.MutableContext), g, attrs, generationTask))
	require.Empty(t, commitGeneration(h, true))
	s, _ = h.root(false)
	protorequire.ProtoEqual(t, before, s.RootState)
	require.Empty(t, s.Operations)
	require.Len(t, s.Manuals, 1)
}

func TestBackfillAcceptanceAvoidsPayloadAndRejectsExhaustedToken(t *testing.T) {
	t.Parallel()
	h := manualHarness(t)
	poisoned := 0
	for path, node := range h.durable {
		if node.Data != nil && (strings.Contains(path, "Definition") || path == "Diagnostics") {
			node.Data.Data = []byte{0xff}
			poisoned++
		}
	}
	require.Equal(t, 2, poisoned)
	h.reload()
	coordinatorBackfill(h, generationStart(), generationStart(), enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	s, ctx := h.root(false)
	require.Len(t, s.Backfills, 1)
	valid, err := (&WorkHandler{}).Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: time.Unix(0, s.Coordination.At)}}, &scheduler2pb.WorkTask{Token: s.Coordination.Token})
	require.NoError(t, err)
	require.True(t, valid)
	require.Panics(t, func() { s.Generator.Get(ctx).Definition.Get(ctx) })
	h = manualHarness(t)
	exhaustCoordinator(h)
	before, physical := cloneNodes(h.durable), len(h.physical)
	s, ctx = h.root(true)
	err = s.AcceptBackfill(ctx.(chasm.MutableContext), &schedulepb.BackfillRequest{StartTime: timestamppb.New(generationStart()), EndTime: timestamppb.New(generationStart())})
	require.ErrorContains(t, err, "coordinator token exhausted")
	requireRejectedCoordinatorUnchanged(h, before, physical)
}
