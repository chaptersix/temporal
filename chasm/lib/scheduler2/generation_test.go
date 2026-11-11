package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func generationStart() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) }
func generationSpec(start, end time.Time, interval time.Duration) *schedulepb.ScheduleSpec {
	return &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(interval)}}, StartTime: timestamppb.New(start), EndTime: timestamppb.New(end)}
}
func generationState(h *treeHarness) *scheduler2pb.GeneratorState {
	h.t.Helper()
	s, ctx := h.root(false)
	return proto.Clone(s.Generator.Get(ctx).GeneratorState).(*scheduler2pb.GeneratorState)
}
func pureObligations(h *treeHarness) int {
	count := 0
	for _, node := range h.durable {
		count += len(node.GetMetadata().GetComponentAttributes().GetPureTasks())
	}
	return count
}

// commitGeneration observes physical timer publication alongside the serialized
// real CHASM tree. Failed publication discards both categories of new tasks.
func commitGeneration(h *treeHarness, publish bool) (timers []tasks.ChasmTaskPure) {
	h.t.Helper()
	_, err := h.node.CloseTransaction(h.t.Context(), true)
	require.NoError(h.t, err)
	if publish {
		h.durable = cloneNodes(h.node.Snapshot(nil).Nodes)
		for _, batch := range h.backend.TasksByCategory {
			for _, task := range batch {
				switch task := task.(type) {
				case *tasks.ChasmTaskPure:
					timers = append(timers, *task)
				case *tasks.ChasmTask:
					cloned := *task
					cloned.Info = proto.Clone(task.Info).(*persistencespb.ChasmTaskInfo)
					h.physical = append(h.physical, &cloned)
				default:
				}
			}
		}
		h.transition++
	}
	h.reload()
	return timers
}
func configureGeneration(h *treeHarness, spec *schedulepb.ScheduleSpec, seed string, revision int64, after time.Time, catchup time.Duration) []tasks.ChasmTaskPure {
	h.t.Helper()
	s, ctx := h.root(true)
	request, _ := frozenRequest(h.t)
	require.NoError(h.t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: spec, Seed: seed, Revision: revision, After: after, CatchupWindow: catchup, Request: request}))
	return commitGeneration(h, true)
}
func deliverGeneration(h *treeHarness, reference time.Time) (int, error) {
	executed := 0
	err := h.node.EachPureTask(reference, func(handler chasm.NodePureTask, attrs chasm.TaskAttributes, task any) (bool, error) {
		ran, err := handler.ExecutePureTask(h.t.Context(), attrs, task)
		if ran && err == nil {
			executed++
		}
		return ran, err
	})
	return executed, err
}
func fireGeneration(h *treeHarness, publish bool) []tasks.ChasmTaskPure {
	h.t.Helper()
	executed, err := deliverGeneration(h, time.Unix(0, h.now))
	require.NoError(h.t, err)
	require.Equal(h.t, 1, executed)
	return commitGeneration(h, publish)
}
func TestGenerationAtomicBatchAndSecondCarryover(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	sequentialBudget(h, 8)
	base := generationStart()
	h.now = base.Add(5 * time.Second).UnixNano()
	timers := configureGeneration(h, generationSpec(base, base.Add(20*time.Second), time.Second), "", 1, base.Add(-time.Nanosecond), time.Minute)
	require.Len(t, timers, 1)
	require.Equal(t, 1, pureObligations(h))
	before := cloneNodes(h.durable)
	token := generationState(h).Token
	timers = fireGeneration(h, false)
	require.Empty(t, timers)
	require.Equal(t, before, h.durable)
	require.Empty(t, h.physical)
	timers = fireGeneration(h, true)
	require.Len(t, timers, 1)
	require.Len(t, h.physical, 1)
	s, _ := h.root(false)
	require.Equal(t, base.Add(3*time.Second).UnixNano(), s.Cursor)
	require.Equal(t, int64(7), s.Remaining)
	require.Len(t, s.Operations, 1)
	first := h.request()
	require.Equal(t, "workflow-2024-01-01T00:00:00Z", first.WorkflowId)
	g := generationState(h)
	require.Greater(t, g.Token, token)
	require.Equal(t, scheduler2pb.ENUMERATION_STATUS_LIMITED, g.LastStatus)
	require.Equal(t, h.now+int64(time.Millisecond), g.WakeAt)
	require.Equal(t, 1, pureObligations(h))
	result := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/plain")}, Data: []byte("first result")}
	sequentialComplete(h, first.RequestId, result)
	h.now = g.WakeAt
	fireGeneration(h, true)
	second := h.request()
	require.NotEqual(t, first.RequestId, second.RequestId)
	require.Equal(t, "workflow-2024-01-01T00:00:04Z", second.WorkflowId)
	protorequire.ProtoEqual(t, &commonpb.Payloads{Payloads: []*commonpb.Payload{result}}, second.LastCompletionResult)
	s, _ = h.root(false)
	require.Equal(t, int64(6), s.Remaining)
	require.Equal(t, int64(1), s.Started)
	require.Equal(t, base.Add(5*time.Second).UnixNano(), s.Cursor)
	g = generationState(h)
	require.Equal(t, base.Add(6*time.Second).UnixNano(), g.WakeAt)
	require.Equal(t, 1, pureObligations(h))
}
func TestGenerationDuplicateTokenAndClockRollback(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	base := generationStart()
	h.now = base.UnixNano()
	configureGeneration(h, generationSpec(base.Add(time.Second), base.Add(3*time.Second), time.Second), "", 1, base, time.Minute)
	stale := generationState(h)
	fireGeneration(h, true)
	current := generationState(h)
	require.Equal(t, base.Add(time.Second).UnixNano(), current.WakeAt)
	s, ctx := h.root(true)
	g := s.Generator.Get(ctx)
	handler := &GenerateHandler{}
	attrs := chasm.TaskAttributes{ScheduledTime: time.Unix(0, stale.WakeAt)}
	task := &scheduler2pb.GenerateTask{Token: stale.Token}
	valid, err := handler.Validate(ctx, g, chasm.TaskInvocation{TaskAttributes: attrs}, task)
	require.NoError(t, err)
	require.False(t, valid)
	require.NoError(t, handler.Execute(ctx.(chasm.MutableContext), g, attrs, task))
	commitGeneration(h, true)
	protorequire.ProtoEqual(t, current, generationState(h))
	require.Equal(t, 1, pureObligations(h))
	before := cloneNodes(h.durable)
	h.now = current.WakeAt - int64(time.Second)
	_, err = deliverGeneration(h, time.Unix(0, current.WakeAt))
	require.ErrorContains(t, err, "not due")
	h.reload()
	require.Equal(t, before, h.durable)
	require.Equal(t, 1, pureObligations(h))
	h.now = current.WakeAt
	fireGeneration(h, true)
	require.NotNil(t, h.request())
	require.Equal(t, 1, pureObligations(h))
}
func TestGenerationJitterUpdateSupersedesPending(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	base := time.Date(2022, 3, 23, 11, 0, 0, 0, time.UTC)
	h.now = base.UnixNano()
	spec := generationSpec(base, base.Add(5*time.Hour), 90*time.Minute)
	spec.Jitter = durationpb.New(24 * time.Hour)
	configureGeneration(h, spec, "", 1, base, time.Minute)
	fireGeneration(h, true)
	old := generationState(h)
	require.Equal(t, time.Date(2022, 3, 23, 12, 14, 36, 524000000, time.UTC).UnixNano(), old.WakeAt)
	require.NotNil(t, old.Continuation.Pending)
	s, _ := h.root(false)
	require.Empty(t, s.OperationId)
	h.now = time.Date(2022, 3, 23, 12, 0, 0, 0, time.UTC).UnixNano()
	next := generationSpec(time.Unix(0, h.now), time.Unix(0, h.now).Add(2*time.Second), time.Second)
	configureGeneration(h, next, "new", 2, time.Unix(0, h.now), time.Minute)
	changed := generationState(h)
	protorequire.ProtoEqual(t, old.Continuation.Pending, changed.Superseded)
	require.Greater(t, changed.Token, old.Token)
	fireGeneration(h, true)
	g := generationState(h)
	require.Equal(t, h.now+int64(time.Second), g.WakeAt)
	h.now = g.WakeAt
	fireGeneration(h, true)
	request := h.request()
	require.Equal(t, "workflow-2022-03-23T12:00:01Z", request.WorkflowId)
}
func TestGenerationCatchupUsesActualTime(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	nominal := time.Date(2022, 3, 23, 12, 0, 0, 0, time.UTC)
	actual := time.Date(2022, 3, 23, 12, 14, 36, 524000000, time.UTC)
	h.now = actual.Add(500 * time.Millisecond).UnixNano()
	spec := generationSpec(nominal, nominal, 90*time.Minute)
	spec.Jitter = durationpb.New(24 * time.Hour)
	configureGeneration(h, spec, "", 1, nominal.Add(-time.Nanosecond), time.Second)
	fireGeneration(h, true)
	require.NotNil(t, h.request(), "nominal is late but actual lies inside catchup window")
	s, _ := h.root(false)
	require.Zero(t, s.Remaining)
}
func TestGenerationRangeEndResumesWithoutUnsupportedClock(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	sequentialBudget(h, 4)
	upper := time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC)
	last := upper.Add(-time.Nanosecond)
	h.now = last.UnixNano()
	configureGeneration(h, generationSpec(upper.Add(-4*time.Second), upper.Add(-time.Second), time.Second), "", 1, upper.Add(-4*time.Second-time.Nanosecond), time.Minute)
	fireGeneration(h, true)
	g := generationState(h)
	require.Equal(t, scheduler2pb.ENUMERATION_STATUS_LIMITED, g.LastStatus)
	require.Equal(t, last.UnixNano(), g.WakeAt)
	require.True(t, g.Armed)
	token := g.Token
	fireGeneration(h, true)
	g = generationState(h)
	require.Greater(t, g.Token, token)
	require.False(t, g.Armed)
	require.Equal(t, scheduler2pb.ENUMERATION_STATUS_EXHAUSTED, g.LastStatus)
	require.Zero(t, pureObligations(h))
}

func TestGenerationExcludedBudgetKeepsSuccessor(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	base := generationStart()
	h.now = base.Add(21 * time.Second).UnixNano()
	spec := generationSpec(base, base.Add(20*time.Second), time.Second)
	ranges := func(start, end int32) []*schedulepb.Range {
		return []*schedulepb.Range{{Start: start, End: end, Step: 1}}
	}
	spec.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{{Second: ranges(0, 59), Minute: ranges(0, 59), Hour: ranges(0, 23), DayOfMonth: ranges(1, 31), Month: ranges(1, 12), DayOfWeek: ranges(0, 6)}}
	configureGeneration(h, spec, "", 1, base.Add(-time.Nanosecond), time.Minute)
	fireGeneration(h, true)
	g := generationState(h)
	require.Equal(t, scheduler2pb.ENUMERATION_STATUS_YIELD, g.LastStatus)
	require.Equal(t, base.Add(7*time.Second).UnixNano(), g.Continuation.DisposedThrough)
	require.Equal(t, h.now+int64(time.Millisecond), g.WakeAt)
	require.Equal(t, 1, pureObligations(h))
	h.now = g.WakeAt
	fireGeneration(h, true)
	g = generationState(h)
	require.Equal(t, base.Add(15*time.Second).UnixNano(), g.Continuation.DisposedThrough)
	h.now = g.WakeAt
	fireGeneration(h, true)
	g = generationState(h)
	require.Equal(t, scheduler2pb.ENUMERATION_STATUS_IMPOSSIBLE, g.LastStatus)
	require.False(t, g.Armed)
	require.Zero(t, pureObligations(h))
	s, _ := h.root(false)
	require.Empty(t, s.OperationId)
	require.Equal(t, int64(1), s.Remaining)
	require.Equal(t, base.Add(20*time.Second).UnixNano(), s.Cursor)
}
func TestGenerationSkippedAdmissionAvoidsCarryover(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"paused", "budget", "catchup"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			sequentialBudget(h, 2)
			first := sequentialDue(h)
			sequentialComplete(h, first.RequestId, &commonpb.Payload{Data: []byte("prior")})
			s, _ := h.root(true)
			if reason == "paused" {
				s.Paused = true
			}
			if reason == "budget" {
				s.Remaining = 0
			}
			h.commit(true)
			for path, node := range h.durable {
				if node.Data != nil && (path == "Carryover") {
					node.Data.Data = []byte{0xff}
				}
			}
			h.reload()
			base := generationStart()
			h.now = base.UnixNano()
			if reason == "catchup" {
				h.now = base.Add(2 * time.Second).UnixNano()
			}
			configureGeneration(h, generationSpec(base, base, time.Second), "", 1, base.Add(-time.Nanosecond), time.Second)
			fireGeneration(h, true)
			s, ctx := h.root(false)
			require.Equal(t, first.RequestId, s.OperationId)
			require.Equal(t, base.UnixNano(), s.Cursor)
			require.Equal(t, int64(1), s.Started)
			require.Len(t, s.Operations, 1)
			require.Panics(t, func() { s.Carryover.Get(ctx) })
			require.Zero(t, pureObligations(h))
		})
	}
}

func TestGenerationConfigurationOwnsInputs(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	base := time.Date(2022, 3, 23, 11, 0, 0, 0, time.UTC)
	h.now = base.UnixNano()
	spec := generationSpec(base, base.Add(5*time.Hour), 90*time.Minute)
	spec.Jitter = durationpb.New(24 * time.Hour)
	request, _ := frozenRequest(t)
	expectedSpec := proto.Clone(spec)
	expectedRequest := proto.Clone(request)
	config := GenerationConfig{Spec: spec, Request: request, Seed: "", Revision: 1, After: base, CatchupWindow: time.Minute}
	s, ctx := h.root(true)
	require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), config))
	spec.Jitter.Seconds = 0
	spec.Interval[0].Interval.Seconds = 1
	request.WorkflowId = "mutated"
	request.Input.Payloads[0].Data[0] ^= 0xff
	config.Seed = "mutated"
	commitGeneration(h, true)
	s, ctx = h.root(false)
	definition := s.Generator.Get(ctx).Definition.Get(ctx)
	actualSpec := &schedulepb.ScheduleSpec{}
	actualRequest := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(definition.Spec, actualSpec))
	require.NoError(t, proto.Unmarshal(definition.Request, actualRequest))
	protorequire.ProtoEqual(t, expectedSpec, actualSpec)
	protorequire.ProtoEqual(t, expectedRequest, actualRequest)
	require.Empty(t, definition.Seed)
	fireGeneration(h, true)
	g := generationState(h)
	require.Equal(t, time.Date(2022, 3, 23, 12, 14, 36, 524000000, time.UTC).UnixNano(), g.WakeAt)
	h.now = g.WakeAt
	fireGeneration(h, true)
	require.Equal(t, "workflow-2022-03-23T12:00:00Z", h.request().WorkflowId)
}
