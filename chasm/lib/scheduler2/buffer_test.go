package scheduler2

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

func configureBuffer(h *treeHarness, spec *schedulepb.ScheduleSpec, revision int64, policy enumspb.ScheduleOverlapPolicy, catchup time.Duration, request *workflowservice.StartWorkflowExecutionRequest) {
	h.t.Helper()
	s, ctx := h.root(true)
	require.NoError(h.t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: spec, Seed: "", Revision: revision, After: generationStart().Add(-time.Nanosecond), CatchupWindow: catchup, Request: request, Policy: policy}))
	commitGeneration(h, true)
}
func bufferedHarness(t *testing.T, policy enumspb.ScheduleOverlapPolicy, count int) (*treeHarness, *workflowservice.StartWorkflowExecutionRequest) {
	t.Helper()
	h := newTreeHarness(t)
	sequentialBudget(h, 10)
	base := generationStart()
	end := base.Add(time.Duration(count-1) * time.Second)
	h.now = end.UnixNano()
	request, _ := frozenRequest(t)
	configureBuffer(h, generationSpec(base, end, time.Second), 1, policy, time.Minute, request)
	for iteration := 0; iteration < 2*count+4 && generationState(h).Armed; iteration++ {
		h.now = max(h.now, generationState(h).WakeAt)
		fireGeneration(h, true)
	}
	require.False(t, generationState(h).Armed)
	require.Zero(t, pureObligations(h))
	return h, h.request()
}
func TestBufferAutomaticCapacity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                   string
		policy                 enumspb.ScheduleOverlapPolicy
		total, queued, dropped int
	}{
		{"one", enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 6, 1, 4}, {"all_below", enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 4, 3, 0}, {"all_exact", enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 5, 4, 0}, {"all_overflow", enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 6, 4, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := bufferedHarness(t, tc.policy, tc.total)
			s, _ := h.root(false)
			require.Len(t, s.Queue, tc.queued)
			require.Len(t, s.Operations, 1)
			require.Equal(t, int64(9), s.Remaining)
			require.Equal(t, int64(tc.dropped), s.BufferDropped)
			for index, queued := range s.Queue {
				nominal := generationStart().Add(time.Duration(index+1) * time.Second)
				protorequire.ProtoEqual(t, &scheduler2pb.QueuedOccurrence{Nominal: nominal.UnixNano(), Actual: nominal.UnixNano(), WorkflowId: "workflow-" + nominal.Format(time.RFC3339), Policy: tc.policy}, queued)
			}
		})
	}
}
func TestBufferCompletionDrainsExhaustedGenerator(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 4)
	updated, _ := frozenRequest(t)
	updated.WorkflowId = "updated-prefix"
	updated.Input.Payloads[0].Data = []byte("updated input")
	configureBuffer(h, &schedulepb.ScheduleSpec{}, 2, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, time.Minute, updated)
	fireGeneration(h, true)
	require.Equal(t, scheduler2pb.ENUMERATION_STATUS_EMPTY, generationState(h).LastStatus)
	result := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte("42")}
	sequentialComplete(h, first.RequestId, result)
	s, _ := h.root(false)
	require.True(t, s.Coordination.Armed)
	require.Equal(t, 1, pureObligations(h))
	require.False(t, generationState(h).Armed)
	fireGeneration(h, true)
	second := h.request()
	nominal := generationStart().Add(time.Second)
	expected := proto.Clone(updated).(*workflowservice.StartWorkflowExecutionRequest)
	expected.WorkflowId = "workflow-" + nominal.Format(time.RFC3339)
	expected.RequestId = fmt.Sprintf("incarnation/automatic/%d/action/start", nominal.UnixNano())
	expected.LastCompletionResult = &commonpb.Payloads{Payloads: []*commonpb.Payload{result}}
	protorequire.ProtoEqual(t, expected, second)
	s, _ = h.root(false)
	require.Len(t, s.Queue, 2)
	require.Equal(t, int64(8), s.Remaining)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, s.Queue[0].Policy)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, s.Policy)
	require.Zero(t, pureObligations(h))
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: first.RequestId, ChainID: "old", Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, Failure: []byte{0xff}}, true)
	protorequire.ProtoEqual(t, second, h.request())
	failure := &failurepb.Failure{Message: "second failed"}
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: second.RequestId, ChainID: "second-chain", Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, Failure: completionWire(t, failure)}, true)
	fireGeneration(h, true)
	third := h.request()
	require.Equal(t, "workflow-2024-01-01T00:00:02Z", third.WorkflowId)
	protorequire.ProtoEqual(t, &commonpb.Payloads{Payloads: []*commonpb.Payload{result}}, third.LastCompletionResult)
	protorequire.ProtoEqual(t, failure, third.ContinuedFailure)
}
func TestBufferDrainPublicationAndStaleToken(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 3)
	sequentialComplete(h, first.RequestId, &commonpb.Payload{Data: []byte("result")})
	s, _ := h.root(false)
	token, at := s.Coordination.Token, s.Coordination.At
	before := cloneNodes(h.durable)
	physical := len(h.physical)
	fireGeneration(h, false)
	require.Equal(t, before, h.durable)
	require.Len(t, h.physical, physical)
	require.Equal(t, 1, pureObligations(h))
	h.now = at - 1
	_, err := deliverGeneration(h, time.Unix(0, at))
	require.ErrorContains(t, err, "not due")
	h.reload()
	require.Equal(t, before, h.durable)
	h.now = at
	fireGeneration(h, true)
	second := h.request()
	require.Len(t, h.physical, physical+1)
	sequentialComplete(h, second.RequestId, &commonpb.Payload{Data: []byte("second")})
	s, ctx := h.root(true)
	require.Greater(t, s.Coordination.Token, token)
	current := proto.Clone(s.RootState).(*scheduler2pb.RootState)
	handler := &WorkHandler{}
	attrs := chasm.TaskAttributes{ScheduledTime: time.Unix(0, at)}
	task := &scheduler2pb.WorkTask{Token: token}
	valid, err := handler.Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: attrs}, task)
	require.NoError(t, err)
	require.False(t, valid)
	require.NoError(t, handler.Execute(ctx.(chasm.MutableContext), s, attrs, task))
	commitGeneration(h, true)
	s, _ = h.root(false)
	protorequire.ProtoEqual(t, current, s.RootState)
	require.Equal(t, 1, pureObligations(h))
	fireGeneration(h, true)
	require.Equal(t, "workflow-2024-01-01T00:00:02Z", h.request().WorkflowId)
}
func TestBufferIneligibleDrainAvoidsPayloads(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"paused", "budget", "catchup"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 4)
			sequentialComplete(h, first.RequestId, &commonpb.Payload{Data: []byte("prior")})
			s, ctx := h.root(true)
			if reason == "paused" {
				s.Paused = true
			}
			if reason == "budget" {
				s.Remaining = 0
			}
			if reason == "catchup" {
				s.Generator.Get(ctx).CatchupWindow = 0
			}
			h.commit(true)
			poisoned := 0
			for path, node := range h.durable {
				if node.Data != nil && (path == "Carryover" || strings.Contains(path, "Definition") || strings.Contains(path, "Request") || strings.Contains(path, "Diagnostics")) {
					node.Data.Data = []byte{0xff}
					poisoned++
				}
			}
			require.Equal(t, 4, poisoned)
			h.reload()
			h.now += int64(time.Second)
			fireGeneration(h, true)
			s, ctx = h.root(false)
			require.Empty(t, s.Queue)
			require.Equal(t, first.RequestId, s.OperationId)
			require.Zero(t, pureObligations(h))
			require.Equal(t, int64(1), s.Started)
			require.Panics(t, func() { s.Generator.Get(ctx).Definition.Get(ctx) })
			require.Panics(t, func() { s.Carryover.Get(ctx) })
			// Refilling or unpausing after disposition cannot resurrect discarded waiters.
			h.apply(decision.Event{Kind: decision.Update, Incarnation: "incarnation", Revision: 2, Limited: true, Remaining: 5, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}, true)
			s, _ = h.root(false)
			require.Empty(t, s.Queue)
			require.False(t, s.Coordination.Armed)
			require.Equal(t, int64(5), s.Remaining)
		})
	}
}
