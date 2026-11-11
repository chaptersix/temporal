package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
)

func TestUpdateFailureBeforeAcknowledgmentUsesCurrentPolicyAndCarryover(t *testing.T) {
	t.Parallel()
	for _, pauseOnFailure := range []bool{false, true} {
		name := "disable_failure_pause"
		if pauseOnFailure {
			name = "enable_failure_pause"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := manualHarness(t)
			initial := localUpdateConfig(t, 2)
			initial.Paused = false
			initial.PauseOnFailure = !pauseOnFailure
			initial.Generation.Policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
			s, ctx := h.root(true)
			require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), initial))
			commitGeneration(h, true)
			coordinatorStep(h, true) // Empty source is scanned before manual ingress.
			prior := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(`"prior-success"`)}
			seedCarryover(h, &scheduler2pb.CarryoverState{Result: completionWire(t, prior)})
			acceptManual(h, &schedulepb.TriggerImmediatelyRequest{}, true)
			transferManual(h, true)
			s, ctx = h.root(false)
			firstID := s.OperationId
			frozenFirst := append([]byte(nil), s.Requests[firstID].Get(ctx).Data...)
			require.Empty(t, s.Operations[firstID].Get(ctx).ChainId, "failure must precede acknowledgment")
			acceptManual(h, &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}, true)
			transferManual(h, true)
			s, _ = h.root(false)
			require.Len(t, s.Queue, 1)
			queued := common.CloneProto(s.Queue[0])
			require.Equal(t, uint64(2), queued.ManualId)
			replacement := localUpdateConfig(t, 3)
			replacement.Paused = false
			replacement.PauseOnFailure = pauseOnFailure
			replacement.Remaining = 0
			replacement.Generation.Request.Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("replacement-input")}}}
			before, physical := cloneNodes(h.durable), len(h.physical)
			s, ctx = h.root(true)
			require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), replacement))
			commitGeneration(h, false)
			require.Equal(t, before, h.durable)
			require.Len(t, h.physical, physical)
			s, ctx = h.root(true)
			require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), replacement))
			commitGeneration(h, true)
			failure := &failurepb.Failure{Message: "early failure", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "updated-policy"}}}
			failureWire := completionWire(t, failure)
			callback := decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: firstID, ChainID: "first-chain", Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, Failure: failureWire}
			before = cloneNodes(h.durable)
			h.apply(callback, false)
			require.Equal(t, before, h.durable)
			h.apply(callback, true)
			s, ctx = h.root(false)
			require.Equal(t, pauseOnFailure, s.Paused, "current replacement policy governs early failure")
			require.Equal(t, int64(1), s.Started)
			require.Zero(t, s.Unfinished)
			require.Zero(t, s.UnconfirmedStarts)
			require.Zero(t, s.Remaining)
			expectedCarryover := &scheduler2pb.CarryoverState{Result: completionWire(t, prior), Failure: failureWire, FailurePresent: true}
			protorequire.ProtoEqual(t, expectedCarryover, s.Carryover.Get(ctx))
			// Select the actual drain quantum; an intervening empty-source scan is a
			// separate transaction, never part of the rejected handoff publication.
			for step := 0; step < 3; step++ {
				s, ctx = h.root(false)
				_, demand, _ := s.planWork(ctx)
				if demand != nil && demand.class == scheduler2pb.WORK_CLASS_DRAIN {
					break
				}
				coordinatorStep(h, true)
			}
			s, ctx = h.root(false)
			_, demand, _ := s.planWork(ctx)
			require.NotNil(t, demand)
			require.Equal(t, scheduler2pb.WORK_CLASS_DRAIN, demand.class)
			before, physical = cloneNodes(h.durable), len(h.physical)
			coordinatorStep(h, false)
			require.Equal(t, before, h.durable)
			require.Len(t, h.physical, physical)
			coordinatorStep(h, true)
			s, ctx = h.root(false)
			successorID := "incarnation/manual/2/action/start"
			require.Equal(t, successorID, s.OperationId)
			require.Empty(t, s.Queue)
			require.Equal(t, int64(1), s.Started)
			require.Equal(t, int64(1), s.Unfinished)
			require.Equal(t, int64(1), s.UnconfirmedStarts)
			require.Zero(t, s.Remaining, "manual successor bypasses replaced budget and failure pause")
			expected := common.CloneProto(replacement.Generation.Request)
			expected.WorkflowId = queued.WorkflowId
			expected.RequestId = successorID
			expected.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
			expected.LastCompletionResult = &commonpb.Payloads{Payloads: []*commonpb.Payload{prior}}
			expected.ContinuedFailure = failure
			protorequire.ProtoEqual(t, expected, allowRequest(h, successorID))
			require.Equal(t, frozenFirst, s.Requests[firstID].Get(ctx).Data)
			// A late first acknowledgment may add observation metadata, but cannot
			// recount completion or replace failure carryover used by the successor.
			h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: firstID, ChainID: "first-chain", StartObserved: true, StartObservedAt: h.now + int64(time.Second)}, true)
			h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: firstID, ChainID: "first-chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, true)
			s, ctx = h.root(false)
			require.Equal(t, int64(1), s.Started)
			require.Equal(t, int64(1), s.Unfinished)
			require.Equal(t, int64(1), s.UnconfirmedStarts)
			require.Zero(t, s.Remaining)
			require.Equal(t, pauseOnFailure, s.Paused)
			require.Equal(t, scheduler2pb.Phase(decision.Completed), s.Operations[firstID].Get(ctx).Phase)
			protorequire.ProtoEqual(t, expectedCarryover, s.Carryover.Get(ctx))
			protorequire.ProtoEqual(t, expected, allowRequest(h, successorID))
		})
	}
}
