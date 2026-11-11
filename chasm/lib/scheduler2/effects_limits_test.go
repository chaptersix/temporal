package scheduler2

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/proto"
)

func TestEffectsRequestSizeNeighborsAndRecovery(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER} {
		for _, size := range []int{131071, 131072, 131073} {
			t.Run(fmt.Sprintf("%s/bytes_%d", policy, size), func(t *testing.T) {
				t.Parallel()
				h, first := bufferedHarness(t, policy, 1)
				// Construct the public RPC independently of freezeEffect. At these
				// sizes, adjusting the chain length does not cross a varint boundary.
				message := func(chain string) proto.Message {
					target := &commonpb.WorkflowExecution{WorkflowId: first.WorkflowId}
					if policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER {
						return &workflowservice.RequestCancelWorkflowExecutionRequest{Namespace: first.Namespace, WorkflowExecution: target, FirstExecutionRunId: chain, RequestId: first.RequestId + "/cancel", Reason: "cancelled by schedule overlap policy", Identity: "scheduler2"}
					}
					return &workflowservice.TerminateWorkflowExecutionRequest{Namespace: first.Namespace, WorkflowExecution: target, FirstExecutionRunId: chain, Reason: "terminated by schedule overlap policy", Identity: "scheduler2"}
				}
				overhead := proto.Size(message(strings.Repeat("r", 131072))) - 131072
				chain := strings.Repeat("r", size-overhead)
				require.Equal(t, size, proto.Size(message(chain)))
				h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: first.RequestId, ChainID: chain}, true)
				s, ctx := h.root(false)
				require.Equal(t, chain, s.Operations[first.RequestId].Get(ctx).ChainId)
				before, physical := cloneNodes(h.durable), len(h.physical)
				h.now++
				s, ctx = h.root(true)
				err := s.applyTransition(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Due, Incarnation: "incarnation", Now: h.now, Nominal: h.now, Actual: h.now, CatchupDeadline: h.now, WorkflowID: "queued"})
				if size > 131072 {
					require.ErrorContains(t, err, "effect request exceeds prototype limit")
					requireRejectedCoordinatorUnchanged(h, before, physical)
					// A different chain cannot replace admitted identity. Authoritative
					// completion releases this blocker, allowing a fresh bounded effect.
					h.apply(decision.Event{Kind: decision.Complete, Incarnation: "incarnation", OperationID: first.RequestId, ChainID: chain}, true)
					allowDue(h, true)
					s, _ = h.root(false)
					allowAck(h, s.OperationId)
					allowDue(h, true)
				} else {
					require.NoError(t, err)
					commitGeneration(h, true)
				}
				s, ctx = h.root(false)
				require.Len(t, s.Effects, 1)
				require.Len(t, s.Queue, 1)
				require.Equal(t, decision.EffectPending, decision.EffectPhase(s.Effects[0].Phase))
				wire := s.EffectRequests[s.Effects[0].Id].Get(ctx).Data
				if size <= 131072 {
					require.Len(t, wire, size)
					expected, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(message(chain))
					require.NoError(t, marshalErr)
					require.Equal(t, expected, wire)
				} else {
					require.Less(t, len(wire), 131072)
				}
			})
		}
	}
}
