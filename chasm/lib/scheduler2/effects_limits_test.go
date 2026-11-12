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

func TestEffectsFormerByteCeilingAndLargeRetention(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER} {
		for _, size := range []int{131071, 131072, 131073, 256 * 1024} {
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
				h.now++
				s, ctx = h.root(true)
				err := s.applyTransition(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Due, Incarnation: "incarnation", Now: h.now, Nominal: h.now, Actual: h.now, CatchupDeadline: h.now, WorkflowID: "queued"})
				require.NoError(t, err)
				commitGeneration(h, true)

				s, ctx = h.root(false)
				require.Len(t, s.Effects, 1)
				require.Len(t, s.Queue, 1)
				require.Equal(t, decision.EffectPending, decision.EffectPhase(s.Effects[0].Phase))
				wire := s.EffectRequests[s.Effects[0].Id].Get(ctx).Data
				require.Len(t, wire, size)
				expected, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(message(chain))
				require.NoError(t, marshalErr)
				require.Equal(t, expected, wire)
				request, err := s.effectRequest(ctx, s.Effects[0].Id)
				require.NoError(t, err)
				require.NotNil(t, request)
				if policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER {
					require.True(t, proto.Equal(message(chain), request.Cancel))
				} else {
					require.True(t, proto.Equal(message(chain), request.Terminate))
				}

			})
		}
	}
}
