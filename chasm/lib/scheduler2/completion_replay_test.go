package scheduler2

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type completionTrace struct {
	Version          int                          `json:"version"`
	Incarnation      string                       `json:"incarnation"`
	AdmissionTime    int64                        `json:"admission_time_ns"`
	InitialRemaining int64                        `json:"initial_remaining"`
	InitialCarryover *scheduler2pb.CarryoverState `json:"initial_carryover"`
	Request          json.RawMessage              `json:"request"`
	Steps            []completionTraceStep        `json:"steps"`
}
type completionTraceStep struct {
	Kind              string                       `json:"kind"`
	OperationID       string                       `json:"operation_id"`
	Chain             string                       `json:"chain"`
	Outcome           string                       `json:"outcome"`
	Status            string                       `json:"status"`
	Result            []byte                       `json:"result"`
	Failure           []byte                       `json:"failure"`
	Revision          int64                        `json:"revision"`
	PauseOnFailure    bool                         `json:"pause_on_failure"`
	ExpectedPhase     string                       `json:"expected_phase"`
	ExpectedStarted   int64                        `json:"expected_started"`
	ExpectedPaused    bool                         `json:"expected_paused"`
	ExpectedCarryover *scheduler2pb.CarryoverState `json:"expected_carryover"`
}
type completionObservation struct {
	Root      *scheduler2pb.RootState
	Operation *scheduler2pb.OperationState
	Carryover *scheduler2pb.CarryoverState
}

// TestCompletionSavedReplay uses explicit independently recorded expectations.
// Each transition is published and restored through the real CHASM serializer.
// It preserves the M1 trace format by using a separate completion trace schema.
func TestCompletionSavedReplay(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/completion_v1.json")
	require.NoError(t, err)
	var trace completionTrace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&trace))
	require.Equal(t, 1, trace.Version)
	require.Equal(t, "incarnation", trace.Incarnation)
	require.Equal(t, int64(1000000000), trace.AdmissionTime)
	require.Equal(t, int64(1), trace.InitialRemaining)
	statuses := map[string]enumspb.WorkflowExecutionStatus{"completed": enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, "failed": enumspb.WORKFLOW_EXECUTION_STATUS_FAILED}
	var previous []completionObservation
	for run := 0; run < 2; run++ {
		h := newTreeHarness(t)
		seedCarryover(h, proto.Clone(trace.InitialCarryover).(*scheduler2pb.CarryoverState))
		request := &workflowservice.StartWorkflowExecutionRequest{}
		require.NoError(t, protojson.Unmarshal(trace.Request, request))
		h.apply(decision.Event{Kind: decision.Due, Incarnation: trace.Incarnation, Nominal: trace.AdmissionTime, CatchupDeadline: trace.AdmissionTime, Request: completionWire(t, request)}, true)
		var observations []completionObservation
		for _, step := range trace.Steps {
			event := decision.Event{Incarnation: trace.Incarnation, OperationID: step.OperationID, ChainID: step.Chain, Result: step.Result, Failure: step.Failure}
			switch step.Kind {
			case "callback":
				event.Kind = decision.Callback
				switch step.Outcome {
				case "success":
					event.Outcome = decision.Success
				case "failure":
					event.Outcome = decision.Failure
				default:
					t.Fatalf("unsupported outcome %q", step.Outcome)
				}
			case "metadata":
				event.Kind = decision.Complete
			case "update":
				event.Kind = decision.Update
				event.Revision = step.Revision
				event.Limited = true
				event.PauseOnFailure = step.PauseOnFailure
			default:
				t.Fatalf("unsupported completion event %q", step.Kind)
			}
			if step.Kind != "update" {
				status, ok := statuses[step.Status]
				require.True(t, ok, "unsupported status %q", step.Status)
				event.Status = status
			}
			h.apply(event, true)
			s, ctx := h.root(false)
			op := s.Operations[s.OperationId].Get(ctx)
			carry := s.Carryover.Get(ctx)
			require.Equal(t, "completed", step.ExpectedPhase)
			require.Equal(t, scheduler2pb.Phase(decision.Completed), op.Phase)
			require.Equal(t, step.ExpectedStarted, s.Started)
			require.Equal(t, step.ExpectedPaused, s.Paused)
			require.Zero(t, s.Remaining)
			require.True(t, s.CarryoverSet)
			require.Zero(t, logicalCount(h.durable))
			protorequire.ProtoEqual(t, step.ExpectedCarryover, carry)
			observations = append(observations, completionObservation{Root: proto.Clone(s.RootState).(*scheduler2pb.RootState), Operation: proto.Clone(op).(*scheduler2pb.OperationState), Carryover: proto.Clone(carry).(*scheduler2pb.CarryoverState)})
		}
		if run == 1 {
			require.Equal(t, previous, observations)
		}
		previous = observations
	}
}
