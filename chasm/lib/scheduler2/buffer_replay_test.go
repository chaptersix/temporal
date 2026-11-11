package scheduler2

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type bufferTrace struct {
	Version     int               `json:"version"`
	ClockDomain string            `json:"clock_domain"`
	Incarnation string            `json:"incarnation"`
	Now         time.Time         `json:"now"`
	After       time.Time         `json:"after"`
	Revision    int64             `json:"revision"`
	Seed        string            `json:"seed"`
	Catchup     string            `json:"catchup"`
	Policy      int32             `json:"policy"`
	Remaining   int64             `json:"remaining"`
	Spec        json.RawMessage   `json:"spec"`
	Request     json.RawMessage   `json:"request"`
	Steps       []bufferTraceStep `json:"steps"`
}
type bufferTraceStep struct {
	Kind       string            `json:"kind"`
	Publish    bool              `json:"publish"`
	Result     *commonpb.Payload `json:"result"`
	Queue      []time.Time       `json:"queue"`
	Cursor     time.Time         `json:"cursor"`
	Remaining  int64             `json:"remaining"`
	Started    int64             `json:"started"`
	Timers     int               `json:"timers"`
	Starts     int               `json:"starts"`
	WorkflowID string            `json:"workflow_id"`
	Carryover  *commonpb.Payload `json:"carryover"`
}

// TestBufferSavedReplay's complete input fixture records independent observations
// at each durable boundary, including a rejected drain publication and restart.
func TestBufferSavedReplay(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/buffer_v1.json")
	require.NoError(t, err)
	var trace bufferTrace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&trace))
	require.Equal(t, 1, trace.Version)
	require.Equal(t, "execution_nanoseconds", trace.ClockDomain)
	require.Equal(t, "incarnation", trace.Incarnation)
	for replay := 0; replay < 2; replay++ {
		h := newTreeHarness(t)
		sequentialBudget(h, trace.Remaining)
		h.now = trace.Now.UnixNano()
		spec := &schedulepb.ScheduleSpec{}
		request := &workflowservice.StartWorkflowExecutionRequest{}
		require.NoError(t, protojson.Unmarshal(trace.Spec, spec))
		require.NoError(t, protojson.Unmarshal(trace.Request, request))
		catchup, err := time.ParseDuration(trace.Catchup)
		require.NoError(t, err)
		s, ctx := h.root(true)
		require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: spec, Request: request, Seed: trace.Seed, Revision: trace.Revision, After: trace.After, CatchupWindow: catchup, Policy: enumspb.ScheduleOverlapPolicy(trace.Policy)}))
		commitGeneration(h, true)
		for index, step := range trace.Steps {
			switch step.Kind {
			case "generate", "drain":
				fireGeneration(h, step.Publish)
			case "restart":
				h.reload()
			case "complete":
				s, _ = h.root(false)
				sequentialComplete(h, s.OperationId, step.Result)
			default:
				t.Fatalf("unsupported buffer event %q", step.Kind)
			}
			s, _ = h.root(false)
			require.Equal(t, step.Cursor.UnixNano(), s.Cursor, "step %d", index)
			require.Equal(t, step.Remaining, s.Remaining, "step %d", index)
			require.Equal(t, step.Started, s.Started, "step %d", index)
			require.Equal(t, step.Timers, pureObligations(h), "step %d", index)
			require.Equal(t, step.Starts, logicalCount(h.durable), "step %d", index)
			require.Len(t, s.Queue, len(step.Queue), "step %d", index)
			for i, nominal := range step.Queue {
				require.Equal(t, nominal.UnixNano(), s.Queue[i].Nominal)
				require.Equal(t, nominal.UnixNano(), s.Queue[i].Actual)
				require.Equal(t, "workflow-"+nominal.Format(time.RFC3339), s.Queue[i].WorkflowId)
				require.Equal(t, enumspb.ScheduleOverlapPolicy(trace.Policy), s.Queue[i].Policy)
			}
			s, readCtx := h.root(false)
			issued := &workflowservice.StartWorkflowExecutionRequest{}
			require.NoError(t, proto.Unmarshal(s.Operations[s.OperationId].Get(readCtx).Request.Get(readCtx).Data, issued))
			require.Equal(t, step.WorkflowID, issued.WorkflowId, "step %d", index)
			var expected *commonpb.Payloads
			if step.Carryover != nil {
				expected = &commonpb.Payloads{Payloads: []*commonpb.Payload{step.Carryover}}
			}
			protorequire.ProtoEqual(t, expected, issued.LastCompletionResult)
		}
	}
}
