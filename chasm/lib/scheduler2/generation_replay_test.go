package scheduler2

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/encoding/protojson"
)

type generationTrace struct {
	Version     int                   `json:"version"`
	ClockDomain string                `json:"clock_domain"`
	Incarnation string                `json:"incarnation"`
	Remaining   int64                 `json:"remaining"`
	Now         time.Time             `json:"now"`
	After       time.Time             `json:"after"`
	Revision    int64                 `json:"revision"`
	Seed        string                `json:"seed"`
	Catchup     string                `json:"catchup"`
	Spec        json.RawMessage       `json:"spec"`
	Request     json.RawMessage       `json:"request"`
	Steps       []generationTraceStep `json:"steps"`
}
type generationTraceStep struct {
	Kind             string            `json:"kind"`
	Publish          bool              `json:"publish"`
	Now              time.Time         `json:"now"`
	Result           *commonpb.Payload `json:"result"`
	Cursor           time.Time         `json:"cursor"`
	Remaining        int64             `json:"remaining"`
	Started          int64             `json:"started"`
	Operations       int               `json:"operations"`
	StartTasks       int               `json:"start_tasks"`
	Timers           int               `json:"timers"`
	GeneratorToken   uint64            `json:"generator_token"`
	PendingNominal   *time.Time        `json:"pending_nominal"`
	Armed            bool              `json:"armed"`
	Token            uint64            `json:"token"`
	Status           string            `json:"status"`
	Wake             time.Time         `json:"wake"`
	WorkflowID       string            `json:"workflow_id"`
	OperationID      string            `json:"operation_id"`
	RequestCarryover *commonpb.Payload `json:"request_carryover"`
}

// TestGenerationSavedReplay replays complete inputs and independently recorded
// observations through real persisted pure tasks, without a property generator.
func TestGenerationSavedReplay(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/generation_v2.json")
	require.NoError(t, err)
	var trace generationTrace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&trace))
	require.Equal(t, 2, trace.Version)
	require.Equal(t, "execution_nanoseconds", trace.ClockDomain)
	require.Equal(t, "incarnation", trace.Incarnation)
	statuses := map[string]scheduler2pb.EnumerationStatus{"active": scheduler2pb.ENUMERATION_STATUS_ACTIVE, "exhausted": scheduler2pb.ENUMERATION_STATUS_EXHAUSTED, "waiting": scheduler2pb.ENUMERATION_STATUS_WAITING}
	var previous []*scheduler2pb.GeneratorState
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
		require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: spec, Request: request, Seed: trace.Seed, Revision: trace.Revision, After: trace.After, CatchupWindow: catchup}))
		commitGeneration(h, true)
		var observed []*scheduler2pb.GeneratorState
		for _, step := range trace.Steps {
			switch step.Kind {
			case "scan":
				require.Nil(t, generationState(h).Continuation.Pending)
				fireGeneration(h, step.Publish)
			case "handoff":
				require.NotNil(t, generationState(h).Continuation.Pending)
				fireGeneration(h, step.Publish)
			case "restart":
				h.reload()
			case "complete":
				s, _ := h.root(false)
				sequentialComplete(h, s.OperationId, step.Result)
			case "advance_time":
				h.now = step.Now.UnixNano()
			default:
				t.Fatalf("unsupported generation event %q", step.Kind)
			}
			s, _ = h.root(false)
			g := generationState(h)
			status, ok := statuses[step.Status]
			require.True(t, ok, "unsupported status")
			require.Equal(t, step.Cursor.UnixNano(), s.Cursor)
			require.Equal(t, step.Remaining, s.Remaining)
			require.Equal(t, step.Started, s.Started)
			require.Len(t, s.Operations, step.Operations)
			require.Equal(t, step.StartTasks, logicalCount(h.durable))
			require.Equal(t, step.Timers, pureObligations(h))
			c := coordinationState(h)
			require.Equal(t, step.Token, c.Token)
			require.Equal(t, step.GeneratorToken, g.Token)
			require.Equal(t, step.Armed, c.Armed)
			if step.PendingNominal == nil {
				require.Nil(t, g.Continuation.Pending)
			} else {
				require.NotNil(t, g.Continuation.Pending)
				require.Equal(t, step.PendingNominal.UnixNano(), g.Continuation.Pending.Nominal)
			}
			require.Equal(t, status, g.LastStatus)
			require.Equal(t, step.Wake.UnixNano(), c.At)
			if step.WorkflowID != "" {
				issued := h.request()
				require.Equal(t, step.WorkflowID, issued.WorkflowId)
				require.Equal(t, step.OperationID, issued.RequestId)
				var expected *commonpb.Payloads
				if step.RequestCarryover != nil {
					expected = &commonpb.Payloads{Payloads: []*commonpb.Payload{step.RequestCarryover}}
				}
				protorequire.ProtoEqual(t, expected, issued.LastCompletionResult)
			}
			observed = append(observed, g)
		}
		if replay == 1 {
			require.Equal(t, previous, observed)
		}
		previous = observed
	}
}
