package scheduler2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type manualTrace struct {
	Version     int               `json:"version"`
	ClockDomain string            `json:"clock_domain"`
	Incarnation string            `json:"incarnation"`
	Now         time.Time         `json:"now"`
	Catchup     string            `json:"catchup"`
	Policy      int32             `json:"policy"`
	Spec        json.RawMessage   `json:"spec"`
	Request     json.RawMessage   `json:"request"`
	Steps       []manualTraceStep `json:"steps"`
}
type manualTraceStep struct {
	Kind       string            `json:"kind"`
	RequestID  string            `json:"request_id"`
	Command    json.RawMessage   `json:"command"`
	Target     uint64            `json:"target"`
	Sequence   uint64            `json:"sequence"`
	Pending    []uint64          `json:"pending"`
	Operations map[uint64]string `json:"operations"`
	Started    int64             `json:"started"`
	Unfinished int64             `json:"unfinished"`
	Timers     int               `json:"timers"`
	Starts     int               `json:"starts"`
}

func TestManualSavedReplay(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/manual_v1.json")
	require.NoError(t, err)
	var trace manualTrace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&trace))
	require.Equal(t, 1, trace.Version)
	require.Equal(t, "execution_nanoseconds", trace.ClockDomain)
	require.Equal(t, "incarnation", trace.Incarnation)
	for replay := 0; replay < 2; replay++ {
		e := newManualCommandEngine(t)
		h := e.h
		h.now = trace.Now.UnixNano()
		spec := &schedulepb.ScheduleSpec{}
		request := &workflowservice.StartWorkflowExecutionRequest{}
		require.NoError(t, protojson.Unmarshal(trace.Spec, spec))
		require.NoError(t, protojson.Unmarshal(trace.Request, request))
		catchup, err := time.ParseDuration(trace.Catchup)
		require.NoError(t, err)
		configureBuffer(h, spec, 2, enumspb.ScheduleOverlapPolicy(trace.Policy), catchup, request)
		fireGeneration(h, true)
		s, _ := h.root(true)
		s.Paused = true
		s.Remaining = 0
		cursor := s.Cursor
		h.commit(true)
		for index, step := range trace.Steps {
			switch step.Kind {
			case "trigger", "trigger_failed":
				command := &schedulepb.TriggerImmediatelyRequest{}
				require.NoError(t, protojson.Unmarshal(step.Command, command))
				e.failPublication = step.Kind == "trigger_failed"
				err := Trigger(e.ctx, e.ref, step.RequestID, command)
				if e.failPublication {
					require.ErrorIs(t, err, errManualPublication)
				} else {
					require.NoError(t, err)
				}
				e.failPublication = false
			case "transfer", "transfer_failed":
				transferManual(h, step.Kind == "transfer")
			case "restart":
				h.reload()
			case "ack":
				allowAck(h, fmt.Sprintf("%s/manual/%d/action/start", trace.Incarnation, step.Target))
			default:
				t.Fatalf("unsupported manual event %q", step.Kind)
			}
			s, ctx := h.root(false)
			require.Equal(t, step.Sequence, s.ManualSequence, "step %d", index)
			require.Equal(t, cursor, s.Cursor)
			require.Zero(t, s.Remaining)
			require.True(t, s.Paused)
			require.Equal(t, step.Started, s.Started)
			require.Equal(t, step.Unfinished, s.Unfinished)
			require.Len(t, s.Manuals, len(step.Pending))
			for i, id := range step.Pending {
				require.Equal(t, id, s.Manuals[i].Id)
			}
			require.Equal(t, step.Timers, pureObligations(h))
			require.Equal(t, step.Starts, logicalCount(h.durable))
			require.Len(t, s.Operations, len(step.Operations))
			for sequence, phase := range step.Operations {
				id := fmt.Sprintf("%s/manual/%d/action/start", trace.Incarnation, sequence)
				field, ok := s.Operations[id]
				require.True(t, ok)
				op := field.Get(ctx)
				expected := decision.Pending
				if phase == "start_only" {
					expected = decision.StartOnly
				} else {
					require.Equal(t, "pending", phase)
				}
				require.Equal(t, expected, decision.Phase(op.Phase))
				require.Equal(t, sequence, op.ManualId)
				if decision.Terminal(expected) {
					require.NotContains(t, s.Requests, id)
					continue
				}
				frozen := &workflowservice.StartWorkflowExecutionRequest{}
				require.NoError(t, proto.Unmarshal(s.Requests[id].Get(ctx).Data, frozen))
				require.Equal(t, request.WorkflowId+"-"+time.Unix(0, op.Nominal).UTC().Format(time.RFC3339), frozen.WorkflowId)
				require.Equal(t, id, frozen.RequestId)
				require.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, frozen.WorkflowIdReusePolicy)
			}
		}
	}
}
