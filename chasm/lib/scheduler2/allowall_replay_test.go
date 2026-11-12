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
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type allowTrace struct {
	Version     int              `json:"version"`
	ClockDomain string           `json:"clock_domain"`
	Incarnation string           `json:"incarnation"`
	Now         time.Time        `json:"now"`
	Revision    int64            `json:"revision"`
	Seed        string           `json:"seed"`
	Catchup     string           `json:"catchup"`
	Remaining   int64            `json:"remaining"`
	Spec        json.RawMessage  `json:"spec"`
	Request     json.RawMessage  `json:"request"`
	Steps       []allowTraceStep `json:"steps"`
}
type allowTraceStep struct {
	Kind       string           `json:"kind"`
	Second     int64            `json:"second"`
	Target     int64            `json:"target"`
	Publish    bool             `json:"publish"`
	Cursor     int64            `json:"cursor"`
	Latest     int64            `json:"latest"`
	Remaining  int64            `json:"remaining"`
	Started    int64            `json:"started"`
	Unfinished int64            `json:"unfinished"`
	Dropped    int64            `json:"dropped"`
	Records    map[int64]string `json:"records"`
}

func TestAllowAllSavedReplay(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/allowall_v1.json")
	require.NoError(t, err)
	var trace allowTrace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&trace))
	require.Equal(t, 1, trace.Version)
	require.Equal(t, "execution_nanoseconds", trace.ClockDomain)
	require.Equal(t, "incarnation", trace.Incarnation)
	id := func(second int64) string {
		return fmt.Sprintf("%s/automatic/%d/action/start", trace.Incarnation, trace.Now.Add(time.Duration(second)*time.Second).UnixNano())
	}
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
		require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: spec, Request: request, Seed: trace.Seed, Revision: trace.Revision, After: trace.Now.Add(-time.Nanosecond), CatchupWindow: catchup, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}))
		commitGeneration(h, true)
		fireGeneration(h, true)
		wire, err := proto.Marshal(request)
		require.NoError(t, err)
		for index, step := range trace.Steps {
			h.now = trace.Now.Add(time.Duration(step.Second) * time.Second).UnixNano()
			switch step.Kind {
			case "due":
				h.apply(decision.Event{Kind: decision.Due, Incarnation: trace.Incarnation, Nominal: h.now, Actual: h.now, CatchupDeadline: h.now, WorkflowID: fmt.Sprintf("occurrence-%d", step.Second), Request: wire}, step.Publish)
			case "ack":
				h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: trace.Incarnation, OperationID: id(step.Target), ChainID: id(step.Target) + "/chain"}, true)
			case "restart":
				h.reload()
			default:
				t.Fatalf("unknown allow-all event %q", step.Kind)
			}
			s, ctx = h.root(false)
			assertAllowPendingStarts(h)
			require.Equal(t, trace.Now.Add(time.Duration(step.Cursor)*time.Second).UnixNano(), s.Cursor, "step %d", index)
			require.Equal(t, id(step.Latest), s.OperationId)
			require.Equal(t, step.Remaining, s.Remaining)
			require.Equal(t, step.Started, s.Started)
			require.Equal(t, step.Unfinished, s.Unfinished)
			require.Equal(t, step.Dropped, s.CapacityDropped)
			require.Empty(t, s.BlockingOperationId)
			require.Empty(t, s.Queue)
			require.Equal(t, int(step.Unfinished), logicalCount(h.durable))
			require.Zero(t, pureObligations(h))
			require.Len(t, s.Operations, len(step.Records))
			for second, phase := range step.Records {
				field, ok := s.Operations[id(second)]
				require.True(t, ok)
				op := field.Get(ctx)
				expected := decision.Pending
				if phase == "start_only" {
					expected = decision.StartOnly
				} else {
					require.Equal(t, "pending", phase)
				}
				require.Equal(t, expected, decision.Phase(op.Phase))
				require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, op.Policy)
				if decision.Terminal(expected) {
					require.NotContains(t, s.Requests, id(second))
					continue
				}
				frozen := &workflowservice.StartWorkflowExecutionRequest{}
				require.NoError(t, proto.Unmarshal(s.Requests[id(second)].Get(ctx).Data, frozen))
				require.Equal(t, id(second), frozen.RequestId)
				require.Equal(t, fmt.Sprintf("occurrence-%d", second), frozen.WorkflowId)
				require.Nil(t, frozen.LastCompletionResult)
				require.Nil(t, frozen.ContinuedFailure)
			}
		}
	}
}
