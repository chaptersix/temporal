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
)

type effectsTrace struct {
	Version     int                `json:"version"`
	ClockDomain string             `json:"clock_domain"`
	Incarnation string             `json:"incarnation"`
	Base        time.Time          `json:"base"`
	Revision    int64              `json:"revision"`
	Seed        string             `json:"seed"`
	Catchup     string             `json:"catchup"`
	Remaining   int64              `json:"remaining"`
	Policy      int32              `json:"policy"`
	Chain       string             `json:"chain"`
	Spec        json.RawMessage    `json:"spec"`
	Request     json.RawMessage    `json:"request"`
	Steps       []effectsTraceStep `json:"steps"`
}
type effectsTraceStep struct {
	Quanta     int    `json:"quanta"`
	Kind       string `json:"kind"`
	Now        int64  `json:"now"`
	Publish    bool   `json:"publish"`
	Phase      string `json:"phase"`
	Deadline   int64  `json:"deadline"`
	Retry      int64  `json:"retry"`
	Remaining  int64  `json:"remaining"`
	Started    int64  `json:"started"`
	Unfinished int64  `json:"unfinished"`
	Queue      int    `json:"queue"`
	Tasks      int    `json:"tasks"`
	Timers     int    `json:"timers"`
	Latest     int64  `json:"latest"`
	Blocking   bool   `json:"blocking"`
}

func TestEffectsSavedReplay(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/effects_v2.json")
	require.NoError(t, err)
	var trace effectsTrace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&trace))
	require.Equal(t, 2, trace.Version)
	require.Equal(t, "execution_nanoseconds", trace.ClockDomain)
	require.Equal(t, "incarnation", trace.Incarnation)
	at := func(second int64) int64 { return trace.Base.Add(time.Duration(second) * time.Second).UnixNano() }
	id := func(second int64) string {
		return fmt.Sprintf("%s/automatic/%d/action/start", trace.Incarnation, at(second))
	}
	phases := map[string]decision.EffectPhase{"awaiting": decision.AwaitingTarget, "pending": decision.EffectPending, "absent": decision.EffectAbsent}
	for replay := 0; replay < 2; replay++ {
		h := newTreeHarness(t)
		sequentialBudget(h, trace.Remaining)
		h.now = at(1)
		spec := &schedulepb.ScheduleSpec{}
		request := &workflowservice.StartWorkflowExecutionRequest{}
		require.NoError(t, protojson.Unmarshal(trace.Spec, spec))
		require.NoError(t, protojson.Unmarshal(trace.Request, request))
		catchup, err := time.ParseDuration(trace.Catchup)
		require.NoError(t, err)
		s, ctx := h.root(true)
		require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: spec, Request: request, Seed: trace.Seed, Revision: trace.Revision, After: trace.Base.Add(-time.Nanosecond), CatchupWindow: catchup, Policy: enumspb.ScheduleOverlapPolicy(trace.Policy)}))
		commitGeneration(h, true)
		for i, step := range trace.Steps {
			h.now = at(step.Now)
			event := decision.Event{Incarnation: trace.Incarnation, OperationID: id(0), ChainID: trace.Chain, EffectID: id(0) + "/cancel"}
			switch step.Kind {
			case "generate", "drain":
				require.True(t, step.Quanta <= 1 || step.Publish, "multi-quantum replay steps require individual publication")
				for range max(step.Quanta, 1) {
					fireGeneration(h, step.Publish)
				}
			case "restart":
				h.reload()
			case "ack":
				event.Kind = decision.Acknowledged
				h.apply(event, step.Publish)
			case "failed":
				event.Kind = decision.EffectFailed
				h.apply(event, step.Publish)
			case "not_found":
				event.Kind = decision.EffectNotFound
				h.apply(event, step.Publish)
			case "complete":
				event.Kind = decision.Complete
				h.apply(event, step.Publish)
			default:
				t.Fatalf("unsupported effects event %q", step.Kind)
			}
			s, _ = h.root(false)
			require.Equal(t, step.Remaining, s.Remaining, "step %d", i)
			require.Equal(t, step.Started, s.Started)
			require.Equal(t, step.Unfinished, s.Unfinished)
			require.Len(t, s.Queue, step.Queue)
			require.Equal(t, step.Tasks, logicalCount(h.durable))
			require.Equal(t, step.Timers, pureObligations(h))
			require.Equal(t, id(step.Latest), s.OperationId)
			if step.Blocking {
				require.Equal(t, id(step.Latest), s.BlockingOperationId)
			} else {
				require.Empty(t, s.BlockingOperationId)
			}
			if step.Phase == "none" {
				require.Empty(t, s.Effects)
				require.Empty(t, s.EffectRequests)
				continue
			}
			phase, ok := phases[step.Phase]
			require.True(t, ok)
			require.Len(t, s.Effects, 1)
			effect := s.Effects[0]
			require.Equal(t, phase, decision.EffectPhase(effect.Phase))
			require.Equal(t, id(0)+"/cancel", effect.Id)
			if phase == decision.AwaitingTarget {
				require.Empty(t, s.EffectRequests)
				require.Empty(t, effect.ChainId)
				require.Zero(t, effect.Deadline)
			} else {
				require.Equal(t, trace.Chain, effect.ChainId)
				require.Equal(t, at(step.Deadline), effect.Deadline)
				require.Equal(t, at(step.Retry), effect.RetryAt)
				if phase == decision.EffectPending && step.Now >= step.Retry {
					frozen := effectSnapshot(h, effect.Id).Cancel
					require.Equal(t, request.Namespace, frozen.Namespace)
					require.Equal(t, request.WorkflowId+"-"+trace.Base.Format(time.RFC3339), frozen.WorkflowExecution.WorkflowId)
					require.Empty(t, frozen.WorkflowExecution.RunId)
					require.Equal(t, trace.Chain, frozen.FirstExecutionRunId)
					require.Equal(t, effect.Id, frozen.RequestId)
				}
			}
		}
	}
}
