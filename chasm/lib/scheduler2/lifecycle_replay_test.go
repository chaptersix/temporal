package scheduler2

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type lifecycleTrace struct {
	Version       int                  `json:"version"`
	ClockDomain   string               `json:"clock_domain"`
	Incarnation   string               `json:"incarnation"`
	Now           time.Time            `json:"now"`
	Remaining     int64                `json:"remaining"`
	IdleDuration  string               `json:"idle_duration"`
	Spec          json.RawMessage      `json:"spec"`
	Request       json.RawMessage      `json:"request"`
	Steps         []lifecycleTraceStep `json:"steps"`
	ManualRequest json.RawMessage      `json:"manual_request"`
}
type lifecycleTraceStep struct {
	Kind          string          `json:"kind"`
	Now           time.Time       `json:"now"`
	Publish       bool            `json:"publish"`
	InputRevision int64           `json:"input_revision"`
	SavedTask     string          `json:"saved_task"`
	Capture       string          `json:"capture"`
	Valid         bool            `json:"valid"`
	Error         string          `json:"error"`
	Revision      int64           `json:"revision"`
	Activity      time.Time       `json:"activity"`
	Deadline      time.Time       `json:"deadline"`
	Token         uint64          `json:"token"`
	Armed         bool            `json:"armed"`
	Closed        bool            `json:"closed"`
	PureTasks     int             `json:"pure_tasks"`
	Command       json.RawMessage `json:"command"`
	Target        string          `json:"target"`
	Observation   *time.Time      `json:"observation"`
	Operations    int             `json:"operations"`
	ManualPending int             `json:"manual_pending"`
	Unconfirmed   int64           `json:"unconfirmed"`
	Unfinished    int64           `json:"unfinished"`
	Started       int64           `json:"started"`
	StartTasks    int             `json:"start_tasks"`
	WorkArmed     bool            `json:"work_armed"`
	Phase         string          `json:"phase"`
	Observed      *time.Time      `json:"observed"`
}

// TestLifecycleSavedReplay checks fixed, independently recorded observations
// through actual CHASM serialization/reconstruction and current-task dispatch.
// Saved stale task payloads go through IdleHandler.Validate/Execute; this does
// not model a production queue executor, History storage or recreation routing.
func TestLifecycleSavedReplay(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"testdata/lifecycle_v1.json", "testdata/lifecycle_late_observation_v1.json"} {
		t.Run(path, func(t *testing.T) { t.Parallel(); runLifecycleTrace(t, path) })
	}
}

func runLifecycleTrace(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var trace lifecycleTrace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&trace))
	require.Equal(t, 1, trace.Version)
	require.Equal(t, "execution_nanoseconds", trace.ClockDomain)
	require.Equal(t, "incarnation", trace.Incarnation)
	duration, err := time.ParseDuration(trace.IdleDuration)
	require.NoError(t, err)
	for replay := 0; replay < 2; replay++ {
		h := newTreeHarness(t)
		h.now = trace.Now.UnixNano()
		sequentialBudget(h, trace.Remaining)
		spec := &schedulepb.ScheduleSpec{}
		request := &workflowservice.StartWorkflowExecutionRequest{}
		require.NoError(t, protojson.Unmarshal(trace.Spec, spec))
		require.NoError(t, protojson.Unmarshal(trace.Request, request))
		s, ctx := h.root(true)
		require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: spec, Request: request, Revision: 1, After: trace.Now, CatchupWindow: time.Minute}))
		commitGeneration(h, true)
		coordinatorStep(h, true)
		s, ctx = h.root(true)
		require.NoError(t, s.ConfigureIdle(ctx.(chasm.MutableContext), duration))
		commitGeneration(h, true)
		type envelope struct {
			attrs chasm.TaskAttributes
			task  *scheduler2pb.IdleTask
		}
		saved := map[string]envelope{}
		attrs, task := storedIdleTask(h)
		saved["initial"] = envelope{attrs, task}
		for index, step := range trace.Steps {
			h.now = step.Now.UnixNano()
			before := cloneNodes(h.durable)
			switch step.Kind {
			case "snapshot":
			case "update":
				h.apply(decision.Event{Kind: decision.Update, Incarnation: trace.Incarnation, Revision: step.InputRevision, Limited: true, Remaining: trace.Remaining}, step.Publish)
				if !step.Publish {
					require.Equal(t, before, h.durable)
				}
			case "accept_manual":
				command := &schedulepb.TriggerImmediatelyRequest{}
				require.NoError(t, protojson.Unmarshal(step.Command, command))
				acceptManual(h, command, step.Publish)
			case "transfer_manual":
				coordinatorStep(h, step.Publish)
			case "complete", "observe":
				event := decision.Event{Kind: decision.Complete, Incarnation: trace.Incarnation, OperationID: step.Target, ChainID: "chain"}
				if step.Kind == "observe" {
					require.NotNil(t, step.Observation)
					event.Kind = decision.Acknowledged
					event.StartObserved = true
					event.StartObservedAt = step.Observation.UnixNano()
				}
				h.apply(event, step.Publish)
				if !step.Publish {
					require.Equal(t, before, h.durable)
				}
			case "restart":
				h.reload()
			case "deliver_saved":
				packet, ok := saved[step.SavedTask]
				require.True(t, ok, "unknown saved task")
				s, ctx = h.root(true)
				valid, err := (&IdleHandler{}).Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: packet.attrs}, packet.task)
				require.NoError(t, err)
				require.Equal(t, step.Valid, valid)
				err = (&IdleHandler{}).Execute(ctx.(chasm.MutableContext), s, packet.attrs, packet.task)
				if step.Error != "" {
					require.ErrorContains(t, err, step.Error)
					h.reload()
					require.Equal(t, before, h.durable)
				} else {
					require.NoError(t, err)
					commitGeneration(h, step.Publish)
				}
			case "deliver_current":
				count, err := deliverGeneration(h, time.Unix(0, h.now))
				require.NoError(t, err)
				require.Equal(t, 1, count)
				commitGeneration(h, step.Publish)
				if !step.Publish {
					require.Equal(t, before, h.durable)
				}
			default:
				t.Fatalf("unsupported lifecycle event %q", step.Kind)
			}
			if step.Capture != "" {
				attrs, task := storedIdleTask(h)
				saved[step.Capture] = envelope{attrs, task}
			}
			s, ctx = h.root(false)
			require.Equal(t, step.Revision, s.Revision, "step %d", index)
			require.True(t, s.ActivitySet)
			require.Equal(t, step.Activity.UnixNano(), s.ActivityAt)
			require.Equal(t, int64(duration), s.IdleDuration)
			require.Equal(t, step.Deadline.UnixNano(), s.IdleAt)
			require.Equal(t, step.Token, s.IdleToken)
			require.Equal(t, step.Armed, s.IdleArmed)
			require.Equal(t, step.Closed, s.Closed)
			lifecycle := chasm.LifecycleStateRunning
			if step.Closed {
				lifecycle = chasm.LifecycleStateCompleted
			}
			require.Equal(t, lifecycle, s.LifecycleState(ctx))
			require.Equal(t, step.PureTasks, pureObligations(h))
			require.Equal(t, step.StartTasks, logicalCount(h.durable))
			require.Equal(t, step.WorkArmed, s.Coordination.Armed)
			require.Equal(t, trace.Remaining, s.Remaining)
			require.Equal(t, trace.Now.UnixNano(), s.Cursor)
			require.Equal(t, step.Unconfirmed, s.UnconfirmedStarts)
			require.Equal(t, step.Unfinished, s.Unfinished)
			require.Equal(t, step.Started, s.Started)
			require.Len(t, s.Operations, step.Operations)
			require.Len(t, s.Manuals, step.ManualPending)
			require.Empty(t, s.Backfills)
			require.Empty(t, s.Queue)
			if step.Operations > 0 {
				expected := &workflowservice.StartWorkflowExecutionRequest{}
				require.NoError(t, protojson.Unmarshal(trace.ManualRequest, expected))
				op := s.Operations[expected.RequestId].Get(ctx)
				phase := decision.Pending
				if step.Phase == "start_only" {
					phase = decision.StartOnly
				} else {
					require.Equal(t, "pending", step.Phase)
				}
				require.Equal(t, phase, decision.Phase(op.Phase))
				require.Equal(t, step.Observed != nil, op.StartObserved)
				if step.Observed != nil {
					require.Equal(t, step.Observed.UnixNano(), op.StartObservedAt)
				} else {
					require.Zero(t, op.StartObservedAt)
				}
				if decision.Terminal(phase) {
					require.NotContains(t, s.Requests, expected.RequestId)
					continue
				}
				actual := &workflowservice.StartWorkflowExecutionRequest{}
				require.NoError(t, proto.Unmarshal(s.Requests[expected.RequestId].Get(ctx).Data, actual))
				require.True(t, proto.Equal(expected, actual), "frozen manual request differs")
			}

		}
	}
}
