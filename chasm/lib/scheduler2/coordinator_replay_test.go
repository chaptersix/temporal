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
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type coordinatorTrace struct {
	Version     int                    `json:"version"`
	ClockDomain string                 `json:"clock_domain"`
	Incarnation string                 `json:"incarnation"`
	Now         time.Time              `json:"now"`
	After       time.Time              `json:"after"`
	Catchup     string                 `json:"catchup"`
	Remaining   int64                  `json:"remaining"`
	Spec        json.RawMessage        `json:"spec"`
	Request     json.RawMessage        `json:"request"`
	Steps       []coordinatorTraceStep `json:"steps"`
}
type coordinatorTraceStep struct {
	Kind                 string            `json:"kind"`
	Publish              bool              `json:"publish"`
	Command              json.RawMessage   `json:"command"`
	Spec                 json.RawMessage   `json:"spec"`
	Request              json.RawMessage   `json:"request"`
	Revision             int64             `json:"revision"`
	Token                uint64            `json:"token"`
	Armed                bool              `json:"armed"`
	NextWork             int32             `json:"next_work"`
	NextQueue            int32             `json:"next_queue"`
	NextOperation        int32             `json:"next_operation"`
	OwnerClass           int32             `json:"owner_class"`
	OwnerKey             string            `json:"owner_key"`
	Focus                uint64            `json:"focus"`
	Last                 uint64            `json:"last"`
	BackfillSequence     uint64            `json:"backfill_sequence"`
	Backfills            int               `json:"backfills"`
	ContinuationRevision int64             `json:"continuation_revision"`
	Disposed             *time.Time        `json:"disposed"`
	Pending              *time.Time        `json:"pending"`
	Requests             []json.RawMessage `json:"requests"`
}

// The saved trace carries complete definitions, inputs, frozen requests and
// independent projections at every publication boundary. No Rapid seed or
// production decision function supplies its expected coordinator state.
func TestCoordinatorSavedReplay(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/coordinator_v1.json")
	require.NoError(t, err)
	var trace coordinatorTrace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&trace))
	require.Equal(t, 1, trace.Version)
	require.Equal(t, "execution_nanoseconds", trace.ClockDomain)
	require.Equal(t, "incarnation", trace.Incarnation)
	for replay := 0; replay < 2; replay++ {
		h := newTreeHarness(t)
		h.now = trace.Now.UnixNano()
		sequentialBudget(h, trace.Remaining)
		catchup, err := time.ParseDuration(trace.Catchup)
		require.NoError(t, err)
		configure := func(specJSON, requestJSON json.RawMessage, revision int64) {
			spec := &schedulepb.ScheduleSpec{}
			request := &workflowservice.StartWorkflowExecutionRequest{}
			require.NoError(t, protojson.Unmarshal(specJSON, spec))
			require.NoError(t, protojson.Unmarshal(requestJSON, request))
			s, ctx := h.root(true)
			require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: spec, Request: request, Revision: revision, After: trace.After, CatchupWindow: catchup, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}))
			commitGeneration(h, true)
		}
		configure(trace.Spec, trace.Request, 1)
		for index, step := range trace.Steps {
			switch step.Kind {
			case "work":
				coordinatorStep(h, step.Publish)
			case "restart":
				h.reload()
			case "accept_backfill":
				command := &schedulepb.BackfillRequest{}
				require.NoError(t, protojson.Unmarshal(step.Command, command))
				s, ctx := h.root(true)
				require.NoError(t, s.AcceptBackfill(ctx.(chasm.MutableContext), command))
				commitGeneration(h, true)
			case "configure":
				configure(step.Spec, step.Request, step.Revision)
			default:
				t.Fatalf("unknown coordinator replay event %q", step.Kind)
			}
			s, ctx := h.root(false)
			expectedCoord := &scheduler2pb.CoordinationState{Token: step.Token, Armed: step.Armed, At: trace.Now.UnixNano(), NextWork: scheduler2pb.WorkClass(step.NextWork), NextQueue: scheduler2pb.WorkClass(step.NextQueue), NextOperation: scheduler2pb.WorkClass(step.NextOperation), OwnerClass: scheduler2pb.WorkClass(step.OwnerClass), OwnerKey: step.OwnerKey}
			protorequire.ProtoEqual(t, expectedCoord, s.Coordination)
			require.Equal(t, step.Revision, s.Revision, "step %d", index)
			require.Equal(t, trace.Now.UnixNano(), s.Cursor)
			require.Equal(t, trace.Remaining, s.Remaining)
			require.Zero(t, s.Started)
			require.Equal(t, int64(len(step.Requests)), s.Unfinished)
			require.Empty(t, s.BlockingOperationId)
			require.Empty(t, s.Queue)
			require.Empty(t, s.Manuals)
			require.Equal(t, step.BackfillSequence, s.BackfillSequence)
			require.Equal(t, step.Focus, s.BackfillFocus)
			require.Equal(t, step.Last, s.LastBackfill)
			require.Len(t, s.Backfills, step.Backfills)
			if step.Backfills > 0 {
				b := s.Backfills[0]
				require.Equal(t, uint64(1), b.Id)
				require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, b.Policy)
				if step.ContinuationRevision == 0 {
					require.Nil(t, b.Continuation)
				} else {
					require.Equal(t, step.ContinuationRevision, b.Continuation.Revision)
					require.NotNil(t, step.Disposed)
					require.Equal(t, step.Disposed.UnixNano(), b.Continuation.DisposedThrough)
					if step.Pending == nil {
						require.Nil(t, b.Continuation.Pending)
					} else {
						require.Equal(t, step.Pending.UnixNano(), b.Continuation.Pending.Nominal)
						require.Equal(t, step.Pending.UnixNano(), b.Continuation.Pending.Actual)
						require.Equal(t, step.ContinuationRevision, b.Continuation.Pending.Revision)
					}
				}
			}
			require.Len(t, s.Operations, len(step.Requests))
			require.Equal(t, len(step.Requests), logicalCount(h.durable))
			timers := 0
			if step.Armed {
				timers = 1
			}
			require.Equal(t, timers, pureObligations(h))
			latest := ""
			for _, requestJSON := range step.Requests {
				expected := &workflowservice.StartWorkflowExecutionRequest{}
				require.NoError(t, protojson.Unmarshal(requestJSON, expected))
				field, ok := s.Operations[expected.RequestId]
				require.True(t, ok)
				op := field.Get(ctx)
				require.Equal(t, uint64(1), op.BackfillId)
				require.Equal(t, decision.Pending, decision.Phase(op.Phase))
				require.Equal(t, expected.RequestId, fmt.Sprintf("%s/backfill/1/%d/action/start", trace.Incarnation, op.Nominal))
				actual := &workflowservice.StartWorkflowExecutionRequest{}
				require.NoError(t, proto.Unmarshal(s.Requests[expected.RequestId].Get(ctx).Data, actual))
				protorequire.ProtoEqual(t, expected, actual)
				latest = expected.RequestId
			}
			require.Equal(t, latest, s.OperationId)
		}
	}
}
