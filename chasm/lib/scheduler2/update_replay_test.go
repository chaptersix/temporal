package scheduler2

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type updateReplayConfig struct {
	Spec           json.RawMessage `json:"spec"`
	Request        json.RawMessage `json:"request"`
	Seed           string          `json:"seed"`
	Revision       int64           `json:"revision"`
	CatchupSeconds int64           `json:"catchup_seconds"`
	Policy         int32           `json:"policy"`
	Paused         bool            `json:"paused"`
	Limited        bool            `json:"limited"`
	Remaining      int64           `json:"remaining"`
	PauseOnFailure bool            `json:"pause_on_failure"`
}

func (c updateReplayConfig) decode(t *testing.T) UpdateConfig {
	t.Helper()
	spec, request := &schedulepb.ScheduleSpec{}, &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, protojson.Unmarshal(c.Spec, spec))
	require.NoError(t, protojson.Unmarshal(c.Request, request))
	return UpdateConfig{Generation: GenerationConfig{Spec: spec, Request: request, Seed: c.Seed, Revision: c.Revision, CatchupWindow: time.Duration(c.CatchupSeconds) * time.Second, Policy: enumspb.ScheduleOverlapPolicy(c.Policy)}, Paused: c.Paused, Limited: c.Limited, Remaining: c.Remaining, PauseOnFailure: c.PauseOnFailure}
}

// Replays actual serialized Node publications and bounded request-ID DATA. The
// engine helper is not History storage or the public UpdateSchedule endpoint.
func TestUpdateSavedReplay(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"testdata/update_v1.json", "testdata/update_obligation_v1.json"} {
		t.Run(path, func(t *testing.T) { t.Parallel(); runUpdateReplay(t, path) })
	}
}

func runUpdateReplay(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var trace struct {
		Version       int                `json:"version"`
		ClockDomain   string             `json:"clock_domain"`
		Now           time.Time          `json:"now"`
		Initial       updateReplayConfig `json:"initial"`
		Replacement   updateReplayConfig `json:"replacement"`
		FrozenRequest json.RawMessage    `json:"frozen_request"`
		Steps         []struct {
			Kind            string `json:"kind"`
			Seconds         int64  `json:"seconds"`
			RequestID       string `json:"request_id"`
			Publish         bool   `json:"publish"`
			Error           string `json:"error"`
			Replacement     bool   `json:"replacement"`
			GenerationDelta uint64 `json:"generation_delta"`
			WorkArmed       bool   `json:"work_armed"`
		} `json:"steps"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&trace))
	require.Equal(t, 1, trace.Version)
	require.Equal(t, "execution_nanoseconds", trace.ClockDomain)
	for replay := 0; replay < 2; replay++ {
		e := newManualCommandEngine(t)
		e.h.now = trace.Now.UnixNano()
		initial := trace.Initial.decode(t)
		s, ctx := e.h.root(true)
		require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), initial))
		commitGeneration(e.h, true)
		coordinatorStep(e.h, true) // empty source proves exhaustion
		acceptManual(e.h, &schedulepb.TriggerImmediatelyRequest{}, true)
		coordinatorStep(e.h, true)
		s, ctx = e.h.root(false)
		id := s.OperationId
		expectedFrozen := &workflowservice.StartWorkflowExecutionRequest{}
		require.NoError(t, protojson.Unmarshal(trace.FrozenRequest, expectedFrozen))
		protorequire.ProtoEqual(t, expectedFrozen, allowRequest(e.h, id))
		baselineGeneration := s.Generator.Get(ctx).Token
		replacement := trace.Replacement.decode(t)
		for index, step := range trace.Steps {
			e.h.now = trace.Now.Add(time.Duration(step.Seconds) * time.Second).UnixNano()
			before := cloneNodes(e.h.durable)
			switch step.Kind {
			case "update":
				e.failPublication = !step.Publish
				err = Update(e.ctx, e.ref, step.RequestID, replacement)
			case "duplicate_invalid":
				err = Update(e.ctx, e.ref, step.RequestID, UpdateConfig{})
			case "restart":
				e.h.reload()
				err = nil
			default:
				t.Fatalf("unknown replay event %q", step.Kind)
			}
			switch step.Error {
			case "publication":
				require.ErrorIs(t, err, errManualPublication)
			case "stale":
				require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			case "":
				require.NoError(t, err)
			default:
				t.Fatal("unknown expected error")
			}
			if step.Error != "" || step.Kind != "update" {
				require.Equal(t, before, e.h.durable)
			}
			want := initial
			if step.Replacement {
				want = replacement
			}
			s, ctx = e.h.root(false)
			require.Equal(t, want.Generation.Revision, s.Revision, "step %d", index)
			require.Equal(t, want.Paused, s.Paused)
			require.Equal(t, want.Limited, s.Limited)
			require.Equal(t, want.Remaining, s.Remaining, "manual admission does not debit automatic budget")
			require.Equal(t, want.PauseOnFailure, s.PauseOnFailure)
			require.Equal(t, want.Generation.Policy, s.Policy)
			g := s.Generator.Get(ctx)
			require.Equal(t, baselineGeneration+step.GenerationDelta, g.Token)
			require.Equal(t, step.WorkArmed, s.Coordination.Armed)
			definition := g.Definition.Get(ctx)
			require.Equal(t, want.Generation.Seed, definition.Seed)
			encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(want.Generation.Request)
			require.NoError(t, err)
			require.Equal(t, encoded, definition.Request)
			var actualSpec schedulepb.ScheduleSpec
			require.NoError(t, proto.Unmarshal(definition.Spec, &actualSpec))
			protorequire.ProtoEqual(t, want.Generation.Spec, &actualSpec)
			require.Equal(t, int64(want.Generation.CatchupWindow), g.CatchupWindow)
			protorequire.ProtoEqual(t, expectedFrozen, allowRequest(e.h, id))
			require.Len(t, s.Operations, 1)
			require.Equal(t, int64(1), s.UnconfirmedStarts)
			require.Equal(t, map[string]int64{id: trace.Now.UnixNano()}, s.PendingStarts)
		}
	}
}
