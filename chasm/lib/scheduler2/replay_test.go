package scheduler2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

// Replay records explicit delivery choices. Operation/occurrence/action identities
// derive from the recorded incarnation and initial occurrence instant (contract I1-I4).
// The remote ledger is deliberately separate from treeHarness snapshots.
type replayTrace struct {
	ClockDomain        string                                         `json:"clock_domain"`
	DefinitionRevision int64                                          `json:"definition_revision"`
	Version            int                                            `json:"version"`
	Incarnation        string                                         `json:"incarnation"`
	InitialTime        int64                                          `json:"initial_time_ns"`
	Remaining          int64                                          `json:"remaining"`
	Request            *workflowservice.StartWorkflowExecutionRequest `json:"request"`
	Events             []replayEvent                                  `json:"events"`
}
type replayEvent struct {
	Kind          string `json:"kind"`
	Attempt       int    `json:"attempt,omitempty"`
	Time          int64  `json:"time_ns,omitempty"`
	Chain         string `json:"chain,omitempty"`
	Outcome       string `json:"outcome,omitempty"`
	ResultPresent bool   `json:"result_present,omitempty"`
}
type replayObservation struct {
	replayProjection
	Event  string
	Issued []byte
}

type replayProjection struct {
	OperationID  string
	ChainID      string
	Revision     int64
	Remaining    int64
	Started      int64
	Cursor       int64
	Phase        decision.Phase
	LogicalTasks int
}

func readTrace(data []byte) (replayTrace, error) {
	var trace replayTrace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&trace); err != nil {
		return trace, err
	}
	return trace, validateTrace(trace)
}
func validateTrace(trace replayTrace) error {
	if trace.Version != 1 {
		return fmt.Errorf("unsupported trace version %d", trace.Version)
	}
	if trace.ClockDomain != "execution_nanoseconds" || trace.DefinitionRevision != 1 || trace.Incarnation != "incarnation" || trace.InitialTime != 1000000000 || trace.Remaining != 1 || trace.Request == nil {
		return errors.New("unsupported initial state")
	}
	admitted, issued, accepted, crashed := false, false, false, false
	attempt := 0
	for _, event := range trace.Events {
		if event.ResultPresent {
			return errors.New("completion payload semantics are unsupported in this trace profile")
		}
		if crashed && event.Kind != "restart" {
			return fmt.Errorf("event %s requires restart", event.Kind)
		}
		switch event.Kind {
		case "occurrence_due":
			if admitted {
				return errors.New("duplicate occurrence")
			}
			admitted = true
		case "deliver_start":
			if !admitted || event.Attempt <= attempt {
				return errors.New("delivery dependency")
			}
			issued = true
			attempt = event.Attempt
		case "external_accept":
			if !issued || event.Chain == "" {
				return errors.New("acceptance dependency")
			}
			accepted = true
		case "complete":
			if !accepted || event.Chain == "" {
				return errors.New("completion dependency")
			}
		case "rpc_result":
			if !issued {
				return errors.New("result dependency")
			}
			if event.Outcome != "lost_ack" && event.Outcome != "success" {
				return errors.New("unsupported RPC outcome")
			}
		case "crash":
			crashed = true
		case "restart":
			if !crashed {
				return errors.New("restart dependency")
			}
			crashed = false
		case "advance_time":
			if event.Time < trace.InitialTime || event.Time > 2*trace.InitialTime {
				return errors.New("time outside first-slice trace domain")
			}
		case "update_definition", "commit_fail":
		default:
			return fmt.Errorf("unsupported event %q", event.Kind)
		}
	}
	if crashed {
		return errors.New("trace ends before restart")
	}
	return nil
}
func replay(t *testing.T, trace replayTrace, mutateRetry bool) ([]replayObservation, error) {
	t.Helper()
	if err := validateTrace(trace); err != nil {
		return nil, err
	}
	h := newTreeHarness(t)
	var issued, accepted *workflowservice.StartWorkflowExecutionRequest
	operationID := fmt.Sprintf("%s/automatic/%d/action/start", trace.Incarnation, trace.InitialTime)
	expected := replayProjection{Revision: 1, Remaining: trace.Remaining}
	frozen := proto.Clone(trace.Request).(*workflowservice.StartWorkflowExecutionRequest)
	frozen.RequestId = operationID
	var observations []replayObservation
	for _, event := range trace.Events {
		next, err := expectedProjection(expected, event, trace.InitialTime, operationID)
		if err != nil {
			return nil, err
		}
		expected = next
		switch event.Kind {
		case "occurrence_due":
			encoded, err := proto.Marshal(trace.Request)
			require.NoError(t, err)
			h.apply(decision.Event{Kind: decision.Due, Incarnation: trace.Incarnation, Nominal: trace.InitialTime, CatchupDeadline: trace.InitialTime, Request: encoded}, true)
		case "deliver_start":
			issued = h.request()
			if issued == nil {
				return nil, errors.New("issuance unavailable")
			}
			if accepted != nil && mutateRetry {
				issued.WorkflowId = "injected-retry-input-defect"
			}
			if !proto.Equal(frozen, issued) || accepted != nil && !proto.Equal(accepted, issued) {
				return nil, errors.New("S2 immutable retry input")
			}
		case "external_accept":
			accepted = proto.Clone(issued).(*workflowservice.StartWorkflowExecutionRequest)
		case "rpc_result":
			kind := decision.RetryableFailure
			if event.Outcome == "success" {
				kind = decision.Acknowledged
			}
			h.apply(decision.Event{Kind: kind, Incarnation: trace.Incarnation, OperationID: operationID, ChainID: event.Chain}, true)
		case "complete":
			h.apply(decision.Event{Kind: decision.Complete, Incarnation: trace.Incarnation, OperationID: operationID, ChainID: event.Chain}, true)
		case "update_definition", "commit_fail":
			s, _ := h.root(false)
			h.apply(decision.Event{Kind: decision.Update, Incarnation: trace.Incarnation, Revision: s.Revision + 1, Limited: true, Remaining: s.Remaining}, event.Kind != "commit_fail")
		case "advance_time":
			h.now = event.Time
		case "crash":
			h.node = nil
			h.backend = nil
		case "restart":
			h.reload()
		default:
			return nil, fmt.Errorf("unsupported replay event %q", event.Kind)
		}
		if h.node == nil {
			continue
		}
		s, ctx := h.root(false)
		observation := replayObservation{Event: event.Kind, replayProjection: replayProjection{OperationID: s.OperationId, Revision: s.Revision, Remaining: s.Remaining, Started: s.Started, Cursor: s.Cursor, LogicalTasks: logicalCount(h.durable)}}
		if s.OperationId != "" {
			op := s.Operations[s.OperationId].Get(ctx)
			observation.Phase = decision.Phase(op.Phase)
			observation.ChainID = op.ChainId
		}
		if event.Kind == "deliver_start" {
			var err error
			observation.Issued, err = proto.MarshalOptions{Deterministic: true}.Marshal(issued)
			require.NoError(t, err)
		}
		if s.Started > 1 || s.Remaining != 0 && s.OperationId != "" {
			return nil, errors.New("S3 admission accounting")
		}
		if observation.Phase == decision.Pending && observation.LogicalTasks == 0 {
			return nil, errors.New("S5 missing obligation")
		}
		if observation.replayProjection != expected {
			return nil, fmt.Errorf("refinement after %s: got %+v, expected %+v", event.Kind, observation.replayProjection, expected)
		}
		observations = append(observations, observation)
	}
	return observations, nil
}

// expectedProjection is the independent abstract relation for this trace profile.
// It never calls the production decision layer or reads component state.
func expectedProjection(before replayProjection, event replayEvent, nominal int64, operationID string) (replayProjection, error) {
	next := before
	switch event.Kind {
	case "occurrence_due":
		next.Cursor = nominal
		next.Remaining--
		next.OperationID = operationID
		next.Phase = decision.Pending
		next.LogicalTasks = 1
	case "update_definition":
		next.Revision++
	case "complete":
		next.Started = 1
		next.ChainID = event.Chain
		next.Phase = decision.Completed
		next.LogicalTasks = 0
	case "rpc_result":
		if event.Outcome == "success" {
			next.Started = 1
			next.ChainID = event.Chain
			next.LogicalTasks = 0
			if next.Phase != decision.Completed {
				next.Phase = decision.Started
			}
		}
	// These inputs stutter in the committed projection. A lost acknowledgment
	// retains one obligation; its concrete retry timestamp is abstracted away.
	case "deliver_start", "external_accept", "advance_time", "crash", "restart", "commit_fail":
	default:
		return before, fmt.Errorf("unsupported oracle event %q", event.Kind)
	}
	return next, nil
}

func savedTrace(t *testing.T) replayTrace {
	t.Helper()
	data, err := os.ReadFile("testdata/lost_ack_v1.json")
	require.NoError(t, err)
	trace, err := readTrace(data)
	require.NoError(t, err)
	return trace
}
func TestReplayFreshObjects(t *testing.T) {
	t.Parallel()
	trace := savedTrace(t)
	first, err := replay(t, trace, false)
	require.NoError(t, err)
	second, err := replay(t, trace, false)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, decision.Completed, first[len(first)-1].Phase)
	for _, mutate := range []func(*replayTrace){func(trace *replayTrace) { trace.Version = 2 }, func(trace *replayTrace) { trace.Events = []replayEvent{{Kind: "unknown"}} }, func(trace *replayTrace) { trace.Events = []replayEvent{{Kind: "complete", ResultPresent: true}} }} {
		invalid := trace
		mutate(&invalid)
		require.Error(t, validateTrace(invalid))
	}
}
func TestReplayShrinksRetryInputDefect(t *testing.T) {
	t.Parallel()
	trace := savedTrace(t)
	_, err := replay(t, trace, true)
	require.ErrorContains(t, err, "S2 immutable retry input")
	// Remove only events whose dependencies remain valid and whose removal keeps
	// the same failure. The resulting explicit JSON reproduces without a seed.
	for i := 0; i < len(trace.Events); {
		candidate := trace
		candidate.Events = append(append([]replayEvent{}, trace.Events[:i]...), trace.Events[i+1:]...)
		if validateTrace(candidate) == nil {
			_, err := replay(t, candidate, true)
			if err != nil && err.Error() == "S2 immutable retry input" {
				trace = candidate
				continue
			}
		}
		i++
	}
	require.Less(t, len(trace.Events), len(savedTrace(t).Events))
	golden, err := os.ReadFile("testdata/retry_input_defect_v1.json")
	require.NoError(t, err)
	saved, err := readTrace(golden)
	require.NoError(t, err)
	require.Equal(t, saved.Events, trace.Events, "minimized standalone trace must stay replayable")
	encoded, err := json.MarshalIndent(trace, "", "  ")
	require.NoError(t, err)
	roundtrip, err := readTrace(encoded)
	require.NoError(t, err)
	_, err = replay(t, roundtrip, true)
	require.ErrorContains(t, err, "S2 immutable retry input")
	_, err = replay(t, roundtrip, false)
	require.NoError(t, err)
	t.Logf("minimized explicit counterexample (%d events): %s", len(trace.Events), encoded)
}
func TestReplayProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		trace := savedTrace(t)
		// Noise preserves the required acceptance/retry/completion dependencies;
		// Rapid shrinks the explicit command slice, never the operation identity.
		noise := rapid.SliceOfN(rapid.SampledFrom([]string{"update_definition", "commit_fail"}), 0, 16).Draw(rt, "commands")
		events := append([]replayEvent{}, trace.Events[:3]...)
		for _, kind := range noise {
			events = append(events, replayEvent{Kind: kind})
		}
		trace.Events = append(events, trace.Events[3:]...)
		observations, err := replay(t, trace, false)
		if err != nil {
			rt.Fatal(err)
		}
		final := observations[len(observations)-1]
		if final.Started != 1 || final.Remaining != 0 || final.Phase != decision.Completed {
			rt.Fatalf("incorrect final observation: %+v", final)
		}
	})
}

func TestSnapshotOwnsSerializedBuffers(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	expected := h.admit()
	oldSnapshot := h.node.Snapshot(nil).Nodes
	h.reload()
	for _, node := range oldSnapshot {
		for i := range node.GetData().GetData() {
			node.Data.Data[i] = 0xff
		}
		node.Metadata = nil
	}
	require.True(t, proto.Equal(expected, h.request()))
	require.Equal(t, 1, logicalCount(h.durable))
	// Mutating one cloned store cannot corrupt the source store either.
	cloned := cloneNodes(h.durable)
	for _, node := range cloned {
		node.Metadata = nil
		node.Data = nil
	}
	h.reload()
	require.True(t, proto.Equal(expected, h.request()))
}
