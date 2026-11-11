package decision

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"pgregory.net/rapid"
)

func completionInput(t *testing.T) State {
	t.Helper()
	state := admitted(t)
	state.Root.PauseOnFailure = true
	state.Root.CarryoverSet = true
	state.Carryover = &Carryover{Result: []byte("previous success"), Failure: []byte("previous failure"), FailurePresent: true}
	return state
}
func detachedCompletionState(t *testing.T, state State) State {
	t.Helper()
	encoded, err := json.Marshal(state)
	require.NoError(t, err)
	var detached State
	require.NoError(t, json.Unmarshal(encoded, &detached))
	return detached
}
func TestCompletionOutcomeMatrix(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		outcome         Outcome
		status          enumspb.WorkflowExecutionStatus
		result, failure []byte
		carryover       Carryover
		paused          bool
	}{
		{"success_absent", Success, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, nil, nil, Carryover{}, false},
		{"success_value", Success, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, []byte("new result"), nil, Carryover{Result: []byte("new result")}, false},
		{"failed", Failure, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, nil, []byte("new failure"), Carryover{Result: []byte("previous success"), Failure: []byte("new failure"), FailurePresent: true}, true},
		{"timed_out_empty_failure", Failure, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, nil, nil, Carryover{Result: []byte("previous success"), FailurePresent: true}, true},
		{"canceled", Failure, enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, nil, []byte("canceled"), Carryover{Result: []byte("previous success"), Failure: []byte("canceled"), FailurePresent: true}, false},
		{"terminated", Failure, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, nil, []byte("terminated"), Carryover{Result: []byte("previous success"), Failure: []byte("terminated"), FailurePresent: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := completionInput(t)
			before := detachedCompletionState(t, input)
			event := Event{Kind: Callback, Incarnation: "inc", OperationID: input.Operation.ID, ChainID: "chain", Outcome: tc.outcome, Status: tc.status, Result: tc.result, Failure: tc.failure}
			output, err := Step(input, event)
			require.NoError(t, err)
			expected := detachedCompletionState(t, before)
			expected.Root.Started = 1
			expected.Root.Paused = tc.paused
			expected.Operation.ChainID = "chain"
			expected.Operation.Phase = Completed
			expected.Carryover = &tc.carryover
			expected = detachedCompletionState(t, expected)
			require.Equal(t, expected, output)
			require.Equal(t, before, input)
			if len(event.Result) > 0 {
				event.Result[0] ^= 0xff
			}
			if len(event.Failure) > 0 {
				event.Failure[0] ^= 0xff
			}
			require.Equal(t, expected, output, "caller-owned outcome buffers must not alias returned state")
		})
	}
}
func TestMetadataCompletionFreezesOutcome(t *testing.T) {
	t.Parallel()
	for _, firstKind := range []Kind{Complete, Callback} {
		t.Run(map[Kind]string{Complete: "metadata_first", Callback: "callback_first"}[firstKind], func(t *testing.T) {
			t.Parallel()
			input := completionInput(t)
			first := Event{Kind: firstKind, Incarnation: "inc", OperationID: input.Operation.ID, ChainID: "chain", Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}
			if firstKind == Callback {
				first.Outcome = Success
				first.Result = []byte("first")
			}
			completed, err := Step(input, first)
			require.NoError(t, err)
			if firstKind == Complete {
				require.Equal(t, input.Carryover, completed.Carryover)
			}
			duplicate := Event{Kind: Callback, Incarnation: "inc", OperationID: input.Operation.ID, ChainID: "chain", Outcome: Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, Failure: []byte("late")}
			after, err := Step(completed, duplicate)
			require.NoError(t, err)
			require.Equal(t, completed, after)
			duplicate.Kind = Complete
			duplicate.Outcome = NoOutcome
			duplicate.Failure = nil
			after, err = Step(completed, duplicate)
			require.NoError(t, err)
			require.Equal(t, completed, after)
		})
	}
}
func TestCurrentPausePolicyAtCompletion(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			t.Parallel()
			state := completionInput(t)
			state.Root.PauseOnFailure = !enabled
			updated, err := Step(state, Event{Kind: Update, Incarnation: "inc", Revision: 1, Limited: true, Remaining: 0, PauseOnFailure: enabled})
			require.NoError(t, err)
			completed, err := Step(updated, Event{Kind: Complete, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "chain", Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED})
			require.NoError(t, err)
			expected := detachedCompletionState(t, updated)
			expected.Root.Paused = enabled
			expected.Root.Started = 1
			expected.Operation.Phase = Completed
			expected.Operation.ChainID = "chain"
			require.Equal(t, expected, completed)
		})
	}
}
func TestCallbackIdentityAndValidation(t *testing.T) {
	t.Parallel()
	state := completionInput(t)
	for _, tc := range []struct {
		name     string
		event    Event
		rejected bool
	}{
		{"old_incarnation", Event{Incarnation: "old", Outcome: Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED}, false},
		{"unknown_operation", Event{Incarnation: "inc", OperationID: "unknown", Outcome: Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED}, false},
		{"success_failed_status", Event{Incarnation: "inc", OperationID: state.Operation.ID, Outcome: Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED}, true},
		{"failure_completed_status", Event{Incarnation: "inc", OperationID: state.Operation.ID, Outcome: Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, true},
		{"missing_outcome", Event{Incarnation: "inc", OperationID: state.Operation.ID, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, true},
		{"running_status", Event{Incarnation: "inc", OperationID: state.Operation.ID, Outcome: Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, true},
		{"oversized", Event{Incarnation: "inc", OperationID: state.Operation.ID, Outcome: Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, Result: make([]byte, MaxCompletionBytes+1)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			event := tc.event
			event.Kind = Callback
			event.ChainID = "chain"
			before := detachedCompletionState(t, state)
			output, err := Step(state, event)
			if tc.rejected {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, state, output)
			}
			require.Equal(t, before, state)
		})
	}
	bound := detachedCompletionState(t, state)
	bound.Operation.ChainID = "known"
	bound.Root.Started = 1
	_, err := Step(bound, Event{Kind: Callback, Incarnation: "inc", OperationID: bound.Operation.ID, ChainID: "other", Outcome: Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED})
	require.ErrorContains(t, err, "conflicting workflow chain")
}
func TestCompletionProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		input := completionInput(t)
		enabled := rapid.Bool().Draw(rt, "pause_policy")
		input.Root.PauseOnFailure = enabled
		statuses := []enumspb.WorkflowExecutionStatus{enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED}
		first := rapid.SampledFrom(statuses).Draw(rt, "first_status")
		outcome := Failure
		if first == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			outcome = Success
		}
		value := rapid.SliceOfN(rapid.Byte(), 0, 32).Draw(rt, "opaque_value")
		event := Event{Kind: Callback, Incarnation: "inc", OperationID: input.Operation.ID, ChainID: "chain", Outcome: outcome, Status: first}
		if outcome == Success {
			event.Result = value
		} else {
			event.Failure = value
		}
		state, err := Step(input, event)
		if err != nil {
			rt.Fatal(err)
		}
		expectedResult := []byte("previous success")
		var expectedFailure []byte
		if outcome == Success {
			expectedResult = value
		} else {
			expectedFailure = value
		}
		expectedPause := enabled && (first == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED || first == enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT)
		for _, status := range rapid.SliceOfN(rapid.SampledFrom(statuses), 1, 16).Draw(rt, "duplicates") {
			later := Event{Kind: Callback, Incarnation: "inc", OperationID: input.Operation.ID, ChainID: "chain", Status: status, Outcome: Failure, Failure: []byte("duplicate")}
			if status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				later.Outcome = Success
				later.Failure = nil
				later.Result = []byte("duplicate")
			}
			state, err = Step(state, later)
			if err != nil {
				rt.Fatal(err)
			}
			if state.Operation.Phase != Completed || state.Root.Started != 1 || state.Root.Remaining != 0 || state.Root.Paused != expectedPause || !state.Root.CarryoverSet || state.Carryover.FailurePresent != (outcome == Failure) || !bytes.Equal(state.Carryover.Result, expectedResult) || !bytes.Equal(state.Carryover.Failure, expectedFailure) {
				rt.Fatalf("first-completion contract violated: %+v", state)
			}
		}
	})
}

func TestCallbackConcurrentBufferOwnership(t *testing.T) {
	t.Parallel()
	input := completionInput(t)
	before := detachedCompletionState(t, input)
	event := Event{Kind: Callback, Incarnation: "inc", OperationID: input.Operation.ID, ChainID: "chain", Outcome: Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, Result: []byte("immutable")}
	outcomes := make(chan error, 8)
	for range 8 {
		go func() {
			output, err := Step(input, event)
			if err == nil {
				output.Carryover.Result[0] ^= 0xff
				output.Operation.Request[0] ^= 0xff
			}
			outcomes <- err
		}()
	}
	for range 8 {
		require.NoError(t, <-outcomes)
	}
	require.Equal(t, before, input)
	require.Equal(t, []byte("immutable"), event.Result)
}
