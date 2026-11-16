package occurrence_test

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"google.golang.org/protobuf/encoding/protojson"
)

type occurrenceTrace struct {
	Version     int                   `json:"version"`
	ClockDomain string                `json:"clock_domain"`
	Seed        string                `json:"seed"`
	Revision    int64                 `json:"revision"`
	After       time.Time             `json:"after"`
	Spec        json.RawMessage       `json:"spec"`
	Steps       []occurrenceTraceStep `json:"steps"`
}
type occurrenceTraceStep struct {
	Through         time.Time              `json:"through"`
	Limits          occurrence.Limits      `json:"limits"`
	ExpectedStatus  string                 `json:"expected_status"`
	ExpectedProbes  int                    `json:"expected_probes"`
	ExpectedPending *occurrence.Candidate  `json:"expected_pending"`
	ExpectedOutputs []occurrence.Candidate `json:"expected_outputs"`
}

// TestSavedPendingJitterReplay requires no generator or seed reconstruction.
// Every step reconstructs both the compiled spec and JSON continuation, retaining
// the exact millisecond candidate across output-window and output-cap boundaries.
func TestSavedPendingJitterReplay(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/pending_jitter_v1.json")
	require.NoError(t, err)
	var trace occurrenceTrace
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&trace))
	require.Equal(t, 1, trace.Version)
	require.Equal(t, "UTC", trace.ClockDomain)
	statuses := map[string]occurrence.Status{"waiting": occurrence.Waiting, "limited": occurrence.Limited, "exhausted": occurrence.Exhausted}
	var previous []occurrence.Result
	for replay := 0; replay < 2; replay++ {
		var state occurrence.State
		var observations []occurrence.Result
		for index, step := range trace.Steps {
			spec := &schedulepb.ScheduleSpec{}
			require.NoError(t, protojson.Unmarshal(trace.Spec, spec))
			enumerator, err := occurrence.New(spec, trace.Seed, trace.Revision)
			require.NoError(t, err)
			if index == 0 {
				state, err = enumerator.Begin(trace.After)
				require.NoError(t, err)
			}
			result, err := enumerator.Run(state, step.Through, step.Limits)
			require.NoError(t, err)
			expectedStatus, ok := statuses[step.ExpectedStatus]
			require.True(t, ok, "unsupported expected status %q", step.ExpectedStatus)
			require.Equal(t, expectedStatus, result.Status, "step %d", index)
			require.Equal(t, step.ExpectedProbes, result.Probes, "step %d", index)
			require.Equal(t, step.ExpectedPending, result.State.Pending, "step %d", index)
			require.Equal(t, step.ExpectedOutputs, result.Occurrences, "step %d", index)
			observations = append(observations, result)
			encoded, err := json.Marshal(result.State)
			require.NoError(t, err)
			state = occurrence.State{}
			require.NoError(t, json.Unmarshal(encoded, &state))
		}
		if replay == 1 {
			require.Equal(t, previous, observations)
		}
		previous = observations
	}
}
