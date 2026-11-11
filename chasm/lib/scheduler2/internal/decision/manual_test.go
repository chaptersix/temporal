package decision

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"pgregory.net/rapid"
)

func TestManualIndependentProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		state := State{Root: Root{Incarnation: "inc", Paused: true, Limited: true}}
		records := map[string]*Operation{}
		type oracleOp struct {
			id      string
			manual  uint64
			phase   Phase
			policy  enumspb.ScheduleOverlapPolicy
			counted bool
		}
		expectedOps := map[string]*oracleOp{}
		var pending []ManualCommand
		var queue []Queued
		latest, blocker := "", ""
		var sequence uint64
		var started, unfinished, superseded, revision, cursor int64
		policy := enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
		policies := []enumspb.ScheduleOverlapPolicy{0, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}
		admit := func(id uint64, nominal int64, p enumspb.ScheduleOverlapPolicy) {
			latest = fmt.Sprintf("inc/manual/%d/action/start", id)
			expectedOps[latest] = &oracleOp{id: latest, manual: id, phase: Pending, policy: p}
			unfinished++
			if p != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
				blocker = latest
			}
		}
		length := rapid.IntRange(1, 128).Draw(t, "length")
		for i := 0; i < length; i++ {
			now := int64(i + 1)
			command := rapid.IntRange(0, 5).Draw(t, "command")
			event := Event{Incarnation: "inc", Now: now, Request: []byte("current"), WorkflowID: fmt.Sprintf("wf-%d", i), CatchupWindow: 0}
			state.Operation = records[latest]
			reject := false
			switch command {
			case 0:
				event.Kind = AcceptManual
				event.Nominal = int64(rapid.IntRange(-100, 1000).Draw(t, "nominal"))
				event.Policy = policies[rapid.IntRange(0, len(policies)-1).Draw(t, "override")]
				if len(pending) == 4 {
					reject = true
				} else {
					sequence++
					pending = append(pending, ManualCommand{ID: sequence, Nominal: event.Nominal, Policy: event.Policy})
				}
			case 1:
				event.Kind = DispatchManual
				if len(pending) > 0 && len(queue) < 4 {
					c := pending[0]
					p := c.Policy
					if p == 0 {
						p = policy
					}
					if p == 0 {
						p = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
					}
					blocked := p != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL && (blocker != "" || len(queue) > 0)
					if blocked || unfinished < 4 {
						pending = pending[1:]
						if blocked && p != enumspb.SCHEDULE_OVERLAP_POLICY_SKIP && (p != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || len(queue) == 0) {
							queue = append(queue, Queued{Nominal: c.Nominal, Actual: c.Nominal, WorkflowID: event.WorkflowID, Policy: p, ManualID: c.ID})
						}
						if !blocked {
							admit(c.ID, c.Nominal, p)
						}
					}
				}
			case 2:
				event.Kind = Update
				revision++
				event.Revision = revision
				event.Paused = true
				event.Limited = true
				event.Remaining = 0
				policy = policies[rapid.IntRange(0, len(policies)-1).Draw(t, "current_policy")]
				event.Policy = policy
			case 3:
				event.Kind = Complete
				target := blocker
				if target == "" {
					target = latest
				}
				event.OperationID = target
				event.ChainID = "chain"
				state.Operation = records[target]
				if op := expectedOps[target]; op != nil && op.phase != Completed && op.phase != StartOnly {
					if !op.counted {
						started++
						op.counted = true
					}
					unfinished--
					op.phase = Completed
					if op.policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
						op.phase = StartOnly
					}
					if blocker == target {
						blocker = ""
					}
				}
			case 4:
				event.Kind = Drain
				if blocker == "" && unfinished < 4 && len(queue) > 0 {
					selected := 0
					for j, q := range queue {
						if q.Policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER || q.Policy == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER {
							selected = j
						}
					}
					chosen := queue[selected]
					var kept []Queued
					for j, q := range queue {
						replaces := j < selected && (j == 0 || q.Policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER || q.Policy == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER)
						if replaces {
							superseded++
						} else if j != selected {
							kept = append(kept, q)
						}
					}
					queue = kept
					admit(chosen.ManualID, chosen.Nominal, chosen.Policy)
				}
			case 5:
				event.Kind = Due
				event.Nominal = now
				event.CatchupDeadline = now
				cursor = now // paused automatic ingress is disposed independently
			default:
				t.Fatal("unsupported command")
			}
			before := state
			next, err := Step(state, event)
			if reject {
				require.Error(t, err)
				continue
			}
			require.NoError(t, err)
			state = next
			if state.Operation != nil {
				records[state.Operation.ID] = state.Operation
			}
			require.Equal(t, sequence, state.Root.ManualSequence)
			require.Equal(t, started, state.Root.Started)
			require.Equal(t, unfinished, state.Root.Unfinished)
			require.Equal(t, blocker, state.Root.BlockingOperationID)
			require.Equal(t, cursor, state.Root.Cursor)
			require.Equal(t, superseded, state.Root.OverlapSuperseded)
			require.Zero(t, state.Root.Remaining)
			require.True(t, state.Root.Paused)
			require.True(t, slices.Equal(pending, state.Manuals), "pending commands differ: want %v got %v", pending, state.Manuals)
			require.True(t, slices.Equal(queue, state.Queue), "queue differs: want %v got %v", queue, state.Queue)
			for id, want := range expectedOps {
				op := records[id]
				require.NotNil(t, op)
				require.Equal(t, want.phase, op.Phase)
				require.Equal(t, want.manual, op.ManualID)
				require.Equal(t, want.policy, op.Policy)
			}
			if len(before.Manuals) > 0 && len(state.Manuals) > 0 {
				saved := before.Manuals[0]
				state.Manuals[0].Nominal++
				require.Equal(t, saved, before.Manuals[0])
				state.Manuals[0].Nominal--
			}
		}
	})
}

func TestManualMixedQueueEligibility(t *testing.T) {
	t.Parallel()
	state := State{Root: Root{Incarnation: "inc", Paused: true, Limited: true}, Queue: []Queued{
		{Nominal: 1, Actual: 1, WorkflowID: "automatic", Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL},
		{Nominal: 999, Actual: 999, WorkflowID: "future-manual", Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, ManualID: 1},
		{Nominal: -100, Actual: -100, WorkflowID: "past-manual", Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, ManualID: 2},
	}}
	next, err := Step(state, Event{Kind: Drain, Incarnation: "inc", Now: 10, CatchupWindow: 0, Request: []byte("current")})
	require.NoError(t, err)
	require.Equal(t, uint64(1), next.Operation.ManualID)
	require.Equal(t, int64(999), next.Operation.Nominal)
	require.Len(t, next.Queue, 1)
	require.Equal(t, uint64(2), next.Queue[0].ManualID)
	require.Zero(t, next.Root.Remaining)
	require.Zero(t, next.Root.Cursor)
}
