package decision

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

// Automatic admission order need not match actual-time order after a clock
// rollback and definition change. Expiry therefore need not remove a prefix.
func TestBufferClockRollbackAllowsNonPrefixExpiry(t *testing.T) {
	t.Parallel()
	state := admitted(t)
	state.Root.Policy = enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	state.Root.Limited = false
	for _, event := range []Event{
		{Kind: Due, Now: 109, Nominal: 100, Actual: 109, CatchupDeadline: 109, WorkflowID: "before-rollback"},
		{Kind: Update, Now: 101, Revision: 1, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL},
		{Kind: Due, Now: 102, Nominal: 102, Actual: 102, CatchupDeadline: 102, WorkflowID: "after-rollback"},
		{Kind: Due, Now: 110, Nominal: 103, Actual: 110, CatchupDeadline: 110, WorkflowID: "after-recovery"},
	} {
		event.Incarnation = "inc"
		var err error
		state, err = Step(state, event)
		require.NoError(t, err)
	}
	before := []Queued{
		{Nominal: 100, Actual: 109, WorkflowID: "before-rollback", Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL},
		{Nominal: 102, Actual: 102, WorkflowID: "after-rollback", Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL},
		{Nominal: 103, Actual: 110, WorkflowID: "after-recovery", Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL},
	}
	require.Equal(t, before, state.Queue)
	state, err := Step(state, Event{Kind: Complete, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "chain", Now: 110})
	require.NoError(t, err)
	state, err = Step(state, Event{Kind: Drain, Incarnation: "inc", Now: 110, CatchupWindow: 3, Request: []byte("current-request")})
	require.NoError(t, err)
	require.Equal(t, "before-rollback", state.Operation.WorkflowID)
	require.Equal(t, before[2:], state.Queue, "expired middle entry must be removed without losing the later live waiter")
	require.Zero(t, state.Root.OverlapSuperseded)
}
