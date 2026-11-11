//go:build test_dep

// Package scheduler2test exposes narrow test-only event fixtures for engine tests.
package scheduler2test

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

// ApplyLifecycleResults exercises the existing result interface, not a production
// callback endpoint or authentication boundary. The malformed failure must be
// ignored by a closed root before decoding the callback payload.
func ApplyLifecycleResults(ctx chasm.MutableContext, root *scheduler2.Scheduler, incarnation, operationID, chain string) error {
	for _, event := range []decision.Event{
		{Kind: decision.Callback, Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, Failure: []byte{0xff}},
		{Kind: decision.Complete},
		{Kind: decision.Expire},
	} {
		event.Incarnation, event.OperationID, event.ChainID = incarnation, operationID, chain
		if err := root.Apply(ctx, event); err != nil {
			return err
		}
	}
	return nil
}
