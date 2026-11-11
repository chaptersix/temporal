// Package decision implements the bounded scheduler2 admission and start protocol.
package decision

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"go.temporal.io/api/serviceerror"
)

const (
	RetryHorizon = int64(60 * time.Second)
	RetryBackoff = int64(time.Second)
)

type Phase int

const (
	Pending Phase = iota + 1
	Started
	Completed
	Ambiguous
)

type Root struct {
	Incarnation string
	Revision    int64
	Cursor      int64
	Paused      bool
	Limited     bool
	Remaining   int64
	Started     int64
}

type Operation struct {
	OccurrenceID string
	ActionID     string
	ID           string
	ChainID      string
	Nominal      int64
	Deadline     int64
	RetryAt      int64
	Phase        Phase
	Request      []byte
}

type State struct {
	Root      Root
	Operation *Operation
}

type Kind int

const (
	Due Kind = iota + 1
	Update
	Acknowledged
	RetryableFailure
	Complete
	Expire
)

type Event struct {
	Kind            Kind
	Incarnation     string
	OperationID     string
	ChainID         string
	Now             int64
	Nominal         int64
	CatchupDeadline int64
	Revision        int64
	Paused          bool
	Limited         bool
	Remaining       int64
	Request         []byte
}

// Step returns an independent state value; callers publish it only after commit.
func Step(state State, event Event) (State, error) {
	next := clone(state)
	if event.Incarnation != state.Root.Incarnation {
		return next, nil
	}
	switch event.Kind {
	case Due:
		if event.Nominal <= state.Root.Cursor || event.Nominal > event.Now {
			return next, nil
		}
		if state.Operation != nil {
			return next, serviceerror.NewFailedPrecondition("single occurrence capacity exhausted")
		}
		if event.Now > math.MaxInt64-RetryHorizon {
			return next, serviceerror.NewInvalidArgument("admission time exceeds supported range")
		}
		next.Root.Cursor = event.Nominal
		if state.Root.Paused || (state.Root.Limited && state.Root.Remaining <= 0) || event.Now > event.CatchupDeadline {
			return next, nil
		}
		if len(event.Request) == 0 || state.Root.Incarnation == "" {
			return clone(state), serviceerror.NewInvalidArgument("admission requires incarnation and request")
		}
		occurrenceID := fmt.Sprintf("automatic/%d", event.Nominal)
		actionID := state.Root.Incarnation + "/" + occurrenceID + "/action"
		next.Operation = &Operation{
			OccurrenceID: occurrenceID, ActionID: actionID, ID: actionID + "/start",
			Nominal: event.Nominal, Deadline: event.Now + RetryHorizon,
			RetryAt: event.Now, Phase: Pending, Request: bytes.Clone(event.Request),
		}
		if next.Root.Limited {
			next.Root.Remaining--
		}
	case Update:
		if event.Revision <= state.Root.Revision {
			return next, nil
		}
		if event.Remaining < 0 {
			return next, serviceerror.NewInvalidArgument("remaining actions must be nonnegative")
		}
		next.Root.Revision = event.Revision
		next.Root.Paused = event.Paused
		next.Root.Limited = event.Limited
		next.Root.Remaining = event.Remaining
	case Acknowledged, Complete, RetryableFailure, Expire:
		op := next.Operation
		if op == nil || event.OperationID != op.ID {
			return next, nil
		}
		if event.ChainID != "" && op.ChainID != "" && event.ChainID != op.ChainID {
			return next, serviceerror.NewFailedPrecondition("conflicting workflow chain")
		}
		switch event.Kind {
		case Acknowledged, Complete:
			if event.ChainID == "" {
				return next, serviceerror.NewInvalidArgument("workflow chain is required")
			}
			if op.ChainID == "" {
				op.ChainID = event.ChainID
				next.Root.Started++
			}
			if event.Kind == Complete {
				op.Phase = Completed
			} else if op.Phase != Completed {
				op.Phase = Started
			}
		case RetryableFailure:
			if op.Phase == Pending {
				if event.Now >= op.Deadline {
					op.Phase = Ambiguous
				} else {
					// Clamp before addition to avoid overflowing a timestamp near its limit.
					op.RetryAt = min(event.Now, op.Deadline-RetryBackoff) + RetryBackoff
				}
			}
		case Expire:
			if op.Phase == Pending && event.Now >= op.Deadline {
				op.Phase = Ambiguous
			}
		}
	default:
		return next, serviceerror.NewInvalidArgument("unknown scheduler event")
	}
	return next, nil
}

// Issue exposes a detached immutable request only while issuance is permitted.
func Issue(state State, now int64) ([]byte, bool) {
	op := state.Operation
	if op == nil || op.Phase != Pending || now < op.RetryAt || now >= op.Deadline {
		return nil, false
	}
	return bytes.Clone(op.Request), true
}

// Wake returns the next obligation while an operation is internally pending.
func Wake(state State) (int64, bool) {
	op := state.Operation
	if op == nil || op.Phase != Pending {
		return 0, false
	}
	return min(op.RetryAt, op.Deadline), true
}

func clone(state State) State {
	if state.Operation != nil {
		op := *state.Operation
		op.Request = bytes.Clone(op.Request)
		state.Operation = &op
	}
	return state
}
