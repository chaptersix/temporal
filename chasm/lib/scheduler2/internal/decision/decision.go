// Package decision implements the bounded scheduler2 admission and start protocol.
package decision

import (
	"bytes"
	"fmt"
	"math"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
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
	StartOnly
)

type Root struct {
	ActivitySet         bool
	ActivityAt          int64
	UnconfirmedStarts   int64
	ManualSequence      uint64
	Incarnation         string
	Revision            int64
	Cursor              int64
	Paused              bool
	Limited             bool
	Remaining           int64
	Started             int64
	PauseOnFailure      bool
	CarryoverSet        bool
	Policy              enumspb.ScheduleOverlapPolicy
	BufferDropped       int64
	OverlapSuperseded   int64
	CapacityDropped     int64
	Unfinished          int64
	BlockingOperationID string
}

type Operation struct {
	NominalTime     Timestamp
	StartObserved   bool
	StartObservedAt int64
	BackfillID      uint64
	ManualID        uint64
	OccurrenceID    string
	ActionID        string
	ID              string
	ChainID         string
	Nominal         int64
	Deadline        int64
	RetryAt         int64
	Phase           Phase
	Request         []byte
	WorkflowID      string
	Policy          enumspb.ScheduleOverlapPolicy
}

type State struct {
	IndexedQueue QueueSummary
	QueueChange  QueueChange
	Capacity     Capacity
	Root         Root
	// Operation is the event-addressed record; admission callers supply the latest record.
	Operation *Operation
	Carryover *Carryover
	Queue     []Queued
	Effects   []Effect
	Manuals   []ManualCommand
}

type Kind int

const (
	Due Kind = iota + 1
	Update
	Acknowledged
	RetryableFailure
	Complete
	Expire
	Callback
	Drain
	EffectSucceeded
	EffectNotFound
	EffectFailed
	EffectExpired
	AcceptManual
	DispatchManual
	DispatchBackfill
)

type Event struct {
	QueueDrain  *QueueDrainPlan
	NominalTime Timestamp
	ActualTime  Timestamp
	// PublicCloseTime is canonical workflow close time, independent of callback arrival.
	PublicCloseTime time.Time
	// PublicManualIngress selects the public pending-command profile.
	PublicManualIngress bool
	// PublicRunID is response metadata, independent of the chain fence and reducer state.
	PublicRunID string
	// BackfillBatchSkip is a persisted public batch overlap disposition.
	BackfillBatchSkip bool

	StartObserved   bool
	StartObservedAt int64
	BackfillID      uint64
	Kind            Kind
	Incarnation     string
	EffectID        string
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
	PauseOnFailure  bool
	Outcome         Outcome
	Result          []byte
	Failure         []byte
	Status          enumspb.WorkflowExecutionStatus
	Policy          enumspb.ScheduleOverlapPolicy
	WorkflowID      string
	Actual          int64
	CatchupWindow   int64
}

// Step returns an independent state value; callers publish it only after commit.
func Step(state State, event Event) (State, error) {
	state.QueueChange = QueueChange{}
	if err := ValidateQueueSummary(state); err != nil {
		return State{}, err
	}
	next := clone(state)
	if event.Incarnation != state.Root.Incarnation {
		return next, nil
	}
	var err error
	switch event.Kind {
	case AcceptManual:
		next, err = acceptManual(next, event)
	case DispatchBackfill:
		next, err = dispatchBackfill(next, event)
	case DispatchManual:
		next, err = dispatchManual(next, event)
	case Due:
		next, err = admit(next, event)
	case Drain:
		next, err = drain(next, event)
	case Update:
		next, err = update(next, event)
	case Acknowledged, Complete, Callback, RetryableFailure, Expire:
		next, err = applyResult(next, event)
	case EffectSucceeded, EffectNotFound, EffectFailed, EffectExpired:
		next = effectResult(next, event)
	default:
		return next, serviceerror.NewInvalidArgument("unknown scheduler event")
	}
	if err != nil {
		return next, err
	}
	next = reconcileActivity(state, next, event)
	next = reconcileEffects(next)
	return BindEffects(next, next.Operation, event.Now)
}

func admit(next State, event Event) (State, error) {
	if event.Nominal <= next.Root.Cursor || event.Nominal > event.Now {
		return next, nil
	}
	if event.Now > math.MaxInt64-RetryHorizon {
		return next, serviceerror.NewInvalidArgument("admission time exceeds supported range")
	}
	next.Root.Cursor = event.Nominal
	if next.Root.Paused || (next.Root.Limited && next.Root.Remaining <= 0) || event.Now > event.CatchupDeadline {
		return next, nil
	}
	if next.Capacity.Public && !QueueAvailable(next) {
		next.Root.BufferDropped++
		return next, nil
	}
	if next.Root.Policy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL && (occupied(next) || QueueLength(next) > 0) {
		return enqueue(next, event)
	}
	if !OperationAvailable(next) {
		next.Root.CapacityDropped++
		return next, nil
	}
	if len(event.Request) == 0 || next.Root.Incarnation == "" {
		return State{}, serviceerror.NewInvalidArgument("admission requires incarnation and request")
	}
	return startOperation(next, event, Queued{Nominal: event.Nominal, WorkflowID: event.WorkflowID, Policy: next.Root.Policy})
}

func startOperation(next State, event Event, candidate Queued) (State, error) {
	nominal, workflowID, policy, manualID := candidate.Nominal, candidate.WorkflowID, candidate.Policy, candidate.ManualID
	if event.Now > math.MaxInt64-RetryHorizon {
		return State{}, serviceerror.NewInvalidArgument("admission time exceeds supported range")
	}

	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	occurrenceID := fmt.Sprintf("automatic/%d", nominal)
	if manualID != 0 {
		occurrenceID = fmt.Sprintf("manual/%d", manualID)
	}
	if candidate.BackfillID != 0 {
		occurrenceID = fmt.Sprintf("backfill/%d/%d", candidate.BackfillID, nominal)
		if candidate.NominalTime.Present {
			occurrenceID = fmt.Sprintf("backfill/%d/%s", candidate.BackfillID, candidate.NominalTime.Key(nominal))
		}
	}
	actionID := next.Root.Incarnation + "/" + occurrenceID + "/action"
	next.Operation = &Operation{
		OccurrenceID: occurrenceID, ActionID: actionID, ID: actionID + "/start",
		BackfillID: candidate.BackfillID, ManualID: manualID, Nominal: nominal, NominalTime: candidate.NominalTime, WorkflowID: workflowID, Policy: policy, Deadline: event.Now + RetryHorizon,
		RetryAt: event.Now, Phase: Pending, Request: bytes.Clone(event.Request),
	}
	next.Root.Unfinished++
	if policy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
		next.Root.BlockingOperationID = next.Operation.ID
	}
	if next.Root.Limited && manualID == 0 && candidate.BackfillID == 0 {
		next.Root.Remaining--
	}
	return next, nil
}

func update(next State, event Event) (State, error) {
	if event.Revision <= next.Root.Revision {
		return next, nil
	}
	if !ValidPolicy(event.Policy) {
		return State{}, serviceerror.NewInvalidArgument("unsupported overlap policy")
	}
	if event.Remaining < 0 {
		return next, serviceerror.NewInvalidArgument("remaining actions must be nonnegative")
	}
	next.Root.Revision = event.Revision
	next.Root.Paused = event.Paused
	next.Root.Limited = event.Limited
	next.Root.Remaining = event.Remaining
	next.Root.PauseOnFailure = event.PauseOnFailure
	next.Root.Policy = event.Policy
	return next, nil
}

func applyResult(next State, event Event) (State, error) {
	op := next.Operation
	if op == nil || event.OperationID != op.ID {
		return next, nil
	}
	if Terminal(op.Phase) {
		return next, nil
	}
	if event.ChainID != "" && op.ChainID != "" && event.ChainID != op.ChainID {
		return next, serviceerror.NewFailedPrecondition("conflicting workflow chain")
	}
	switch event.Kind {
	case Acknowledged, Complete, Callback:
		if err := acceptResult(&next, event); err != nil {
			return State{}, err
		}
	case RetryableFailure:
		if op.Phase == Pending {
			if event.Now >= op.Deadline {
				op.Phase = Ambiguous
			} else {
				// Clamp before addition to avoid overflowing a timestamp near its limit.
				op.RetryAt = min(max(event.Now, op.RetryAt), op.Deadline-RetryBackoff) + RetryBackoff
			}
		}
	case Expire:
		if op.Phase == Pending && event.Now >= op.Deadline {
			op.Phase = Ambiguous
		}
	default:
		return next, serviceerror.NewInvalidArgument("unknown operation result")
	}
	return next, nil
}

// Terminal reports whether an intent is reclaimable, independently of workflow lifetime.
func Terminal(phase Phase) bool { return phase == Completed || phase == StartOnly }

func acceptResult(next *State, event Event) error {
	op := next.Operation
	if event.ChainID == "" {
		return serviceerror.NewInvalidArgument("workflow chain is required")
	}
	if event.Kind != Acknowledged {
		if err := complete(next, event); err != nil {
			return err
		}
	}
	if op.ChainID == "" {
		op.ChainID = event.ChainID
		next.Root.Started++
	}
	if op.Policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
		op.Phase = StartOnly
	} else if event.Kind == Complete || event.Kind == Callback {
		op.Phase = Completed
	} else {
		op.Phase = Started
	}
	if Terminal(op.Phase) {
		next.Root.Unfinished--
		if next.Root.BlockingOperationID == op.ID {
			next.Root.BlockingOperationID = ""
		}
	}
	return nil
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
	state.Queue = append([]Queued(nil), state.Queue...)
	state.Manuals = append([]ManualCommand(nil), state.Manuals...)
	state.Effects = append([]Effect(nil), state.Effects...)
	if state.Carryover != nil {
		c := *state.Carryover
		c.Result = bytes.Clone(c.Result)
		c.Failure = bytes.Clone(c.Failure)
		state.Carryover = &c
	}

	if state.Operation != nil {
		op := *state.Operation
		op.Request = bytes.Clone(op.Request)
		state.Operation = &op
	}
	return state
}
