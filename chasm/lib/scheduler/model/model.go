package model

import (
	"fmt"
	"slices"
	"time"
)

func Transition(config Config, input State, event Event) (Outcome, error) {
	state := cloneState(input)
	outcome := Outcome{State: state}

	switch event := event.(type) {
	case Create:
		if state.Lifecycle != LifecycleAbsent {
			return Outcome{State: input}, ErrAlreadyCreated
		}
		state.Now = config.StartTime
		state.Lifecycle = LifecycleRunning
		state.Paused = event.Paused
		state.Notes = event.Notes
		state.ConflictToken = 1
		state.HighWatermark = config.StartTime
		outcome.State = state
		outcome.Response = MutationResponse{ConflictToken: state.ConflictToken}
		outcome.Effects = nextWakeup(config, state)
		return outcome, nil
	case Describe:
		if err := requireRunning(state); err != nil {
			return Outcome{State: input}, err
		}
		outcome.Response = DescribeResponse{Observable: Observe(state)}
		return outcome, nil
	case Pause:
		return setPaused(config, input, state, true, event.Note)
	case Unpause:
		return setPaused(config, input, state, false, event.Note)
	case Trigger:
		if err := requireRunning(state); err != nil {
			return Outcome{State: input}, err
		}
		if event.ID == "" || hasAction(state, event.ID) {
			return Outcome{State: input}, fmt.Errorf("%w: invalid or duplicate trigger ID", ErrInvalidEvent)
		}
		action := Action{ID: event.ID, NominalTime: state.Now, Manual: true}
		state.Pending = append(state.Pending, action)
		state.ConflictToken++
		outcome.State = state
		outcome.Response = MutationResponse{ConflictToken: state.ConflictToken}
		outcome.Effects = []Effect{StartWorkflow{Action: action}}
		return outcome, nil
	case AdvanceTime:
		return advance(config, input, state, event.Time)
	case StartSucceeded:
		return startSucceeded(input, state, event)
	case CompleteWorkflow:
		return completeWorkflow(config, input, state, event)
	case Delete:
		if err := requireRunning(state); err != nil {
			return Outcome{State: input}, err
		}
		state.Lifecycle = LifecycleClosed
		state.Pending = nil
		state.Running = nil
		state.ConflictToken++
		outcome.State = state
		outcome.Response = MutationResponse{ConflictToken: state.ConflictToken}
		outcome.Effects = []Effect{CloseSchedule{}}
		return outcome, nil
	default:
		return Outcome{State: input}, fmt.Errorf("%w: %T", ErrInvalidEvent, event)
	}
}

func setPaused(config Config, input, state State, paused bool, note string) (Outcome, error) {
	if err := requireRunning(state); err != nil {
		return Outcome{State: input}, err
	}
	state.Paused = paused
	state.Notes = note
	state.ConflictToken++
	outcome := Outcome{
		State:    state,
		Response: MutationResponse{ConflictToken: state.ConflictToken},
	}
	outcome.Effects = nextWakeup(config, state)
	return outcome, nil
}

func startSucceeded(input, state State, event StartSucceeded) (Outcome, error) {
	if err := requireRunning(state); err != nil {
		return Outcome{State: input}, err
	}
	if event.RunID == "" {
		return Outcome{State: input}, fmt.Errorf("%w: empty run ID", ErrInvalidEvent)
	}
	action, remaining, ok := takeAction(state.Pending, event.ActionID)
	if !ok {
		return Outcome{State: input}, fmt.Errorf("%w: action %q is not pending", ErrInvalidEvent, event.ActionID)
	}
	action.RunID = event.RunID
	state.Pending = remaining
	state.Running = append(state.Running, action)
	state.ActionCount++
	return Outcome{State: state}, nil
}

func completeWorkflow(config Config, input, state State, event CompleteWorkflow) (Outcome, error) {
	if err := requireRunning(state); err != nil {
		return Outcome{State: input}, err
	}
	action, remaining, ok := takeAction(state.Running, event.ActionID)
	if !ok {
		return Outcome{State: input}, fmt.Errorf("%w: action %q is not running", ErrInvalidEvent, event.ActionID)
	}
	state.Running = remaining
	state.Recent = append(state.Recent, action)
	if limit := config.RecentActionLimit; limit >= 0 && len(state.Recent) > limit {
		state.Recent = slices.Delete(state.Recent, 0, len(state.Recent)-limit)
	}
	return Outcome{State: state}, nil
}

func advance(config Config, input, state State, now time.Time) (Outcome, error) {
	if err := requireRunning(state); err != nil {
		return Outcome{State: input}, err
	}
	if now.Before(state.Now) {
		return Outcome{State: input}, fmt.Errorf("%w: time moved backwards", ErrInvalidEvent)
	}
	state.Now = now
	if state.Paused {
		state.HighWatermark = now
		return Outcome{State: state, Effects: nextWakeup(config, state)}, nil
	}

	maxGenerated := config.MaxGenerated
	if maxGenerated <= 0 {
		maxGenerated = 1000
	}
	effects := make([]Effect, 0)
	for next, ok := matchingTimeAfter(config, state.HighWatermark); ok && !next.After(now); next, ok = matchingTimeAfter(config, state.HighWatermark) {
		state.HighWatermark = next
		if config.CatchupWindow > 0 && next.Before(now.Add(-config.CatchupWindow)) {
			continue
		}
		if len(effects) >= maxGenerated {
			return Outcome{State: input}, fmt.Errorf("%w: generated action limit exceeded", ErrInvalidEvent)
		}
		action := Action{ID: scheduledActionID(next), NominalTime: next}
		state.Pending = append(state.Pending, action)
		effects = append(effects, StartWorkflow{Action: action})
	}
	effects = append(effects, nextWakeup(config, state)...)
	return Outcome{State: state, Effects: effects}, nil
}

func requireRunning(state State) error {
	switch state.Lifecycle {
	case LifecycleAbsent:
		return ErrNotCreated
	case LifecycleClosed:
		return ErrClosed
	default:
		return nil
	}
}

func cloneState(state State) State {
	state.Pending = slices.Clone(state.Pending)
	state.Running = slices.Clone(state.Running)
	state.Recent = slices.Clone(state.Recent)
	return state
}

func nextWakeup(config Config, state State) []Effect {
	next, ok := matchingTimeAfter(config, state.HighWatermark)
	if !ok {
		return nil
	}
	return []Effect{ScheduleWakeup{At: next}}
}

func matchingTimeAfter(config Config, after time.Time) (time.Time, bool) {
	if config.Interval <= 0 {
		return time.Time{}, false
	}
	anchor := config.StartTime.Truncate(config.Interval).Add(config.Phase % config.Interval)
	if !config.SpecStart.IsZero() && anchor.Before(config.SpecStart) {
		steps := config.SpecStart.Sub(anchor) / config.Interval
		anchor = anchor.Add(steps * config.Interval)
		if anchor.Before(config.SpecStart) {
			anchor = anchor.Add(config.Interval)
		}
	}
	if !anchor.After(after) {
		steps := after.Sub(anchor)/config.Interval + 1
		anchor = anchor.Add(steps * config.Interval)
	}
	if !config.SpecEnd.IsZero() && anchor.After(config.SpecEnd) {
		return time.Time{}, false
	}
	return anchor, true
}

func hasAction(state State, id string) bool {
	for _, actions := range [][]Action{state.Pending, state.Running, state.Recent} {
		for _, action := range actions {
			if action.ID == id {
				return true
			}
		}
	}
	return false
}

func takeAction(actions []Action, id string) (Action, []Action, bool) {
	for i, action := range actions {
		if action.ID == id {
			remaining := slices.Delete(slices.Clone(actions), i, i+1)
			if len(remaining) == 0 {
				remaining = nil
			}
			return action, remaining, true
		}
	}
	return Action{}, actions, false
}

func scheduledActionID(nominal time.Time) string {
	return fmt.Sprintf("scheduled:%d", nominal.UnixNano())
}
