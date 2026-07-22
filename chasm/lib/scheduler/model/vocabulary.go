package model

import (
	"errors"
	"time"
)

var (
	ErrAlreadyCreated = errors.New("schedule already created")
	ErrNotCreated     = errors.New("schedule not created")
	ErrClosed         = errors.New("schedule closed")
	ErrInvalidEvent   = errors.New("invalid scheduler event")
)

type Config struct {
	StartTime         time.Time
	Interval          time.Duration
	Phase             time.Duration
	SpecStart         time.Time
	SpecEnd           time.Time
	CatchupWindow     time.Duration
	RecentActionLimit int
	MaxGenerated      int
}

type Lifecycle int

const (
	LifecycleAbsent Lifecycle = iota
	LifecycleRunning
	LifecycleClosed
)

type Action struct {
	ID          string
	NominalTime time.Time
	Manual      bool
	RunID       string
}

type State struct {
	Now           time.Time
	Lifecycle     Lifecycle
	Paused        bool
	Notes         string
	ConflictToken int64
	HighWatermark time.Time
	Pending       []Action
	Running       []Action
	Recent        []Action
	ActionCount   int64
}

type Event interface {
	isEvent()
}

type Create struct {
	Paused bool
	Notes  string
}

type Describe struct{}

type Pause struct {
	Note string
}

type Unpause struct {
	Note string
}

type Trigger struct {
	ID string
}

type AdvanceTime struct {
	Time time.Time
}

type StartSucceeded struct {
	ActionID string
	RunID    string
}

type CompleteWorkflow struct {
	ActionID string
}

type Delete struct{}

func (Create) isEvent()           {}
func (Describe) isEvent()         {}
func (Pause) isEvent()            {}
func (Unpause) isEvent()          {}
func (Trigger) isEvent()          {}
func (AdvanceTime) isEvent()      {}
func (StartSucceeded) isEvent()   {}
func (CompleteWorkflow) isEvent() {}
func (Delete) isEvent()           {}

type Effect interface {
	isEffect()
}

type StartWorkflow struct {
	Action Action
}

type ScheduleWakeup struct {
	At time.Time
}

type CloseSchedule struct{}

func (StartWorkflow) isEffect()  {}
func (ScheduleWakeup) isEffect() {}
func (CloseSchedule) isEffect()  {}

type Response interface {
	isResponse()
}

type MutationResponse struct {
	ConflictToken int64
}

type DescribeResponse struct {
	Observable Observable
}

func (MutationResponse) isResponse() {}
func (DescribeResponse) isResponse() {}

type Outcome struct {
	State    State
	Effects  []Effect
	Response Response
}

type Observable struct {
	Now           time.Time
	Lifecycle     Lifecycle
	Paused        bool
	Notes         string
	ConflictToken int64
	Pending       int
	Running       int
	Recent        int
	ActionCount   int64
}

func Observe(state State) Observable {
	return Observable{
		Now:           state.Now,
		Lifecycle:     state.Lifecycle,
		Paused:        state.Paused,
		Notes:         state.Notes,
		ConflictToken: state.ConflictToken,
		Pending:       len(state.Pending),
		Running:       len(state.Running),
		Recent:        len(state.Recent),
		ActionCount:   state.ActionCount,
	}
}
