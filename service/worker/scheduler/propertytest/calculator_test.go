package propertytest

import (
	"errors"
	"fmt"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
)

const iterationDefinitionVersion = "v1"

var (
	ErrInvalidSpec       = errors.New("invalid schedule spec")
	ErrInvalidQueryRange = errors.New("invalid query range")
	ErrInvalidOptions    = errors.New("invalid compute options")
	ErrIterationLimit    = errors.New("iteration limit exceeded")
)

type ComputeOptions struct {
	MaxResults    int
	MaxIterations int
}

type WorkBreakdown struct {
	Total                    int
	NextTimeCalls            int
	InclusionSourceChecks    int
	CalendarSearchSteps      int
	IntervalChecks           int
	ExclusionChecks          int
	ExcludedCandidateRetries int
	ResultLoopSteps          int
}

type ComputeResult struct {
	Times []time.Time
	Work  WorkBreakdown
}

type IterationLimitError struct {
	Limit    int
	Consumed int
	LastTime time.Time
	Work     WorkBreakdown
}

func (e *IterationLimitError) Error() string {
	return fmt.Sprintf(
		"%v: limit=%d consumed=%d last_time=%s definition=%s",
		ErrIterationLimit,
		e.Limit,
		e.Consumed,
		e.LastTime.Format(time.RFC3339Nano),
		iterationDefinitionVersion,
	)
}

func (e *IterationLimitError) Unwrap() error {
	return ErrIterationLimit
}

type workKind int

const (
	workNextTime workKind = iota
	workInclusionSource
	workCalendarSearch
	workInterval
	workExclusion
	workExcludedRetry
	workResultLoop
)

type iterationBudget struct {
	limit    int
	lastTime time.Time
	work     WorkBreakdown
}

func newIterationBudget(limit int) *iterationBudget {
	return &iterationBudget{limit: limit}
}

func (b *iterationBudget) at(t time.Time) {
	b.lastTime = t
}

func (b *iterationBudget) tick(kind workKind) error {
	if b.work.Total >= b.limit {
		return &IterationLimitError{
			Limit:    b.limit,
			Consumed: b.work.Total,
			LastTime: b.lastTime,
			Work:     b.work,
		}
	}
	b.work.Total++
	switch kind {
	case workNextTime:
		b.work.NextTimeCalls++
	case workInclusionSource:
		b.work.InclusionSourceChecks++
	case workCalendarSearch:
		b.work.CalendarSearchSteps++
	case workInterval:
		b.work.IntervalChecks++
	case workExclusion:
		b.work.ExclusionChecks++
	case workExcludedRetry:
		b.work.ExcludedCandidateRetries++
	case workResultLoop:
		b.work.ResultLoopSteps++
	default:
		panic("unknown work kind")
	}
	return nil
}

func (b *iterationBudget) snapshot() WorkBreakdown {
	return b.work
}

func ComputeMatchingTimes(
	spec *schedulepb.ScheduleSpec,
	start time.Time,
	end time.Time,
	jitterSeed string,
	options ComputeOptions,
) (ComputeResult, error) {
	if options.MaxResults <= 0 || options.MaxIterations <= 0 {
		return ComputeResult{}, fmt.Errorf(
			"%w: max results and max iterations must be positive",
			ErrInvalidOptions,
		)
	}
	if end.Before(start) {
		return ComputeResult{}, fmt.Errorf("%w: end is before start", ErrInvalidQueryRange)
	}

	compiled, err := NewSpecBuilder().NewCompiledSpec(spec)
	if err != nil {
		return ComputeResult{}, fmt.Errorf("%w: %v", ErrInvalidSpec, err)
	}

	budget := newIterationBudget(options.MaxIterations)
	result := ComputeResult{Times: make([]time.Time, 0, options.MaxResults)}
	after := start
	for range options.MaxResults {
		budget.at(after)
		if err := budget.tick(workResultLoop); err != nil {
			result.Work = budget.snapshot()
			return result, err
		}
		next, err := compiled.getNextTime(jitterSeed, after, budget)
		if err != nil {
			result.Work = budget.snapshot()
			return result, err
		}
		after = next.Next
		if after.IsZero() || after.After(end) {
			break
		}
		result.Times = append(result.Times, after)
	}
	result.Work = budget.snapshot()
	return result, nil
}
