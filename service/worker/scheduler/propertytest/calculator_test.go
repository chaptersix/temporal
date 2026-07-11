package propertytest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
)

const iterationDefinitionVersion = "v1"

const operationalCheckDefinitionVersion = "context-check-v1"

const maxAnalysisResults = 10_000

var backgroundContext = context.Background()

var (
	ErrInvalidSpec       = errors.New("invalid schedule spec")
	ErrUnsatisfiableSpec = errors.New("unsatisfiable schedule spec")
	ErrInvalidQueryRange = errors.New("invalid query range")
	ErrInvalidOptions    = errors.New("invalid compute options")
	ErrValidationLimit   = errors.New("validation limit exceeded")
	ErrIterationLimit    = errors.New("iteration limit exceeded")
)

type ComputeOptions struct {
	MaxResults              int
	MaxIterations           int
	MaxValidationIterations int
	WorkTickHook            WorkTickHook
	faults                  analysisFaults
}

type WorkPhase string

const (
	WorkPhaseValidation WorkPhase = "validation"
	WorkPhaseMatching   WorkPhase = "matching"
)

type WorkTick struct {
	Phase     WorkPhase
	Kind      string
	Total     int
	LastTime  time.Time
	Component string
}

type WorkTickHook func(WorkTick)

type WorkBreakdown struct {
	Total                    int
	NextTimeCalls            int
	InclusionSourceChecks    int
	CalendarSearchSteps      int
	IntervalChecks           int
	ExclusionChecks          int
	ExcludedCandidateRetries int
	ResultLoopSteps          int
	CancellationChecks       int
}

type ComputeResult struct {
	Times      []time.Time
	Work       WorkBreakdown
	Validation ValidationResult
	Complete   bool
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
	context  context.Context
	hook     WorkTickHook
}

func newIterationBudget(limit int, contexts ...context.Context) *iterationBudget {
	ctx := backgroundContext
	if len(contexts) > 0 && contexts[0] != nil {
		ctx = contexts[0]
	}
	return &iterationBudget{limit: limit, context: ctx}
}

func newIterationBudgetWithHook(limit int, ctx context.Context, hook WorkTickHook) *iterationBudget {
	budget := newIterationBudget(limit, ctx)
	budget.hook = hook
	return budget
}

func (b *iterationBudget) at(t time.Time) {
	b.lastTime = t
}

func (b *iterationBudget) tick(kind workKind) error {
	if b.hook != nil {
		b.hook(WorkTick{
			Phase: WorkPhaseMatching, Kind: kind.String(), Total: b.work.Total, LastTime: b.lastTime,
		})
	}
	b.work.CancellationChecks++
	if b.context == nil {
		b.context = backgroundContext
	}
	select {
	case <-b.context.Done():
		return b.context.Err()
	default:
	}
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

func (k workKind) String() string {
	switch k {
	case workNextTime:
		return "next-time"
	case workInclusionSource:
		return "inclusion-source"
	case workCalendarSearch:
		return "calendar-search"
	case workInterval:
		return "interval"
	case workExclusion:
		return "exclusion"
	case workExcludedRetry:
		return "excluded-retry"
	case workResultLoop:
		return "result-loop"
	default:
		return "unknown"
	}
}

func (b *iterationBudget) snapshot() WorkBreakdown {
	return b.work
}

func ComputeMatchingTimes(
	ctx context.Context,
	spec *schedulepb.ScheduleSpec,
	start time.Time,
	end time.Time,
	jitterSeed string,
	options ComputeOptions,
) (ComputeResult, error) {
	if options.MaxResults <= 0 || options.MaxResults > maxAnalysisResults || options.MaxIterations <= 0 || options.MaxValidationIterations < 0 {
		return ComputeResult{}, fmt.Errorf(
			"%w: result limit must be in [1,%d], matching limit must be positive, and validation limit must not be negative",
			ErrInvalidOptions,
			maxAnalysisResults,
		)
	}
	if end.Before(start) {
		return ComputeResult{}, fmt.Errorf("%w: end is before start", ErrInvalidQueryRange)
	}
	if ctx == nil {
		return ComputeResult{}, fmt.Errorf("%w: context must not be nil", ErrInvalidOptions)
	}

	validationLimit := options.MaxValidationIterations
	if validationLimit == 0 {
		validationLimit = defaultValidationIterations
	}
	validation, err := ValidateSchedule(ctx, spec, ValidationOptions{
		MaxIterations: validationLimit,
		WorkTickHook:  options.WorkTickHook,
		faults:        options.faults,
	})
	if err != nil {
		if options.faults.validationIndeterminateIsInvalid && errors.Is(err, ErrValidationLimit) {
			err = newValidationClassificationError(ErrInvalidSpec, validation.Reason)
		}
		return ComputeResult{Validation: validation}, err
	}

	compiled, err := NewSpecBuilder().NewCompiledSpec(spec)
	if err != nil {
		return ComputeResult{Validation: validation}, fmt.Errorf("%w: %v", ErrInvalidSpec, err)
	}
	compiled.faults = options.faults

	budget := newIterationBudgetWithHook(options.MaxIterations, ctx, options.WorkTickHook)
	result := ComputeResult{
		Times:      make([]time.Time, 0, options.MaxResults),
		Validation: validation,
	}
	after := start
	for range options.MaxResults {
		budget.at(after)
		if err := budget.tick(workResultLoop); err != nil {
			result.Work = budget.snapshot()
			return matchingBudgetOutcome(result, err, options.faults)
		}
		next, err := compiled.getNextTime(jitterSeed, after, budget)
		if err != nil {
			result.Work = budget.snapshot()
			return matchingBudgetOutcome(result, err, options.faults)
		}
		after = next.Next
		if after.IsZero() || after.After(end) {
			break
		}
		result.Times = append(result.Times, after)
		if options.faults.allowDuplicateUnionResults && len(compiled.calendar)+len(compiled.spec.Interval) > 1 {
			result.Times = append(result.Times, after)
		}
	}
	result.Work = budget.snapshot()
	result.Complete = true
	return result, nil
}

func matchingBudgetOutcome(result ComputeResult, err error, faults analysisFaults) (ComputeResult, error) {
	if faults.iterationLimitIsEmpty && errors.Is(err, ErrIterationLimit) {
		result.Times = nil
		return result, nil
	}
	result.Times = slices.Clip(result.Times)
	return result, err
}
