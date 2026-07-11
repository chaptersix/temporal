package propertytest

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"pgregory.net/rapid"
)

var analysisBudgetLadder = []int{10, 100, 1_000, 10_000, 100_000, 1_000_000}
var analysisResultLimits = []int{1, 10, 100, 1_000}

type budgetMatrixResult struct {
	largestFailingBudget  int
	smallestSuccessBudget int
	requiredIterations    int
	succeeded             bool
}

func TestQueryRangeProfilesStayWithinBounds(t *testing.T) {
	profiles := []queryRangeProfile{realisticQueryRangeProfile, shortQueryRangeProfile, longQueryRangeProfile}
	for _, profile := range profiles {
		t.Run(profile.name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				tc := scheduleCaseGeneratorForProfile(profile, false).Draw(t, "case")
				if tc.start.Before(profile.earliestStart) || tc.end.Before(profile.earliestEnd) || tc.end.After(profile.latestEnd) {
					t.Fatalf("generated range outside profile: start=%s end=%s profile=%+v", tc.start, tc.end, profile)
				}
				window := tc.end.Sub(tc.start)
				if window < profile.minWindow || window > profile.maxWindow {
					t.Fatalf("generated window %s outside [%s,%s] for profile=%+v", window, profile.minWindow, profile.maxWindow, profile)
				}
			})
		})
	}
}

func TestExperimentBudgetMatrix(t *testing.T) {
	var total, blockedAtTenThousand, noSuccess int
	var transitionExamples, noSuccessExamples []string
	rapid.Check(t, func(t *rapid.T) {
		total++
		tc := scheduleCaseGeneratorForProfile(realisticQueryRangeProfile, true).Draw(t, "case")
		resultLimit := rapid.SampledFrom(analysisResultLimits).Draw(t, "result-limit")
		matrix, err := runBudgetMatrix(tc, resultLimit, analysisBudgetLadder)
		if err != nil {
			t.Fatalf("budget matrix failed: %v for %s", err, describeCase(tc))
		}
		if !matrix.succeeded {
			noSuccess++
			if len(noSuccessExamples) < 3 {
				noSuccessExamples = append(noSuccessExamples, describeCase(tc))
			}
		}
		if matrix.largestFailingBudget >= 10_000 && matrix.smallestSuccessBudget > 10_000 {
			blockedAtTenThousand++
			if len(transitionExamples) < 5 {
				transitionExamples = append(transitionExamples, fmt.Sprintf(
					"fail=%d success=%d required=%d %s",
					matrix.largestFailingBudget,
					matrix.smallestSuccessBudget,
					matrix.requiredIterations,
					describeCase(tc),
				))
			}
		}
	})
	t.Logf("budget matrix cases=%d blocked_at_10000=%d no_success_at_max=%d", total, blockedAtTenThousand, noSuccess)
	for _, example := range transitionExamples {
		t.Logf("10,000 transition: %s", example)
	}
	for _, example := range noSuccessExamples {
		t.Logf("no success at 1,000,000: %s", example)
	}
}

func TestAllExcludedDenseIntervalValidationClassification(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	spec := &schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
		ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
			Second:     []*schedulepb.Range{{Start: 0, End: 59, Step: 1}},
			Minute:     []*schedulepb.Range{{Start: 0, End: 59, Step: 1}},
			Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
			DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
			Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
			DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
		}},
	}
	lowBudget, err := ComputeMatchingTimes(spec, start, start.Add(time.Minute), "", ComputeOptions{
		MaxResults:              1,
		MaxIterations:           10_000,
		MaxValidationIterations: 100,
	})
	if !errors.Is(err, ErrValidationLimit) || lowBudget.Validation.Status != ValidationIndeterminateBudget {
		t.Fatalf("expected low-budget validation to remain indeterminate, result=%+v error=%v", lowBudget, err)
	}
	if lowBudget.Work.Total != 0 {
		t.Fatalf("matching computation ran after indeterminate validation, work=%+v", lowBudget.Work)
	}

	provedEmpty, err := ComputeMatchingTimes(spec, start, start.Add(time.Minute), "", ComputeOptions{
		MaxResults:              1,
		MaxIterations:           10_000,
		MaxValidationIterations: 500_000,
	})
	if !errors.Is(err, ErrUnsatisfiableSpec) || provedEmpty.Validation.Status != ValidationInvalidEffectiveSetEmpty {
		t.Fatalf("expected effective-set unsatisfiability with sufficient proof budget, result=%+v error=%v", provedEmpty, err)
	}
	if provedEmpty.Work.Total != 0 {
		t.Fatalf("matching computation ran after invalid validation, work=%+v", provedEmpty.Work)
	}
}

func TestExperimentLongHorizonBudgetMatrix(t *testing.T) {
	if os.Getenv("SCHEDULE_PROPERTY_LONG_EXPERIMENT") == "" {
		t.Skip("set SCHEDULE_PROPERTY_LONG_EXPERIMENT=1 to run long-horizon budget analysis")
	}
	rapid.Check(t, func(t *rapid.T) {
		tc := scheduleCaseGeneratorForProfile(longQueryRangeProfile, true).Draw(t, "case")
		matrix, err := runBudgetMatrix(tc, 100, analysisBudgetLadder)
		if err != nil {
			t.Fatalf("long-horizon budget matrix failed: %v for %s", err, describeCase(tc))
		}
		if !matrix.succeeded || matrix.smallestSuccessBudget > 10_000 {
			t.Logf("high-work long-horizon case: %+v %s", matrix, describeCase(tc))
		}
	})
}

func runBudgetMatrix(tc scheduleCase, maxResults int, budgets []int) (budgetMatrixResult, error) {
	var matrix budgetMatrixResult
	var successful ComputeResult
	for _, budget := range budgets {
		result, err := ComputeMatchingTimes(tc.spec, tc.start, tc.end, tc.seed, ComputeOptions{
			MaxResults:    maxResults,
			MaxIterations: budget,
		})
		if errors.Is(err, ErrIterationLimit) {
			if matrix.succeeded {
				return matrix, errors.New("larger budget failed after smaller budget succeeded")
			}
			matrix.largestFailingBudget = budget
			continue
		}
		if err != nil {
			return matrix, err
		}
		if !matrix.succeeded {
			matrix.succeeded = true
			matrix.smallestSuccessBudget = budget
			matrix.requiredIterations = result.Work.Total
			successful = result
			continue
		}
		if !slices.Equal(successful.Times, result.Times) || successful.Work != result.Work {
			return matrix, errors.New("successful result changed under a larger budget")
		}
	}
	return matrix, nil
}

func TestBudgetLadderIsStrictlyIncreasing(t *testing.T) {
	for i := 1; i < len(analysisBudgetLadder); i++ {
		if analysisBudgetLadder[i] <= analysisBudgetLadder[i-1] {
			t.Fatalf("budget ladder is not strictly increasing: %v", analysisBudgetLadder)
		}
	}
	for _, limit := range analysisResultLimits {
		if limit <= 0 {
			t.Fatalf("invalid result limit %d", limit)
		}
	}
}

func TestDenseIntervalAndSparseCalendarBudgetTransition(t *testing.T) {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	spec := &schedulepb.ScheduleSpec{
		StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
			Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
			Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
			Hour:       []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
			DayOfMonth: []*schedulepb.Range{{Start: 1, End: 1, Step: 1}},
			Month:      []*schedulepb.Range{{Start: 1, End: 1, Step: 1}},
			DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 3, Step: 1}},
		}},
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
	}
	tc := scheduleCase{spec: spec, start: start, end: start.Add(time.Hour)}

	atTenThousand, err := ComputeMatchingTimes(spec, tc.start, tc.end, "", ComputeOptions{
		MaxResults:    1_000,
		MaxIterations: 10_000,
	})
	if !errors.Is(err, ErrIterationLimit) || atTenThousand.Work.Total != 10_000 {
		t.Fatalf("expected 10,000 budget exhaustion, result=%+v error=%v", atTenThousand, err)
	}

	atOneMillion, err := ComputeMatchingTimes(spec, tc.start, tc.end, "", ComputeOptions{
		MaxResults:    1_000,
		MaxIterations: 1_000_000,
	})
	if err != nil {
		t.Fatalf("expected case to succeed at 1,000,000: %v", err)
	}
	if len(atOneMillion.Times) != 1_000 {
		t.Fatalf("expected 1,000 results, got %d", len(atOneMillion.Times))
	}
	t.Logf("dense interval plus sparse calendar required %d work units for 1,000 results", atOneMillion.Work.Total)
}
