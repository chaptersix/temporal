package propertytest

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	productionscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

const propertyAnalysisMaxIterations = 1_000_000

func TestPropertySafetyOrderingAndDeterminism(t *testing.T) {
	t.Parallel()
	rapid.Check(t, checkSafetyOrderingAndDeterminism)
}

func checkSafetyOrderingAndDeterminism(t *rapid.T) {
	tc := scheduleCaseGenerator(365*24*time.Hour, true).Draw(t, "case")
	options := ComputeOptions{MaxResults: 100, MaxIterations: propertyAnalysisMaxIterations}

	first, firstErr := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, tc.end, tc.seed, options)
	second, secondErr := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, tc.end, tc.seed, options)

	if fmt.Sprint(firstErr) != fmt.Sprint(secondErr) || !slices.Equal(first.Times, second.Times) || first.Work != second.Work {
		t.Fatalf("nondeterministic result for %s\nfirst=%+v err=%v\nsecond=%+v err=%v", describeCase(tc), first, firstErr, second, secondErr)
	}
	if !workConsistent(first.Work) {
		t.Fatalf("inconsistent work breakdown: %+v for %s", first.Work, describeCase(tc))
	}
	if firstErr != nil {
		if !errors.Is(firstErr, ErrIterationLimit) {
			t.Fatalf("unexpected error: %v for %s", firstErr, describeCase(tc))
		}
		if first.Work.Total != options.MaxIterations {
			t.Fatalf("limit error consumed %d, want %d for %s", first.Work.Total, options.MaxIterations, describeCase(tc))
		}
		return
	}
	if len(first.Times) > options.MaxResults {
		t.Fatalf("returned %d results with max %d for %s", len(first.Times), options.MaxResults, describeCase(tc))
	}
	for i, current := range first.Times {
		if !current.After(tc.start) || current.After(tc.end) {
			t.Fatalf("result %s outside (%s, %s] for %s", current, tc.start, tc.end, describeCase(tc))
		}
		if i > 0 && !current.After(first.Times[i-1]) {
			t.Fatalf("results not strictly increasing: %v for %s", first.Times, describeCase(tc))
		}
	}
}

func TestPropertyBudgetBoundary(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		tc := scheduleCaseGenerator(30*24*time.Hour, false).Draw(t, "case")
		baseline, err := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, tc.end, tc.seed, ComputeOptions{
			MaxResults:    10,
			MaxIterations: propertyAnalysisMaxIterations,
		})

		if errors.Is(err, ErrIterationLimit) {
			return
		}
		if err != nil {
			t.Fatalf("baseline failed: %v for %s", err, describeCase(tc))
		}
		if baseline.Work.Total <= 0 {
			t.Fatalf("successful computation reported no work for %s", describeCase(tc))
		}

		exact, err := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, tc.end, tc.seed, ComputeOptions{
			MaxResults:    10,
			MaxIterations: baseline.Work.Total,
		})

		if err != nil || !slices.Equal(exact.Times, baseline.Times) || exact.Work != baseline.Work {
			t.Fatalf("exact budget changed result: baseline=%+v exact=%+v err=%v for %s", baseline, exact, err, describeCase(tc))
		}

		short, err := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, tc.end, tc.seed, ComputeOptions{
			MaxResults:    10,
			MaxIterations: baseline.Work.Total - 1,
		})

		if !errors.Is(err, ErrIterationLimit) || short.Work.Total != baseline.Work.Total-1 {
			t.Fatalf("budget N-1 did not fail exactly: required=%d short=%+v err=%v for %s", baseline.Work.Total, short, err, describeCase(tc))
		}
	})
}

func TestPropertyInvalidSpecsReturnErrors(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		invalid := invalidStructuralScheduleCaseGenerator().Draw(t, "case")
		_, err := ComputeMatchingTimes(backgroundContext,
			invalid.spec,
			time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
			"",
			ComputeOptions{MaxResults: 10, MaxIterations: 10_000})

		if !errors.Is(err, ErrInvalidSpec) {
			t.Fatalf("invalid mutation %q returned %v, want ErrInvalidSpec; spec=%s", invalid.label, err, formatSpec(invalid.spec))
		}
	})
}

func TestPropertyProductionParity(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		tc := scheduleCaseGenerator(30*24*time.Hour, true).Draw(t, "case")
		copied, err := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, tc.end, tc.seed, ComputeOptions{
			MaxResults:    20,
			MaxIterations: propertyAnalysisMaxIterations,
		})

		if errors.Is(err, ErrIterationLimit) {
			return
		}
		if err != nil {
			t.Fatalf("copied calculator failed valid generated spec: %v for %s", err, describeCase(tc))
		}
		production, err := productionscheduler.NewSpecBuilder().NewCompiledSpec(tc.spec)
		if err != nil {
			t.Fatalf("production compiler rejected generated spec: %v for %s", err, describeCase(tc))
		}
		expected := productionMatchingTimes(production, tc.start, tc.end, tc.seed, 20)
		if !slices.Equal(copied.Times, expected) {
			t.Fatalf("production parity mismatch: copied=%v production=%v for %s", copied.Times, expected, describeCase(tc))
		}
	})
}

func TestPropertySmallWindowOracle(t *testing.T) {
	t.Parallel()
	rapid.Check(t, checkSmallWindowOracle)
}

func checkSmallWindowOracle(t *rapid.T) {
	tc := scheduleCaseGenerator(time.Minute, false).Draw(t, "case")
	const maxResults = 1_000
	actual, err := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, tc.end, "", ComputeOptions{
		MaxResults:    maxResults,
		MaxIterations: propertyAnalysisMaxIterations,
	})

	if err != nil {
		t.Fatalf("calculator failed: %v for %s", err, describeCase(tc))
	}
	expected := bruteForceMatchingTimes(tc.model, tc.start, tc.end, maxResults)
	if !slices.Equal(actual.Times, expected) {
		t.Fatalf("oracle mismatch for %s\nactual=%v\nexpected=%v\nwork=%+v", describeCase(tc), actual.Times, expected, actual.Work)
	}
}

func TestPropertyRepresentationEquivalence(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		tc := simpleCalendarCaseGenerator().Draw(t, "case")
		calendar := tc.model.calendars[0]
		structured := tc.model.renderStructured()
		calendarSpec := tc.model.renderStructured()
		calendarSpec.StructuredCalendar = nil
		calendarSpec.Calendar = []*schedulepb.CalendarSpec{calendar.renderCalendar()}
		cronSpec := tc.model.renderStructured()
		cronSpec.StructuredCalendar = nil
		cronSpec.CronString = []string{calendar.renderCron()}
		options := ComputeOptions{MaxResults: 10, MaxIterations: propertyAnalysisMaxIterations}

		structuredResult, structuredErr := ComputeMatchingTimes(backgroundContext, structured, tc.start, tc.end, "", options)
		calendarResult, calendarErr := ComputeMatchingTimes(backgroundContext, calendarSpec, tc.start, tc.end, "", options)
		cronResult, cronErr := ComputeMatchingTimes(backgroundContext, cronSpec, tc.start, tc.end, "", options)
		if structuredErr != nil || calendarErr != nil || cronErr != nil {
			t.Fatalf("equivalent representations returned errors: structured=%v calendar=%v cron=%v model=%+v", structuredErr, calendarErr, cronErr, tc.model)
		}
		if !slices.Equal(structuredResult.Times, calendarResult.Times) || !slices.Equal(structuredResult.Times, cronResult.Times) {
			t.Fatalf("representation mismatch: structured=%v calendar=%v cron=%v model=%+v", structuredResult.Times, calendarResult.Times, cronResult.Times, tc.model)
		}
		if structuredResult.Validation.Status != ValidationValid ||
			calendarResult.Validation.Status != ValidationValid ||
			cronResult.Validation.Status != ValidationValid ||
			structuredResult.Validation.Witness != calendarResult.Validation.Witness ||
			structuredResult.Validation.Witness != cronResult.Validation.Witness {
			t.Fatalf("representation validity mismatch: structured=%+v calendar=%+v cron=%+v model=%+v", structuredResult.Validation, calendarResult.Validation, cronResult.Validation, tc.model)
		}
	})
}

func TestPropertyResultPrefixAndSuffix(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		tc := scheduleCaseGenerator(24*time.Hour, false).Draw(t, "case")
		short, shortErr := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, tc.end, "", ComputeOptions{MaxResults: 5, MaxIterations: propertyAnalysisMaxIterations})
		long, longErr := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, tc.end, "", ComputeOptions{MaxResults: 20, MaxIterations: propertyAnalysisMaxIterations})
		if shortErr != nil || longErr != nil {
			t.Fatalf("prefix computation failed: short=%v long=%v for %s", shortErr, longErr, describeCase(tc))
		}
		if len(short.Times) > len(long.Times) || !slices.Equal(short.Times, long.Times[:len(short.Times)]) {
			t.Fatalf("result limit did not preserve prefix: short=%v long=%v for %s", short.Times, long.Times, describeCase(tc))
		}
		if len(long.Times) == 0 {
			return
		}
		suffix, err := ComputeMatchingTimes(backgroundContext, tc.spec, long.Times[0], tc.end, "", ComputeOptions{MaxResults: 20, MaxIterations: propertyAnalysisMaxIterations})
		if err != nil {
			t.Fatalf("suffix computation failed: %v for %s", err, describeCase(tc))
		}
		comparable := min(len(suffix.Times), len(long.Times)-1)
		if !slices.Equal(suffix.Times[:comparable], long.Times[1:1+comparable]) {
			t.Fatalf("suffix mismatch: full=%v suffix=%v for %s", long.Times, suffix.Times, describeCase(tc))
		}
	})
}

func TestPropertyRangeDecomposition(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		tc := scheduleCaseGenerator(time.Minute, false).Draw(t, "case")
		splitSeconds := rapid.Int64Range(0, int64(tc.end.Sub(tc.start)/time.Second)).Draw(t, "split-seconds")
		split := tc.start.Add(time.Duration(splitSeconds) * time.Second)
		options := ComputeOptions{MaxResults: 5_000, MaxIterations: propertyAnalysisMaxIterations}

		full, fullErr := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, tc.end, "", options)
		left, leftErr := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, split, "", options)
		right, rightErr := ComputeMatchingTimes(backgroundContext, tc.spec, split, tc.end, "", options)
		if fullErr != nil || leftErr != nil || rightErr != nil {
			t.Fatalf("range decomposition failed: full=%v left=%v right=%v for %s", fullErr, leftErr, rightErr, describeCase(tc))
		}
		combined := append(slices.Clone(left.Times), right.Times...)
		if !slices.Equal(full.Times, combined) {
			t.Fatalf("range decomposition mismatch: split=%s full=%v left=%v right=%v for %s", split, full.Times, left.Times, right.Times, describeCase(tc))
		}
	})
}

func TestPropertyJitterBounds(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		tc := scheduleCaseGenerator(24*time.Hour, true).Draw(t, "case")
		withoutJitter := proto.Clone(tc.spec).(*schedulepb.ScheduleSpec)
		withoutJitter.Jitter = nil
		options := ComputeOptions{MaxResults: 10, MaxIterations: propertyAnalysisMaxIterations}

		nominal, nominalErr := ComputeMatchingTimes(backgroundContext, withoutJitter, tc.start, tc.end, tc.seed, options)
		jittered, jitterErr := ComputeMatchingTimes(backgroundContext, tc.spec, tc.start, tc.end, tc.seed, options)
		if nominalErr != nil || jitterErr != nil {
			t.Fatalf("jitter comparison failed: nominal=%v jittered=%v for %s", nominalErr, jitterErr, describeCase(tc))
		}
		if len(jittered.Times) > len(nominal.Times) {
			t.Fatalf("jitter added results: nominal=%v jittered=%v for %s", nominal.Times, jittered.Times, describeCase(tc))
		}
		for i, actual := range jittered.Times {
			base := nominal.Times[i]
			if actual.Before(base) || actual.Sub(base) > tc.model.jitter {
				t.Fatalf("jitter outside bounds: nominal=%s actual=%s max=%s for %s", base, actual, tc.model.jitter, describeCase(tc))
			}
			if i+1 < len(nominal.Times) && actual.After(nominal.Times[i+1]) {
				t.Fatalf("jitter crossed following nominal time: nominal=%s actual=%s following=%s for %s", base, actual, nominal.Times[i+1], describeCase(tc))
			}
		}
	})
}

func TestPropertyIntervalUnion(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		firstInterval := rapid.Int64Range(1, 3_600).Draw(t, "first-interval")
		firstPhase := rapid.Int64Range(0, firstInterval-1).Draw(t, "first-phase")
		secondInterval := rapid.Int64Range(1, 3_600).Draw(t, "second-interval")
		secondPhase := rapid.Int64Range(0, secondInterval-1).Draw(t, "second-phase")
		start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		end := start.Add(time.Hour)
		first := scheduleModel{intervals: []intervalModel{{intervalSeconds: firstInterval, phaseSeconds: firstPhase}}}
		second := scheduleModel{intervals: []intervalModel{{intervalSeconds: secondInterval, phaseSeconds: secondPhase}}}
		combined := scheduleModel{intervals: []intervalModel{
			{intervalSeconds: firstInterval, phaseSeconds: firstPhase},
			{intervalSeconds: secondInterval, phaseSeconds: secondPhase},
		}}
		options := ComputeOptions{MaxResults: 10_000, MaxIterations: propertyAnalysisMaxIterations}

		firstResult, firstErr := ComputeMatchingTimes(backgroundContext, first.renderStructured(), start, end, "", options)
		secondResult, secondErr := ComputeMatchingTimes(backgroundContext, second.renderStructured(), start, end, "", options)
		combinedResult, combinedErr := ComputeMatchingTimes(backgroundContext, combined.renderStructured(), start, end, "", options)
		if firstErr != nil || secondErr != nil || combinedErr != nil {
			t.Fatalf("interval union failed: first=%v second=%v combined=%v", firstErr, secondErr, combinedErr)
		}
		expected := sortedUniqueTimes(firstResult.Times, secondResult.Times)
		if !slices.Equal(combinedResult.Times, expected) {
			t.Fatalf("interval union mismatch: first=%v second=%v combined=%v expected=%v", firstResult.Times, secondResult.Times, combinedResult.Times, expected)
		}
	})
}

func TestPropertyExclusionIsSetSubtraction(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		intervalSeconds := rapid.Int64Range(1, 60).Draw(t, "interval")
		phaseSeconds := rapid.Int64Range(0, intervalSeconds-1).Draw(t, "phase")
		start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		end := start.Add(time.Hour)
		exclusionOffset := rapid.Int64Range(1, int64(end.Sub(start)/time.Second)).Draw(t, "exclusion-offset")
		exclusion := exactCalendarModel(start.Add(time.Duration(exclusionOffset) * time.Second))
		baseModel := scheduleModel{intervals: []intervalModel{{intervalSeconds: intervalSeconds, phaseSeconds: phaseSeconds}}, timezone: "UTC"}
		excludedModel := baseModel
		excludedModel.exclusions = []calendarModel{exclusion}
		options := ComputeOptions{MaxResults: 10_000, MaxIterations: propertyAnalysisMaxIterations}

		base, baseErr := ComputeMatchingTimes(backgroundContext, baseModel.renderStructured(), start, end, "", options)
		excluded, excludedErr := ComputeMatchingTimes(backgroundContext, excludedModel.renderStructured(), start, end, "", options)
		if baseErr != nil || excludedErr != nil {
			t.Fatalf("exclusion comparison failed: base=%v excluded=%v", baseErr, excludedErr)
		}
		expected := make([]time.Time, 0, len(base.Times))
		for _, candidate := range base.Times {
			if !exclusion.matches(candidate, time.UTC) {
				expected = append(expected, candidate)
			}
		}
		if !slices.Equal(excluded.Times, expected) {
			t.Fatalf("exclusion is not set subtraction: base=%v excluded=%v expected=%v exclusion=%+v", base.Times, excluded.Times, expected, exclusion)
		}
	})
}

func FuzzMatchingTimesSafety(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{1, 2, 3, 4})
	f.Fuzz(rapid.MakeFuzz(checkSafetyOrderingAndDeterminism))
}

func FuzzMatchingTimesOracle(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{5, 8, 13, 21})
	f.Fuzz(rapid.MakeFuzz(checkSmallWindowOracle))
}

func workConsistent(work WorkBreakdown) bool {
	return work.Total ==
		work.NextTimeCalls+
			work.InclusionSourceChecks+
			work.CalendarSearchSteps+
			work.IntervalChecks+
			work.ExclusionChecks+
			work.ExcludedCandidateRetries+
			work.ResultLoopSteps
}

func describeCase(tc scheduleCase) string {
	return fmt.Sprintf(
		"start=%s end=%s seed=%q spec=%s",
		tc.start.Format(time.RFC3339Nano),
		tc.end.Format(time.RFC3339Nano),
		tc.seed,
		formatSpec(tc.spec),
	)
}

func formatSpec(spec proto.Message) string {
	data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(spec)
	if err != nil {
		return fmt.Sprintf("<marshal error: %v>", err)
	}
	return string(data)
}

func sortedUniqueTimes(groups ...[]time.Time) []time.Time {
	var combined []time.Time
	for _, group := range groups {
		combined = append(combined, group...)
	}
	slices.SortFunc(combined, time.Time.Compare)
	return slices.CompactFunc(combined, time.Time.Equal)
}
