package propertytest

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

const validationPropertyMaxIterations = 2_000_000

func TestPropertyValidClassificationHasSoundWitness(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		tc := validSatisfiableScheduleCaseGenerator(24*time.Hour, false).Draw(t, "case")
		result, err := ValidateSchedule(tc.spec, ValidationOptions{MaxIterations: validationPropertyMaxIterations})
		if err != nil {
			t.Fatalf("valid generated schedule failed validation: %v for %s", err, describeCase(tc))
		}
		if result.Status != ValidationValid || result.Witness.IsZero() {
			t.Fatalf("valid classification lacks witness: %+v for %s", result, describeCase(tc))
		}
		if !tc.model.matchesNominal(result.Witness) {
			t.Fatalf("validation witness does not satisfy independent model: witness=%s result=%+v for %s", result.Witness, result, describeCase(tc))
		}
		if !validationWorkConsistent(result.Work) {
			t.Fatalf("inconsistent validation work: %+v", result.Work)
		}
	})
}

func TestPropertyStructuralInvalidClassification(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		tc := invalidStructuralScheduleCaseGenerator().Draw(t, "case")
		result, err := ValidateSchedule(tc.spec, ValidationOptions{MaxIterations: validationPropertyMaxIterations})
		if !errors.Is(err, ErrInvalidSpec) || result.Status != ValidationInvalidStructural {
			t.Fatalf("structural mutation %q classified as status=%s err=%v spec=%s", tc.label, result.Status, err, formatSpec(tc.spec))
		}
	})
}

func TestPropertyUnsatisfiableComponentClassification(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		tc := invalidComponentScheduleCaseGenerator().Draw(t, "case")
		result, err := ValidateSchedule(tc.spec, ValidationOptions{MaxIterations: validationPropertyMaxIterations})
		if !errors.Is(err, ErrUnsatisfiableSpec) || !errors.Is(err, ErrInvalidSpec) || result.Status != tc.expected {
			t.Fatalf("component mutation %q classified as status=%s err=%v spec=%s", tc.label, result.Status, err, formatSpec(tc.spec))
		}
	})
}

func TestPropertyEmptyEffectiveSetClassification(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		tc := invalidEffectiveSetScheduleCaseGenerator().Draw(t, "case")
		result, err := ValidateSchedule(tc.spec, ValidationOptions{MaxIterations: validationPropertyMaxIterations})
		if !errors.Is(err, ErrUnsatisfiableSpec) || result.Status != tc.expected {
			t.Fatalf("empty effective set %q classified as status=%s err=%v spec=%s", tc.label, result.Status, err, formatSpec(tc.spec))
		}
	})
}

func TestPropertyValidationBudgetBoundaryAndMonotonicity(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		tc := reducedValidationCaseGenerator().Draw(t, "case")
		baseline, baselineErr := ValidateSchedule(tc.spec, ValidationOptions{MaxIterations: validationPropertyMaxIterations})
		if errors.Is(baselineErr, ErrValidationLimit) {
			t.Fatalf("baseline validation exhausted for %s", tc.label)
		}
		exact, exactErr := ValidateSchedule(tc.spec, ValidationOptions{MaxIterations: baseline.Work.Total})
		if exact.Status != baseline.Status || exact.Witness != baseline.Witness || !sameErrorClass(exactErr, baselineErr) {
			t.Fatalf("classification changed at exact budget: baseline=%+v err=%v exact=%+v err=%v", baseline, baselineErr, exact, exactErr)
		}
		if baseline.Work.Total <= 1 {
			return
		}
		short, shortErr := ValidateSchedule(tc.spec, ValidationOptions{MaxIterations: baseline.Work.Total - 1})
		if !errors.Is(shortErr, ErrValidationLimit) || short.Status != ValidationIndeterminateBudget {
			t.Fatalf("N-1 budget did not remain indeterminate: required=%d result=%+v err=%v", baseline.Work.Total, short, shortErr)
		}
	})
}

func TestPropertyQueryRangeDoesNotChangeValidation(t *testing.T) {
	t.Parallel()

	witness := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	model := scheduleModel{calendars: []calendarModel{exactCalendarModel(witness)}, timezone: "UTC"}
	spec := model.renderStructured()
	options := ComputeOptions{MaxResults: 10, MaxIterations: 100_000}

	empty, err := ComputeMatchingTimes(spec, witness.Add(24*time.Hour), witness.Add(25*time.Hour), "", options)
	require.NoError(t, err)
	require.Empty(t, empty.Times)

	included, err := ComputeMatchingTimes(spec, witness.Add(-time.Second), witness, "", options)
	require.NoError(t, err)
	require.Equal(t, []time.Time{witness}, included.Times)
	require.Equal(t, empty.Validation, included.Validation)
}

func TestInvalidImpossibleUnionMemberIsNotExcusedByInterval(t *testing.T) {
	t.Parallel()

	spec := impossibleFebruary30Spec()
	spec.Interval = []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}
	result, err := ValidateSchedule(spec, ValidationOptions{MaxIterations: validationPropertyMaxIterations})
	require.ErrorIs(t, err, ErrUnsatisfiableSpec)
	require.Equal(t, ValidationInvalidComponentUnsatisfiable, result.Status)
}

func TestInvalidExclusionRepresentations(t *testing.T) {
	t.Parallel()

	base := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}}
	structured := proto.Clone(base).(*schedulepb.ScheduleSpec)
	structured.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{{Month: []*schedulepb.Range{{Start: 13}}}}
	calendar := proto.Clone(base).(*schedulepb.ScheduleSpec)
	calendar.ExcludeCalendar = []*schedulepb.CalendarSpec{{Month: "13"}}
	for name, spec := range map[string]*schedulepb.ScheduleSpec{"structured": structured, "calendar": calendar} {
		t.Run(name, func(t *testing.T) {
			result, err := ValidateSchedule(spec, ValidationOptions{MaxIterations: validationPropertyMaxIterations})
			require.ErrorIs(t, err, ErrInvalidSpec)
			require.Equal(t, ValidationInvalidStructural, result.Status)
		})
	}
}

func TestValidationAcceptsEitherRepeatedHourOccurrence(t *testing.T) {
	t.Parallel()

	witness := time.Date(2020, 4, 4, 14, 30, 0, 0, time.UTC)
	location, err := time.LoadLocation("Australia/Lord_Howe")
	require.NoError(t, err)
	model := scheduleModel{
		calendars: []calendarModel{exactCalendarModel(witness.In(location))},
		startTime: &witness,
		endTime:   &witness,
		timezone:  "Australia/Lord_Howe",
	}
	result, err := ValidateSchedule(model.renderStructured(), ValidationOptions{MaxIterations: validationPropertyMaxIterations})
	require.NoError(t, err)
	require.Equal(t, ValidationValid, result.Status)
	require.Equal(t, witness, result.Witness)
}

func TestCopiedCalculatorLordHoweRepeatedHalfHourDivergence(t *testing.T) {
	t.Parallel()

	start := time.Date(2020, 4, 4, 14, 29, 8, 0, time.UTC)
	end := start.Add(time.Minute)
	witness := time.Date(2020, 4, 4, 14, 30, 0, 0, time.UTC)
	location, err := time.LoadLocation("Australia/Lord_Howe")
	require.NoError(t, err)
	model := scheduleModel{
		calendars: []calendarModel{exactCalendarModel(witness.In(location))},
		timezone:  "Australia/Lord_Howe",
	}
	actual, err := ComputeMatchingTimes(model.renderStructured(), start, end, "", ComputeOptions{
		MaxResults: 10, MaxIterations: propertyAnalysisMaxIterations,
	})
	require.NoError(t, err)
	require.Empty(t, actual.Times)
	require.Equal(t, []time.Time{witness}, bruteForceMatchingTimes(model, start, end, 10))
}

func TestPropertyReducedDomainValidatorDifferential(t *testing.T) {
	t.Parallel()
	rapid.Check(t, checkReducedDomainValidatorDifferential)
}

func checkReducedDomainValidatorDifferential(t *rapid.T) {
	tc := reducedValidationCaseGenerator().Draw(t, "case")
	start := tc.spec.StartTime.AsTime()
	end := tc.spec.EndTime.AsTime()
	referenceWitness := bruteForceValidationWitness(tc.model, start, end)
	actual, err := ValidateSchedule(tc.spec, ValidationOptions{MaxIterations: validationPropertyMaxIterations})
	if actual.Status != tc.expected {
		t.Fatalf("validator/reference classification mismatch: expected=%s actual=%+v err=%v label=%s spec=%s", tc.expected, actual, err, tc.label, formatSpec(tc.spec))
	}
	if tc.expected == ValidationValid {
		if err != nil || referenceWitness.IsZero() || actual.Witness.IsZero() || !tc.model.matchesNominal(actual.Witness) {
			t.Fatalf("valid differential witness mismatch: reference=%s actual=%+v err=%v spec=%s", referenceWitness, actual, err, formatSpec(tc.spec))
		}
	} else if !referenceWitness.IsZero() || !errors.Is(err, ErrUnsatisfiableSpec) {
		t.Fatalf("empty differential mismatch: reference=%s actual=%+v err=%v spec=%s", referenceWitness, actual, err, formatSpec(tc.spec))
	}
}

func FuzzScheduleValidation(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{1, 2, 3, 4})
	f.Fuzz(rapid.MakeFuzz(checkReducedDomainValidatorDifferential))
}

func TestMutationKillMatrix(t *testing.T) {
	t.Run("PROP-ORDERING-START-EXCLUSIVE", func(t *testing.T) {
		start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}}
		mutant, err := ComputeMatchingTimes(spec, start, start.Add(2*time.Hour), "", ComputeOptions{
			MaxResults: 2, MaxIterations: 100, faults: analysisFaults{queryStartInclusive: true},
		})
		require.NoError(t, err)
		require.False(t, strictlyOrderedWithin(mutant.Times, start, start.Add(2*time.Hour)))
	})

	t.Run("PROP-EXCLUSION-SUBTRACTION", func(t *testing.T) {
		start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		excluded := start.Add(time.Second)
		model := scheduleModel{
			intervals:  []intervalModel{{intervalSeconds: 1}},
			exclusions: []calendarModel{exactCalendarModel(excluded)},
			timezone:   "UTC",
		}
		options := ComputeOptions{MaxResults: 2, MaxIterations: 1_000, faults: analysisFaults{ignoreExclusions: true}}
		mutant, err := ComputeMatchingTimes(model.renderStructured(), start, start.Add(3*time.Second), "", options)
		require.NoError(t, err)
		require.Contains(t, mutant.Times, excluded)
	})

	t.Run("PROP-INVALID-EXCLUSION-STRUCTURE", func(t *testing.T) {
		spec := &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
			ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
				Second: allRanges(0, 59), Minute: allRanges(0, 59), Hour: allRanges(0, 23),
				DayOfMonth: allRanges(1, 31), Month: []*schedulepb.Range{{Start: 13}}, DayOfWeek: allRanges(0, 6),
			}},
		}
		mutant, err := ValidateSchedule(spec, ValidationOptions{MaxIterations: 100_000, faults: analysisFaults{stopValidatingExclusions: true}})
		require.NoError(t, err)
		require.Equal(t, ValidationValid, mutant.Status)
	})

	t.Run("PROP-EMPTY-STRUCTURED-INCLUSION", func(t *testing.T) {
		mutant, err := ValidateSchedule(
			&schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{}}},
			ValidationOptions{MaxIterations: 100_000, faults: analysisFaults{acceptEmptyStructuredInclusion: true}},
		)
		require.NoError(t, err)
		require.Equal(t, ValidationValid, mutant.Status)
	})

	t.Run("PROP-REDUCED-COMPLETENESS-FEBRUARY-30", func(t *testing.T) {
		spec := impossibleFebruary30Spec()
		mutant, err := ValidateSchedule(spec, ValidationOptions{MaxIterations: 100_000, faults: analysisFaults{treatFebruary30AsSatisfiable: true}})
		require.NoError(t, err)
		require.Equal(t, ValidationValid, mutant.Status)
	})

	t.Run("PROP-WITNESS-SOUNDNESS-DAY-OF-WEEK", func(t *testing.T) {
		day := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		calendar := exactCalendarModel(day)
		calendar.dayOfWeek[0] = rangeModel{start: int((day.Weekday() + 1) % 7), end: int((day.Weekday() + 1) % 7), step: 1}
		model := scheduleModel{calendars: []calendarModel{calendar}, startTime: &day, endTime: &day, timezone: "UTC"}
		mutant, err := ValidateSchedule(model.renderStructured(), ValidationOptions{MaxIterations: 100_000, faults: analysisFaults{ignoreDayOfWeek: true}})
		require.NoError(t, err)
		require.False(t, model.matchesNominal(mutant.Witness))
	})

	t.Run("PROP-INVERTED-BOUNDS-CLASSIFICATION", func(t *testing.T) {
		start := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
		spec := &schedulepb.ScheduleSpec{
			Interval:  []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
			StartTime: timestamppb.New(start), EndTime: timestamppb.New(start.Add(-time.Hour)),
		}
		mutant, _ := ValidateSchedule(spec, ValidationOptions{MaxIterations: 100_000, faults: analysisFaults{permitInvertedScheduleBounds: true}})
		require.NotEqual(t, ValidationInvalidStructural, mutant.Status)
	})

	t.Run("PROP-MATCHING-BUDGET-TAXONOMY", func(t *testing.T) {
		start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}}
		mutant, err := ComputeMatchingTimes(spec, start, start.Add(time.Minute), "", ComputeOptions{
			MaxResults: 10, MaxIterations: 1, faults: analysisFaults{iterationLimitIsEmpty: true},
		})
		require.NoError(t, err)
		require.Empty(t, mutant.Times)
	})

	t.Run("PROP-STRICT-ORDERING-NO-DUPLICATES", func(t *testing.T) {
		start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{
			{Interval: durationpb.New(time.Second)}, {Interval: durationpb.New(time.Second)},
		}}
		mutant, err := ComputeMatchingTimes(spec, start, start.Add(time.Minute), "", ComputeOptions{
			MaxResults: 2, MaxIterations: 100, faults: analysisFaults{allowDuplicateUnionResults: true},
		})
		require.NoError(t, err)
		require.False(t, strictlyOrderedWithin(mutant.Times, start, start.Add(time.Minute)))
	})

	t.Run("PROP-JITTER-DOES-NOT-CROSS-NEXT-NOMINAL", func(t *testing.T) {
		start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		spec := &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
			Jitter:   durationpb.New(time.Hour),
		}
		killed := false
		for _, seed := range []string{"a", "b", "mutation", "jitter-cross"} {
			mutant, err := ComputeMatchingTimes(spec, start, start.Add(2*time.Hour), seed, ComputeOptions{
				MaxResults: 1, MaxIterations: 100, faults: analysisFaults{jitterCrossesNextNominal: true},
			})
			require.NoError(t, err)
			if len(mutant.Times) == 1 && mutant.Times[0].After(start.Add(2*time.Second)) {
				killed = true
				break
			}
		}
		require.True(t, killed)
	})

	t.Run("PROP-VALIDATION-BUDGET-IS-INDETERMINATE", func(t *testing.T) {
		start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}}
		mutant, err := ComputeMatchingTimes(spec, start, start.Add(time.Hour), "", ComputeOptions{
			MaxResults: 1, MaxIterations: 100, MaxValidationIterations: 1,
			faults: analysisFaults{validationIndeterminateIsInvalid: true},
		})
		require.ErrorIs(t, err, ErrInvalidSpec)
		require.NotErrorIs(t, err, ErrValidationLimit)
		require.Equal(t, ValidationIndeterminateBudget, mutant.Validation.Status)
	})
}

func sameErrorClass(a error, b error) bool {
	classes := []error{ErrInvalidSpec, ErrUnsatisfiableSpec, ErrValidationLimit}
	for _, class := range classes {
		if errors.Is(a, class) != errors.Is(b, class) {
			return false
		}
	}
	return true
}

func strictlyOrderedWithin(times []time.Time, start time.Time, end time.Time) bool {
	for i, candidate := range times {
		if !candidate.After(start) || candidate.After(end) || i > 0 && !candidate.After(times[i-1]) {
			return false
		}
	}
	return true
}

func allRanges(start int32, end int32) []*schedulepb.Range {
	return []*schedulepb.Range{{Start: start, End: end, Step: 1}}
}

func impossibleFebruary30Spec() *schedulepb.ScheduleSpec {
	return &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
		Second: allRanges(0, 0), Minute: allRanges(0, 0), Hour: allRanges(0, 0),
		DayOfMonth: allRanges(30, 30), Month: allRanges(2, 2), DayOfWeek: allRanges(0, 6),
	}}}
}
