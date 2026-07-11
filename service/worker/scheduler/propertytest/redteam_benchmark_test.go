package propertytest

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func BenchmarkPlan2WorkCategory(b *testing.B) {
	base := time.Date(2025, 3, 9, 1, 59, 59, 0, time.UTC)
	calendar := exactCalendarModel(base.Add(time.Second))
	compiledCalendar := newCompiledCalendar(calendar.render(), time.UTC)
	spec := redTeamSeedCandidates()[0]
	compiledSpec, err := NewSpecBuilder().NewCompiledSpec(renderRedTeamCandidate(spec))
	if err != nil {
		b.Fatal(err)
	}
	exclusionCandidate := redTeamSeedCandidates()[1]
	exclusionCompiled, err := NewSpecBuilder().NewCompiledSpec(renderRedTeamCandidate(exclusionCandidate))
	if err != nil {
		b.Fatal(err)
	}

	b.Run("calendar-outer-search-step", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			budget := newIterationBudget(1_000)
			if _, err := compiledCalendar.next(base, budget); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("calendar-field-advancement", func(b *testing.B) {
		fieldCalendar := calendar
		fieldCalendar.second = []rangeModel{{start: 59, end: 59, step: 1}}
		compiled := newCompiledCalendar(fieldCalendar.render(), time.UTC)
		b.ReportAllocs()
		for range b.N {
			budget := newIterationBudget(1_000)
			if _, err := compiled.next(base.Add(-59*time.Second), budget); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("inclusion-source-check", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			budget := newIterationBudget(10_000)
			if _, err := compiledSpec.rawNextTime(base, budget); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("interval-modular-calculation", func(b *testing.B) {
		interval := spec.Model.intervals[0].render()
		b.ReportAllocs()
		for range b.N {
			_ = compiledSpec.nextIntervalTime(interval, base.Unix())
		}
	})
	b.Run("exclusion-match", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			budget := newIterationBudget(1_000)
			if _, err := exclusionCompiled.excluded(base, budget); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("excluded-candidate-retry", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			budget := newIterationBudget(100_000)
			if _, err := exclusionCompiled.getNextTime("benchmark", base, budget); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("jitter-following-nominal", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			budget := newIterationBudget(100_000)
			if _, err := compiledSpec.getNextTime("benchmark", base, budget); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("timezone-conversion-dst", func(b *testing.B) {
		location, err := time.LoadLocation("America/Chicago")
		if err != nil {
			b.Fatal(err)
		}
		compiled := newCompiledCalendar(calendar.render(), location)
		candidate := time.Date(2025, 11, 2, 6, 30, 0, 0, time.UTC)
		b.ReportAllocs()
		for range b.N {
			_ = compiled.matches(candidate)
		}
	})
	b.Run("canonicalization-validation", func(b *testing.B) {
		candidate := redTeamSeedCandidates()[5]
		rendered := renderRedTeamCandidate(candidate)
		b.ReportAllocs()
		for range b.N {
			if _, err := ValidateSchedule(backgroundContext, rendered, ValidationOptions{MaxIterations: redTeamDefaultValidationWork}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPlan2Candidate(b *testing.B) {
	candidates := plan2BenchmarkCandidates()
	for _, named := range candidates {
		b.Run(named.name, func(b *testing.B) {
			spec := renderRedTeamCandidate(named.candidate)
			b.ReportAllocs()
			var last ComputeResult
			for range b.N {
				result, err := ComputeMatchingTimes(backgroundContext, spec, named.candidate.QueryStart, named.candidate.QueryEnd, named.candidate.JitterSeed, ComputeOptions{
					MaxResults: named.candidate.ResultLimit, MaxIterations: redTeamCampaignMatchingWork,
					MaxValidationIterations: redTeamDefaultValidationWork,
				})

				if err != nil {
					b.Fatal(err)
				}
				last = result
			}
			b.ReportMetric(float64(last.Work.Total), "matching-work/op")
			b.ReportMetric(float64(last.Work.CalendarSearchSteps), "calendar-work/op")
			b.ReportMetric(float64(last.Work.ExcludedCandidateRetries), "excluded-retries/op")
			b.ReportMetric(float64(last.Validation.Work.Total), "validation-work/op")
		})
	}
}

type namedPlan2Candidate struct {
	name      string
	candidate RedTeamCandidate
}

func plan2BenchmarkCandidates() []namedPlan2Candidate {
	seeds := redTeamSeedCandidates()
	low := cloneRedTeamCandidate(seeds[3])
	low.CandidateID = "plan2-benchmark-low"
	low.ResultLimit = 1
	median := cloneRedTeamCandidate(seeds[1])
	median.CandidateID = "plan2-benchmark-median"
	median.ResultLimit = 10
	transition := cloneRedTeamCandidate(seeds[0])
	transition.CandidateID = "plan2-benchmark-transition"
	transition.ResultLimit = 100
	p99 := cloneRedTeamCandidate(seeds[5])
	p99.CandidateID = "plan2-benchmark-p99-like"
	p99.ResultLimit = 1_000
	maximum := cloneRedTeamCandidate(seeds[0])
	maximum.CandidateID = "plan2-benchmark-maximum-work"
	maximum.ResultLimit = 1_000
	return []namedPlan2Candidate{
		{"low", low}, {"median", median}, {"budget-transition", transition},
		{"p99-like", p99}, {"maximum-work", maximum},
	}
}

func TestPlan2BenchmarkCampaign(t *testing.T) {
	if os.Getenv("SCHEDULE_PROPERTY_BENCHMARK_CAMPAIGN") == "" {
		t.Skip("set SCHEDULE_PROPERTY_BENCHMARK_CAMPAIGN=1 to record the Plan 2 benchmark campaign")
	}
	output := os.Getenv("SCHEDULE_PROPERTY_RESULTS_DIR")
	campaignID := os.Getenv("SCHEDULE_PROPERTY_CAMPAIGN_ID")
	if output == "" || campaignID == "" {
		t.Fatal("SCHEDULE_PROPERTY_RESULTS_DIR and SCHEDULE_PROPERTY_CAMPAIGN_ID are required")
	}
	planned := len(plan2BenchmarkCandidates())
	manifest := campaignManifest{
		SchemaVersion: "campaign-v1", CampaignID: campaignID, Plan: "plan2", Name: "work-category-and-candidate-benchmarks",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: campaignProvenance(), Population: "curated",
		PlannedCases: &planned, Commands: []string{
			"timeout 300s go test -tags test_dep ./service/worker/scheduler/propertytest -run '^$' -bench '^(BenchmarkPlan2WorkCategory|BenchmarkPlan2Candidate)$' -benchmem -benchtime=200ms -count=3",
		}, Environment: map[string]string{
			"GOMAXPROCS": "1", "SCHEDULE_PROPERTY_BENCHMARK_CAMPAIGN": "1",
			"SCHEDULE_PROPERTY_RESULTS_DIR": output, "SCHEDULE_PROPERTY_CAMPAIGN_ID": campaignID,
			"weighted_cost_model": redTeamWeightedCostVersion,
		},
	}
	directory := filepath.Join(output, campaignID)
	writer, err := newCampaignWriter(directory, manifest)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "test", "-tags", "test_dep", "./service/worker/scheduler/propertytest", "-run", "^$", "-bench", "^(BenchmarkPlan2WorkCategory|BenchmarkPlan2Candidate)$", "-benchmem", "-benchtime=200ms", "-count=3")
	command.Dir = repositoryRootForPropertytest(t)
	command.Env = append(filteredEnvironment("SCHEDULE_PROPERTY_BENCHMARK_CAMPAIGN"), "GOMAXPROCS=1")
	benchmarkOutput, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("benchmark subprocess failed: %v\n%s", err, benchmarkOutput)
	}
	if err := writeBytesAtomic(filepath.Join(directory, "benchmark.txt"), benchmarkOutput); err != nil {
		t.Fatal(err)
	}
	config := redTeamSearchConfig{
		Seed: 20260710, Population: 1, Generations: 1, Elite: 1,
		MaxMatchingWork: redTeamCampaignMatchingWork, MaxValidationWork: redTeamDefaultValidationWork,
		CandidateWallLimit: redTeamDefaultCandidateWall,
	}
	evaluations := make([]redTeamEvaluation, 0, planned)
	for _, named := range plan2BenchmarkCandidates() {
		evaluation, err := evaluateRedTeamCandidate(config, named.candidate)
		if err != nil {
			t.Fatalf("benchmark case %s: %v", named.name, err)
		}
		evaluations = append(evaluations, evaluation)
		if err := writer.addCase(makePlan2MaterialCase(campaignID, evaluation, evaluation, nil)); err != nil {
			t.Fatal(err)
		}
	}
	correlations := plan2WorkCPUCorrelations(evaluations)
	summary := map[string]any{
		"schema_version": "summary-v1", "campaign_id": campaignID, "completed_cases": len(evaluations),
		"microbenchmark_categories": []string{
			"calendar-outer-search-step", "calendar-field-advancement", "inclusion-source-check",
			"interval-modular-calculation", "exclusion-match", "excluded-candidate-retry",
			"jitter-following-nominal", "timezone-conversion-dst", "canonicalization-validation",
		},
		"candidate_classes":     []string{"low", "median", "budget-transition", "p99-like", "maximum-work"},
		"work_cpu_correlations": correlations, "weighted_cost_model_version": redTeamWeightedCostVersion,
		"raw_work_vectors": evaluationFitnessVectors(evaluations),
	}
	results := fmt.Sprintf(`# Plan 2 Work Calibration Results

OBSERVATION: Campaign %s recorded ns/op, B/op, and allocs/op for all nine required work categories and the low, median, budget-transition, P99-like, and maximum-work candidate classes. Raw output is retained in benchmark.txt.

OBSERVATION: Raw iteration-v1 vectors and work/CPU correlations are retained in summary.json. Validation and matching work are separate.

INFERENCE: Benchmark ns/op is a host-specific CPU proxy and is not a deterministic request budget.

RECOMMENDATION: Keep weighted model %s separate from iteration definition v1 and do not tune or enforce it as a production guard in Plan 2.

NEGATIVE EVIDENCE: All bounded benchmark repetitions completed under the five-minute subprocess deadline without panic or budget exhaustion.
`, campaignID, redTeamWeightedCostVersion)
	if err := writer.finalizeWithArtifacts(summary, results, "", []string{"benchmark.txt"}); err != nil {
		t.Fatal(err)
	}
}

func repositoryRootForPropertytest(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for !strings.HasSuffix(filepath.ToSlash(directory), "/service/worker/scheduler/propertytest") {
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("cannot find repository root from %s", directory)
		}
		directory = parent
	}
	return filepath.Clean(filepath.Join(directory, "../../../.."))
}

func filteredEnvironment(removed string) []string {
	prefix := removed + "="
	result := make([]string, 0, len(os.Environ()))
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, prefix) {
			result = append(result, value)
		}
	}
	return result
}

func plan2WorkCPUCorrelations(evaluations []redTeamEvaluation) map[string]float64 {
	cpu := make([]float64, 0, len(evaluations))
	total := make([]float64, 0, len(evaluations))
	calendar := make([]float64, 0, len(evaluations))
	exclusion := make([]float64, 0, len(evaluations))
	validation := make([]float64, 0, len(evaluations))
	for _, evaluation := range evaluations {
		cpu = append(cpu, float64(evaluation.Fitness.CPUNanoseconds))
		total = append(total, float64(evaluation.Fitness.TotalMatchingWork))
		calendar = append(calendar, float64(evaluation.Fitness.CalendarSearchWork))
		exclusion = append(exclusion, float64(evaluation.Fitness.ExcludedCandidateRetries))
		validation = append(validation, float64(evaluation.Fitness.ValidationProofWork))
	}
	return map[string]float64{
		"matching_total": pearsonCorrelation(total, cpu), "calendar_search": pearsonCorrelation(calendar, cpu),
		"excluded_retries": pearsonCorrelation(exclusion, cpu), "validation_total": pearsonCorrelation(validation, cpu),
	}
}

func pearsonCorrelation(left []float64, right []float64) float64 {
	if len(left) != len(right) || len(left) < 2 {
		return 0
	}
	var leftMean, rightMean float64
	for i := range left {
		leftMean += left[i]
		rightMean += right[i]
	}
	leftMean /= float64(len(left))
	rightMean /= float64(len(right))
	var numerator, leftSquare, rightSquare float64
	for i := range left {
		leftDelta := left[i] - leftMean
		rightDelta := right[i] - rightMean
		numerator += leftDelta * rightDelta
		leftSquare += leftDelta * leftDelta
		rightSquare += rightDelta * rightDelta
	}
	denominator := math.Sqrt(leftSquare * rightSquare)
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}

func evaluationFitnessVectors(evaluations []redTeamEvaluation) map[string]redTeamFitness {
	result := make(map[string]redTeamFitness, len(evaluations))
	for _, evaluation := range evaluations {
		result[evaluation.Candidate.CandidateID] = evaluation.Fitness
	}
	return result
}

func TestPlan2WorkCPUCorrelationIsFinite(t *testing.T) {
	value := pearsonCorrelation([]float64{1, 2, 3}, []float64{2, 4, 6})
	if math.IsNaN(value) || math.IsInf(value, 0) || math.Abs(value-1) > 1e-9 {
		t.Fatalf("invalid correlation %v", value)
	}
	if runtime.GOOS == "" {
		t.Fatal("runtime platform unavailable")
	}
}
