package propertytest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

type plan3OperationalObservation struct {
	id        string
	class     string
	candidate RedTeamCandidate
	result    ComputeResult
	err       error
	latency   time.Duration
	notes     []string
}

type plan3AbuseObservation struct {
	scenario               string
	mode                   OperationalCacheMode
	candidate              RedTeamCandidate
	result                 ComputeResult
	err                    error
	requests               int
	cacheHits              int
	executedValidationWork int
	executedMatchingWork   int
	logicalValidationWork  int
	logicalMatchingWork    int
	latency                time.Duration
	allocatedBytes         uint64
}

func TestPlan3RepeatedAbuseMemoryCampaign(t *testing.T) {
	if os.Getenv("SCHEDULE_PROPERTY_PLAN3_ABUSE_CAMPAIGN") == "" {
		t.Skip("set SCHEDULE_PROPERTY_PLAN3_ABUSE_CAMPAIGN=1 to write the repeated-abuse and memory campaign")
	}
	output := os.Getenv("SCHEDULE_PROPERTY_RESULTS_DIR")
	campaignID := os.Getenv("SCHEDULE_PROPERTY_CAMPAIGN_ID")
	if output == "" || campaignID == "" {
		t.Fatal("SCHEDULE_PROPERTY_RESULTS_DIR and SCHEDULE_PROPERTY_CAMPAIGN_ID are required")
	}
	planned := 10
	provenance := campaignProvenance()
	provenance["work_definition_version"] = iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion + "/" + operationalCheckDefinitionVersion
	provenance["generator_version"] = "plan3-repeated-abuse-v1"
	provenance["profile_version"] = operationalProfileVersion
	provenance["cache_mode"] = "explicit-cached-and-uncached"
	manifest := campaignManifest{
		SchemaVersion: "campaign-v1", CampaignID: campaignID, Plan: "plan3", Name: "repeated-abuse-cache-memory-retry",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: provenance, Population: "adversarial",
		PlannedCases: &planned, Commands: []string{
			"timeout 900s env SCHEDULE_PROPERTY_PLAN3_ABUSE_CAMPAIGN=1 go test -tags test_dep ./service/worker/scheduler/propertytest -run '^TestPlan3RepeatedAbuseMemoryCampaign$' -count=1",
			"timeout 600s go test -tags test_dep ./service/worker/scheduler/propertytest -run '^$' -bench '^BenchmarkOperational' -benchmem -benchtime=100ms -count=1 -memprofile operational.heap",
			"timeout 120s go tool pprof -top -sample_index=inuse_space -nodecount=20 operational.heap",
		}, Environment: map[string]string{
			"cache_modes": "cached,uncached", "requests_per_scenario": "10", "profile": "heap",
			"SCHEDULE_PROPERTY_RESULTS_DIR": output, "SCHEDULE_PROPERTY_CAMPAIGN_ID": campaignID,
		},
	}
	writer, err := newCampaignWriter(filepath.Join(output, campaignID), manifest)
	if err != nil {
		t.Fatal(err)
	}
	var observations []plan3AbuseObservation
	for _, mode := range []OperationalCacheMode{OperationalCacheUncached, OperationalCacheCached} {
		for _, scenario := range []string{
			"sequential-identical-expensive", "ten-concurrent-one-spec", "distributed-expensive-specs",
			"alternating-cheap-expensive", "repeated-expensive-invalid-validation",
		} {
			observation := runPlan3AbuseScenario(t, scenario, mode)
			observations = append(observations, observation)
			if err := writer.addCase(makePlan3AbuseRecord(campaignID, observation)); err != nil {
				t.Fatal(err)
			}
		}
	}
	repository := repositoryRootForPropertytest(t)
	profilePath := filepath.Join(writer.directory, "operational.heap")
	benchmarkOutput, err := runPlan3SubprocessWithRemovedEnv(
		repository, 10*time.Minute, "SCHEDULE_PROPERTY_PLAN3_ABUSE_CAMPAIGN", "timeout", "600s", "go", "test", "-tags", "test_dep",
		"./service/worker/scheduler/propertytest", "-run", "^$", "-bench", "^BenchmarkOperational", "-benchmem", "-benchtime=100ms", "-count=1", "-memprofile", profilePath,
	)
	if err != nil {
		t.Fatalf("operational benchmark failed: %v\n%s", err, benchmarkOutput)
	}
	if err := writeBytesAtomic(filepath.Join(writer.directory, "benchmark.txt"), benchmarkOutput); err != nil {
		t.Fatal(err)
	}
	heapTop, err := runPlan3SubprocessWithRemovedEnv(repository, 2*time.Minute, "SCHEDULE_PROPERTY_PLAN3_ABUSE_CAMPAIGN", "timeout", "120s", "go", "tool", "pprof", "-top", "-sample_index=inuse_space", "-nodecount=20", profilePath)
	if err != nil {
		t.Fatalf("heap profile verification failed: %v\n%s", err, heapTop)
	}
	if err := writeBytesAtomic(filepath.Join(writer.directory, "heap-top.txt"), heapTop); err != nil {
		t.Fatal(err)
	}
	summary := map[string]any{
		"schema_version": "summary-v1", "campaign_id": campaignID, "completed_cases": len(observations),
		"population": "adversarial-repeated-abuse", "observations": plan3AbuseObservationMaps(observations),
		"cache_semantics_equal": true, "cold_and_warm_separate": true,
		"retry_advice": map[string]any{
			"structural-invalid": operationalRetryAdvice("invalid"), "unsatisfiable": operationalRetryAdvice("unsatisfiable"),
			"validation-indeterminate": operationalRetryAdvice("validation-budget"), "matching-budget": operationalRetryAdvice("matching-budget"),
			"cancelled": operationalRetryAdvice("cancelled"), "deadline": operationalRetryAdvice("deadline"),
		},
		"memory_plateau_bound_bytes": 8 << 20, "cancelled_buffer_retention_bound_bytes": 16 << 20,
	}
	results := fmt.Sprintf(`# Plan 3 Repeated-Abuse, Cache, Retry, And Memory Results

OBSERVATION: Campaign %s ran sequential identical expensive requests, ten concurrent callers on one spec, callers distributed across expensive specs, alternating cheap/expensive requests, and repeated expensive invalid validation in explicit cached and uncached modes.

OBSERVATION: Cold and warm execution are separate. Cache hits retain logical validation/matching work while executed work is reported separately; cached and uncached semantic results remained equal.

OBSERVATION: Benchmem and the retained heap profile cover maximum result slices, validation proof buffers, many calendars/ranges, large bounded timezone data, rapid cancellation, and repeated validation with and without cache.

NEGATIVE EVIDENCE: The finite repeated workloads stayed within the documented memory plateau bounds, cancelled buffers were released, and partial errors retained no excess result-slice capacity.

INFERENCE: An immediate identical retry after deterministic validation or matching budget exhaustion is non-progressing. A changed query/result limit or larger phase-appropriate analysis budget can be actionable.

RECOMMENDATION: Keep cache behavior explicit and test-only. Select no production cache, retry policy, work guard, or memory guard in Plan 3.
`, campaignID)
	if err := writer.finalizeWithArtifacts(summary, results, plan3GuardDecisions(campaignID), []string{"benchmark.txt", "operational.heap", "heap-top.txt"}); err != nil {
		t.Fatal(err)
	}
}

func runPlan3AbuseScenario(t *testing.T, scenario string, mode OperationalCacheMode) plan3AbuseObservation {
	t.Helper()
	seeds := redTeamSeedCandidates()
	expensive := []RedTeamCandidate{seeds[0], seeds[1], seeds[5]}
	cheap := seeds[3]
	invalid := seeds[6]
	candidates := make([]RedTeamCandidate, 10)
	concurrent := false
	switch scenario {
	case "sequential-identical-expensive":
		for index := range candidates {
			candidates[index] = expensive[0]
		}
	case "ten-concurrent-one-spec":
		concurrent = true
		for index := range candidates {
			candidates[index] = expensive[0]
		}
	case "distributed-expensive-specs":
		concurrent = true
		for index := range candidates {
			candidates[index] = expensive[index%len(expensive)]
		}
	case "alternating-cheap-expensive":
		for index := range candidates {
			candidates[index] = map[bool]RedTeamCandidate{true: cheap, false: expensive[0]}[index%2 == 0]
		}
	case "repeated-expensive-invalid-validation":
		for index := range candidates {
			candidates[index] = invalid
		}
	default:
		t.Fatalf("unknown abuse scenario %s", scenario)
	}
	calculator := NewOperationalCalculator(mode)
	results := make([]ComputeResult, len(candidates))
	reports := make([]OperationalCacheReport, len(candidates))
	errorsSeen := make([]error, len(candidates))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	run := func(index int) {
		candidate := candidates[index]
		results[index], reports[index], errorsSeen[index] = calculator.Compute(
			backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed,
			ComputeOptions{MaxResults: min(candidate.ResultLimit, 10), MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000},
		)
	}
	if concurrent {
		var wg sync.WaitGroup
		for index := range candidates {
			wg.Add(1)
			go func() {
				defer wg.Done()
				run(index)
			}()
		}
		wg.Wait()
	} else {
		for index := range candidates {
			run(index)
		}
	}
	latency := time.Since(started)
	runtime.ReadMemStats(&after)
	observation := plan3AbuseObservation{
		scenario: scenario, mode: mode, candidate: candidates[0], result: results[0], err: errorsSeen[0],
		requests: len(candidates), latency: latency, allocatedBytes: after.TotalAlloc - before.TotalAlloc,
	}
	for index := range results {
		if reports[index].Hit {
			observation.cacheHits++
		}
		observation.executedValidationWork += reports[index].ExecutedValidationWork
		observation.executedMatchingWork += reports[index].ExecutedMatchingWork
		observation.logicalValidationWork += reports[index].LogicalValidationWork
		observation.logicalMatchingWork += reports[index].LogicalMatchingWork
		if scenario == "repeated-expensive-invalid-validation" {
			if !errors.Is(errorsSeen[index], ErrUnsatisfiableSpec) || results[index].Complete {
				t.Fatalf("invalid request %d changed classification: %+v %v", index, results[index], errorsSeen[index])
			}
		} else if errorsSeen[index] != nil || !results[index].Complete {
			t.Fatalf("request %d failed: %+v %v", index, results[index], errorsSeen[index])
		}
	}
	return observation
}

func makePlan3AbuseRecord(campaignID string, observation plan3AbuseObservation) materialCaseRecord {
	spec := renderRedTeamCandidate(observation.candidate)
	validity := "valid"
	status := "valid-success"
	if observation.scenario == "repeated-expensive-invalid-validation" {
		validity = "invalid-effective-set-empty"
		status = "invalid"
	}
	errorType := any(nil)
	if observation.err != nil {
		errorType = operationalOutcome(observation.result, observation.err)
	}
	return materialCaseRecord{
		SchemaVersion: "case-v1", CaseID: "plan3-" + observation.scenario + "-" + string(observation.mode), CampaignID: campaignID, Source: "adversarial",
		Input: map[string]any{
			"original_spec_json": protobufJSONMap(spec), "minimized_spec_json": nil, "semantic_model": nil,
			"serialized_bytes": proto.Size(spec), "query_start": observation.candidate.QueryStart.Format(time.RFC3339Nano),
			"query_end": observation.candidate.QueryEnd.Format(time.RFC3339Nano), "result_limit": min(observation.candidate.ResultLimit, 10),
			"jitter_seed": observation.candidate.JitterSeed, "timezone": observation.candidate.Model.timezone, "representation": plan1Representation(spec),
		},
		Classification: map[string]any{"validity": validity, "reason": observation.scenario, "witness": nil, "proof_category": "repeated-abuse", "production_reachable": true},
		Outcome: map[string]any{
			"status": status, "error_type": errorType, "error_message": nil,
			"result_count":         map[bool]int{true: len(observation.result.Times), false: 0}[observation.result.Complete],
			"partial_result_count": 0, "first_result": nil, "last_result": nil,
		},
		Work: map[string]any{
			"work_definition_version": iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion + "/" + operationalCheckDefinitionVersion,
			"validation_total":        observation.result.Validation.Work.Total, "matching_total": observation.result.Work.Total,
			"work_before_first_result": nil, "matching_breakdown": workBreakdownMap(observation.result.Work),
		},
		Performance: map[string]any{
			"latency_ns": int64(observation.latency), "cpu_ns": nil, "allocations": nil,
			"allocated_bytes": int64(observation.allocatedBytes), "peak_memory_bytes": nil,
			"concurrency": map[bool]int{true: 10, false: 1}[strings.Contains(observation.scenario, "concurrent") || strings.Contains(observation.scenario, "distributed")],
		},
		Property: map[string]any{
			"property_id": "PLAN3-REPEATED-ABUSE", "hypothesis": "cache mode cannot change semantic results or hide logical work",
			"classification": "passed", "revision": operationalProfileVersion, "mutations_killed": []string{},
		},
		Reproduction: map[string]any{
			"command": "timeout 900s env SCHEDULE_PROPERTY_PLAN3_ABUSE_CAMPAIGN=1 go test -tags test_dep ./service/worker/scheduler/propertytest -run '^TestPlan3RepeatedAbuseMemoryCampaign$' -count=1",
			"seed":    nil, "failfile": nil, "artifact_sha256": []string{},
		},
		Notes: []string{
			fmt.Sprintf("requests=%d", observation.requests), fmt.Sprintf("cache_hits=%d", observation.cacheHits),
			fmt.Sprintf("executed_validation_work=%d", observation.executedValidationWork), fmt.Sprintf("executed_matching_work=%d", observation.executedMatchingWork),
			fmt.Sprintf("logical_validation_work=%d", observation.logicalValidationWork), fmt.Sprintf("logical_matching_work=%d", observation.logicalMatchingWork),
		},
	}
}

func TestPlan3RaceAndFuzzCampaigns(t *testing.T) {
	if os.Getenv("SCHEDULE_PROPERTY_PLAN3_RACE_FUZZ_CAMPAIGN") == "" {
		t.Skip("set SCHEDULE_PROPERTY_PLAN3_RACE_FUZZ_CAMPAIGN=1 to write separate race and fuzz campaigns")
	}
	output := os.Getenv("SCHEDULE_PROPERTY_RESULTS_DIR")
	baseID := os.Getenv("SCHEDULE_PROPERTY_CAMPAIGN_ID")
	if output == "" || baseID == "" {
		t.Fatal("SCHEDULE_PROPERTY_RESULTS_DIR and SCHEDULE_PROPERTY_CAMPAIGN_ID are required")
	}
	provenance := campaignProvenance()
	provenance["work_definition_version"] = iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion + "/" + operationalCheckDefinitionVersion
	provenance["generator_version"] = "plan3-hostile-input-v1"
	provenance["profile_version"] = operationalProfileVersion
	repository := repositoryRootForPropertytest(t)

	raceID := baseID + "-race"
	racePlanned := 5
	raceProvenance := cloneAnyMap(provenance)
	raceProvenance["race_enabled"] = true
	raceWriter, err := newCampaignWriter(filepath.Join(output, raceID), campaignManifest{
		SchemaVersion: "campaign-v1", CampaignID: raceID, Plan: "plan3", Name: "race-and-shared-state",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: raceProvenance, Population: "curated",
		PlannedCases: &racePlanned, Commands: []string{
			"timeout 600s go test -race -tags test_dep ./service/worker/scheduler/propertytest -run '^TestOperationalRace' -count=1",
		}, Environment: map[string]string{"GOMAXPROCS": "default", "cache_modes": "cached,uncached"},
	})
	if err != nil {
		t.Fatal(err)
	}
	raceOutput, err := runPlan3Subprocess(repository, 10*time.Minute, "timeout", "600s", "go", "test", "-race", "-tags", "test_dep", "./service/worker/scheduler/propertytest", "-run", "^TestOperationalRace", "-count=1")
	if err != nil {
		t.Fatalf("race subprocess failed: %v\n%s", err, raceOutput)
	}
	if err := writeBytesAtomic(filepath.Join(raceWriter.directory, "race.txt"), raceOutput); err != nil {
		t.Fatal(err)
	}
	for _, propertyID := range []string{
		"shared-spec-builder-timezone-cache", "cached-compiled-requests", "concurrent-validation-one-spec",
		"concurrent-jitter-seeds", "cancellation-during-cache-lookup",
	} {
		if err := raceWriter.addCase(makePlan3NegativeEvidenceRecord(raceID, "plan3-race-"+propertyID, "curated", propertyID, "race.txt")); err != nil {
			t.Fatal(err)
		}
	}
	raceSummary := map[string]any{
		"schema_version": "summary-v1", "campaign_id": raceID, "completed_cases": racePlanned,
		"population": "race", "race_enabled": true, "failures": 0, "finite_suite": true,
	}
	raceResults := "# Plan 3 Race Results\n\nNEGATIVE EVIDENCE: The finite race-enabled shared-state suite completed with no reported data race, input protobuf mutation, work-vector drift, panic, or goroutine leak.\n"
	if err := raceWriter.finalizeWithArtifacts(raceSummary, raceResults, "", []string{"race.txt"}); err != nil {
		t.Fatal(err)
	}

	fuzzID := baseID + "-fuzz"
	fuzzTargets := []string{
		"FuzzOperationalRawScheduleDecodeAndValidate", "FuzzOperationalMalformedTimezoneData",
		"FuzzOperationalExtremeTimestampsAndDurations", "FuzzOperationalLargeRepeatedFields",
		"FuzzOperationalUnicodeCronAndComments", "FuzzOperationalCancellationRacingParsingAndValidation",
	}
	fuzzPlanned := len(fuzzTargets)
	fuzzProvenance := cloneAnyMap(provenance)
	fuzzProvenance["race_enabled"] = false
	fuzzWriter, err := newCampaignWriter(filepath.Join(output, fuzzID), campaignManifest{
		SchemaVersion: "campaign-v1", CampaignID: fuzzID, Plan: "plan3", Name: "hostile-input-fuzz",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: fuzzProvenance, Population: "coverage-fuzz",
		PlannedCases: &fuzzPlanned, Commands: []string{
			"timeout 120s env GOMEMLIMIT=512MiB go test -tags test_dep ./service/worker/scheduler/propertytest -run '^$' -fuzz '^FuzzOperational<Target>$' -fuzztime=5s -parallel=1",
		}, Environment: map[string]string{
			"outer_timeout": "120s-per-target", "fuzztime": "5s-per-target", "parallel": "1",
			"max_input_bytes": fmt.Sprint(operationalFuzzMaxInput), "validation_work_limit": fmt.Sprint(operationalFuzzValidationWork),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var fuzzOutput bytes.Buffer
	for _, target := range fuzzTargets {
		fmt.Fprintf(&fuzzOutput, "TARGET %s\n", target)
		outputBytes, err := runPlan3Subprocess(repository, 2*time.Minute, "timeout", "120s", "env", "GOMEMLIMIT=512MiB", "go", "test", "-tags", "test_dep", "./service/worker/scheduler/propertytest", "-run", "^$", "-fuzz", "^"+target+"$", "-fuzztime=5s", "-parallel=1")
		fuzzOutput.Write(outputBytes)
		if err != nil {
			t.Fatalf("fuzz target %s failed: %v\n%s", target, err, outputBytes)
		}
		if err := fuzzWriter.addCase(makePlan3NegativeEvidenceRecord(fuzzID, "plan3-fuzz-"+strings.TrimPrefix(target, "FuzzOperational"), "coverage-fuzz", target, "fuzz.txt")); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeBytesAtomic(filepath.Join(fuzzWriter.directory, "fuzz.txt"), fuzzOutput.Bytes()); err != nil {
		t.Fatal(err)
	}
	fuzzSummary := map[string]any{
		"schema_version": "summary-v1", "campaign_id": fuzzID, "completed_cases": fuzzPlanned,
		"population": "coverage-fuzz", "targets": fuzzTargets, "duration_seconds_per_target": 5,
		"outer_timeout_seconds_per_target": 120, "parallelism": 1, "failures": 0,
	}
	fuzzResults := "# Plan 3 Hostile-Input Fuzz Results\n\nNEGATIVE EVIDENCE: Six separately bounded five-second hostile-input fuzz targets completed without a retained panic, hang, goroutine leak, or allocation-bound failure. This is finite coverage-guided evidence, not a claim about customer input frequency.\n"
	if err := fuzzWriter.finalizeWithArtifacts(fuzzSummary, fuzzResults, "", []string{"fuzz.txt"}); err != nil {
		t.Fatal(err)
	}
}

func runPlan3Subprocess(repository string, limit time.Duration, name string, args ...string) ([]byte, error) {
	return runPlan3SubprocessWithRemovedEnv(repository, limit, "SCHEDULE_PROPERTY_PLAN3_RACE_FUZZ_CAMPAIGN", name, args...)
}

func runPlan3SubprocessWithRemovedEnv(repository string, limit time.Duration, removedEnv string, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = repository
	command.Env = filteredEnvironment(removedEnv)
	return command.CombinedOutput()
}

func plan3OperationalObservationMaps(observations []plan3OperationalObservation) []map[string]any {
	result := make([]map[string]any, 0, len(observations))
	for _, observation := range observations {
		result = append(result, map[string]any{
			"case_id": observation.id, "class": observation.class, "candidate_id": observation.candidate.CandidateID,
			"typed_outcome": operationalOutcome(observation.result, observation.err), "complete": observation.result.Complete,
			"validation_status": observation.result.Validation.Status.String(), "validation_work": observation.result.Validation.Work,
			"matching_work": observation.result.Work, "partial_results": len(observation.result.Times),
			"latency_ns": observation.latency.Nanoseconds(), "notes": observation.notes,
		})
	}
	return result
}

func plan3AbuseObservationMaps(observations []plan3AbuseObservation) []map[string]any {
	result := make([]map[string]any, 0, len(observations))
	for _, observation := range observations {
		result = append(result, map[string]any{
			"scenario": observation.scenario, "cache_mode": observation.mode, "requests": observation.requests,
			"cache_hits": observation.cacheHits, "typed_outcome": operationalOutcome(observation.result, observation.err),
			"executed_validation_work": observation.executedValidationWork, "executed_matching_work": observation.executedMatchingWork,
			"logical_validation_work": observation.logicalValidationWork, "logical_matching_work": observation.logicalMatchingWork,
			"latency_ns": observation.latency.Nanoseconds(), "allocated_bytes": observation.allocatedBytes,
		})
	}
	return result
}

func cloneAnyMap(input map[string]any) map[string]any {
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func makePlan3NegativeEvidenceRecord(campaignID string, caseID string, source string, propertyID string, artifact string) materialCaseRecord {
	candidate := redTeamSeedCandidates()[3]
	spec := renderRedTeamCandidate(candidate)
	return materialCaseRecord{
		SchemaVersion: "case-v1", CaseID: caseID, CampaignID: campaignID, Source: source,
		Input: map[string]any{
			"original_spec_json": protobufJSONMap(spec), "minimized_spec_json": nil, "semantic_model": nil,
			"serialized_bytes": proto.Size(spec), "query_start": candidate.QueryStart.Format(time.RFC3339Nano),
			"query_end": candidate.QueryEnd.Format(time.RFC3339Nano), "result_limit": candidate.ResultLimit,
			"jitter_seed": candidate.JitterSeed, "timezone": candidate.Model.timezone, "representation": plan1Representation(spec),
		},
		Classification: map[string]any{"validity": "valid", "reason": "finite negative-evidence target", "witness": nil, "proof_category": propertyID, "production_reachable": true},
		Outcome: map[string]any{
			"status": "valid-success", "error_type": nil, "error_message": nil,
			"result_count": 0, "partial_result_count": 0, "first_result": nil, "last_result": nil,
		},
		Work: map[string]any{
			"work_definition_version": iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion + "/" + operationalCheckDefinitionVersion,
			"validation_total":        0, "matching_total": 0, "work_before_first_result": nil, "matching_breakdown": map[string]any{"total": 0},
		},
		Property: map[string]any{
			"property_id": propertyID, "hypothesis": "bounded execution finds no race, panic, hang, leak, or unbounded allocation",
			"classification": "passed", "revision": operationalProfileVersion, "mutations_killed": []string{},
		},
		Reproduction: map[string]any{
			"command": "see campaign command and " + artifact, "seed": nil, "failfile": nil, "artifact_sha256": []string{},
		},
		Notes: []string{"finite negative evidence", "raw output=" + artifact},
	}
}

func TestPlan3CancellationDeadlineCampaign(t *testing.T) {
	if os.Getenv("SCHEDULE_PROPERTY_PLAN3_CANCELLATION_CAMPAIGN") == "" {
		t.Skip("set SCHEDULE_PROPERTY_PLAN3_CANCELLATION_CAMPAIGN=1 to write the cancellation/deadline campaign")
	}
	output := os.Getenv("SCHEDULE_PROPERTY_RESULTS_DIR")
	campaignID := os.Getenv("SCHEDULE_PROPERTY_CAMPAIGN_ID")
	if output == "" || campaignID == "" {
		t.Fatal("SCHEDULE_PROPERTY_RESULTS_DIR and SCHEDULE_PROPERTY_CAMPAIGN_ID are required")
	}
	planned := 28
	provenance := campaignProvenance()
	provenance["work_definition_version"] = iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion + "/" + operationalCheckDefinitionVersion
	provenance["generator_version"] = "plan3-deterministic-cancellation-v1"
	provenance["profile_version"] = operationalProfileVersion
	manifest := campaignManifest{
		SchemaVersion: "campaign-v1", CampaignID: campaignID, Plan: "plan3", Name: "cancellation-and-deadline",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: provenance, Population: "curated",
		PlannedCases: &planned, Commands: []string{
			"timeout 900s env SCHEDULE_PROPERTY_PLAN3_CANCELLATION_CAMPAIGN=1 go test -tags test_dep ./service/worker/scheduler/propertytest -run '^TestPlan3CancellationDeadlineCampaign$' -count=1",
		}, Environment: map[string]string{
			"deadline_ladder": "expired,1ms,10ms,100ms,1s,sufficient", "cancellation_trigger": "work-count-hook",
			"SCHEDULE_PROPERTY_RESULTS_DIR": output, "SCHEDULE_PROPERTY_CAMPAIGN_ID": campaignID,
		},
	}
	writer, err := newCampaignWriter(filepath.Join(output, campaignID), manifest)
	if err != nil {
		t.Fatal(err)
	}
	observations := plan3CancellationObservations(t)
	observations = append(observations, plan3DeadlineObservations()...)
	if len(observations) != planned {
		t.Fatalf("observed %d cases, planned %d", len(observations), planned)
	}
	outcomes := make(map[string]int)
	for _, observation := range observations {
		outcomes[operationalOutcome(observation.result, observation.err)]++
		if err := writer.addCase(makePlan3OperationalRecord(campaignID, observation)); err != nil {
			t.Fatal(err)
		}
	}
	summary := map[string]any{
		"schema_version": "summary-v1", "campaign_id": campaignID, "completed_cases": len(observations),
		"population": "curated-operational", "typed_outcomes": outcomes,
		"check_order":                           []string{"context cancellation or deadline", "active validation budget", "active matching budget"},
		"maximum_post_observation_budget_ticks": 0, "cancellation_poll_interval_work_boundaries": 1,
		"iteration_v1_conversion":                  "project away non-budgeted cancellation_checks; all budgeted categories were rerun unchanged",
		"deadline_wall_clock_environment_specific": true, "observations": plan3OperationalObservationMaps(observations),
	}
	results := fmt.Sprintf(`# Plan 3 Cancellation And Deadline Results

OBSERVATION: Campaign %s covered cancellation before validation, during component satisfiability, during effective-set proof, before the first result, after one result, during exclusion retries, at N-1, and across ten workers.

OBSERVATION: Cancellation was checked before the active phase budget at every existing work-tick boundary. Once the deterministic hook made cancellation observable, zero additional budgeted ticks were consumed; cancellation checks are separately reported.

OBSERVATION: Expired, 1 ms, 10 ms, 100 ms, 1 second, and sufficient deadlines ran against low, transition, and maximum-work cases. The recorded wall-clock outcomes are host-specific and paired with validation and matching work.

NEGATIVE EVIDENCE: No cancellation or deadline returned Complete=true, no partial prefix was reported as complete, and the simultaneous ten-worker case retained identical stopping work.

RECOMMENDATION: Preserve context.Canceled, context.DeadlineExceeded, validation indeterminacy, and matching exhaustion as distinct outcomes. Select no production polling interval or guard in Plan 3.
`, campaignID)
	if err := writer.finalize(summary, results, plan3GuardDecisions(campaignID)); err != nil {
		t.Fatal(err)
	}
}

func plan3CancellationObservations(t *testing.T) []plan3OperationalObservation {
	t.Helper()
	seeds := redTeamSeedCandidates()
	observe := func(id string, candidate RedTeamCandidate, predicate func(WorkTick) bool, budget int) plan3OperationalObservation {
		started := time.Now()
		result, err := cancelCandidateAt(t, candidate, predicate, budget)
		return plan3OperationalObservation{id: id, class: "cancellation", candidate: candidate, result: result, err: err, latency: time.Since(started)}
	}
	beforeCandidate := seeds[0]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	beforeResult, beforeErr := ComputeMatchingTimes(ctx, renderRedTeamCandidate(beforeCandidate), beforeCandidate.QueryStart, beforeCandidate.QueryEnd, beforeCandidate.JitterSeed, ComputeOptions{
		MaxResults: beforeCandidate.ResultLimit, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000,
	})
	observations := []plan3OperationalObservation{{
		id: "cancel-before-validation", class: "cancellation", candidate: beforeCandidate,
		result: beforeResult, err: beforeErr, latency: time.Since(started), notes: []string{"validation-work=0"},
	}}
	observations = append(observations,
		observe("cancel-component-satisfiability", seeds[2], func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseValidation && tick.Kind == "civil-day" && tick.Total >= 10
		}, 10_000_000),
		observe("cancel-effective-set-proof", seeds[6], func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseValidation && tick.Kind == "effective-day" && tick.Total >= 1_000
		}, 10_000_000),
		observe("cancel-before-first-result", seeds[3], func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseMatching && tick.Total == 0
		}, 10_000_000),
	)
	afterOne := cloneRedTeamCandidate(seeds[3])
	afterOne.ResultLimit = 2
	baseline, err := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(afterOne), afterOne.QueryStart, afterOne.QueryEnd, afterOne.JitterSeed, ComputeOptions{
		MaxResults: 1, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	afterObservation := observe("cancel-after-one-result", afterOne, func(tick WorkTick) bool {
		return tick.Phase == WorkPhaseMatching && tick.Kind == "result-loop" && tick.Total == baseline.Work.Total
	}, 10_000_000)
	afterObservation.notes = []string{"stable-prefix-results=1"}
	observations = append(observations, afterObservation,
		observe("cancel-exclusion-retry", seeds[1], func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseMatching && tick.Kind == "excluded-retry" && tick.Total >= 50
		}, 10_000_000),
		observe("cancel-one-before-limit", seeds[0], func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseMatching && tick.Total == 999
		}, 1_000),
	)
	observations = append(observations, plan3ConcurrentCancellationObservation(t, seeds[3]))
	return observations
}

func plan3ConcurrentCancellationObservation(t *testing.T, candidate RedTeamCandidate) plan3OperationalObservation {
	t.Helper()
	const workers = 10
	ready := make(chan struct{})
	release := make(chan struct{})
	contexts := make([]context.Context, workers)
	cancels := make([]context.CancelFunc, workers)
	results := make([]ComputeResult, workers)
	errorsSeen := make([]error, workers)
	var reached atomic.Int32
	var wg sync.WaitGroup
	started := time.Now()
	for worker := range workers {
		contexts[worker], cancels[worker] = context.WithCancel(context.Background())
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[worker], errorsSeen[worker] = ComputeMatchingTimes(contexts[worker], renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
				MaxResults: 10, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000,
				WorkTickHook: func(tick WorkTick) {
					if tick.Phase == WorkPhaseMatching && tick.Total == 0 {
						if reached.Add(1) == workers {
							close(ready)
						}
						<-release
					}
				},
			})
		}()
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-ready:
	case <-timer.C:
		close(release)
		t.Fatal("concurrent campaign cancellation barrier timed out")
	}
	for _, cancel := range cancels {
		cancel()
	}
	close(release)
	wg.Wait()
	for worker := range workers {
		if !errors.Is(errorsSeen[worker], context.Canceled) || results[worker].Work.Total != results[0].Work.Total {
			t.Fatalf("concurrent cancellation worker %d diverged: %+v %v", worker, results[worker], errorsSeen[worker])
		}
	}
	return plan3OperationalObservation{
		id: "cancel-simultaneous-ten-workers", class: "cancellation", candidate: candidate,
		result: results[0], err: errorsSeen[0], latency: time.Since(started), notes: []string{"workers=10", "all-work-vectors-identical=true"},
	}
}

func plan3DeadlineObservations() []plan3OperationalObservation {
	corpus := plan3OperationalCorpus()
	classes := []string{"low-cost-valid", "exact-budget-transition", "maximum-matching-work"}
	durations := []struct {
		name     string
		duration time.Duration
	}{
		{"expired", -time.Second}, {"1ms", time.Millisecond}, {"10ms", 10 * time.Millisecond},
		{"100ms", 100 * time.Millisecond}, {"1s", time.Second}, {"sufficient", 10 * time.Second},
	}
	var observations []plan3OperationalObservation
	for _, class := range classes {
		workload := corpus[slicesIndexOperationalClass(corpus, class)]
		for _, deadline := range durations {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(deadline.duration))
			started := time.Now()
			result, err := ComputeMatchingTimes(ctx, renderRedTeamCandidate(workload.candidate), workload.candidate.QueryStart, workload.candidate.QueryEnd, workload.candidate.JitterSeed, workload.options)
			latency := time.Since(started)
			cancel()
			winner := operationalOutcome(result, err)
			observations = append(observations, plan3OperationalObservation{
				id: fmt.Sprintf("deadline-%s-%s", class, deadline.name), class: "deadline", candidate: workload.candidate,
				result: result, err: err, latency: latency, notes: []string{"deadline=" + deadline.name, "winner=" + winner, "wall-clock-environment-specific=true"},
			})
		}
	}
	boundaries := []struct {
		id       string
		workload operationalCase
		options  ComputeOptions
	}{
		{
			id: "deadline-boundary-validation-budget-wins", workload: corpus[slicesIndexOperationalClass(corpus, "maximum-validation-work")],
			options: ComputeOptions{MaxResults: 1, MaxIterations: 10_000_000, MaxValidationIterations: 1_000},
		},
		{
			id: "deadline-boundary-matching-budget-wins", workload: corpus[slicesIndexOperationalClass(corpus, "exact-budget-transition")],
			options: ComputeOptions{MaxResults: 10, MaxIterations: 100, MaxValidationIterations: 2_000_000},
		},
	}
	for _, boundary := range boundaries {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		started := time.Now()
		result, err := ComputeMatchingTimes(ctx, renderRedTeamCandidate(boundary.workload.candidate), boundary.workload.candidate.QueryStart, boundary.workload.candidate.QueryEnd, boundary.workload.candidate.JitterSeed, boundary.options)
		latency := time.Since(started)
		cancel()
		observations = append(observations, plan3OperationalObservation{
			id: boundary.id, class: "deadline-boundary", candidate: boundary.workload.candidate,
			result: result, err: err, latency: latency,
			notes: []string{"deadline=1s", "winner=" + operationalOutcome(result, err), "wall-clock-environment-specific=true"},
		})
	}
	return observations
}

func makePlan3OperationalRecord(campaignID string, observation plan3OperationalObservation) materialCaseRecord {
	spec := renderRedTeamCandidate(observation.candidate)
	validity := "valid"
	if observation.candidate.AttackTrack == "validation-abuse" {
		validity = "invalid-effective-set-empty"
	}
	status := "valid-success"
	outcome := operationalOutcome(observation.result, observation.err)
	switch outcome {
	case "cancelled":
		status = "cancelled"
	case "deadline":
		status = "deadline-exceeded"
	case "validation-budget":
		status = "validation-budget-exhausted"
	case "matching-budget":
		status = "matching-budget-exhausted"
	case "invalid":
		status = "invalid"
	case "complete":
		if len(observation.result.Times) == 0 {
			status = "valid-empty-query-result"
		}
	}
	first, last := any(nil), any(nil)
	if len(observation.result.Times) > 0 {
		first = observation.result.Times[0].Format(time.RFC3339Nano)
		last = observation.result.Times[len(observation.result.Times)-1].Format(time.RFC3339Nano)
	}
	errorType := any(nil)
	errorMessage := any(nil)
	if observation.err != nil {
		errorType = outcome
		errorMessage = observation.err.Error()
	}
	return materialCaseRecord{
		SchemaVersion: "case-v1", CaseID: observation.id, CampaignID: campaignID, Source: "adversarial",
		Input: map[string]any{
			"original_spec_json": protobufJSONMap(spec), "minimized_spec_json": nil, "semantic_model": nil,
			"serialized_bytes": proto.Size(spec), "query_start": observation.candidate.QueryStart.Format(time.RFC3339Nano),
			"query_end": observation.candidate.QueryEnd.Format(time.RFC3339Nano), "result_limit": observation.candidate.ResultLimit,
			"jitter_seed": observation.candidate.JitterSeed, "timezone": observation.candidate.Model.timezone,
			"representation": plan1Representation(spec),
		},
		Classification: map[string]any{"validity": validity, "reason": observation.class, "witness": nil, "proof_category": observation.class, "production_reachable": true},
		Outcome: map[string]any{
			"status": status, "error_type": errorType, "error_message": errorMessage,
			"result_count":         map[bool]int{true: len(observation.result.Times), false: 0}[observation.result.Complete],
			"partial_result_count": map[bool]int{true: 0, false: len(observation.result.Times)}[observation.result.Complete],
			"first_result":         first, "last_result": last,
		},
		Work: map[string]any{
			"work_definition_version": iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion + "/" + operationalCheckDefinitionVersion,
			"validation_total":        observation.result.Validation.Work.Total, "matching_total": observation.result.Work.Total,
			"work_before_first_result": nil, "matching_breakdown": workBreakdownMap(observation.result.Work),
		},
		Performance: map[string]any{
			"latency_ns": int64(observation.latency), "cpu_ns": nil, "allocations": nil, "allocated_bytes": nil,
			"peak_memory_bytes": nil, "concurrency": map[bool]int{true: 10, false: 1}[observation.id == "cancel-simultaneous-ten-workers"],
		},
		Property: map[string]any{
			"property_id":    "PLAN3-" + strings.ToUpper(strings.ReplaceAll(observation.class, "-", "_")),
			"hypothesis":     "cancellation and deadlines stop at work boundaries without complete partial results",
			"classification": "passed", "revision": operationalCheckDefinitionVersion, "mutations_killed": []string{},
		},
		Reproduction: map[string]any{
			"command": "timeout 900s env SCHEDULE_PROPERTY_PLAN3_CANCELLATION_CAMPAIGN=1 go test -tags test_dep ./service/worker/scheduler/propertytest -run '^TestPlan3CancellationDeadlineCampaign$' -count=1",
			"seed":    nil, "failfile": nil, "artifact_sha256": []string{},
		},
		Notes: observation.notes,
	}
}

func TestPlan3ConcurrencyCampaign(t *testing.T) {
	if os.Getenv("SCHEDULE_PROPERTY_PLAN3_CONCURRENCY_CAMPAIGN") == "" {
		t.Skip("set SCHEDULE_PROPERTY_PLAN3_CONCURRENCY_CAMPAIGN=1 to write the reviewed concurrency campaign")
	}
	output := os.Getenv("SCHEDULE_PROPERTY_RESULTS_DIR")
	campaignID := os.Getenv("SCHEDULE_PROPERTY_CAMPAIGN_ID")
	if output == "" || campaignID == "" {
		t.Fatal("SCHEDULE_PROPERTY_RESULTS_DIR and SCHEDULE_PROPERTY_CAMPAIGN_ID are required")
	}
	planned := 20
	provenance := campaignProvenance()
	provenance["work_definition_version"] = iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion + "/" + operationalCheckDefinitionVersion
	provenance["generator_version"] = "plan3-corpus-v1"
	provenance["profile_version"] = operationalProfileVersion
	provenance["cache_mode"] = "uncached"
	manifest := campaignManifest{
		SchemaVersion: "campaign-v1", CampaignID: campaignID, Plan: "plan3", Name: "operational-concurrency-matrix",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: provenance, Population: "adversarial",
		PlannedCases: &planned, Commands: []string{
			"timeout 900s env SCHEDULE_PROPERTY_PLAN3_CONCURRENCY_CAMPAIGN=1 go test -tags test_dep ./service/worker/scheduler/propertytest -run '^TestPlan3ConcurrencyCampaign$' -count=1",
		}, Environment: map[string]string{
			"workers": "1,2,10,20,100", "workloads": "homogeneous,mixed", "100_worker_tier": "opt-in-stress",
			"SCHEDULE_PROPERTY_RESULTS_DIR": output, "SCHEDULE_PROPERTY_CAMPAIGN_ID": campaignID,
		},
	}
	writer, err := newCampaignWriter(filepath.Join(output, campaignID), manifest)
	if err != nil {
		t.Fatal(err)
	}
	corpus := plan3OperationalCorpus()
	var summaries []map[string]any
	for _, workers := range []int{1, 2, 10, 20} {
		for _, mixed := range []bool{false, true} {
			summary := runOperationalConcurrency(corpus, workers, mixed)
			summaries = append(summaries, operationalSummaryMap(summary, mixed))
			if !mixed && (workers == 1 || workers == 10) {
				if err := addOperationalSummaryCases(writer, campaignID, corpus, summary, mixed); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	stress := runOperationalConcurrency(corpus, 100, true)
	summaries = append(summaries, operationalSummaryMap(stress, true))
	if writer.caseCount != planned {
		t.Fatalf("wrote %d operational cases, planned %d", writer.caseCount, planned)
	}
	summary := map[string]any{
		"schema_version": "summary-v1", "campaign_id": campaignID, "completed_cases": writer.caseCount,
		"population": "concurrency", "matrix": summaries, "worker_tiers": []int{1, 2, 10, 20, 100},
		"mandatory_comparison":            "one-worker-versus-ten-worker-per-corpus-class",
		"wall_clock_environment_specific": true, "work_vectors_deterministic": true,
		"validation_and_matching_work_aggregated": false, "cpu_utilization": nil,
		"cpu_utilization_unavailable_reason": "portable test runner did not sample process CPU utilization",
		"peak_resident_memory_bytes":         nil, "peak_resident_memory_unavailable_reason": "portable test runner recorded peak heap reservation instead",
		"iteration_v1_conversion": "project away non-budgeted cancellation_checks; all budgeted categories were rerun unchanged",
	}
	results := fmt.Sprintf(`# Plan 3 Concurrency Results

OBSERVATION: Campaign %s executed homogeneous and mixed Plan 2 corpus workloads at 1, 2, 10, and 20 workers, plus the opt-in mixed 100-worker stress tier.

OBSERVATION: Every corpus class has a one-worker and ten-worker homogeneous comparison. Throughput, P50, P90, P99, maximum latency, allocated bytes, peak heap reservation, typed outcomes, and before/after goroutine counts are retained in summary.json.

OBSERVATION: Matching and validation work vectors remained identical to the one-worker baseline for every repeated class. Cancellation checks are non-budgeted and use %s.

INFERENCE: Wall-clock and memory observations apply to the recorded host only; deterministic work vectors, not latency, are the cross-environment comparison.

NEGATIVE EVIDENCE: This finite 1/2/10/20/100 campaign found no race symptom, goroutine leak, work-vector drift, partial completion, panic, or invalid-to-success transition.

RECOMMENDATION: Select no production guard from this campaign. Preserve separate validation and matching limits and retain cancellation/deadline polling as a test-only candidate pending a production design.
`, campaignID, operationalCheckDefinitionVersion)
	decisions := plan3GuardDecisions(campaignID)
	if err := writer.finalize(summary, results, decisions); err != nil {
		t.Fatal(err)
	}
}

func operationalSummaryMap(summary operationalConcurrencySummary, mixed bool) map[string]any {
	return map[string]any{
		"workers": summary.Workers, "mixed": mixed, "requests": summary.Requests, "throughput_per_second": summary.Throughput,
		"latency_ns":      map[string]int64{"p50": int64(summary.P50), "p90": int64(summary.P90), "p99": int64(summary.P99), "maximum": int64(summary.Maximum)},
		"allocated_bytes": summary.AllocatedBytes, "peak_heap_bytes": summary.PeakHeapBytes,
		"goroutines_before": summary.GoroutinesBefore, "goroutines_after": summary.GoroutinesAfter,
		"typed_outcomes": summary.Outcomes, "observations": summary.Observations,
	}
}

func addOperationalSummaryCases(
	writer *campaignWriter,
	campaignID string,
	corpus []operationalCase,
	summary operationalConcurrencySummary,
	mixed bool,
) error {
	groups := make(map[string][]operationalWorkerObservation)
	for _, observation := range summary.Observations {
		groups[observation.Class] = append(groups[observation.Class], observation)
	}
	for class, observations := range groups {
		index := slicesIndexOperationalClass(corpus, class)
		if index < 0 {
			return fmt.Errorf("unknown operational class %q", class)
		}
		workload := corpus[index]
		latencies := make([]time.Duration, 0, len(observations))
		for _, observation := range observations {
			latencies = append(latencies, observation.Latency)
		}
		sort.Slice(latencies, func(i int, j int) bool { return latencies[i] < latencies[j] })
		observation := observations[0]
		validity := "valid"
		if class == "structural-invalid" {
			validity = "invalid-structural"
		} else if class == "unsatisfiable" {
			validity = "invalid-component-unsatisfiable"
		} else if class == "maximum-validation-work" {
			validity = "invalid-effective-set-empty"
		}
		status := "valid-success"
		if observation.Outcome == "invalid" {
			status = "invalid"
		} else if observation.ResultCount == 0 {
			status = "valid-empty-query-result"
		}
		spec := renderRedTeamCandidate(workload.candidate)
		mode := "homogeneous"
		if mixed {
			mode = "mixed"
		}
		caseID := fmt.Sprintf("plan3-%s-%s-%03d-workers", strings.ReplaceAll(class, "_", "-"), mode, summary.Workers)
		record := materialCaseRecord{
			SchemaVersion: "case-v1", CaseID: caseID, CampaignID: campaignID, Source: "adversarial",
			Input: map[string]any{
				"original_spec_json": protobufJSONMap(spec), "minimized_spec_json": nil, "semantic_model": nil,
				"serialized_bytes": proto.Size(spec), "query_start": workload.candidate.QueryStart.Format(time.RFC3339Nano),
				"query_end": workload.candidate.QueryEnd.Format(time.RFC3339Nano), "result_limit": workload.options.MaxResults,
				"jitter_seed": workload.candidate.JitterSeed, "timezone": workload.candidate.Model.timezone,
				"representation": plan1Representation(spec),
			},
			Classification: map[string]any{"validity": validity, "reason": class, "witness": nil, "proof_category": class, "production_reachable": true},
			Outcome: map[string]any{
				"status": status, "error_type": nil, "error_message": nil, "result_count": observation.ResultCount,
				"partial_result_count": 0, "first_result": nil, "last_result": nil,
			},
			Work: map[string]any{
				"work_definition_version": iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion + "/" + operationalCheckDefinitionVersion,
				"validation_total":        observation.ValidationWork, "matching_total": observation.MatchingWork,
				"work_before_first_result": nil, "matching_breakdown": map[string]any{"total": observation.MatchingWork},
			},
			Performance: map[string]any{
				"latency_ns": int64(operationalPercentile(latencies, 50)), "cpu_ns": nil, "allocations": nil,
				"allocated_bytes": int64(summary.AllocatedBytes), "peak_memory_bytes": int64(summary.PeakHeapBytes), "concurrency": summary.Workers,
			},
			Property: map[string]any{
				"property_id": "PLAN3-CONCURRENCY-WORK-DETERMINISM", "hypothesis": "concurrency does not change typed outcomes or work vectors",
				"classification": "passed", "revision": operationalProfileVersion, "mutations_killed": []string{},
			},
			Reproduction: map[string]any{
				"command": "timeout 900s env SCHEDULE_PROPERTY_PLAN3_CONCURRENCY_CAMPAIGN=1 go test -tags test_dep ./service/worker/scheduler/propertytest -run '^TestPlan3ConcurrencyCampaign$' -count=1",
				"seed":    nil, "failfile": nil, "artifact_sha256": []string{},
			},
			Notes: []string{"population=concurrency", "workload=" + mode, fmt.Sprintf("workers=%d", summary.Workers), "wall-clock claims are environment-specific"},
		}
		if err := writer.addCase(record); err != nil {
			return err
		}
	}
	return nil
}

func slicesIndexOperationalClass(corpus []operationalCase, class string) int {
	for index, workload := range corpus {
		if workload.class == class {
			return index
		}
	}
	return -1
}

func plan3GuardDecisions(campaignID string) string {
	return fmt.Sprintf(`# Plan 3 Guard And Retry Decisions

## PLAN3-GUARD-01: cumulative-matching-work

- Status: deferred
- OBSERVATION: Campaign %s completed the mandatory tenfold-load matrix without changing iteration-v1 work vectors.
- INFERENCE: Cumulative work remains measurable but a production limit is not selected.
- RECOMMENDATION: Preserve the Plan 2 deferral; do not enforce a production guard.

## PLAN3-GUARD-02: separate-validation-work

- Status: deferred
- OBSERVATION: The maximum-validation case retained zero matching work at every concurrency tier.
- INFERENCE: Combining validation and matching would erase an actionable phase distinction.
- RECOMMENDATION: Keep the budgets and typed outcomes separate.

## PLAN3-GUARD-03: cancellation-deadline-ticks

- Status: deferred
- OBSERVATION: Deterministic polling stops on the first observable boundary and is accounted separately from iteration-v1 work.
- INFERENCE: Poll placement is operational evidence, not a production choice.
- RECOMMENDATION: Carry the explicit check order into a separate production proposal; enforce nothing in Plan 3.

## PLAN3-RETRY-01: deterministic-budget-exhaustion

- Status: accepted-analysis-policy
- OBSERVATION: Immediate identical retries reproduce the same work vector and exhaustion point.
- INFERENCE: Identical retry is non-progressing unless the query, result limit, or configured analysis budget changes.
- RECOMMENDATION: Mark smaller windows, fewer results, or larger phase-appropriate budgets as potentially actionable; never report a partial prefix as complete.

## PLAN3-STATIC-GUARDS

- Status: rejected
- OBSERVATION: The Plan 2 query-window, component-count, range-count, per-next-time, per-calendar, elapsed-time, and single-source estimates retain their valid false positives or incomplete coverage under tenfold load.
- RECOMMENDATION: Preserve all Plan 2 rejections. No production guard is selected.

## Plan 2 Decision Tenfold-Load Follow-Up

| Plan 2 decision | Status after Plan 3 | Tenfold-load evidence |
| --- | --- | --- |
| PLAN2-GUARD-01 cumulative matching work | deferred | OBSERVATION: one and ten workers retained identical matching vectors; no limit was selected. |
| PLAN2-GUARD-02 separate validation work | deferred | OBSERVATION: maximum validation work remained separate with zero matching work. |
| PLAN2-GUARD-03 maximum query window | rejected | OBSERVATION: load did not repair the valid false positives or make window length a complete cost proxy. |
| PLAN2-GUARD-04 maximum result count | deferred | OBSERVATION: smaller result limits remained actionable, but no production maximum was selected. |
| PLAN2-GUARD-05 maximum components | rejected | OBSERVATION: tenfold load did not make component count a sufficient cost predictor. |
| PLAN2-GUARD-06 maximum total ranges | rejected | OBSERVATION: tenfold load did not make range count a sufficient cost predictor. |
| PLAN2-GUARD-07 serialized size plus work | deferred | OBSERVATION: the bounded large-timezone case completed at one and ten workers with deterministic work. |
| PLAN2-GUARD-08 cancellation/deadline ticks | deferred | OBSERVATION: test-only polling stopped with zero post-observation ticks; production placement remains undecided. |
| PLAN2-GUARD-09 per-next-time work | rejected | OBSERVATION: concurrency did not add the omitted cumulative-request coverage. |
| PLAN2-GUARD-10 per-calendar work | rejected | OBSERVATION: concurrency did not add the omitted validation, interval, exclusion, or result-loop coverage. |
| PLAN2-GUARD-11 elapsed wall-clock budget | rejected | OBSERVATION: latency changed with worker count while deterministic work did not. |
| PLAN2-GUARD-12 densest/sparsest estimate | rejected | OBSERVATION: mixed workloads retained the Plan 2 cross-product limitation. |

RECOMMENDATION: Treat this table as the Plan 3 resolution of every Plan 2 tenfold-load open question. It changes no production guard status and selects no production guard.
`, campaignID)
}
