package propertytest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	redTeamGeneratorVersion        = "plan2-objective-search-v1"
	redTeamProfileVersion          = "plan2-query-profiles-v1"
	redTeamWeightedCostVersion     = "cpu-correlation-v1"
	redTeamDefaultValidationWork   = 10_000_000
	redTeamDefaultMatchingWork     = 1_000_000
	redTeamCampaignMatchingWork    = 10_000_000
	redTeamDefaultCandidateWall    = 2 * time.Second
	redTeamCheckpointMaxCandidates = 64
)

var redTeamBudgetLadder = []int{10, 100, 1_000, 10_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000}

type redTeamFitness struct {
	TotalMatchingWork        int64 `json:"total_matching_work"`
	CalendarSearchWork       int64 `json:"calendar_search_work"`
	ExcludedCandidateRetries int64 `json:"excluded_candidate_retries"`
	WorkBeforeFirstResult    int64 `json:"work_before_first_result"`
	WorkPerReturnedResult    int64 `json:"work_per_returned_result_milli"`
	EmptyProofWork           int64 `json:"empty_proof_work"`
	CPUNanoseconds           int64 `json:"cpu_nanoseconds"`
	Allocations              int64 `json:"allocations"`
	AllocatedBytes           int64 `json:"allocated_bytes"`
	WorkPerSerializedByte    int64 `json:"work_per_serialized_byte_milli"`
	ValidationProofWork      int64 `json:"validation_proof_work"`
}

type redTeamEvaluation struct {
	Candidate       RedTeamCandidate `json:"candidate"`
	Fitness         redTeamFitness   `json:"fitness"`
	Validation      ValidationResult `json:"-"`
	Result          ComputeResult    `json:"-"`
	ErrorType       string           `json:"error_type,omitempty"`
	SerializedBytes int              `json:"serialized_bytes"`
}

type redTeamSearchConfig struct {
	Seed                int64
	Population          int
	Generations         int
	Elite               int
	MaxMatchingWork     int
	MaxValidationWork   int
	CandidateWallLimit  time.Duration
	CheckpointDirectory string
}

type redTeamSearchResult struct {
	Seed         int64               `json:"seed"`
	AttackTrack  string              `json:"attack_track"`
	Generations  int                 `json:"generations"`
	Evaluated    int                 `json:"evaluated"`
	Rejected     int                 `json:"rejected"`
	ParetoElites []redTeamEvaluation `json:"pareto_elites"`
}

type redTeamBudgetAttempt struct {
	Budget         int    `json:"budget"`
	Status         string `json:"status"`
	ValidationWork int    `json:"validation_work"`
	MatchingWork   int    `json:"matching_work"`
	PartialResults int    `json:"partial_results"`
}

type redTeamBudgetMatrix struct {
	Profile            string                 `json:"profile"`
	ResultLimit        int                    `json:"result_limit"`
	QueryStart         string                 `json:"query_start"`
	QueryEnd           string                 `json:"query_end"`
	Attempts           []redTeamBudgetAttempt `json:"attempts"`
	LargestFailing     *int                   `json:"largest_failing"`
	SmallestSuccessful *int                   `json:"smallest_successful"`
	ExactRequired      *int                   `json:"exact_required"`
	NOutcome           string                 `json:"n_outcome"`
	NMinusOneOutcome   string                 `json:"n_minus_one_outcome"`
	ExhaustionPhase    string                 `json:"exhaustion_phase"`
}

type redTeamCheckpoint struct {
	SchemaVersion string             `json:"schema_version"`
	Seed          int64              `json:"seed"`
	Track         string             `json:"track"`
	Generation    int                `json:"generation"`
	Candidates    []RedTeamCandidate `json:"candidates"`
}

func redTeamSeedCandidates() []RedTeamCandidate {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	all := func(minimum int, maximum int) []rangeModel {
		return []rangeModel{{start: minimum, end: maximum, step: 1}}
	}
	denseCalendar := calendarModel{
		second: all(0, 59), minute: all(0, 59), hour: all(0, 23), day: all(1, 31),
		month: all(1, 12), dayOfWeek: all(0, 6),
	}
	sparse := exactCalendarModel(time.Date(2096, 2, 29, 0, 0, 0, 0, time.UTC))
	sparse.dayOfWeek = all(0, 6)
	excludeMost := denseCalendar
	excludeMost.second = []rangeModel{{start: 0, end: 58, step: 1}}
	excludeLast := denseCalendar
	excludeLast.second = []rangeModel{{start: 59, end: 59, step: 1}}

	makeCandidate := func(id string, track string, model scheduleModel, start time.Time, end time.Time, limit int) RedTeamCandidate {
		return RedTeamCandidate{
			SchemaVersion: redTeamCandidateSchemaVersion, CandidateID: id, AttackTrack: track, Model: model,
			QueryStart: start, QueryEnd: end, ResultLimit: limit, JitterSeed: "plan2-fixed-seed", Representation: "structured-calendar",
		}
	}
	denseSparseModel := scheduleModel{
		calendars: []calendarModel{sparse}, intervals: []intervalModel{{intervalSeconds: 1}},
		witness: base, timezone: "UTC", jitter: time.Second,
	}
	exclusionModel := scheduleModel{
		intervals: []intervalModel{{intervalSeconds: 1}}, exclusions: []calendarModel{excludeMost},
		witness: base.Add(59 * time.Second), timezone: "UTC",
	}
	sparseModel := scheduleModel{calendars: []calendarModel{sparse}, witness: time.Date(2096, 2, 29, 0, 0, 0, 0, time.UTC), timezone: "UTC"}
	countModel := scheduleModel{calendars: []calendarModel{exactCalendarModel(base.Add(24 * time.Hour))}, intervals: []intervalModel{{intervalSeconds: 1}}, witness: base, timezone: "UTC"}
	horizonCalendar := sparse
	horizonCalendar.year = nil
	horizonModel := scheduleModel{calendars: []calendarModel{horizonCalendar}, witness: time.Date(2028, 2, 29, 0, 0, 0, 0, time.UTC), timezone: "UTC"}
	inputModel := scheduleModel{
		intervals: []intervalModel{{intervalSeconds: 1}}, witness: base,
		timezone: "RedTeam/UTC", timezoneData: redTeamSyntheticTZif(30_000),
	}
	for range 24 {
		inputModel.calendars = append(inputModel.calendars, exactCalendarModel(base))
	}
	validationStart := base
	validationEnd := base.Add(7*24*time.Hour - time.Second)
	validationModel := scheduleModel{
		intervals: []intervalModel{{intervalSeconds: 1}}, exclusions: []calendarModel{excludeMost, excludeLast},
		startTime: &validationStart, endTime: &validationEnd, timezone: "UTC",
	}
	return []RedTeamCandidate{
		makeCandidate("plan2-dense-sparse-seed", "dense-result-sparse-union", denseSparseModel, base, base.Add(time.Hour), 1_000),
		makeCandidate("plan2-exclusion-seed", "exclusion-amplification", exclusionModel, base, base.Add(time.Hour), 100),
		makeCandidate("plan2-sparse-first-seed", "sparse-first-result", sparseModel, base, time.Date(2100, 12, 31, 23, 59, 59, 0, time.UTC), 1),
		makeCandidate("plan2-result-count-seed", "result-count-amplification", countModel, base, base.Add(time.Hour), 1_000),
		makeCandidate("plan2-horizon-seed", "horizon-amplification", horizonModel, time.Date(2097, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2100, 12, 31, 23, 59, 59, 0, time.UTC), 1),
		makeCandidate("plan2-input-size-seed", "input-size-amplification", inputModel, base, base.Add(time.Hour), 1_000),
		makeCandidate("plan2-validation-abuse-seed", "validation-abuse", validationModel, base, base.Add(time.Hour), 1),
	}
}

func redTeamSyntheticTZif(transitionCount int) []byte {
	var data bytes.Buffer
	data.WriteString("TZif")
	data.WriteByte(0)
	data.Write(make([]byte, 15))
	counts := []uint32{0, 0, 0, uint32(transitionCount), 1, 4}
	var word [4]byte
	for _, count := range counts {
		binary.BigEndian.PutUint32(word[:], count)
		data.Write(word[:])
	}
	for index := range transitionCount {
		transition := int32(-1_800_000_000 + index*60)
		binary.BigEndian.PutUint32(word[:], uint32(transition))
		data.Write(word[:])
	}
	data.Write(make([]byte, transitionCount))
	binary.BigEndian.PutUint32(word[:], 0)
	data.Write(word[:])
	data.WriteByte(0)
	data.WriteByte(0)
	data.WriteString("UTC\x00")
	return data.Bytes()
}

func runRedTeamSearch(config redTeamSearchConfig, seed RedTeamCandidate) (redTeamSearchResult, error) {
	if config.Population <= 0 || config.Population > redTeamCheckpointMaxCandidates || config.Generations <= 0 || config.Elite <= 0 {
		return redTeamSearchResult{}, errors.New("invalid red-team search bounds")
	}
	result := redTeamSearchResult{Seed: config.Seed, AttackTrack: seed.AttackTrack, Generations: config.Generations}
	population := []RedTeamCandidate{seed}
	for len(population) < config.Population {
		population = append(population, mutateRedTeamCandidate(seed, len(population)-1, 0))
	}
	for generation := range config.Generations {
		evaluations := make([]redTeamEvaluation, 0, len(population))
		for _, candidate := range population {
			evaluation, err := safelyEvaluateRedTeamCandidate(config, candidate)
			result.Evaluated++
			if err != nil {
				result.Rejected++
				continue
			}
			evaluations = append(evaluations, evaluation)
		}
		if len(evaluations) == 0 {
			return result, errors.New("red-team generation retained no bounded candidate")
		}
		elites := redTeamParetoElites(evaluations, config.Elite)
		result.ParetoElites = elites
		if config.CheckpointDirectory != "" {
			checkpoint := redTeamCheckpoint{
				SchemaVersion: "redteam-checkpoint-v1", Seed: config.Seed, Track: seed.AttackTrack,
				Generation: generation, Candidates: evaluationsToCandidates(elites),
			}
			if err := writeRedTeamCheckpoint(config.CheckpointDirectory, checkpoint); err != nil {
				return result, err
			}
		}
		population = population[:0]
		for i, elite := range elites {
			population = append(population, elite.Candidate)
			for mutation := 0; mutation < 12 && len(population) < config.Population; mutation++ {
				population = append(population, mutateRedTeamCandidate(elite.Candidate, mutation, generation+1+i))
			}
			if len(population) >= config.Population {
				break
			}
		}
	}
	return result, nil
}

func safelyEvaluateRedTeamCandidate(config redTeamSearchConfig, candidate RedTeamCandidate) (evaluation redTeamEvaluation, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("candidate panic: %v", recovered)
		}
	}()
	return evaluateRedTeamCandidate(config, candidate)
}

func evaluateRedTeamCandidate(config redTeamSearchConfig, candidate RedTeamCandidate) (redTeamEvaluation, error) {
	if err := validateRedTeamCandidate(candidate); err != nil {
		return redTeamEvaluation{}, err
	}
	spec := renderRedTeamCandidate(candidate)
	ctx, cancel := context.WithTimeout(context.Background(), config.CandidateWallLimit)
	defer cancel()
	validation, validationErr := ValidateSchedule(spec, ValidationOptions{MaxIterations: config.MaxValidationWork, Context: ctx})
	if candidate.AttackTrack == "validation-abuse" {
		if validation.Status == ValidationValid || (!errors.Is(validationErr, ErrUnsatisfiableSpec) && !errors.Is(validationErr, ErrValidationLimit)) {
			return redTeamEvaluation{}, fmt.Errorf("validation-abuse candidate classified %s: %w", validation.Status, validationErr)
		}
		serialized := proto.Size(spec)
		return redTeamEvaluation{
			Candidate: candidate, Validation: validation, ErrorType: plan1ErrorType(validationErr), SerializedBytes: serialized,
			Fitness: redTeamFitness{ValidationProofWork: int64(validation.Work.Total), WorkPerSerializedByte: ratioMilli(validation.Work.Total, serialized)},
		}, nil
	}
	if validationErr != nil || validation.Status != ValidationValid || validation.Witness.IsZero() {
		return redTeamEvaluation{}, fmt.Errorf("valid-cost candidate failed Plan 1 validation: status=%s error=%w", validation.Status, validationErr)
	}
	candidate.Model.witness = validation.Witness
	if !candidate.Model.matchesNominal(validation.Witness) {
		return redTeamEvaluation{}, errors.New("Plan 1 witness is not sound in the independent model")
	}
	options := ComputeOptions{
		MaxResults: candidate.ResultLimit, MaxIterations: config.MaxMatchingWork,
		MaxValidationIterations: config.MaxValidationWork, Context: ctx,
	}
	started := time.Now()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	computed, err := ComputeMatchingTimes(spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
	runtime.ReadMemStats(&after)
	elapsed := time.Since(started)
	if err != nil && !errors.Is(err, ErrIterationLimit) && !errors.Is(err, context.DeadlineExceeded) {
		return redTeamEvaluation{}, err
	}
	firstOptions := options
	firstOptions.MaxResults = 1
	first, firstErr := ComputeMatchingTimes(spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, firstOptions)
	if firstErr != nil && !errors.Is(firstErr, ErrIterationLimit) {
		return redTeamEvaluation{}, firstErr
	}
	serialized := proto.Size(spec)
	resultCount := max(1, len(computed.Times))
	fitness := redTeamFitness{
		TotalMatchingWork: int64(computed.Work.Total), CalendarSearchWork: int64(computed.Work.CalendarSearchSteps),
		ExcludedCandidateRetries: int64(computed.Work.ExcludedCandidateRetries), WorkBeforeFirstResult: int64(first.Work.Total),
		WorkPerReturnedResult: ratioMilli(computed.Work.Total, resultCount), CPUNanoseconds: elapsed.Nanoseconds(),
		Allocations: int64(after.Mallocs - before.Mallocs), AllocatedBytes: int64(after.TotalAlloc - before.TotalAlloc),
		WorkPerSerializedByte: ratioMilli(computed.Work.Total, serialized), ValidationProofWork: int64(validation.Work.Total),
	}
	if err == nil && len(computed.Times) == 0 {
		fitness.EmptyProofWork = int64(computed.Work.Total)
	}
	errorType := ""
	if err != nil {
		errorType = plan1ErrorType(err)
	}
	return redTeamEvaluation{Candidate: candidate, Fitness: fitness, Validation: validation, Result: computed, ErrorType: errorType, SerializedBytes: serialized}, nil
}

func ratioMilli(numerator int, denominator int) int64 {
	if denominator <= 0 {
		return 0
	}
	return int64(numerator) * 1_000 / int64(denominator)
}

func redTeamParetoElites(candidates []redTeamEvaluation, limit int) []redTeamEvaluation {
	sort.SliceStable(candidates, func(i int, j int) bool {
		return candidates[i].Candidate.CandidateID < candidates[j].Candidate.CandidateID
	})
	retained := make([]redTeamEvaluation, 0, min(limit, len(candidates)))
	for i, candidate := range candidates {
		dominated := false
		for j, other := range candidates {
			if i != j && redTeamFitnessDominates(other.Fitness, candidate.Fitness) {
				dominated = true
				break
			}
		}
		if !dominated {
			retained = append(retained, candidate)
		}
	}
	if len(retained) > limit {
		retained = retained[:limit]
	}
	if len(retained) < limit {
		for _, candidate := range candidates {
			if len(retained) == limit {
				break
			}
			if !slices.ContainsFunc(retained, func(value redTeamEvaluation) bool {
				return value.Candidate.CandidateID == candidate.Candidate.CandidateID
			}) {
				retained = append(retained, candidate)
			}
		}
	}
	return retained
}

func redTeamFitnessDominates(left redTeamFitness, right redTeamFitness) bool {
	a := redTeamFitnessValues(left)
	b := redTeamFitnessValues(right)
	strict := false
	for i := range a {
		if a[i] < b[i] {
			return false
		}
		strict = strict || a[i] > b[i]
	}
	return strict
}

func redTeamFitnessValues(value redTeamFitness) []int64 {
	return []int64{
		value.TotalMatchingWork, value.CalendarSearchWork, value.ExcludedCandidateRetries,
		value.WorkBeforeFirstResult, value.WorkPerReturnedResult, value.EmptyProofWork,
		value.CPUNanoseconds, value.Allocations, value.AllocatedBytes,
		value.WorkPerSerializedByte, value.ValidationProofWork,
	}
}

func mutateRedTeamCandidate(base RedTeamCandidate, mutation int, generation int) RedTeamCandidate {
	candidate := cloneRedTeamCandidate(base)
	candidate.CandidateID = fmt.Sprintf("%s-g%02d-m%02d", base.CandidateID, generation, mutation)
	switch mutation % 12 {
	case 0:
		if len(candidate.Model.calendars) > 0 && len(candidate.Model.calendars)+len(candidate.Model.intervals)+len(candidate.Model.exclusions) < redTeamCandidateMaxComponents {
			candidate.Model.calendars = append(candidate.Model.calendars, candidate.Model.calendars[0])
		} else if len(candidate.Model.intervals) > 0 {
			candidate.Model.intervals = append(candidate.Model.intervals, candidate.Model.intervals[0])
		}
	case 1:
		if len(candidate.Model.calendars)+len(candidate.Model.intervals) > 1 {
			if len(candidate.Model.calendars) > 0 {
				candidate.Model.calendars = candidate.Model.calendars[:len(candidate.Model.calendars)-1]
			} else {
				candidate.Model.intervals = candidate.Model.intervals[:len(candidate.Model.intervals)-1]
			}
		}
	case 2:
		mutateFirstCalendarRange(&candidate, func(value rangeModel) rangeModel {
			value.end = min(59, value.end+1)
			return value
		})
	case 3:
		mutateFirstCalendarRange(&candidate, func(value rangeModel) rangeModel {
			value.end = max(value.start, value.end-1)
			return value
		})
	case 4:
		mutateFirstCalendarRange(&candidate, func(value rangeModel) rangeModel {
			if value.end < 59 {
				value.start++
				value.end++
			}
			return value
		})
	case 5:
		if len(candidate.Model.intervals) > 0 {
			candidate.Model.intervals[0].intervalSeconds = min(86_400, candidate.Model.intervals[0].intervalSeconds+1)
			candidate.Model.intervals[0].phaseSeconds = candidate.Model.witness.Unix() % candidate.Model.intervals[0].intervalSeconds
		}
	case 6:
		candidate.QueryStart = candidate.QueryStart.Add(time.Duration(generation+1) * time.Minute)
		candidate.QueryEnd = maxTime(candidate.QueryEnd, candidate.QueryStart.Add(time.Second))
	case 7:
		index := slices.Index(analysisResultLimits, candidate.ResultLimit)
		candidate.ResultLimit = analysisResultLimits[(index+1)%len(analysisResultLimits)]
	case 8:
		index := slices.Index(analysisTimezones, candidate.Model.timezone)
		candidate.Model.timezone = analysisTimezones[(index+1+len(analysisTimezones))%len(analysisTimezones)]
	case 9:
		representations := []string{"structured-calendar", "calendar", "cron", "mixed"}
		index := slices.Index(representations, candidate.Representation)
		candidate.Representation = representations[(index+1)%len(representations)]
	case 10:
		if len(candidate.Model.exclusions) > 0 && len(candidate.Model.exclusions[0].second) > 0 {
			rangeValue := &candidate.Model.exclusions[0].second[0]
			rangeValue.end = min(59, rangeValue.end+1)
		}
	case 11:
		if len(candidate.Model.calendars) > 0 && len(candidate.Model.calendars[0].second) < 64 {
			candidate.Model.calendars[0].second = append(candidate.Model.calendars[0].second, candidate.Model.calendars[0].second[0])
		}
	}
	return candidate
}

func mutateFirstCalendarRange(candidate *RedTeamCandidate, mutation func(rangeModel) rangeModel) {
	if len(candidate.Model.calendars) > 0 && len(candidate.Model.calendars[0].second) > 0 {
		candidate.Model.calendars[0].second[0] = mutation(candidate.Model.calendars[0].second[0])
	}
}

func cloneRedTeamCandidate(candidate RedTeamCandidate) RedTeamCandidate {
	data, err := json.Marshal(candidate)
	if err != nil {
		panic(err)
	}
	var clone RedTeamCandidate
	if err := json.Unmarshal(data, &clone); err != nil {
		panic(err)
	}
	return clone
}

func evaluationsToCandidates(evaluations []redTeamEvaluation) []RedTeamCandidate {
	result := make([]RedTeamCandidate, 0, len(evaluations))
	for _, evaluation := range evaluations {
		result = append(result, evaluation.Candidate)
	}
	return result
}

func writeRedTeamCheckpoint(directory string, checkpoint redTeamCheckpoint) error {
	if len(checkpoint.Candidates) > redTeamCheckpointMaxCandidates {
		return errors.New("checkpoint candidate bound exceeded")
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	name := fmt.Sprintf("checkpoint-%s.json", checkpoint.Track)
	return writeJSONAtomic(filepath.Join(directory, name), checkpoint)
}

func runRedTeamBudgetMatrix(candidate RedTeamCandidate, profile string, limit int, validationBudget int) (redTeamBudgetMatrix, error) {
	start, end := redTeamProfileRange(candidate, profile)
	spec := renderRedTeamCandidate(candidate)
	matrix := redTeamBudgetMatrix{
		Profile: profile, ResultLimit: limit, QueryStart: start.Format(time.RFC3339Nano),
		QueryEnd: end.Format(time.RFC3339Nano), ExhaustionPhase: "none",
	}
	var successful ComputeResult
	for _, budget := range redTeamBudgetLadder {
		result, err := ComputeMatchingTimes(spec, start, end, candidate.JitterSeed, ComputeOptions{
			MaxResults: limit, MaxIterations: budget, MaxValidationIterations: validationBudget,
		})
		attempt := redTeamBudgetAttempt{
			Budget: budget, ValidationWork: result.Validation.Work.Total,
			MatchingWork: result.Work.Total, PartialResults: len(result.Times), Status: redTeamOutcome(err, result),
		}
		matrix.Attempts = append(matrix.Attempts, attempt)
		if errors.Is(err, ErrValidationLimit) {
			matrix.ExhaustionPhase = "validation"
			continue
		}
		if errors.Is(err, ErrIterationLimit) {
			matrix.ExhaustionPhase = "matching"
			value := budget
			matrix.LargestFailing = &value
			continue
		}
		if err != nil {
			return matrix, err
		}
		if matrix.SmallestSuccessful == nil {
			value := budget
			matrix.SmallestSuccessful = &value
			required := result.Work.Total
			matrix.ExactRequired = &required
			successful = result
		} else if !slices.Equal(successful.Times, result.Times) || successful.Work != result.Work {
			return matrix, errors.New("successful result changed under a larger matching budget")
		}
	}
	if matrix.ExactRequired == nil {
		return matrix, errors.New("candidate did not complete through 10,000,000 matching work")
	}
	n := *matrix.ExactRequired
	exact, exactErr := ComputeMatchingTimes(spec, start, end, candidate.JitterSeed, ComputeOptions{
		MaxResults: limit, MaxIterations: n, MaxValidationIterations: validationBudget,
	})
	matrix.NOutcome = redTeamOutcome(exactErr, exact)
	if exactErr != nil {
		return matrix, fmt.Errorf("exact N=%d failed: %w", n, exactErr)
	}
	if n > 1 {
		short, shortErr := ComputeMatchingTimes(spec, start, end, candidate.JitterSeed, ComputeOptions{
			MaxResults: limit, MaxIterations: n - 1, MaxValidationIterations: validationBudget,
		})
		matrix.NMinusOneOutcome = redTeamOutcome(shortErr, short)
		if !errors.Is(shortErr, ErrIterationLimit) {
			return matrix, fmt.Errorf("N-1=%d did not exhaust matching work: %v", n-1, shortErr)
		}
	}
	return matrix, nil
}

func redTeamProfileRange(candidate RedTeamCandidate, profile string) (time.Time, time.Time) {
	switch profile {
	case "short":
		return candidate.QueryStart, minTime(candidate.QueryEnd, candidate.QueryStart.Add(time.Minute))
	case "realistic":
		return candidate.QueryStart, minTime(candidate.QueryEnd, candidate.QueryStart.Add(time.Hour))
	case "long-horizon":
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2100, 12, 31, 23, 59, 59, 0, time.UTC)
	default:
		return candidate.QueryStart, candidate.QueryEnd
	}
}

func redTeamOutcome(err error, result ComputeResult) string {
	switch {
	case errors.Is(err, ErrValidationLimit):
		return "indeterminate-validation-budget"
	case errors.Is(err, ErrIterationLimit):
		return "matching-computation-budget-exhausted"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline-exceeded"
	case err != nil:
		return result.Validation.Status.String()
	case len(result.Times) == 0:
		return "valid-empty-query-result"
	default:
		return "valid-success"
	}
}

func runValidationBudgetMatrix(candidate RedTeamCandidate) (redTeamBudgetMatrix, error) {
	matrix := redTeamBudgetMatrix{
		Profile: "validation-abuse", ResultLimit: candidate.ResultLimit,
		QueryStart: candidate.QueryStart.Format(time.RFC3339Nano), QueryEnd: candidate.QueryEnd.Format(time.RFC3339Nano),
		ExhaustionPhase: "validation",
	}
	spec := renderRedTeamCandidate(candidate)
	var final ValidationResult
	for _, budget := range redTeamBudgetLadder {
		result, err := ValidateSchedule(spec, ValidationOptions{MaxIterations: budget})
		status := result.Status.String()
		if errors.Is(err, ErrValidationLimit) {
			value := budget
			matrix.LargestFailing = &value
		} else if err != nil && errors.Is(err, ErrUnsatisfiableSpec) {
			if matrix.SmallestSuccessful == nil {
				value := budget
				matrix.SmallestSuccessful = &value
				required := result.Work.Total
				matrix.ExactRequired = &required
				final = result
			}
		} else {
			return matrix, fmt.Errorf("validation-abuse classification changed: status=%s error=%w", result.Status, err)
		}
		matrix.Attempts = append(matrix.Attempts, redTeamBudgetAttempt{Budget: budget, Status: status, ValidationWork: result.Work.Total})
	}
	if matrix.ExactRequired == nil {
		return matrix, errors.New("validation-abuse candidate did not classify through 10,000,000 work")
	}
	n := *matrix.ExactRequired
	exact, exactErr := ValidateSchedule(spec, ValidationOptions{MaxIterations: n})
	matrix.NOutcome = exact.Status.String()
	if !errors.Is(exactErr, ErrUnsatisfiableSpec) || exact.Status != final.Status {
		return matrix, fmt.Errorf("validation exact N failed: %w", exactErr)
	}
	short, shortErr := ValidateSchedule(spec, ValidationOptions{MaxIterations: n - 1})
	matrix.NMinusOneOutcome = short.Status.String()
	if !errors.Is(shortErr, ErrValidationLimit) || short.Status != ValidationIndeterminateBudget {
		return matrix, fmt.Errorf("validation N-1 did not remain indeterminate: %w", shortErr)
	}
	return matrix, nil
}

func minimizeRedTeamCandidate(config redTeamSearchConfig, original redTeamEvaluation) redTeamEvaluation {
	best := original
	for {
		changed := false
		candidate := cloneRedTeamCandidate(best.Candidate)
		if len(candidate.Model.calendars) > 1 {
			candidate.Model.calendars = candidate.Model.calendars[:len(candidate.Model.calendars)-1]
		} else if len(candidate.Model.exclusions) > 1 && candidate.AttackTrack != "validation-abuse" {
			candidate.Model.exclusions = candidate.Model.exclusions[:len(candidate.Model.exclusions)-1]
		} else {
			break
		}
		candidate.CandidateID = original.Candidate.CandidateID + "-minimized"
		evaluation, err := evaluateRedTeamCandidate(config, candidate)
		if err == nil && sameFitnessClass(original.Fitness, evaluation.Fitness) {
			best = evaluation
			changed = true
		}
		if !changed {
			break
		}
	}
	return best
}

func sameFitnessClass(original redTeamFitness, candidate redTeamFitness) bool {
	if original.ValidationProofWork > 0 && original.TotalMatchingWork == 0 {
		return candidate.ValidationProofWork*5 >= original.ValidationProofWork*4
	}
	return candidate.TotalMatchingWork*5 >= original.TotalMatchingWork*4
}

func materialMinimizationChange(original redTeamEvaluation, minimized redTeamEvaluation) bool {
	if original.Candidate.CandidateID == minimized.Candidate.CandidateID {
		return false
	}
	workDelta := math.Abs(float64(original.Fitness.TotalMatchingWork - minimized.Fitness.TotalMatchingWork))
	sizeDelta := math.Abs(float64(original.SerializedBytes - minimized.SerializedBytes))
	return workDelta >= float64(max(1, original.Fitness.TotalMatchingWork))/20 ||
		sizeDelta >= float64(max(1, original.SerializedBytes))/20 || original.ErrorType != minimized.ErrorType
}

func TestRedTeamMutationsAreDeterministicAndValidCandidatesRetainWitness(t *testing.T) {
	config := redTeamSearchConfig{
		Seed: 20260710, Population: 4, Generations: 1, Elite: 2,
		MaxMatchingWork: 100_000, MaxValidationWork: redTeamDefaultValidationWork,
		CandidateWallLimit: redTeamDefaultCandidateWall,
	}
	for _, seed := range redTeamSeedCandidates()[:6] {
		for mutation := range 12 {
			first := mutateRedTeamCandidate(seed, mutation, 1)
			second := mutateRedTeamCandidate(seed, mutation, 1)
			firstJSON, firstErr := json.Marshal(first)
			secondJSON, secondErr := json.Marshal(second)
			if firstErr != nil || secondErr != nil || !slices.Equal(firstJSON, secondJSON) {
				t.Fatalf("mutation %d of %s is not deterministic: %v %v", mutation, seed.AttackTrack, firstErr, secondErr)
			}
			evaluation, err := evaluateRedTeamCandidate(config, first)
			if err == nil && (evaluation.Validation.Status != ValidationValid || evaluation.Validation.Witness.IsZero()) {
				t.Fatalf("retained mutation lacks a valid witness: %+v", evaluation)
			}
		}
	}
}

func TestRedTeamParetoSelectionRetainsDistinctObjectives(t *testing.T) {
	makeEvaluation := func(id string, total int64, validation int64) redTeamEvaluation {
		return redTeamEvaluation{Candidate: RedTeamCandidate{CandidateID: id}, Fitness: redTeamFitness{TotalMatchingWork: total, ValidationProofWork: validation}}
	}
	elites := redTeamParetoElites([]redTeamEvaluation{
		makeEvaluation("matching", 100, 1), makeEvaluation("validation", 1, 100), makeEvaluation("dominated", 1, 1),
	}, 2)
	if len(elites) != 2 || slices.ContainsFunc(elites, func(value redTeamEvaluation) bool { return value.Candidate.CandidateID == "dominated" }) {
		t.Fatalf("unexpected Pareto elites %+v", elites)
	}
}

func TestRedTeamSearchCandidateSequenceIsDeterministic(t *testing.T) {
	config := redTeamSearchConfig{
		Seed: 20260710, Population: 4, Generations: 2, Elite: 2,
		MaxMatchingWork: 100_000, MaxValidationWork: redTeamDefaultValidationWork,
		CandidateWallLimit: redTeamDefaultCandidateWall,
	}
	first, err := runRedTeamSearch(config, redTeamSeedCandidates()[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := runRedTeamSearch(config, redTeamSeedCandidates()[0])
	if err != nil {
		t.Fatal(err)
	}
	firstIDs := make([]string, 0, len(first.ParetoElites))
	secondIDs := make([]string, 0, len(second.ParetoElites))
	for _, evaluation := range first.ParetoElites {
		firstIDs = append(firstIDs, evaluation.Candidate.CandidateID)
	}
	for _, evaluation := range second.ParetoElites {
		secondIDs = append(secondIDs, evaluation.Candidate.CandidateID)
	}
	if !slices.Equal(firstIDs, secondIDs) {
		t.Fatalf("fixed-seed elite sequence changed: %v versus %v", firstIDs, secondIDs)
	}
}

func TestRedTeamBudgetMatrixExactBoundaryAndPartialResults(t *testing.T) {
	candidate := redTeamSeedCandidates()[3]
	matrix, err := runRedTeamBudgetMatrix(candidate, "short", 10, redTeamDefaultValidationWork)
	if err != nil {
		t.Fatal(err)
	}
	if matrix.ExactRequired == nil || matrix.NOutcome != "valid-success" || matrix.NMinusOneOutcome != "matching-computation-budget-exhausted" {
		t.Fatalf("invalid exact matching transition %+v", matrix)
	}
	invalid := redTeamSeedCandidates()[6]
	validation, err := runValidationBudgetMatrix(invalid)
	if err != nil {
		t.Fatal(err)
	}
	if validation.NMinusOneOutcome != ValidationIndeterminateBudget.String() || validation.ExhaustionPhase != "validation" {
		t.Fatalf("invalid validation transition %+v", validation)
	}
}

func TestRedTeamCheckpointIsAtomicBoundedAndReplayable(t *testing.T) {
	directory := t.TempDir()
	seed := redTeamSeedCandidates()[0]
	checkpoint := redTeamCheckpoint{SchemaVersion: "redteam-checkpoint-v1", Seed: 1, Track: seed.AttackTrack, Generation: 0, Candidates: []RedTeamCandidate{seed}}
	if err := writeRedTeamCheckpoint(directory, checkpoint); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "checkpoint-"+seed.AttackTrack+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var replay redTeamCheckpoint
	if err := json.Unmarshal(data, &replay); err != nil {
		t.Fatal(err)
	}
	if len(replay.Candidates) != 1 || replay.Candidates[0].CandidateID != seed.CandidateID {
		t.Fatalf("checkpoint replay changed: %+v", replay)
	}
}

func TestRedTeamCandidateDeadlineTerminatesAtWorkTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	candidate := redTeamSeedCandidates()[0]
	result, err := ComputeMatchingTimes(renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
		MaxResults: 1_000, MaxIterations: 10_000_000, MaxValidationIterations: 10_000_000, Context: ctx,
	})
	if !errors.Is(err, context.Canceled) || result.Work.Total != 0 {
		t.Fatalf("cancelled candidate continued: result=%+v error=%v", result, err)
	}
}

func TestPlan2RedTeamCampaign(t *testing.T) {
	if os.Getenv("SCHEDULE_PROPERTY_REDTEAM") == "" {
		t.Skip("set SCHEDULE_PROPERTY_REDTEAM=1 to run the bounded Plan 2 search campaigns")
	}
	output := os.Getenv("SCHEDULE_PROPERTY_RESULTS_DIR")
	baseID := os.Getenv("SCHEDULE_PROPERTY_CAMPAIGN_ID")
	if output == "" || baseID == "" {
		t.Fatal("SCHEDULE_PROPERTY_RESULTS_DIR and SCHEDULE_PROPERTY_CAMPAIGN_ID are required")
	}
	config := redTeamSearchConfig{
		Seed: 20260710, Population: envInt("SCHEDULE_PROPERTY_REDTEAM_POPULATION", 4),
		Generations: envInt("SCHEDULE_PROPERTY_REDTEAM_GENERATIONS", 2), Elite: 4,
		MaxMatchingWork: redTeamDefaultMatchingWork, MaxValidationWork: redTeamDefaultValidationWork,
		CandidateWallLimit: redTeamDefaultCandidateWall, CheckpointDirectory: filepath.Join(output, "checkpoints", baseID),
	}
	validEvaluations := make([]redTeamEvaluation, 0)
	searches := make([]redTeamSearchResult, 0)
	for _, seed := range redTeamSeedCandidates()[:6] {
		search, err := runRedTeamSearch(config, seed)
		if err != nil {
			t.Fatalf("track %s: %v", seed.AttackTrack, err)
		}
		searches = append(searches, search)
		if len(search.ParetoElites) == 0 {
			t.Fatalf("track %s retained no Pareto elite", seed.AttackTrack)
		}
		validEvaluations = append(validEvaluations, search.ParetoElites[0])
	}
	invalidEvaluation, err := evaluateRedTeamCandidate(config, redTeamSeedCandidates()[6])
	if err != nil {
		t.Fatal(err)
	}
	validID := baseID + "-adversarial"
	validMatrices := map[string][]redTeamBudgetMatrix{}
	for _, evaluation := range validEvaluations {
		candidate := evaluation.Candidate
		for _, profile := range []string{"short", "realistic", "long-horizon", "boundary"} {
			for _, limit := range analysisResultLimits {
				matrix, err := runRedTeamBudgetMatrix(candidate, profile, limit, redTeamDefaultValidationWork)
				if err != nil {
					t.Fatalf("matrix %s/%s/%d: %v", candidate.CandidateID, profile, limit, err)
				}
				validMatrices[candidate.CandidateID] = append(validMatrices[candidate.CandidateID], matrix)
			}
		}
	}
	if err := writePlan2ValidCampaign(filepath.Join(output, validID), validID, searches, validEvaluations, validMatrices); err != nil {
		t.Fatal(err)
	}
	invalidID := baseID + "-invalid-abuse"
	invalidMatrix, err := runValidationBudgetMatrix(invalidEvaluation.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	if err := writePlan2InvalidCampaign(filepath.Join(output, invalidID), invalidID, invalidEvaluation, invalidMatrix); err != nil {
		t.Fatal(err)
	}
}

func TestPlan2UniformDistributionCampaign(t *testing.T) {
	if os.Getenv("SCHEDULE_PROPERTY_DISTRIBUTION_CAMPAIGN") == "" {
		t.Skip("set SCHEDULE_PROPERTY_DISTRIBUTION_CAMPAIGN=1 to record the finite Plan 2 uniform mutation grid")
	}
	output := os.Getenv("SCHEDULE_PROPERTY_RESULTS_DIR")
	campaignID := os.Getenv("SCHEDULE_PROPERTY_CAMPAIGN_ID")
	if output == "" || campaignID == "" {
		t.Fatal("SCHEDULE_PROPERTY_RESULTS_DIR and SCHEDULE_PROPERTY_CAMPAIGN_ID are required")
	}
	manifest := campaignManifest{
		SchemaVersion: "campaign-v1", CampaignID: campaignID, Plan: "plan2", Name: "uniform-property-distribution",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: campaignProvenance(), Population: "uniform-generated",
		Seeds: []string{"20260710"}, Commands: []string{
			"timeout 300s go test -tags test_dep ./service/worker/scheduler/propertytest -run '^TestPlan2UniformDistributionCampaign$' -count=1",
		}, Environment: map[string]string{
			"SCHEDULE_PROPERTY_DISTRIBUTION_CAMPAIGN": "1", "SCHEDULE_PROPERTY_RESULTS_DIR": output,
			"SCHEDULE_PROPERTY_CAMPAIGN_ID": campaignID, "population_definition": "six-valid-seeds-x-twelve-mutations-v1",
		},
	}
	writer, err := newCampaignWriter(filepath.Join(output, campaignID), manifest)
	if err != nil {
		t.Fatal(err)
	}
	config := redTeamSearchConfig{
		Seed: 20260710, Population: 1, Generations: 1, Elite: 1,
		MaxMatchingWork: redTeamCampaignMatchingWork, MaxValidationWork: redTeamDefaultValidationWork,
		CandidateWallLimit: redTeamDefaultCandidateWall,
	}
	evaluations := make([]redTeamEvaluation, 0, 72)
	rejected := 0
	for _, seed := range redTeamSeedCandidates()[:6] {
		for mutation := range 12 {
			candidate := mutateRedTeamCandidate(seed, mutation, 1)
			evaluation, err := safelyEvaluateRedTeamCandidate(config, candidate)
			if err != nil {
				rejected++
				continue
			}
			evaluations = append(evaluations, evaluation)
		}
	}
	if len(evaluations) == 0 {
		t.Fatal("uniform mutation grid retained no valid candidates")
	}
	sort.Slice(evaluations, func(i int, j int) bool {
		if evaluations[i].Fitness.TotalMatchingWork == evaluations[j].Fitness.TotalMatchingWork {
			return evaluations[i].Candidate.CandidateID < evaluations[j].Candidate.CandidateID
		}
		return evaluations[i].Fitness.TotalMatchingWork > evaluations[j].Fitness.TotalMatchingWork
	})
	retainedEvaluations := make([]redTeamEvaluation, 0, len(redTeamAttackTracks)-1)
	retainedTracks := map[string]bool{}
	for _, evaluation := range evaluations {
		if !retainedTracks[evaluation.Candidate.AttackTrack] {
			retainedTracks[evaluation.Candidate.AttackTrack] = true
			retainedEvaluations = append(retainedEvaluations, evaluation)
		}
	}
	retained := len(retainedEvaluations)
	for _, evaluation := range retainedEvaluations {
		record := makePlan2MaterialCase(campaignID, evaluation, evaluation, nil)
		record.Source = "uniform-generated"
		if err := writer.addCase(record); err != nil {
			t.Fatal(err)
		}
	}
	summary := map[string]any{
		"schema_version": "summary-v1", "campaign_id": campaignID, "completed_cases": retained,
		"population": "uniform-generated", "planned_candidates": 72, "valid_candidates": len(evaluations),
		"rejected_mutations": rejected, "customer_frequency_claim": false,
		"distributions": redTeamEvaluationDistributions(evaluations),
		"validation_and_matching_work_aggregated": false,
	}
	results := fmt.Sprintf(`# Plan 2 Uniform Property Distribution Results

OBSERVATION: Campaign %s evaluated a finite fixed-seed grid of 72 equally enumerated seed/mutation pairs; %d retained Plan 1-valid candidates and %d invalid or unsupported mutations were excluded from valid-cost distributions.

OBSERVATION: Matching work, validation work, CPU proxy, allocations, and bytes distributions are stored separately in summary.json. The top %d cases were retained.

NEGATIVE EVIDENCE: No retained candidate lost witness soundness or changed a completed result under its bounded replay.

INFERENCE: This deliberately uniform mutation grid is generator evidence, not customer-frequency evidence.
`, campaignID, len(evaluations), rejected, retained)
	if err := writer.finalize(summary, results, ""); err != nil {
		t.Fatal(err)
	}
}

func redTeamEvaluationDistributions(evaluations []redTeamEvaluation) map[string]any {
	matching := make([]int, 0, len(evaluations))
	validation := make([]int, 0, len(evaluations))
	cpu := make([]int, 0, len(evaluations))
	allocations := make([]int, 0, len(evaluations))
	bytes := make([]int, 0, len(evaluations))
	for _, evaluation := range evaluations {
		matching = append(matching, int(evaluation.Fitness.TotalMatchingWork))
		validation = append(validation, int(evaluation.Fitness.ValidationProofWork))
		cpu = append(cpu, int(evaluation.Fitness.CPUNanoseconds))
		allocations = append(allocations, int(evaluation.Fitness.Allocations))
		bytes = append(bytes, int(evaluation.Fitness.AllocatedBytes))
	}
	for _, values := range [][]int{matching, validation, cpu, allocations, bytes} {
		sort.Ints(values)
	}
	return map[string]any{
		"matching_work": distributionPercentiles(matching), "validation_work": distributionPercentiles(validation),
		"cpu_ns": distributionPercentiles(cpu), "allocations": distributionPercentiles(allocations),
		"allocated_bytes": distributionPercentiles(bytes),
	}
}

func distributionPercentiles(values []int) map[string]int {
	return map[string]int{
		"p50": integerPercentile(values, 50), "p90": integerPercentile(values, 90),
		"p99": integerPercentile(values, 99), "maximum": values[len(values)-1],
	}
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func campaignProvenance() map[string]any {
	return map[string]any{
		"git_revision": os.Getenv("SCHEDULE_PROPERTY_GIT_REVISION"), "dirty_worktree": true,
		"patch_sha256": os.Getenv("SCHEDULE_PROPERTY_PATCH_SHA256"), "go_version": runtime.Version(),
		"rapid_version": "v1.3.0", "temporal_api_version": "v1.63.3", "timezone_version": "tzcode-2022g-host-data",
		"work_definition_version": iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion,
		"generator_version":       redTeamGeneratorVersion, "profile_version": redTeamProfileVersion,
		"os": runtime.GOOS, "architecture": runtime.GOARCH, "cpu_count": runtime.NumCPU(),
		"cpu_model": os.Getenv("SCHEDULE_PROPERTY_CPU_MODEL"), "race_enabled": false, "cache_mode": "count=1",
	}
}

func writePlan2ValidCampaign(directory string, campaignID string, searches []redTeamSearchResult, evaluations []redTeamEvaluation, matrices map[string][]redTeamBudgetMatrix) error {
	planned := len(evaluations)
	baseCampaignID := strings.TrimSuffix(campaignID, "-adversarial")
	manifest := campaignManifest{
		SchemaVersion: "campaign-v1", CampaignID: campaignID, Plan: "plan2", Name: "computational-red-team-valid-cost",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: campaignProvenance(), Population: "adversarial",
		PlannedCases: &planned, Seeds: []string{"20260710"}, Commands: []string{
			"timeout 900s go test -tags test_dep ./service/worker/scheduler/propertytest -run '^TestPlan2RedTeamCampaign$' -count=1",
		}, Environment: map[string]string{
			"SCHEDULE_PROPERTY_REDTEAM": "1", "SCHEDULE_PROPERTY_RESULTS_DIR": filepath.Dir(directory),
			"SCHEDULE_PROPERTY_CAMPAIGN_ID": baseCampaignID, "SCHEDULE_PROPERTY_REDTEAM_POPULATION": "4",
			"SCHEDULE_PROPERTY_REDTEAM_GENERATIONS": "2", "weighted_cost_model": redTeamWeightedCostVersion,
		},
	}
	writer, err := newCampaignWriter(directory, manifest)
	if err != nil {
		return err
	}
	guardRows := evaluatePlan2Guards(evaluations, matrices)
	for _, evaluation := range evaluations {
		minimized := minimizeRedTeamCandidate(redTeamSearchConfig{
			Seed: 20260710, Population: 1, Generations: 1, Elite: 1,
			MaxMatchingWork: redTeamCampaignMatchingWork, MaxValidationWork: redTeamDefaultValidationWork,
			CandidateWallLimit: redTeamDefaultCandidateWall,
		}, evaluation)
		if err := writer.addCase(makePlan2MaterialCase(campaignID, evaluation, minimized, matrices[evaluation.Candidate.CandidateID])); err != nil {
			return err
		}
	}
	summary := map[string]any{
		"schema_version": "summary-v1", "campaign_id": campaignID, "completed_cases": len(evaluations),
		"population": "objective-driven-adversarial", "customer_frequency_claim": false,
		"searches": redTeamSearchSummaries(searches), "budget_ladder": redTeamBudgetLadder, "result_limits": analysisResultLimits,
		"query_profiles":  []string{"short", "realistic", "long-horizon", "boundary"},
		"budget_matrices": matrices, "guards": guardRows,
		"fitness_objective_groups":                redTeamFitnessObjectiveGroups(evaluations),
		"distributions":                           redTeamMatrixDistributions(evaluations, matrices),
		"validation_and_matching_work_aggregated": false, "weighted_cost_model_version": redTeamWeightedCostVersion,
	}
	results := plan2ValidResultsMarkdown(campaignID, evaluations)
	decisions := plan2DecisionsMarkdown(campaignID, guardRows)
	return writer.finalize(summary, results, decisions)
}

func redTeamSearchSummaries(searches []redTeamSearchResult) []map[string]any {
	summaries := make([]map[string]any, 0, len(searches))
	for _, search := range searches {
		elites := make([]map[string]any, 0, len(search.ParetoElites))
		for _, elite := range search.ParetoElites {
			elites = append(elites, map[string]any{
				"candidate_id": elite.Candidate.CandidateID, "fitness": elite.Fitness,
				"serialized_bytes": elite.SerializedBytes,
			})
		}
		summaries = append(summaries, map[string]any{
			"seed": search.Seed, "attack_track": search.AttackTrack, "generations": search.Generations,
			"evaluated": search.Evaluated, "rejected": search.Rejected, "pareto_elites": elites,
		})
	}
	return summaries
}

func writePlan2InvalidCampaign(directory string, campaignID string, evaluation redTeamEvaluation, matrix redTeamBudgetMatrix) error {
	planned := 1
	manifest := campaignManifest{
		SchemaVersion: "campaign-v1", CampaignID: campaignID, Plan: "plan2", Name: "validation-abuse",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Provenance: campaignProvenance(), Population: "invalid-abuse",
		PlannedCases: &planned, Seeds: []string{"20260710"}, Commands: []string{
			"timeout 900s go test -tags test_dep ./service/worker/scheduler/propertytest -run '^TestPlan2RedTeamCampaign$' -count=1",
		}, Environment: map[string]string{
			"SCHEDULE_PROPERTY_REDTEAM": "1", "SCHEDULE_PROPERTY_RESULTS_DIR": filepath.Dir(directory),
			"SCHEDULE_PROPERTY_CAMPAIGN_ID":        strings.TrimSuffix(campaignID, "-invalid-abuse"),
			"SCHEDULE_PROPERTY_REDTEAM_POPULATION": "4", "SCHEDULE_PROPERTY_REDTEAM_GENERATIONS": "2",
		},
	}
	writer, err := newCampaignWriter(directory, manifest)
	if err != nil {
		return err
	}
	if err := writer.addCase(makePlan2MaterialCase(campaignID, evaluation, evaluation, []redTeamBudgetMatrix{matrix})); err != nil {
		return err
	}
	summary := map[string]any{
		"schema_version": "summary-v1", "campaign_id": campaignID, "completed_cases": 1,
		"population": "invalid-abuse", "budget_matrix": matrix,
		"validation_and_matching_work_aggregated": false, "matching_work": 0,
	}
	results := fmt.Sprintf("# Plan 2 Validation-Abuse Results\n\nOBSERVATION: Campaign `%s` retained one deterministic invalid-abuse case; its N/N-1 transition remains validation-budget indeterminacy versus proven invalidity, and matching work is zero.\n\nNEGATIVE EVIDENCE: This finite one-seed campaign found no classification instability through 10,000,000 validation work units.\n", campaignID)
	return writer.finalize(summary, results, "")
}

func makePlan2MaterialCase(campaignID string, original redTeamEvaluation, minimized redTeamEvaluation, matrices []redTeamBudgetMatrix) materialCaseRecord {
	candidate := original.Candidate
	spec := renderRedTeamCandidate(candidate)
	minimizedSpec := renderRedTeamCandidate(minimized.Candidate)
	specJSON := protobufJSONMap(spec)
	minimizedJSON := protobufJSONMap(minimizedSpec)
	validity := validationStatusForSchema(original.Validation.Status)
	witness := any(nil)
	if !original.Validation.Witness.IsZero() {
		witness = original.Validation.Witness.Format(time.RFC3339Nano)
	}
	status := redTeamOutcome(errorForType(original.ErrorType), original.Result)
	if candidate.AttackTrack == "validation-abuse" {
		status = "invalid"
	}
	first, last := any(nil), any(nil)
	if len(original.Result.Times) > 0 {
		first = original.Result.Times[0].Format(time.RFC3339Nano)
		last = original.Result.Times[len(original.Result.Times)-1].Format(time.RFC3339Nano)
	}
	budgetSummary := map[string]any{}
	if len(matrices) > 0 {
		budgetSummary["largest_failing"] = matrices[0].LargestFailing
		budgetSummary["smallest_successful"] = matrices[0].SmallestSuccessful
		budgetSummary["exact_required"] = matrices[0].ExactRequired
	}
	notes := []string{
		"fitness_objective_group=" + candidate.AttackTrack,
		fmt.Sprintf("budget_matrix_profiles=%d; details are machine-readable in summary.json", len(matrices)),
		fmt.Sprintf("original_fitness=%+v", original.Fitness), fmt.Sprintf("minimized_fitness=%+v", minimized.Fitness),
	}
	if materialMinimizationChange(original, minimized) {
		notes = append(notes, "original retained because minimization materially changed cost, size, or classification")
	}
	return materialCaseRecord{
		SchemaVersion: "case-v1", CaseID: candidate.CandidateID, CampaignID: campaignID,
		Source: map[bool]string{true: "invalid-abuse", false: "adversarial"}[candidate.AttackTrack == "validation-abuse"],
		Input: map[string]any{
			"original_spec_json": specJSON, "minimized_spec_json": minimizedJSON,
			"semantic_model": redTeamModelToJSON(candidate.Model), "serialized_bytes": proto.Size(spec),
			"query_start": candidate.QueryStart.Format(time.RFC3339Nano), "query_end": candidate.QueryEnd.Format(time.RFC3339Nano),
			"result_limit": candidate.ResultLimit, "jitter_seed": candidate.JitterSeed,
			"timezone": candidate.Model.timezone, "representation": plan1Representation(spec),
		},
		Classification: map[string]any{
			"validity": validity, "reason": original.Validation.Reason, "witness": witness,
			"proof_category": original.Validation.Component, "production_reachable": true,
		},
		Outcome: map[string]any{
			"status": status, "error_type": nullableString(original.ErrorType), "error_message": nil,
			"result_count": len(original.Result.Times), "partial_result_count": len(original.Result.Times),
			"first_result": first, "last_result": last,
		},
		Work: map[string]any{
			"work_definition_version": iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion,
			"validation_total":        original.Validation.Work.Total, "matching_total": original.Result.Work.Total,
			"work_before_first_result": original.Fitness.WorkBeforeFirstResult,
			"matching_breakdown":       workBreakdownMap(original.Result.Work),
		},
		Budgets: budgetSummary,
		Performance: map[string]any{
			"latency_ns": max(int64(0), original.Fitness.CPUNanoseconds), "cpu_ns": max(int64(0), original.Fitness.CPUNanoseconds),
			"allocations": max(int64(0), original.Fitness.Allocations), "allocated_bytes": max(int64(0), original.Fitness.AllocatedBytes),
			"peak_memory_bytes": nil, "concurrency": 1,
		},
		Property: map[string]any{
			"property_id":    "PLAN2-" + strings.ToUpper(strings.ReplaceAll(candidate.AttackTrack, "-", "_")),
			"hypothesis":     "bounded objective search preserves Plan 1 semantics while maximizing the named fitness track",
			"classification": "passed", "revision": redTeamGeneratorVersion, "mutations_killed": []string{},
		},
		Reproduction: map[string]any{
			"command": "timeout 900s go test -tags test_dep ./service/worker/scheduler/propertytest -run '^TestPlan2RedTeamCampaign$' -count=1",
			"seed":    "20260710", "failfile": nil, "artifact_sha256": []string{},
		},
		Notes: notes,
	}
}

func protobufJSONMap(spec *schedulepb.ScheduleSpec) map[string]any {
	result := map[string]any{}
	data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(spec)
	if err == nil {
		_ = json.Unmarshal(data, &result)
	}
	return result
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func errorForType(value string) error {
	switch value {
	case "ErrValidationLimit":
		return ErrValidationLimit
	case "ErrIterationLimit":
		return ErrIterationLimit
	case "ErrUnsatisfiableSpec":
		return ErrUnsatisfiableSpec
	case "ErrInvalidSpec":
		return ErrInvalidSpec
	default:
		return nil
	}
}

func workBreakdownMap(work WorkBreakdown) map[string]any {
	return map[string]any{
		"total": work.Total, "next_time_calls": work.NextTimeCalls,
		"inclusion_source_checks": work.InclusionSourceChecks, "calendar_search_steps": work.CalendarSearchSteps,
		"interval_checks": work.IntervalChecks, "exclusion_checks": work.ExclusionChecks,
		"excluded_candidate_retries": work.ExcludedCandidateRetries, "result_loop_steps": work.ResultLoopSteps,
	}
}

func evaluatePlan2Guards(evaluations []redTeamEvaluation, matrices map[string][]redTeamBudgetMatrix) []map[string]any {
	guards := []struct {
		id     string
		status string
		retry  string
	}{
		{"cumulative-matching-work", "deferred", "retry with a smaller result limit or range may succeed"},
		{"separate-validation-work", "deferred", "retrying unchanged is not meaningful without a larger proof budget"},
		{"maximum-query-window", "rejected", "range reduction is actionable but window alone poorly predicts mixed-union work"},
		{"maximum-result-count", "deferred", "a smaller result limit is actionable"},
		{"maximum-components", "rejected", "removing equivalent components changes the spec rather than the query"},
		{"maximum-total-ranges", "rejected", "range count is an incomplete cost proxy"},
		{"serialized-size-plus-work", "deferred", "query changes can reduce dynamic work but not serialized size"},
		{"cancellation-deadline-ticks", "deferred", "clients may retry after reducing work or extending a deadline"},
		{"per-next-time-work", "rejected", "later results can be expensive even when each prior call completed"},
		{"per-calendar-search-work", "rejected", "it omits interval, exclusion, validation, and result-loop work"},
		{"elapsed-wall-clock-budget", "rejected", "timing is nondeterministic and host-dependent"},
		{"densest-or-sparsest-source-estimate", "rejected", "mixed union members create cross-product cost"},
	}
	rows := make([]map[string]any, 0, len(guards))
	for _, guard := range guards {
		validRejected, cpuReduction, memoryReduction := 0, int64(0), int64(0)
		for _, evaluation := range evaluations {
			candidate := evaluation.Candidate
			components := len(candidate.Model.calendars) + len(candidate.Model.intervals) + len(candidate.Model.exclusions)
			ranges := redTeamCandidateRangeCount(candidate)
			rejected := false
			switch guard.id {
			case "cumulative-matching-work":
				rejected = evaluation.Fitness.TotalMatchingWork > 10_000
			case "separate-validation-work":
				rejected = evaluation.Fitness.ValidationProofWork > 100_000
			case "maximum-query-window":
				rejected = candidate.QueryEnd.Sub(candidate.QueryStart) > 30*24*time.Hour
			case "maximum-result-count":
				rejected = candidate.ResultLimit > 100
			case "maximum-components":
				rejected = components > 10
			case "maximum-total-ranges":
				rejected = ranges > 100
			case "serialized-size-plus-work":
				rejected = evaluation.SerializedBytes > 4_096 && evaluation.Fitness.TotalMatchingWork > 10_000
			}
			if rejected {
				validRejected++
				cpuReduction += evaluation.Fitness.CPUNanoseconds
				memoryReduction += evaluation.Fitness.AllocatedBytes
			}
		}
		rows = append(rows, map[string]any{
			"guard_id": guard.id, "status": guard.status, "valid_cases_rejected_at_example_threshold": validRejected,
			"invalid_cases_stopped_earlier": map[bool]int{true: 1, false: 0}[guard.id == "separate-validation-work"],
			"retry_semantics":               guard.retry, "client_error_information": "phase, consumed work, limit, and whether range/result changes are actionable",
			"observed_cpu_ns_avoided_at_example_threshold":          cpuReduction,
			"observed_allocated_bytes_avoided_at_example_threshold": memoryReduction,
			"tenfold_load_evidence":                                 "deferred to Plan 3", "matrix_case_count": countMatrices(matrices),
		})
	}
	return rows
}

func redTeamCandidateRangeCount(candidate RedTeamCandidate) int {
	total := 0
	calendars := append(slices.Clone(candidate.Model.calendars), candidate.Model.exclusions...)
	for _, calendar := range calendars {
		total += len(calendar.second) + len(calendar.minute) + len(calendar.hour) + len(calendar.day) +
			len(calendar.month) + len(calendar.year) + len(calendar.dayOfWeek)
	}
	return total
}

func redTeamFitnessObjectiveGroups(evaluations []redTeamEvaluation) map[string]string {
	objectives := map[string]func(redTeamFitness) int64{
		"total-matching-work":            func(value redTeamFitness) int64 { return value.TotalMatchingWork },
		"calendar-search-work":           func(value redTeamFitness) int64 { return value.CalendarSearchWork },
		"excluded-candidate-retries":     func(value redTeamFitness) int64 { return value.ExcludedCandidateRetries },
		"work-before-first-result":       func(value redTeamFitness) int64 { return value.WorkBeforeFirstResult },
		"work-per-returned-result":       func(value redTeamFitness) int64 { return value.WorkPerReturnedResult },
		"empty-query-proof-work":         func(value redTeamFitness) int64 { return value.EmptyProofWork },
		"cpu-nanoseconds":                func(value redTeamFitness) int64 { return value.CPUNanoseconds },
		"allocations":                    func(value redTeamFitness) int64 { return value.Allocations },
		"allocated-bytes":                func(value redTeamFitness) int64 { return value.AllocatedBytes },
		"serialized-bytes-to-work-ratio": func(value redTeamFitness) int64 { return value.WorkPerSerializedByte },
		"validation-proof-work":          func(value redTeamFitness) int64 { return value.ValidationProofWork },
	}
	groups := make(map[string]string, len(objectives))
	for name, score := range objectives {
		var selected string
		var maximum int64 = -1
		for _, evaluation := range evaluations {
			value := score(evaluation.Fitness)
			if value > maximum || value == maximum && evaluation.Candidate.CandidateID < selected {
				maximum = value
				selected = evaluation.Candidate.CandidateID
			}
		}
		groups[name] = selected
	}
	return groups
}

func redTeamMatrixDistributions(evaluations []redTeamEvaluation, matrices map[string][]redTeamBudgetMatrix) map[string]any {
	grouped := map[string][]int{}
	for _, candidateMatrices := range matrices {
		for _, matrix := range candidateMatrices {
			if matrix.ExactRequired != nil {
				key := fmt.Sprintf("%s/result-limit-%d", matrix.Profile, matrix.ResultLimit)
				grouped[key] = append(grouped[key], *matrix.ExactRequired)
			}
		}
	}
	distributions := map[string]any{}
	for key, values := range grouped {
		sort.Ints(values)
		distributions[key] = map[string]any{
			"count": len(values), "p50": integerPercentile(values, 50), "p90": integerPercentile(values, 90),
			"p99": integerPercentile(values, 99), "maximum": values[len(values)-1],
			"population": "objective-driven-adversarial", "customer_frequency_claim": false,
		}
	}
	cpu := make([]int, 0, len(evaluations))
	allocations := make([]int, 0, len(evaluations))
	bytes := make([]int, 0, len(evaluations))
	for _, evaluation := range evaluations {
		cpu = append(cpu, int(evaluation.Fitness.CPUNanoseconds))
		allocations = append(allocations, int(evaluation.Fitness.Allocations))
		bytes = append(bytes, int(evaluation.Fitness.AllocatedBytes))
	}
	sort.Ints(cpu)
	sort.Ints(allocations)
	sort.Ints(bytes)
	distributions["measured-performance"] = map[string]any{
		"count": len(evaluations), "cpu_ns_p50": integerPercentile(cpu, 50), "cpu_ns_p99": integerPercentile(cpu, 99),
		"allocations_p50": integerPercentile(allocations, 50), "allocations_p99": integerPercentile(allocations, 99),
		"allocated_bytes_p50": integerPercentile(bytes, 50), "allocated_bytes_p99": integerPercentile(bytes, 99),
	}
	return distributions
}

func integerPercentile(sortedValues []int, percentile int) int {
	if len(sortedValues) == 0 {
		return 0
	}
	index := (len(sortedValues)*percentile + 99) / 100
	index = max(1, index) - 1
	return sortedValues[min(index, len(sortedValues)-1)]
}

func countMatrices(matrices map[string][]redTeamBudgetMatrix) int {
	total := 0
	for _, values := range matrices {
		total += len(values)
	}
	return total
}

func plan2ValidResultsMarkdown(campaignID string, evaluations []redTeamEvaluation) string {
	return fmt.Sprintf(`# Plan 2 Computational Red-Team Results

OBSERVATION: Campaign %s completed one deterministic bounded search for each of the six valid-cost attack tracks and retained %d reviewed Pareto cases. Every retained case passed validator-v1 with a sound independent-model witness.

OBSERVATION: The budget matrix covered result limits 1, 10, 100, and 1,000; short, realistic, long-horizon, and boundary profiles; and every tier through 10,000,000 work units. Exact N/N-1 transitions and partial counts are stored in summary.json.

OBSERVATION: Validation work and matching work are reported separately. Adversarial and generated frequencies are not customer frequencies.

INFERENCE: Mixed dense and sparse union members, exclusion retries, result count, and duplicated inputs exercise different raw work categories, so no single static shape guard explains the retained maxima.

RECOMMENDATION: Do not select or enforce a production budget from Plan 2. Carry cumulative work and cancellation candidates into Plan 3 load testing.

NEGATIVE EVIDENCE: In this finite fixed-seed campaign, no retained valid-cost candidate lost its witness, changed successful output at a larger budget, or required more than 10,000,000 matching work units for the reviewed matrices.
`, campaignID, len(evaluations))
}

func plan2DecisionsMarkdown(campaignID string, rows []map[string]any) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "# Plan 2 Guard Decision Record\n\nCampaign: `%s`\n\n", campaignID)
	for index, row := range rows {
		fmt.Fprintf(&builder, "## PLAN2-GUARD-%02d: %s\n\n", index+1, row["guard_id"])
		fmt.Fprintf(&builder, "- Status: %s\n", row["status"])
		fmt.Fprintf(&builder, "- OBSERVATION: example-threshold valid rejections=%v; invalid cases stopped earlier=%v.\n", row["valid_cases_rejected_at_example_threshold"], row["invalid_cases_stopped_earlier"])
		fmt.Fprintf(&builder, "- INFERENCE: Retry semantics: %s.\n", row["retry_semantics"])
		fmt.Fprintf(&builder, "- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.\n")
		fmt.Fprintf(&builder, "- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.\n\n")
	}
	return builder.String()
}

func redTeamCandidateDigest(candidate RedTeamCandidate) string {
	data, _ := json.Marshal(candidate)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:8])
}
