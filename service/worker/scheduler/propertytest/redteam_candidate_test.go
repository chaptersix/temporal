package propertytest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	redTeamCandidateSchemaVersion = "redteam-candidate-v1"
	redTeamCandidateMaxBytes      = 256 << 10
	redTeamCandidateMaxComponents = 64
	redTeamCandidateMaxRanges     = 1_024
)

var redTeamAttackTracks = []string{
	"dense-result-sparse-union",
	"exclusion-amplification",
	"sparse-first-result",
	"result-count-amplification",
	"horizon-amplification",
	"input-size-amplification",
	"validation-abuse",
}

type RedTeamCandidate struct {
	SchemaVersion  string
	CandidateID    string
	AttackTrack    string
	Model          scheduleModel
	QueryStart     time.Time
	QueryEnd       time.Time
	ResultLimit    int
	JitterSeed     string
	Representation string
}

type redTeamCandidateJSON struct {
	SchemaVersion  string                   `json:"schema_version"`
	CandidateID    string                   `json:"candidate_id"`
	AttackTrack    string                   `json:"attack_track"`
	Model          redTeamScheduleModelJSON `json:"model"`
	QueryStart     time.Time                `json:"query_start"`
	QueryEnd       time.Time                `json:"query_end"`
	ResultLimit    int                      `json:"result_limit"`
	JitterSeed     string                   `json:"jitter_seed"`
	Representation string                   `json:"representation"`
}

type redTeamScheduleModelJSON struct {
	Calendars    []redTeamCalendarJSON `json:"calendars"`
	Intervals    []redTeamIntervalJSON `json:"intervals"`
	Exclusions   []redTeamCalendarJSON `json:"exclusions"`
	Witness      *time.Time            `json:"witness"`
	StartTime    *time.Time            `json:"start_time"`
	EndTime      *time.Time            `json:"end_time"`
	JitterNS     int64                 `json:"jitter_ns"`
	Timezone     string                `json:"timezone"`
	TimezoneData []byte                `json:"timezone_data"`
}

type redTeamCalendarJSON struct {
	Second     []redTeamRangeJSON `json:"second"`
	Minute     []redTeamRangeJSON `json:"minute"`
	Hour       []redTeamRangeJSON `json:"hour"`
	DayOfMonth []redTeamRangeJSON `json:"day_of_month"`
	Month      []redTeamRangeJSON `json:"month"`
	Year       []redTeamRangeJSON `json:"year"`
	DayOfWeek  []redTeamRangeJSON `json:"day_of_week"`
}

type redTeamRangeJSON struct {
	Start int `json:"start"`
	End   int `json:"end"`
	Step  int `json:"step"`
}

type redTeamIntervalJSON struct {
	IntervalSeconds int64 `json:"interval_seconds"`
	PhaseSeconds    int64 `json:"phase_seconds"`
}

func (candidate RedTeamCandidate) MarshalJSON() ([]byte, error) {
	if err := validateRedTeamCandidate(candidate); err != nil {
		return nil, err
	}
	return json.Marshal(redTeamCandidateJSON{
		SchemaVersion: candidate.SchemaVersion, CandidateID: candidate.CandidateID,
		AttackTrack: candidate.AttackTrack, Model: redTeamModelToJSON(candidate.Model),
		QueryStart: candidate.QueryStart, QueryEnd: candidate.QueryEnd, ResultLimit: candidate.ResultLimit,
		JitterSeed: candidate.JitterSeed, Representation: candidate.Representation,
	})
}

func (candidate *RedTeamCandidate) UnmarshalJSON(data []byte) error {
	if len(data) > redTeamCandidateMaxBytes {
		return fmt.Errorf("candidate exceeds %d bytes", redTeamCandidateMaxBytes)
	}
	var required map[string]json.RawMessage
	if err := json.Unmarshal(data, &required); err != nil {
		return err
	}
	for _, field := range []string{"schema_version", "candidate_id", "attack_track", "model", "query_start", "query_end", "result_limit", "jitter_seed", "representation"} {
		if _, present := required[field]; !present {
			return fmt.Errorf("candidate is missing required field %q", field)
		}
	}
	var modelRequired map[string]json.RawMessage
	if err := json.Unmarshal(required["model"], &modelRequired); err != nil {
		return err
	}
	if _, present := modelRequired["timezone_data"]; !present {
		return errors.New("candidate model is missing required field \"timezone_data\"")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var value redTeamCandidateJSON
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return err
	}
	decoded := RedTeamCandidate{
		SchemaVersion: value.SchemaVersion, CandidateID: value.CandidateID, AttackTrack: value.AttackTrack,
		Model: redTeamModelFromJSON(value.Model), QueryStart: value.QueryStart, QueryEnd: value.QueryEnd,
		ResultLimit: value.ResultLimit, JitterSeed: value.JitterSeed, Representation: value.Representation,
	}
	if err := validateRedTeamCandidate(decoded); err != nil {
		return err
	}
	*candidate = decoded
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("candidate contains trailing JSON")
		}
		return err
	}
	return nil
}

func validateRedTeamCandidate(candidate RedTeamCandidate) error {
	if candidate.SchemaVersion != redTeamCandidateSchemaVersion {
		return fmt.Errorf("unsupported candidate schema %q", candidate.SchemaVersion)
	}
	if candidate.CandidateID == "" || len(candidate.CandidateID) > 128 {
		return errors.New("candidate ID must contain 1-128 bytes")
	}
	if !slices.Contains(redTeamAttackTracks, candidate.AttackTrack) {
		return fmt.Errorf("unknown attack track %q", candidate.AttackTrack)
	}
	if candidate.QueryStart.IsZero() || candidate.QueryEnd.Before(candidate.QueryStart) {
		return errors.New("candidate query range is invalid")
	}
	if !slices.Contains(analysisResultLimits, candidate.ResultLimit) {
		return fmt.Errorf("result limit %d is outside the replay corpus", candidate.ResultLimit)
	}
	if len(candidate.JitterSeed) > 128 || len(candidate.Model.timezone) > 128 {
		return errors.New("candidate seed or timezone exceeds its bound")
	}
	if len(candidate.Model.timezoneData) > 192<<10 {
		return errors.New("candidate timezone data exceeds 192 KiB")
	}
	if !slices.Contains([]string{"structured-calendar", "calendar", "cron", "mixed"}, candidate.Representation) {
		return fmt.Errorf("unknown representation %q", candidate.Representation)
	}
	componentCount := len(candidate.Model.calendars) + len(candidate.Model.intervals) + len(candidate.Model.exclusions)
	if componentCount == 0 || componentCount > redTeamCandidateMaxComponents {
		return fmt.Errorf("candidate component count %d is outside [1,%d]", componentCount, redTeamCandidateMaxComponents)
	}
	rangeCount := 0
	calendars := append(slices.Clone(candidate.Model.calendars), candidate.Model.exclusions...)
	for _, calendar := range calendars {
		for _, ranges := range [][]rangeModel{calendar.second, calendar.minute, calendar.hour, calendar.day, calendar.month, calendar.year, calendar.dayOfWeek} {
			if len(ranges) > 64 {
				return errors.New("candidate field contains more than 64 ranges")
			}
			rangeCount += len(ranges)
			for _, value := range ranges {
				if value.step <= 0 || value.end < value.start {
					return errors.New("candidate contains an invalid range")
				}
			}
		}
	}
	if rangeCount > redTeamCandidateMaxRanges {
		return fmt.Errorf("candidate range count %d exceeds %d", rangeCount, redTeamCandidateMaxRanges)
	}
	for _, interval := range candidate.Model.intervals {
		if interval.intervalSeconds <= 0 || interval.phaseSeconds < 0 || interval.phaseSeconds >= interval.intervalSeconds {
			return errors.New("candidate contains an invalid interval")
		}
	}
	return nil
}

func renderRedTeamCandidate(candidate RedTeamCandidate) *schedulepb.ScheduleSpec {
	model := candidate.Model
	spec := &schedulepb.ScheduleSpec{TimezoneName: model.timezone, TimezoneData: slices.Clone(model.timezoneData), Jitter: durationpb.New(model.jitter)}
	for _, interval := range model.intervals {
		spec.Interval = append(spec.Interval, interval.render())
	}
	for _, calendar := range model.calendars {
		switch candidate.Representation {
		case "calendar":
			spec.Calendar = append(spec.Calendar, calendar.renderCalendar())
		case "cron":
			spec.CronString = append(spec.CronString, calendar.renderCron())
		case "mixed":
			if len(spec.Calendar) <= len(spec.StructuredCalendar) {
				spec.Calendar = append(spec.Calendar, calendar.renderCalendar())
			} else {
				spec.StructuredCalendar = append(spec.StructuredCalendar, calendar.render())
			}
		default:
			spec.StructuredCalendar = append(spec.StructuredCalendar, calendar.render())
		}
	}
	for _, exclusion := range model.exclusions {
		if candidate.Representation == "calendar" {
			spec.ExcludeCalendar = append(spec.ExcludeCalendar, exclusion.renderCalendar())
		} else {
			spec.ExcludeStructuredCalendar = append(spec.ExcludeStructuredCalendar, exclusion.render())
		}
	}
	if model.startTime != nil {
		spec.StartTime = timestamppb.New(*model.startTime)
	}
	if model.endTime != nil {
		spec.EndTime = timestamppb.New(*model.endTime)
	}
	return spec
}

func redTeamModelToJSON(model scheduleModel) redTeamScheduleModelJSON {
	value := redTeamScheduleModelJSON{
		Calendars:  make([]redTeamCalendarJSON, 0, len(model.calendars)),
		Intervals:  make([]redTeamIntervalJSON, 0, len(model.intervals)),
		Exclusions: make([]redTeamCalendarJSON, 0, len(model.exclusions)),
		Witness:    cloneTimePointer(&model.witness), StartTime: cloneTimePointer(model.startTime), EndTime: cloneTimePointer(model.endTime),
		JitterNS: int64(model.jitter), Timezone: model.timezone,
		TimezoneData: slices.Clone(model.timezoneData),
	}
	if model.witness.IsZero() {
		value.Witness = nil
	}
	for _, calendar := range model.calendars {
		value.Calendars = append(value.Calendars, redTeamCalendarToJSON(calendar))
	}
	for _, interval := range model.intervals {
		value.Intervals = append(value.Intervals, redTeamIntervalJSON{IntervalSeconds: interval.intervalSeconds, PhaseSeconds: interval.phaseSeconds})
	}
	for _, calendar := range model.exclusions {
		value.Exclusions = append(value.Exclusions, redTeamCalendarToJSON(calendar))
	}
	return value
}

func redTeamModelFromJSON(value redTeamScheduleModelJSON) scheduleModel {
	model := scheduleModel{
		startTime: cloneTimePointer(value.StartTime), endTime: cloneTimePointer(value.EndTime),
		jitter: time.Duration(value.JitterNS), timezone: value.Timezone, timezoneData: slices.Clone(value.TimezoneData),
	}
	if value.Witness != nil {
		model.witness = value.Witness.UTC()
	}
	for _, calendar := range value.Calendars {
		model.calendars = append(model.calendars, redTeamCalendarFromJSON(calendar))
	}
	for _, interval := range value.Intervals {
		model.intervals = append(model.intervals, intervalModel{intervalSeconds: interval.IntervalSeconds, phaseSeconds: interval.PhaseSeconds})
	}
	for _, calendar := range value.Exclusions {
		model.exclusions = append(model.exclusions, redTeamCalendarFromJSON(calendar))
	}
	return model
}

func redTeamCalendarToJSON(calendar calendarModel) redTeamCalendarJSON {
	return redTeamCalendarJSON{
		Second: redTeamRangesToJSON(calendar.second), Minute: redTeamRangesToJSON(calendar.minute),
		Hour: redTeamRangesToJSON(calendar.hour), DayOfMonth: redTeamRangesToJSON(calendar.day),
		Month: redTeamRangesToJSON(calendar.month), Year: redTeamRangesToJSON(calendar.year),
		DayOfWeek: redTeamRangesToJSON(calendar.dayOfWeek),
	}
}

func redTeamCalendarFromJSON(calendar redTeamCalendarJSON) calendarModel {
	return calendarModel{
		second: redTeamRangesFromJSON(calendar.Second), minute: redTeamRangesFromJSON(calendar.Minute),
		hour: redTeamRangesFromJSON(calendar.Hour), day: redTeamRangesFromJSON(calendar.DayOfMonth),
		month: redTeamRangesFromJSON(calendar.Month), year: redTeamRangesFromJSON(calendar.Year),
		dayOfWeek: redTeamRangesFromJSON(calendar.DayOfWeek),
	}
}

func redTeamRangesToJSON(ranges []rangeModel) []redTeamRangeJSON {
	result := make([]redTeamRangeJSON, 0, len(ranges))
	for _, value := range ranges {
		result = append(result, redTeamRangeJSON{Start: value.start, End: value.end, Step: value.step})
	}
	return result
}

func redTeamRangesFromJSON(ranges []redTeamRangeJSON) []rangeModel {
	result := make([]rangeModel, 0, len(ranges))
	for _, value := range ranges {
		result = append(result, rangeModel{start: value.Start, end: value.End, step: value.Step})
	}
	return result
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := value.UTC()
	return &clone
}

func readRedTeamCandidate(path string) (RedTeamCandidate, error) {
	file, err := os.Open(path)
	if err != nil {
		return RedTeamCandidate{}, err
	}
	defer file.Close()
	limited := io.LimitReader(file, redTeamCandidateMaxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return RedTeamCandidate{}, err
	}
	if len(data) > redTeamCandidateMaxBytes {
		return RedTeamCandidate{}, fmt.Errorf("candidate exceeds %d bytes", redTeamCandidateMaxBytes)
	}
	var candidate RedTeamCandidate
	if err := json.Unmarshal(data, &candidate); err != nil {
		return RedTeamCandidate{}, err
	}
	return candidate, nil
}

func TestRedTeamCandidateSchemaAndRoundTrip(t *testing.T) {
	schema, err := os.ReadFile("results/candidate-v1.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schemaDocument map[string]any
	if err := json.Unmarshal(schema, &schemaDocument); err != nil {
		t.Fatal(err)
	}
	if schemaDocument["$id"] != "https://temporal.io/schedule-property-analysis/redteam-candidate-v1.schema.json" {
		t.Fatalf("unexpected candidate schema ID %v", schemaDocument["$id"])
	}
	candidate := redTeamSeedCandidates()[0]
	data, err := json.Marshal(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > redTeamCandidateMaxBytes {
		t.Fatalf("candidate encoded to %d bytes", len(data))
	}
	var replay RedTeamCandidate
	if err := json.Unmarshal(data, &replay); err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(replay)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, second) {
		t.Fatalf("candidate replay changed\nfirst: %s\nsecond: %s", data, second)
	}
}

func TestRedTeamCandidateRejectsUnknownAndUnboundedInput(t *testing.T) {
	candidate := redTeamSeedCandidates()[0]
	data, err := json.Marshal(candidate)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), `"candidate_id":`, `"unknown":true,"candidate_id":`, 1))
	var decoded RedTeamCandidate
	if err := json.Unmarshal(data, &decoded); err == nil {
		t.Fatal("candidate with an unknown field was accepted")
	}
	tooLarge := append([]byte{'{'}, bytes.Repeat([]byte{' '}, redTeamCandidateMaxBytes)...)
	if err := json.Unmarshal(tooLarge, &decoded); err == nil {
		t.Fatal("oversized candidate was accepted")
	}
}

func TestReviewedRedTeamCandidateCorpusReplays(t *testing.T) {
	paths, err := filepath.Glob("testdata/redteam/v1/*/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != len(redTeamAttackTracks) {
		t.Fatalf("reviewed candidate count %d, want one for each of %d tracks", len(paths), len(redTeamAttackTracks))
	}
	seen := map[string]bool{}
	config := redTeamSearchConfig{
		Seed: 20260710, Population: 1, Generations: 1, Elite: 1,
		MaxMatchingWork: redTeamCampaignMatchingWork, MaxValidationWork: redTeamDefaultValidationWork,
		CandidateWallLimit: redTeamDefaultCandidateWall,
	}
	for _, path := range paths {
		candidate, err := readRedTeamCandidate(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if seen[candidate.AttackTrack] {
			t.Fatalf("duplicate reviewed track %s", candidate.AttackTrack)
		}
		seen[candidate.AttackTrack] = true
		evaluation, err := safelyEvaluateRedTeamCandidate(config, candidate)
		if err != nil {
			t.Fatalf("replay %s: %v", path, err)
		}
		if candidate.AttackTrack == "validation-abuse" {
			if evaluation.Validation.Status == ValidationValid || evaluation.Result.Work.Total != 0 {
				t.Fatalf("invalid-abuse replay entered matching: %+v", evaluation)
			}
		} else if evaluation.Validation.Status != ValidationValid || evaluation.Validation.Witness.IsZero() {
			t.Fatalf("valid replay lacks witness: %+v", evaluation)
		}
	}
}
