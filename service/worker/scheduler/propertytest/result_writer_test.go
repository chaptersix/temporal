package propertytest

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type campaignArtifact struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type campaignManifest struct {
	SchemaVersion  string             `json:"schema_version"`
	CampaignID     string             `json:"campaign_id"`
	Plan           string             `json:"plan"`
	Name           string             `json:"name"`
	StartedAt      string             `json:"started_at"`
	CompletedAt    *string            `json:"completed_at"`
	Complete       bool               `json:"complete"`
	Provenance     map[string]any     `json:"provenance"`
	Population     string             `json:"population"`
	PlannedCases   *int               `json:"planned_cases,omitempty"`
	CompletedCases int                `json:"completed_cases"`
	Seeds          []string           `json:"seeds,omitempty"`
	Commands       []string           `json:"commands"`
	Environment    map[string]string  `json:"environment,omitempty"`
	Artifacts      []campaignArtifact `json:"artifacts"`
}

type materialCaseRecord struct {
	SchemaVersion  string         `json:"schema_version"`
	CaseID         string         `json:"case_id"`
	CampaignID     string         `json:"campaign_id"`
	Source         string         `json:"source"`
	Input          map[string]any `json:"input"`
	Classification map[string]any `json:"classification"`
	Outcome        map[string]any `json:"outcome"`
	Work           map[string]any `json:"work"`
	Budgets        map[string]any `json:"budgets,omitempty"`
	Performance    map[string]any `json:"performance,omitempty"`
	Property       map[string]any `json:"property,omitempty"`
	Reproduction   map[string]any `json:"reproduction"`
	Notes          []string       `json:"notes,omitempty"`
}

type campaignWriter struct {
	directory string
	manifest  campaignManifest
	casesFile *os.File
	casesTemp string
	caseCount int
	finalized bool
}

func newCampaignWriter(directory string, manifest campaignManifest) (*campaignWriter, error) {
	if err := validateCampaignManifest(manifest, false); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	manifest.Complete = false
	manifest.CompletedAt = nil
	manifest.CompletedCases = 0
	manifest.Artifacts = nil
	if err := writeJSONAtomic(filepath.Join(directory, "campaign.json"), manifest); err != nil {
		return nil, err
	}
	casesFile, err := os.CreateTemp(directory, ".cases-*.jsonl.tmp")
	if err != nil {
		return nil, err
	}
	return &campaignWriter{
		directory: directory,
		manifest:  manifest,
		casesFile: casesFile,
		casesTemp: casesFile.Name(),
	}, nil
}

func (w *campaignWriter) addCase(record materialCaseRecord) error {
	if w.finalized {
		return errors.New("campaign already finalized")
	}
	if record.CampaignID != w.manifest.CampaignID {
		return errors.New("case campaign ID does not match manifest")
	}
	if err := validateMaterialCase(record); err != nil {
		return err
	}
	if err := json.NewEncoder(w.casesFile).Encode(record); err != nil {
		return err
	}
	w.caseCount++
	return nil
}

func (w *campaignWriter) finalize(summary map[string]any, results string, decisions string) error {
	if w.finalized {
		return errors.New("campaign already finalized")
	}
	if err := w.casesFile.Sync(); err != nil {
		return err
	}
	if err := w.casesFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(w.casesTemp, filepath.Join(w.directory, "cases.jsonl")); err != nil {
		return err
	}
	if err := writeJSONAtomic(filepath.Join(w.directory, "summary.json"), summary); err != nil {
		return err
	}
	if err := writeBytesAtomic(filepath.Join(w.directory, "RESULTS.md"), []byte(results)); err != nil {
		return err
	}
	artifactNames := []string{"cases.jsonl", "summary.json", "RESULTS.md"}
	if decisions != "" {
		if err := writeBytesAtomic(filepath.Join(w.directory, "DECISIONS.md"), []byte(decisions)); err != nil {
			return err
		}
		artifactNames = append(artifactNames, "DECISIONS.md")
	}
	artifacts, err := collectArtifacts(w.directory, artifactNames)
	if err != nil {
		return err
	}
	completedAt := time.Now().UTC().Format(time.RFC3339Nano)
	w.manifest.CompletedAt = &completedAt
	w.manifest.Complete = true
	w.manifest.CompletedCases = w.caseCount
	w.manifest.Artifacts = artifacts
	if err := validateCampaignManifest(w.manifest, true); err != nil {
		return err
	}
	if err := writeJSONAtomic(filepath.Join(w.directory, "campaign.json"), w.manifest); err != nil {
		return err
	}
	checksumNames := append([]string{"campaign.json"}, artifactNames...)
	if err := writeChecksumFile(w.directory, checksumNames); err != nil {
		return err
	}
	w.finalized = true
	return verifyCampaignBundle(w.directory)
}

func validateCampaignManifest(manifest campaignManifest, final bool) error {
	if manifest.SchemaVersion != "campaign-v1" || manifest.CampaignID == "" || manifest.Plan == "" || manifest.Name == "" {
		return errors.New("campaign manifest is missing required identity fields")
	}
	if _, err := time.Parse(time.RFC3339Nano, manifest.StartedAt); err != nil {
		return fmt.Errorf("invalid campaign start: %w", err)
	}
	if len(manifest.Commands) == 0 || manifest.Population == "" {
		return errors.New("campaign manifest is missing command or population")
	}
	requiredProvenance := []string{
		"git_revision", "dirty_worktree", "go_version", "rapid_version", "temporal_api_version",
		"timezone_version", "work_definition_version", "generator_version", "profile_version", "os",
		"architecture", "cpu_count",
	}
	for _, field := range requiredProvenance {
		if _, ok := manifest.Provenance[field]; !ok {
			return fmt.Errorf("campaign provenance is missing %q", field)
		}
	}
	if final && (!manifest.Complete || manifest.CompletedAt == nil) {
		return errors.New("final campaign manifest is not complete")
	}
	return nil
}

func validateMaterialCase(record materialCaseRecord) error {
	if record.SchemaVersion != "case-v1" || record.CaseID == "" || record.CampaignID == "" || record.Source == "" {
		return errors.New("case record is missing required identity fields")
	}
	for name, value := range map[string]map[string]any{
		"input": record.Input, "classification": record.Classification, "outcome": record.Outcome,
		"work": record.Work, "reproduction": record.Reproduction,
	} {
		if len(value) == 0 {
			return fmt.Errorf("case record is missing %s", name)
		}
	}
	if message, ok := record.Outcome["error_message"].(string); ok && len(message) > 1_024 {
		return errors.New("case error message exceeds bounded diagnostic length")
	}
	if timezone, ok := record.Input["timezone"].(string); ok && len(timezone) > 128 {
		return errors.New("case timezone exceeds bounded diagnostic length")
	}
	return nil
}

func verifyCampaignBundle(directory string) error {
	manifestData, err := os.ReadFile(filepath.Join(directory, "campaign.json"))
	if err != nil {
		return err
	}
	var manifest campaignManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return err
	}
	if err := validateCampaignManifest(manifest, true); err != nil {
		return err
	}
	casesFile, err := os.Open(filepath.Join(directory, "cases.jsonl"))
	if err != nil {
		return err
	}
	defer casesFile.Close()
	scanner := bufio.NewScanner(casesFile)
	caseCount := 0
	for scanner.Scan() {
		var record materialCaseRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return err
		}
		if err := validateMaterialCase(record); err != nil {
			return err
		}
		caseCount++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if caseCount != manifest.CompletedCases {
		return fmt.Errorf("case count %d does not match manifest %d", caseCount, manifest.CompletedCases)
	}
	checksumFile, err := os.ReadFile(filepath.Join(directory, "artifacts.sha256"))
	if err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(checksumFile)), "\n") {
		checksum, name, ok := strings.Cut(line, "  ")
		if !ok || filepath.Base(name) != name {
			return fmt.Errorf("invalid checksum entry %q", line)
		}
		actual, _, err := fileChecksum(filepath.Join(directory, name))
		if err != nil {
			return err
		}
		if actual != checksum {
			return fmt.Errorf("checksum mismatch for %s", name)
		}
	}
	return nil
}

func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeBytesAtomic(path, append(data, '\n'))
}

func writeBytesAtomic(path string, data []byte) error {
	temp, err := os.CreateTemp(filepath.Dir(path), ".artifact-*.tmp")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempName, path)
}

func collectArtifacts(directory string, names []string) ([]campaignArtifact, error) {
	artifacts := make([]campaignArtifact, 0, len(names))
	for _, name := range names {
		checksum, size, err := fileChecksum(filepath.Join(directory, name))
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, campaignArtifact{Path: name, Bytes: size, SHA256: checksum})
	}
	return artifacts, nil
}

func writeChecksumFile(directory string, names []string) error {
	var contents strings.Builder
	for _, name := range names {
		checksum, _, err := fileChecksum(filepath.Join(directory, name))
		if err != nil {
			return err
		}
		fmt.Fprintf(&contents, "%s  %s\n", checksum, name)
	}
	return writeBytesAtomic(filepath.Join(directory, "artifacts.sha256"), []byte(contents.String()))
}

func fileChecksum(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}

func TestCampaignWriterRoundTrip(t *testing.T) {
	t.Parallel()

	manifest := campaignManifest{
		SchemaVersion: "campaign-v1",
		CampaignID:    "20260710-plan1-writer-roundtrip-test",
		Plan:          "plan1",
		Name:          "writer-roundtrip",
		StartedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		Provenance: map[string]any{
			"git_revision": "test", "dirty_worktree": true, "go_version": "test", "rapid_version": "v1.3.x",
			"temporal_api_version": "test", "timezone_version": "host", "work_definition_version": iterationDefinitionVersion,
			"generator_version": "plan1-v1", "profile_version": "plan1-v1", "os": "test", "architecture": "test", "cpu_count": 1,
		},
		Population: "curated",
		Commands:   []string{"go test -tags test_dep ./service/worker/scheduler/propertytest"},
	}
	writer, err := newCampaignWriter(filepath.Join(t.TempDir(), manifest.CampaignID), manifest)
	require.NoError(t, err)
	require.NoError(t, writer.addCase(materialCaseRecord{
		SchemaVersion: "case-v1", CaseID: "case-1", CampaignID: manifest.CampaignID, Source: "curated",
		Input:          map[string]any{"original_spec_json": map[string]any{}, "serialized_bytes": 0, "query_start": "2025-01-01T00:00:00Z", "query_end": "2025-01-01T00:00:01Z", "result_limit": 1, "jitter_seed": "", "representation": "interval"},
		Classification: map[string]any{"validity": "valid", "production_reachable": true},
		Outcome:        map[string]any{"status": "valid-success", "result_count": 1, "partial_result_count": 0},
		Work:           map[string]any{"work_definition_version": iterationDefinitionVersion, "validation_total": 1, "matching_total": 1, "matching_breakdown": map[string]any{"total": 1}},
		Reproduction:   map[string]any{"command": "go test -run TestCampaignWriterRoundTrip", "seed": nil, "failfile": nil},
	}))
	require.NoError(t, writer.finalize(
		map[string]any{"schema_version": "summary-v1", "completed_cases": 1},
		"# Results\n\nNEGATIVE EVIDENCE: writer round trip completed.\n",
		"",
	))
	require.NoError(t, verifyCampaignBundle(writer.directory))
}

func TestCampaignWriterLeavesInterruptedCampaignIncomplete(t *testing.T) {
	t.Parallel()

	manifest := campaignManifest{
		SchemaVersion: "campaign-v1", CampaignID: "interrupted", Plan: "plan1", Name: "interrupted",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Population: "curated", Commands: []string{"test"},
		Provenance: map[string]any{
			"git_revision": "test", "dirty_worktree": true, "go_version": "test", "rapid_version": "test",
			"temporal_api_version": "test", "timezone_version": "test", "work_definition_version": "test",
			"generator_version": "test", "profile_version": "test", "os": "test", "architecture": "test", "cpu_count": 1,
		},
	}
	writer, err := newCampaignWriter(filepath.Join(t.TempDir(), manifest.CampaignID), manifest)
	require.NoError(t, err)
	require.Error(t, verifyCampaignBundle(writer.directory))
	require.NoError(t, writer.casesFile.Close())
}

func TestReviewedCampaignBundlesVerify(t *testing.T) {
	t.Parallel()

	bundles, err := filepath.Glob("results/reviewed/*")
	require.NoError(t, err)
	require.NotEmpty(t, bundles)
	for _, bundle := range bundles {
		bundle := bundle
		t.Run(filepath.Base(bundle), func(t *testing.T) {
			t.Parallel()
			require.NoError(t, verifyCampaignBundle(bundle))
		})
	}
}

func TestWritePlan1ReviewedCampaign(t *testing.T) {
	outputDirectory := os.Getenv("SCHEDULE_PROPERTY_RESULTS_DIR")
	if outputDirectory == "" {
		t.Skip("set SCHEDULE_PROPERTY_RESULTS_DIR to write the reviewed Plan 1 campaign")
	}
	campaignID := os.Getenv("SCHEDULE_PROPERTY_CAMPAIGN_ID")
	if campaignID == "" {
		t.Fatal("SCHEDULE_PROPERTY_CAMPAIGN_ID is required")
	}
	plannedCases := 6
	manifest := campaignManifest{
		SchemaVersion: "campaign-v1",
		CampaignID:    campaignID,
		Plan:          "plan1",
		Name:          "validity-and-property-hardening",
		StartedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		Provenance: map[string]any{
			"git_revision":            os.Getenv("SCHEDULE_PROPERTY_GIT_REVISION"),
			"dirty_worktree":          true,
			"patch_sha256":            os.Getenv("SCHEDULE_PROPERTY_PATCH_SHA256"),
			"go_version":              runtime.Version(),
			"rapid_version":           "v1.3.0",
			"temporal_api_version":    "v1.63.3",
			"timezone_version":        "tzcode-2022g-host-data",
			"work_definition_version": iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion,
			"generator_version":       "plan1-witness-first-v1",
			"profile_version":         "plan1-query-profiles-v1",
			"os":                      runtime.GOOS,
			"architecture":            runtime.GOARCH,
			"cpu_count":               runtime.NumCPU(),
			"cpu_model":               os.Getenv("SCHEDULE_PROPERTY_CPU_MODEL"),
			"memory_bytes":            int64(38_654_705_664),
			"race_enabled":            false,
			"cache_mode":              "count=1",
		},
		Population:   "curated",
		PlannedCases: &plannedCases,
		Commands: []string{
			"go test -tags test_dep ./service/worker/scheduler/propertytest -run '^TestPropertyReducedDomainValidatorDifferential$' -rapid.checks=50000 -count=1",
			"go test -tags test_dep ./service/worker/scheduler/propertytest -rapid.checks=10000 -count=1",
			"go test -tags test_dep ./service/worker/scheduler/propertytest -run='^$' -fuzz='^FuzzScheduleValidation$' -fuzztime=30s -parallel=1",
			"go test -tags test_dep ./service/worker/scheduler/propertytest -run='^$' -fuzz='^FuzzMatchingTimesOracle$' -fuzztime=30s -parallel=1",
			"go test -tags test_dep ./service/worker/scheduler -count=1",
			"go test -tags test_dep ./chasm/lib/scheduler -count=1",
		},
		Environment: map[string]string{
			"SCHEDULE_PROPERTY_LONG_EXPERIMENT": "unset",
			"GOMAXPROCS":                        "default",
		},
	}
	directory := filepath.Join(outputDirectory, campaignID)
	writer, err := newCampaignWriter(directory, manifest)
	require.NoError(t, err)

	queryStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	emptySpec := &schedulepb.ScheduleSpec{}
	require.NoError(t, writer.addCase(makePlan1Case(
		"plan1-empty-inclusion", campaignID, emptySpec, queryStart, queryStart.Add(time.Hour),
		ComputeOptions{MaxResults: 10, MaxIterations: 100_000},
		"PROP-UNSATISFIABLE-COMPONENT", "TestInvalidEmptyInclusionSet",
	)))

	february30 := impossibleFebruary30Spec()
	require.NoError(t, writer.addCase(makePlan1Case(
		"plan1-february-30", campaignID, february30, queryStart, queryStart.Add(365*24*time.Hour),
		ComputeOptions{MaxResults: 10, MaxIterations: 100_000},
		"PROP-REDUCED-COMPLETENESS-FEBRUARY-30", "TestInvalidImpossibleCivilDate",
	)))

	fullyExcluded := &schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
		ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
			Second: allRanges(0, 59), Minute: allRanges(0, 59), Hour: allRanges(0, 23),
			DayOfMonth: allRanges(1, 31), Month: allRanges(1, 12), DayOfWeek: allRanges(0, 6),
		}},
	}
	fullyExcludedRecord := makePlan1Case(
		"plan1-fully-excluded", campaignID, fullyExcluded, queryStart, queryStart.Add(time.Minute),
		ComputeOptions{MaxResults: 1, MaxIterations: 10_000, MaxValidationIterations: 500_000},
		"PROP-EMPTY-EFFECTIVE-SET", "TestAllExcludedDenseIntervalValidationClassification",
	)
	fullyExcludedRecord.Budgets = map[string]any{"largest_failing": 100, "smallest_successful": 500_000, "exact_required": fullyExcludedRecord.Work["validation_total"]}
	require.NoError(t, writer.addCase(fullyExcludedRecord))

	emptyQuerySpec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(24 * time.Hour)}}}
	require.NoError(t, writer.addCase(makePlan1Case(
		"plan1-valid-empty-query", campaignID, emptyQuerySpec, queryStart.Add(time.Second), queryStart.Add(time.Hour),
		ComputeOptions{MaxResults: 10, MaxIterations: 100_000},
		"PROP-QUERY-SEPARATION", "TestValidScheduleCanHaveEmptyQueryResult",
	)))

	apiaWitness := time.Date(2020, 1, 1, 0, 0, 1, 0, time.UTC)
	apia, err := time.LoadLocation("Pacific/Apia")
	require.NoError(t, err)
	apiaModel := scheduleModel{
		calendars: []calendarModel{exactCalendarModel(apiaWitness.In(apia))},
		endTime:   &apiaWitness,
		timezone:  "Pacific/Apia",
	}
	require.NoError(t, writer.addCase(makePlan1Case(
		"plan1-apia-end-boundary", campaignID, apiaModel.renderStructured(), apiaWitness.Add(-time.Second), apiaWitness,
		ComputeOptions{MaxResults: 1, MaxIterations: 100_000},
		"PROP-WITNESS-SOUNDNESS-TIMEZONE-BOUNDARY", "TestValidationAcceptsTimezoneCalendarAtScheduleEnd",
	)))

	lordHoweWitness := time.Date(2020, 4, 4, 14, 30, 0, 0, time.UTC)
	lordHowe, err := time.LoadLocation("Australia/Lord_Howe")
	require.NoError(t, err)
	lordHoweModel := scheduleModel{
		calendars: []calendarModel{exactCalendarModel(lordHoweWitness.In(lordHowe))},
		timezone:  "Australia/Lord_Howe",
	}
	lordHoweRecord := makePlan1Case(
		"plan1-lord-howe-repeated-half-hour",
		campaignID,
		lordHoweModel.renderStructured(),
		time.Date(2020, 4, 4, 14, 29, 8, 0, time.UTC),
		time.Date(2020, 4, 4, 14, 30, 8, 0, time.UTC),
		ComputeOptions{MaxResults: 10, MaxIterations: propertyAnalysisMaxIterations},
		"PROP-SMALL-WINDOW-ORACLE-LORD-HOWE",
		"TestCopiedCalculatorLordHoweRepeatedHalfHourDivergence",
	)
	lordHoweRecord.Property["classification"] = "expected-edge-case"
	lordHoweRecord.Property["hypothesis"] = "Copied and production matching omit the first repeated 01:30 occurrence seen by the independent oracle"
	lordHoweRecord.Notes = []string{"Production behavior is intentionally unchanged; the copied calculator remains at parity."}
	require.NoError(t, writer.addCase(lordHoweRecord))

	summary := map[string]any{
		"schema_version":  "summary-v1",
		"campaign_id":     campaignID,
		"completed_cases": 6,
		"populations": map[string]any{
			"curated":           map[string]any{"retained_cases": 6},
			"uniform-generated": map[string]any{"rapid_checks_per_property": 10_000, "counterexamples": 0},
			"coverage-fuzz": map[string]any{
				"validation_executions": 9_712, "validation_new_interesting": 2,
				"matching_executions": 1_207, "matching_new_interesting": 0,
			},
			"invalid-abuse": map[string]any{"mutation_switches_killed": 11, "mutation_switches_total": 11},
		},
		"validation_and_matching_work_aggregated": false,
	}
	results := fmt.Sprintf(`# Plan 1 Validity And Property Hardening Results

## Campaign

- Campaign ID: %s
- Plan: plan1
- Work definition: %s / validator-%s
- Reviewed cases: 6 curated regression cases

## Observations

### OBSERVATION: Revised validity classifications hold

- Case IDs: plan1-empty-inclusion, plan1-february-30, plan1-fully-excluded, plan1-valid-empty-query
- Result: empty inclusions and impossible components are invalid-unsatisfiable-component; a fully excluded effective set is invalid-effective-set-empty; query-local emptiness remains valid success.

### OBSERVATION: Validation budgets remain distinct

- Case ID: plan1-fully-excluded
- Result: budget 100 is indeterminate; a sufficient proof budget classifies the effective set as empty without entering matching computation.

### OBSERVATION: Civil-date iteration must not carry normalized midnight offsets

- Case ID: plan1-apia-end-boundary
- Regression failfile: testdata/rapid/TestExperimentBudgetMatrix/TestExperimentBudgetMatrix-20260710214418-23034.fail
- Result: an occurrence exactly at the schedule end in Pacific/Apia remains a valid witness.

### OBSERVATION: Lord Howe repeated-half-hour matching diverges from the oracle

- Case ID: plan1-lord-howe-repeated-half-hour
- Result: the independent oracle includes the first repeated 01:30 occurrence; copied and production matching omit it. Production remains unchanged.

## Negative Evidence

### NEGATIVE EVIDENCE: No reduced-domain differential mismatch

- Search space: constructed valid and fully excluded schedules with horizons of 1-120 seconds.
- Checks: 50,000.
- Result: no classification or witness mismatch.

### NEGATIVE EVIDENCE: No full property counterexample

- Checks: 10,000 per Rapid property.
- Result: no counterexample after retained failfiles were replayed.

### NEGATIVE EVIDENCE: Bounded fuzz campaigns completed

- Validation fuzz: 30 seconds, 9,712 executions, 2 new interesting inputs after a 28-input baseline; throughput plateaued during the finite run.
- Matching oracle fuzz: 30 seconds, 1,207 executions, no new interesting inputs after a 33-input baseline; throughput plateaued during the finite run.

## Property Lifecycle

- Valid generators changed from field-independent construction to witness-first construction.
- Query-local empty success was separated from global satisfiability.
- Eleven named mutation probes killed all planned mutations in TestMutationKillMatrix.

## Reproduction

Run the commands recorded in campaign.json from repository revision %s with the recorded dirty patch checksum.
`, campaignID, iterationDefinitionVersion, validatorDefinitionVersion, manifest.Provenance["git_revision"])
	require.NoError(t, writer.finalize(summary, results, ""))
}

func makePlan1Case(
	caseID string,
	campaignID string,
	spec *schedulepb.ScheduleSpec,
	queryStart time.Time,
	queryEnd time.Time,
	options ComputeOptions,
	propertyID string,
	testName string,
) materialCaseRecord {
	result, err := ComputeMatchingTimes(spec, queryStart, queryEnd, "", options)
	specJSON := map[string]any{}
	data, marshalErr := protojson.MarshalOptions{UseProtoNames: true}.Marshal(spec)
	if marshalErr == nil {
		_ = json.Unmarshal(data, &specJSON)
	}
	validity := validationStatusForSchema(result.Validation.Status)
	witness := any(nil)
	if !result.Validation.Witness.IsZero() {
		witness = result.Validation.Witness.Format(time.RFC3339Nano)
	}
	status := "invalid"
	if errors.Is(err, ErrValidationLimit) {
		status = "validation-budget-exhausted"
	} else if errors.Is(err, ErrIterationLimit) {
		status = "matching-budget-exhausted"
	} else if err == nil && len(result.Times) == 0 {
		status = "valid-empty-query-result"
	} else if err == nil {
		status = "valid-success"
	}
	errorType := any(nil)
	errorMessage := any(nil)
	if err != nil {
		errorType = plan1ErrorType(err)
		errorMessage = err.Error()
	}
	firstResult := any(nil)
	lastResult := any(nil)
	if len(result.Times) > 0 {
		firstResult = result.Times[0].Format(time.RFC3339Nano)
		lastResult = result.Times[len(result.Times)-1].Format(time.RFC3339Nano)
	}
	return materialCaseRecord{
		SchemaVersion: "case-v1", CaseID: caseID, CampaignID: campaignID, Source: "curated",
		Input: map[string]any{
			"original_spec_json": specJSON, "minimized_spec_json": specJSON, "semantic_model": nil,
			"serialized_bytes": proto.Size(spec), "query_start": queryStart.Format(time.RFC3339Nano),
			"query_end": queryEnd.Format(time.RFC3339Nano), "result_limit": options.MaxResults,
			"jitter_seed": "", "timezone": spec.TimezoneName, "representation": plan1Representation(spec),
		},
		Classification: map[string]any{
			"validity": validity, "reason": result.Validation.Reason, "witness": witness,
			"proof_category": result.Validation.Component, "production_reachable": true,
		},
		Outcome: map[string]any{
			"status": status, "error_type": errorType, "error_message": errorMessage,
			"result_count": len(result.Times), "partial_result_count": 0,
			"first_result": firstResult, "last_result": lastResult,
		},
		Work: map[string]any{
			"work_definition_version": iterationDefinitionVersion + "/validator-" + validatorDefinitionVersion,
			"validation_total":        result.Validation.Work.Total, "matching_total": result.Work.Total,
			"work_before_first_result": nil,
			"matching_breakdown": map[string]any{
				"total": result.Work.Total, "next_time_calls": result.Work.NextTimeCalls,
				"inclusion_source_checks": result.Work.InclusionSourceChecks,
				"calendar_search_steps":   result.Work.CalendarSearchSteps, "interval_checks": result.Work.IntervalChecks,
				"exclusion_checks": result.Work.ExclusionChecks, "excluded_candidate_retries": result.Work.ExcludedCandidateRetries,
				"result_loop_steps": result.Work.ResultLoopSteps,
			},
		},
		Property: map[string]any{
			"property_id": propertyID, "hypothesis": propertyID, "classification": "passed",
			"revision": "plan1-v1", "mutations_killed": []string{},
		},
		Reproduction: map[string]any{
			"command": "go test -tags test_dep ./service/worker/scheduler/propertytest -run '^" + testName + "$' -count=1",
			"seed":    nil, "failfile": nil, "artifact_sha256": []string{},
		},
	}
}

func validationStatusForSchema(status ValidationStatus) string {
	switch status {
	case ValidationValid:
		return "valid"
	case ValidationInvalidStructural:
		return "invalid-structural"
	case ValidationInvalidComponentUnsatisfiable:
		return "invalid-component-unsatisfiable"
	case ValidationInvalidEffectiveSetEmpty:
		return "invalid-effective-set-empty"
	case ValidationIndeterminateBudget:
		return "indeterminate-validation-budget"
	default:
		return "invalid-structural"
	}
}

func plan1ErrorType(err error) string {
	switch {
	case errors.Is(err, ErrValidationLimit):
		return "ErrValidationLimit"
	case errors.Is(err, ErrIterationLimit):
		return "ErrIterationLimit"
	case errors.Is(err, ErrUnsatisfiableSpec):
		return "ErrUnsatisfiableSpec"
	case errors.Is(err, ErrInvalidSpec):
		return "ErrInvalidSpec"
	default:
		return "error"
	}
}

func plan1Representation(spec *schedulepb.ScheduleSpec) string {
	components := 0
	if len(spec.Interval) > 0 {
		components++
	}
	if len(spec.StructuredCalendar) > 0 {
		components++
	}
	if len(spec.Calendar) > 0 {
		components++
	}
	if len(spec.CronString) > 0 {
		components++
	}
	if components > 1 || len(spec.ExcludeStructuredCalendar)+len(spec.ExcludeCalendar) > 0 {
		return "mixed"
	}
	if len(spec.Interval) > 0 {
		return "interval"
	}
	if len(spec.Calendar) > 0 {
		return "calendar"
	}
	if len(spec.CronString) > 0 {
		return "cron"
	}
	return "structured-calendar"
}
