package flakereport

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.temporal.io/server/tools/common/github"
)

const (
	maxAnalysisClusters      = 8
	maxAnalysisTestsPerGroup = 10
	maxEvidencePerTest       = 3
	maxEvidenceBytes         = 32 * 1024
)

type AnalysisInput struct {
	Repository  string            `json:"repository"`
	GeneratedAt time.Time         `json:"generated_at"`
	SourceRuns  []AnalysisRun     `json:"source_runs"`
	Clusters    []AnalysisCluster `json:"clusters"`
}

type AnalysisRun struct {
	RunID      int64     `json:"run_id"`
	HeadSHA    string    `json:"head_sha"`
	HeadBranch string    `json:"head_branch"`
	CreatedAt  time.Time `json:"created_at"`
}

type AnalysisCluster struct {
	ID       string         `json:"id"`
	Suite    string         `json:"suite"`
	Severity string         `json:"severity"`
	Tests    []AnalysisTest `json:"tests"`
	score    float64
}

type AnalysisTest struct {
	TestName       string            `json:"test_name"`
	FailureType    string            `json:"failure_type"`
	FailureCount   int               `json:"failure_count"`
	TotalRuns      int               `json:"total_runs"`
	FailureLinks   []string          `json:"failure_links"`
	Evidence       []FailureEvidence `json:"evidence"`
	BisectSuspects []AnalysisSuspect `json:"bisect_suspects,omitempty"`
	LastFailure    time.Time         `json:"last_failure"`
	severityRank   int
}

type FailureEvidence struct {
	RunID      int64     `json:"run_id"`
	JobID      string    `json:"job_id"`
	MatrixName string    `json:"matrix_name"`
	Timestamp  time.Time `json:"timestamp"`
	Link       string    `json:"link"`
	Message    string    `json:"message,omitempty"`
	Body       string    `json:"body,omitempty"`
	SystemOut  string    `json:"system_out,omitempty"`
	SystemErr  string    `json:"system_err,omitempty"`
}

type AnalysisSuspect struct {
	CommitSHA   string  `json:"commit_sha"`
	Probability float64 `json:"probability"`
	Title       string  `json:"title"`
	Author      string  `json:"author"`
	Date        string  `json:"date"`
}

type categorizedReport struct {
	report       TestReport
	failureType  string
	severityRank int
}

func buildAnalysisInput(
	repo string,
	runs []github.Run,
	summary *ReportSummary,
	allFailures []TestFailure,
	bisectReports []TestBisectReport,
) AnalysisInput {
	failuresByTest := make(map[string][]TestFailure)
	for _, failure := range allFailures {
		name := normalizeTestName(failure.Name)
		failuresByTest[name] = append(failuresByTest[name], failure)
	}
	for name := range failuresByTest {
		sort.Slice(failuresByTest[name], func(i, j int) bool {
			return failuresByTest[name][i].Timestamp.After(failuresByTest[name][j].Timestamp)
		})
	}

	bisectByTest := make(map[string][]AnalysisSuspect)
	for _, report := range bisectReports {
		if report.Skipped {
			continue
		}
		for _, suspect := range report.TopSuspects[:min(3, len(report.TopSuspects))] {
			bisectByTest[report.TestName] = append(bisectByTest[report.TestName], AnalysisSuspect{
				CommitSHA:   suspect.CommitSHA,
				Probability: suspect.Probability,
				Title:       suspect.CommitTitle,
				Author:      suspect.CommitAuthor,
				Date:        suspect.CommitDate,
			})
		}
	}

	byName := make(map[string]categorizedReport)
	addReports := func(reports []TestReport, failureType string, rank int) {
		for _, report := range reports {
			current, ok := byName[report.TestName]
			if !ok || rank > current.severityRank {
				byName[report.TestName] = categorizedReport{report: report, failureType: failureType, severityRank: rank}
			}
		}
	}
	addReports(summary.FlakyTests, "flaky", 1)
	addReports(summary.Timeouts, "timeout", 2)
	addReports(summary.Crashes, "crash", 3)
	addReports(summary.CIBreakers, "ci_breaker", 4)

	clusters := make(map[string]*AnalysisCluster)
	for testName, categorized := range byName {
		failures := failuresByTest[testName]
		suite := clusterSuite(failures)
		cluster := clusters[suite]
		if cluster == nil {
			cluster = &AnalysisCluster{Suite: suite}
			clusters[suite] = cluster
		}

		report := categorized.report
		test := AnalysisTest{
			TestName:       testName,
			FailureType:    categorized.failureType,
			FailureCount:   report.FailureCount,
			TotalRuns:      report.TotalRuns,
			FailureLinks:   report.GitHubURLs,
			BisectSuspects: bisectByTest[testName],
			LastFailure:    report.LastFailure,
			severityRank:   categorized.severityRank,
		}
		for _, failure := range failures[:min(maxEvidencePerTest, len(failures))] {
			test.Evidence = append(test.Evidence, boundedFailureEvidence(repo, failure))
		}
		cluster.Tests = append(cluster.Tests, test)
		if categorized.severityRank > severityRank(cluster.Severity) {
			cluster.Severity = categorized.failureType
		}
		rate := float64(report.FailureCount) / float64(max(1, report.TotalRuns))
		cluster.score += float64(categorized.severityRank*1000) + rate*100
	}

	resultClusters := make([]AnalysisCluster, 0, len(clusters))
	for _, cluster := range clusters {
		sort.Slice(cluster.Tests, func(i, j int) bool {
			if cluster.Tests[i].severityRank != cluster.Tests[j].severityRank {
				return cluster.Tests[i].severityRank > cluster.Tests[j].severityRank
			}
			ri := float64(cluster.Tests[i].FailureCount) / float64(max(1, cluster.Tests[i].TotalRuns))
			rj := float64(cluster.Tests[j].FailureCount) / float64(max(1, cluster.Tests[j].TotalRuns))
			return ri > rj
		})
		cluster.Tests = cluster.Tests[:min(maxAnalysisTestsPerGroup, len(cluster.Tests))]
		resultClusters = append(resultClusters, *cluster)
	}
	sort.Slice(resultClusters, func(i, j int) bool { return resultClusters[i].score > resultClusters[j].score })
	resultClusters = resultClusters[:min(maxAnalysisClusters, len(resultClusters))]
	for i := range resultClusters {
		resultClusters[i].ID = fmt.Sprintf("%02d-%s", i+1, slug(resultClusters[i].Suite))
		resultClusters[i].score = 0
		for j := range resultClusters[i].Tests {
			resultClusters[i].Tests[j].severityRank = 0
		}
	}

	input := AnalysisInput{
		Repository:  repo,
		GeneratedAt: time.Now().UTC(),
		Clusters:    resultClusters,
	}
	for _, run := range runs {
		input.SourceRuns = append(input.SourceRuns, AnalysisRun{
			RunID: run.DatabaseID, HeadSHA: run.HeadSHA, HeadBranch: run.HeadBranch, CreatedAt: run.CreatedAt,
		})
	}
	return input
}

func clusterSuite(failures []TestFailure) string {
	for _, failure := range failures {
		if failure.SuiteName != "" {
			return failure.SuiteName
		}
		if failure.ClassName != "" {
			return failure.ClassName
		}
	}
	return "unknown"
}

func severityRank(severity string) int {
	switch severity {
	case "ci_breaker":
		return 4
	case "crash":
		return 3
	case "timeout":
		return 2
	case "flaky":
		return 1
	default:
		return 0
	}
}

func boundedFailureEvidence(repo string, failure TestFailure) FailureEvidence {
	remaining := maxEvidenceBytes
	bound := func(value string) string {
		if remaining <= 0 {
			return ""
		}
		if len(value) > remaining {
			value = value[:remaining]
		}
		remaining -= len(value)
		return value
	}
	return FailureEvidence{
		RunID:      failure.RunID,
		JobID:      failure.JobID,
		MatrixName: failure.MatrixName,
		Timestamp:  failure.Timestamp,
		Link:       buildGitHubURL(repo, fmt.Sprintf("%d", failure.RunID), failure.JobID),
		Message:    bound(failure.Message),
		Body:       bound(failure.Body),
		SystemOut:  bound(failure.SystemOut),
		SystemErr:  bound(failure.SystemErr),
	}
}

var nonSlug = regexp.MustCompile(`[^a-z0-9]+`)

func slug(value string) string {
	value = strings.Trim(nonSlug.ReplaceAllString(strings.ToLower(value), "-"), "-")
	if value == "" {
		return "unknown"
	}
	return value
}

func writeAnalysisInput(outputDir string, input AnalysisInput) error {
	data, err := json.MarshalIndent(input, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal analysis input: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "analysis-input.json"), data, 0644); err != nil {
		return fmt.Errorf("failed to write analysis-input.json: %w", err)
	}
	return nil
}
