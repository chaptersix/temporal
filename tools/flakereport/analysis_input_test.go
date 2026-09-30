package flakereport

import (
	"strings"
	"testing"
	"time"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/common/github"
)

func TestExtractFailuresRetainsEvidence(t *testing.T) {
	suites := &junit.Testsuites{Suites: []junit.Testsuite{{
		Testcases: []junit.Testcase{{
			Name: "TestExample", Classname: "example/package",
			Failure:   &junit.Result{Message: "boom", Data: "stack trace"},
			SystemOut: &junit.Output{Data: "stdout"}, SystemErr: &junit.Output{Data: "stderr"},
		}},
	}}}

	failures := extractFailures(suites, "junit-xml--42--99--1--unit-test", 42, time.Unix(1, 0))
	require.Len(t, failures, 1)
	require.Equal(t, "boom", failures[0].Message)
	require.Equal(t, "stack trace", failures[0].Body)
	require.Equal(t, "stdout", failures[0].SystemOut)
	require.Equal(t, "stderr", failures[0].SystemErr)
}

func TestBoundedFailureEvidence(t *testing.T) {
	failure := TestFailure{
		RunID: 1, JobID: "2", Message: strings.Repeat("m", maxEvidenceBytes),
		Body: strings.Repeat("b", 100), SystemOut: "out", SystemErr: "err",
	}
	evidence := boundedFailureEvidence("temporalio/temporal", failure)
	require.Len(t, evidence.Message, maxEvidenceBytes)
	require.Empty(t, evidence.Body)
	require.Empty(t, evidence.SystemOut)
	require.Empty(t, evidence.SystemErr)
}

func TestBuildAnalysisInputClustersAndBounds(t *testing.T) {
	var flaky []TestReport
	var failures []TestFailure
	for i := 0; i < 12; i++ {
		name := "Test" + string(rune('A'+i))
		suite := "TestSuite" + string(rune('A'+i))
		flaky = append(flaky, TestReport{TestName: name, FailureCount: i + 1, TotalRuns: 20})
		for evidenceIndex := 0; evidenceIndex < 5; evidenceIndex++ {
			failures = append(failures, TestFailure{Name: name, SuiteName: suite, RunID: int64(i + 1), Timestamp: time.Unix(int64(evidenceIndex), 0)})
		}
	}
	summary := &ReportSummary{FlakyTests: flaky}
	input := buildAnalysisInput("temporalio/temporal", []github.Run{{DatabaseID: 1}}, summary, failures, nil)
	require.Len(t, input.Clusters, maxAnalysisClusters)
	for _, cluster := range input.Clusters {
		require.NotEmpty(t, cluster.ID)
		require.LessOrEqual(t, len(cluster.Tests), maxAnalysisTestsPerGroup)
		for _, test := range cluster.Tests {
			require.LessOrEqual(t, len(test.Evidence), maxEvidencePerTest)
		}
	}
}
