package flakereport

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseModelInvestigation(t *testing.T) {
	raw := []byte(`{"type":"text","part":{"text":"{\"affected_tests\":[\"TestA\"],\"test_purpose\":\"checks A\",\"likely_cause\":\"race\",\"evidence\":[\"stack\"],\"relevant_source_paths\":[\"a.go\"],\"confidence\":0.8,\"suggested_next_action\":\"add synchronization\"}"}}`)
	result, err := parseModelInvestigation(raw)
	require.NoError(t, err)
	require.Equal(t, []string{"TestA"}, result.AffectedTests)
	require.Equal(t, 0.8, result.Confidence)
}

func TestParseModelInvestigationRejectsMalformedOutput(t *testing.T) {
	_, err := parseModelInvestigation([]byte(`{"type":"text","text":"not json"}`))
	require.Error(t, err)

	_, err = parseModelInvestigation([]byte(`{"affected_tests":["TestA"],"confidence":2}`))
	require.Error(t, err)
}

func TestInvestigateClusterOmitsLowConfidence(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key")
	repoRoot := t.TempDir()
	command := writeFakeOpenCode(t, `{"affected_tests":["TestA"],"test_purpose":"checks A","likely_cause":"unknown","evidence":["weak signal"],"relevant_source_paths":[],"confidence":0.2,"suggested_next_action":"collect more failures"}`)

	result := investigateCluster(context.Background(), repoRoot, command, "openai/test", 0.6, codeOwners{}, AnalysisCluster{ID: "01", Suite: "TestSuite"})
	require.Equal(t, "omitted", result.investigation.Status)
	require.Contains(t, result.investigation.OmissionReason, "below threshold")
	require.Empty(t, result.investigation.LikelyCause)
	require.Empty(t, result.investigation.Owners)
}

func TestInvestigateClusterHandlesTimeout(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key")
	repoRoot := t.TempDir()
	command := writeFakeOpenCode(t, `{"affected_tests":["TestA"]}`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := investigateCluster(ctx, repoRoot, command, "openai/test", 0.6, codeOwners{}, AnalysisCluster{ID: "01", Suite: "TestSuite"})
	require.Equal(t, "omitted", result.investigation.Status)
	require.Contains(t, strings.ToLower(result.investigation.OmissionReason), "timed out")
}

func TestInvestigateClusterHandlesMalformedOutput(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key")
	command := writeFakeOpenCode(t, "not-json")
	result := investigateCluster(context.Background(), t.TempDir(), command, "openai/test", 0.6, codeOwners{}, AnalysisCluster{ID: "01", Suite: "TestSuite"})
	require.Contains(t, result.investigation.OmissionReason, "malformed OpenCode output")
}

func TestInvestigateClusterHandlesMissingCredentials(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	result := investigateCluster(context.Background(), t.TempDir(), "opencode", "openai/test", 0.6, codeOwners{}, AnalysisCluster{ID: "01", Suite: "TestSuite"})
	require.Equal(t, "omitted", result.investigation.Status)
	require.Contains(t, result.investigation.OmissionReason, "unavailable")
}

func TestIsolatedOpenCodeEnvironment(t *testing.T) {
	env := isolatedOpenCodeEnv(
		[]string{"OPENAI_API_KEY=test-key", "OPENCODE_CONFIG_CONTENT=personal", "XDG_CONFIG_HOME=personal"},
		"/tmp/isolated",
		"/tmp/repository",
	)
	values := make(map[string]string)
	for _, item := range env {
		key, value, _ := strings.Cut(item, "=")
		values[key] = value
	}
	require.Equal(t, "test-key", values["OPENAI_API_KEY"])
	require.Equal(t, "/tmp/isolated/config", values["XDG_CONFIG_HOME"])
	require.Equal(t, "true", values["OPENCODE_DISABLE_DEFAULT_PLUGINS"])
	require.Equal(t, "true", values["OPENCODE_DISABLE_CLAUDE_CODE"])
	require.Equal(t, "/tmp/isolated/focused-test-ran", values["FLAKEREPORT_TEST_MARKER"])
	require.NotContains(t, values["OPENCODE_CONFIG_CONTENT"], "personal")

	var config map[string]any
	require.NoError(t, json.Unmarshal([]byte(values["OPENCODE_CONFIG_CONTENT"]), &config))
	permission := config["permission"].(map[string]any)
	require.Equal(t, "deny", permission["skill"])
	require.Equal(t, "deny", permission["websearch"])
}

func writeFakeOpenCode(t *testing.T, output string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "opencode")
	script := "#!/usr/bin/env bash\nprintf '%s\\n' " + shellQuote(output) + "\n"
	require.NoError(t, os.WriteFile(path, []byte(script), 0755))
	return path
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
