// mutationtest checks that targeted regression tests detect deliberate
// production-code mutations. For each mutation it builds a mutated copy of one
// production file, points `go test -overlay` at it, runs the named tests, and
// requires a genuine assertion failure (not a build failure, not a pass).
//
// Edits are described by anchors that must each match exactly once in the
// unmutated file, so a refactor that moves or renames the anchored code makes
// this tool fail loudly instead of silently testing nothing.
//
// Run from anywhere inside the repository: go run ./cmd/tools/mutationtest
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// edit describes a single textual change to a file.
//
// If End is empty, the text Begin is replaced by Replacement.
// Otherwise the region from the start of Begin up to (but not including) the
// start of End is replaced by Replacement; End must appear after Begin.
// Begin and End must each occur exactly once in the file.
type edit struct {
	Begin       string
	End         string
	Replacement string
}

type mutation struct {
	Name    string
	File    string // repo-relative path of the production file to mutate
	Package string // package passed to go test
	Test    string // -run regex
	Edit    edit
}

const (
	schedulerDir  = "chasm/lib/scheduler/"
	schedulerTest = "./chasm/lib/scheduler/schedulertest"
)

var mutations = []mutation{
	{
		// Replace the guard so the callback is attached before a RunId exists.
		Name:    "early-callback",
		File:    schedulerDir + "completion.go",
		Package: schedulerTest,
		Test:    "TestStartAcceptanceCallbackAndRecording/AllowAll/before",
		Edit: edit{
			Begin:       "\tif !internal.TracksCompletionResult(start.GetOverlapPolicy()) && start.GetRunId() == \"\" {",
			Replacement: "\tif start.GetCompleted() != nil {",
		},
	},
	{
		// Delete the code between the discarded-selection comment and the
		// re-arm comment, so deferred starts are never re-armed.
		Name:    "deferred-rearm",
		File:    schedulerDir + "invoker.go",
		Package: schedulerTest,
		Test:    "TestExpiredSelectionReleasesRemainingBuffer|TestLedger_DeferredStartsStrandedByPausedCompletion",
		Edit: edit{
			Begin: "\t// A discarded selection cannot produce",
			End:   "\t// Re-arm tasks if this call changed state",
		},
	},
	{
		Name:    "capacity-progress",
		File:    schedulerDir + "backfiller_tasks.go",
		Package: "./chasm/lib/scheduler",
		Test:    "TestBackfillCapacityProgress",
		Edit: edit{
			Begin:       "return min(available, max(1, available/backfillerCount))",
			Replacement: "return available / backfillerCount",
		},
	},
	{
		// Short-circuit the selected-start predicate so that selected starts
		// awaiting a RunId no longer hold the overlap slot.
		Name:    "selected-start-reservation",
		File:    schedulerDir + "invoker_tasks.go",
		Package: schedulerTest,
		Test:    "TestSelectedStartReservesOverlapSlot",
		Edit: edit{
			Begin:       "start.GetAttempt() > 0 && start.GetCompleted() == nil &&\n\t\t\t\t",
			End:         "internal.TracksCompletionResult(start.GetOverlapPolicy())\n\t\t})",
			Replacement: "false && ",
		},
	},
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	for _, m := range mutations {
		if err := runMutation(root, m); err != nil {
			return fmt.Errorf("%s: %w", m.Name, err)
		}
		fmt.Printf("%s: detected\n", m.Name)
	}
	return nil
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found above working directory")
		}
		dir = parent
	}
}

// apply returns text with e applied, or an error if any anchor does not match
// exactly once.
func apply(text string, e edit) (string, error) {
	if n := strings.Count(text, e.Begin); n != 1 {
		return "", fmt.Errorf("begin anchor matched %d times, want exactly 1: %q", n, e.Begin)
	}
	first := strings.Index(text, e.Begin)
	if e.End == "" {
		return text[:first] + e.Replacement + text[first+len(e.Begin):], nil
	}
	if n := strings.Count(text, e.End); n != 1 {
		return "", fmt.Errorf("end anchor matched %d times, want exactly 1: %q", n, e.End)
	}
	last := strings.Index(text, e.End)
	if last < first {
		return "", fmt.Errorf("end anchor %q appears before begin anchor %q", e.End, e.Begin)
	}
	return text[:first] + e.Replacement + text[last:], nil
}

func runMutation(root string, m mutation) error {
	source := filepath.Join(root, m.File)
	original, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	mutated, err := apply(string(original), m.Edit)
	if err != nil {
		return fmt.Errorf("%s: %w", m.File, err)
	}

	temp, err := os.MkdirTemp("", "scheduler-mutation-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(temp) }()

	replacement := filepath.Join(temp, filepath.Base(m.File))
	if err := os.WriteFile(replacement, []byte(mutated), 0o644); err != nil {
		return err
	}
	overlay, err := json.Marshal(map[string]map[string]string{"Replace": {source: replacement}})
	if err != nil {
		return err
	}
	overlayPath := filepath.Join(temp, "overlay.json")
	if err := os.WriteFile(overlayPath, overlay, 0o644); err != nil {
		return err
	}

	cmd := exec.Command("go", "test", "-tags", "test_dep", "-overlay", overlayPath,
		m.Package, "-run", m.Test, "-count=1")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	output := string(out)
	if err == nil || !strings.Contains(output, "--- FAIL: ") || strings.Contains(output, "[build failed]") {
		return fmt.Errorf("mutation did not produce an assertion failure\n%s", output)
	}
	return nil
}
