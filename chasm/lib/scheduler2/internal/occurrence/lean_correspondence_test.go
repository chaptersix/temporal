//go:build lean_correspondence

package occurrence_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

// This explicitly selected suite requires the independently compiled oracle.
// Example: SCHEDULER2_LEAN_ORACLE=/path/oracle go test -tags test_dep,lean_correspondence
// ./chasm/lib/scheduler2/internal/occurrence -run '^TestLeanCorrespondence' .
// Missing binaries fail: ordinary package runs do not silently count as oracle evidence.
type leanCandidate struct {
	Nominal  string `json:"nominal"`
	Actual   string `json:"actual"`
	Revision string `json:"revision"`
	Excluded bool   `json:"excluded"`
}
type leanSource struct {
	Identity      string          `json:"identity"`
	Revision      string          `json:"revision"`
	Lower         string          `json:"lower"`
	NominalEnd    string          `json:"nominalEnd"`
	Upper         string          `json:"upper"`
	HasInclusions bool            `json:"hasInclusions"`
	Raw           []leanCandidate `json:"raw"`
}
type leanState struct {
	Identity        string         `json:"identity"`
	Revision        string         `json:"revision"`
	Origin          string         `json:"origin"`
	SearchAfter     string         `json:"searchAfter"`
	DisposedThrough string         `json:"disposedThrough"`
	Pending         *leanCandidate `json:"pending"`
	FullRange       bool           `json:"fullRange"`
	EligibleSeen    bool           `json:"eligibleSeen"`
	Terminal        string         `json:"terminal"`
}
type leanRequest struct {
	Version int        `json:"version"`
	Source  leanSource `json:"source"`
	State   *leanState `json:"state"`
	Op      string     `json:"op"`
	After   string     `json:"after,omitempty"`
	Start   string     `json:"start,omitempty"`
	End     string     `json:"end,omitempty"`
	Through string     `json:"through,omitempty"`
	Probes  string     `json:"probes,omitempty"`
	Outputs string     `json:"outputs,omitempty"`
	At      string     `json:"at,omitempty"`
}
type leanResponse struct {
	Version    int             `json:"version"`
	OK         bool            `json:"ok"`
	State      *leanState      `json:"state,omitempty"`
	Status     string          `json:"status,omitempty"`
	Outputs    []leanCandidate `json:"outputs,omitempty"`
	Probes     string          `json:"probes,omitempty"`
	Superseded *leanCandidate  `json:"superseded,omitempty"`
	Error      string          `json:"error,omitempty"`
}

var leanEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond)
var leanUpper = time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond)

func leanTime(t time.Time) string { return strconv.FormatInt(t.Sub(leanEpoch).Nanoseconds(), 10) }
func leanStatus(s occurrence.Status) string {
	return map[occurrence.Status]string{occurrence.Active: "active", occurrence.Yield: "yield", occurrence.Limited: "limited", occurrence.Waiting: "waiting", occurrence.Empty: "empty", occurrence.Impossible: "impossible", occurrence.Exhausted: "exhausted"}[s]
}
func leanCandidateOf(c *occurrence.Candidate) *leanCandidate {
	if c == nil {
		return nil
	}
	return &leanCandidate{leanTime(c.Nominal), leanTime(c.Actual), strconv.FormatInt(c.Revision, 10), c.Excluded}
}
func leanStateOf(s occurrence.State) *leanState {
	return &leanState{s.DefinitionID, strconv.FormatInt(s.Revision, 10), leanTime(s.Origin), leanTime(s.SearchAfter), leanTime(s.DisposedThrough), leanCandidateOf(s.Pending), s.FullRange, s.EligibleSeen, leanStatus(s.Terminal)}
}

type leanProcess struct {
	input   io.WriteCloser
	output  *bufio.Scanner
	command *exec.Cmd
	stderr  bytes.Buffer
}

func newLeanProcess(t *testing.T) *leanProcess {
	t.Helper()
	path := os.Getenv("SCHEDULER2_LEAN_ORACLE")
	require.NotEmpty(t, path, "required executable oracle: set SCHEDULER2_LEAN_ORACLE; no skip/fallback is supported")
	// testing cancels t.Context before cleanup. Own the process context so EOF
	// and Wait can complete normally; the independent deadline still kills a
	// hung oracle, and cleanup reports every nonzero exit.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	p := &leanProcess{command: exec.CommandContext(ctx, path)}
	var err error
	p.input, err = p.command.StdinPipe()
	require.NoError(t, err)
	out, err := p.command.StdoutPipe()
	require.NoError(t, err)
	p.output = bufio.NewScanner(out)
	p.output.Buffer(make([]byte, 4096), 4*1024*1024)
	p.command.Stderr = &p.stderr
	require.NoError(t, p.command.Start())
	t.Cleanup(func() {
		_ = p.input.Close()
		err := p.command.Wait()
		cancel()
		require.NoError(t, err, "oracle stderr: %s", p.stderr.String())
	})
	return p
}
func (p *leanProcess) call(r leanRequest) (leanResponse, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return leanResponse{}, err
	}
	return p.callRaw(b)
}
func (p *leanProcess) callRaw(b []byte) (leanResponse, error) {
	if _, err := p.input.Write(append(b, '\n')); err != nil {
		return leanResponse{}, err
	}
	if !p.output.Scan() {
		return leanResponse{}, fmt.Errorf("oracle response missing: %v", p.output.Err())
	}
	var response leanResponse
	decoder := json.NewDecoder(bytes.NewReader(p.output.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return response, fmt.Errorf("oracle response %q: %w", p.output.Text(), err)
	}
	return response, nil
}

// This fixture defines an independently evaluable subset: union of integer-second
// intervals, UTC second exclusions, explicit finite nominal bounds, and jitter.
// It does not call production calendar search, canonicalization or NextTimeCandidate.
// time.MarshalBinary and Farm32 are trusted shared primitives, not proven by this test.
type leanFixture struct {
	Start    time.Time `json:"start"`
	Span     int       `json:"span"`
	Periods  []int     `json:"periods"`
	Exclude  []int     `json:"exclude"`
	JitterMS int       `json:"jitter_ms"`
	Seed     string    `json:"seed"`
	Revision int64     `json:"revision"`
}

func (f leanFixture) compile() (*occurrence.Enumerator, leanSource, error) {
	end := f.Start.Add(time.Duration(f.Span) * time.Second)
	spec := &schedulepb.ScheduleSpec{StartTime: timestamppb.New(f.Start), EndTime: timestamppb.New(end), Jitter: durationpb.New(time.Duration(f.JitterMS) * time.Millisecond)}
	for _, period := range f.Periods {
		spec.Interval = append(spec.Interval, &schedulepb.IntervalSpec{Interval: durationpb.New(time.Duration(period) * time.Second)})
	}
	for _, second := range f.Exclude {
		spec.ExcludeStructuredCalendar = append(spec.ExcludeStructuredCalendar, &schedulepb.StructuredCalendarSpec{
			Second: []*schedulepb.Range{{Start: int32(second)}}, Minute: []*schedulepb.Range{{Start: 0, End: 59}}, Hour: []*schedulepb.Range{{Start: 0, End: 23}},
			DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}}, Month: []*schedulepb.Range{{Start: 1, End: 12}}, DayOfWeek: []*schedulepb.Range{{Start: 0, End: 6}}, Year: []*schedulepb.Range{{Start: 2000, End: 2100}},
		})
	}
	e, err := occurrence.New(spec, f.Seed, f.Revision)
	if err != nil {
		return nil, leanSource{}, err
	}
	// DefinitionID is opaque authentication, obtained from the public initial state.
	// This is deliberately not evidence that the production hash is collision-free.
	initial, err := e.Begin(f.Start.Add(-time.Nanosecond))
	if err != nil {
		return nil, leanSource{}, err
	}
	src := leanSource{Identity: initial.DefinitionID, Revision: strconv.FormatInt(f.Revision, 10), Lower: leanTime(f.Start), NominalEnd: leanTime(end), Upper: leanTime(leanUpper), HasInclusions: len(f.Periods) > 0, Raw: []leanCandidate{}}
	// Our periods are at most seven seconds; 8 seconds provides the following
	// nominal even beyond the spec end, necessary to cap the last candidate's jitter.
	var raw []time.Time
	for at := f.Start.Truncate(time.Second); !at.After(end.Add(8 * time.Second)); at = at.Add(time.Second) {
		if at.Before(f.Start) {
			continue
		}
		for _, period := range f.Periods {
			if at.Unix()%int64(period) == 0 {
				raw = append(raw, at)
				break
			}
		}
	}
	for i, nominal := range raw {
		if nominal.After(end) {
			break
		}
		excluded := false
		for _, second := range f.Exclude {
			excluded = excluded || nominal.Second() == second
		}
		actual := nominal
		if !excluded {
			bound := time.Duration(f.JitterMS) * time.Millisecond
			if i+1 < len(raw) {
				bound = min(bound, raw[i+1].Sub(nominal))
			}
			b, err := nominal.MarshalBinary()
			if err != nil {
				return nil, leanSource{}, err
			}
			fraction := uint64(farm.Fingerprint32(append(b, []byte(f.Seed)...)))
			millis := min(uint64(bound/time.Millisecond), uint64(math.MaxUint32))
			actual = nominal.Add(time.Duration(fraction*millis>>32) * time.Millisecond)
		}
		src.Raw = append(src.Raw, *leanCandidateOf(&occurrence.Candidate{Nominal: nominal, Actual: actual, Revision: f.Revision, Excluded: excluded}))
	}
	return e, src, nil
}

// Error mapping is exact and closed over concrete public messages. A new error is
// a test failure, not a catch-all normalization that can hide a changed contract.
func leanError(err error, op string) (string, error) {
	if err == nil {
		return "", nil
	}
	switch err.Error() {
	case "continuation belongs to another definition revision":
		return "identity-mismatch", nil
	case "invalid enumeration continuation", "backfill handoff requires an eligible pending candidate":
		return "state-invalid", nil
	case "cursor is outside the prototype range", "invalid inclusive backfill range", "backfill end precedes origin", "unsupported backfill range or probe limit", "unsupported window or enumeration limits":
		return "input-invalid", nil
	case "range rebase requires a newer revision":
		return "stale-revision", nil
	case "update requires a newer revision and supported time":
		return "", fmt.Errorf("classify update rejection using its explicit inputs: %w", err)
	case "jittered time is outside the prototype range":
		return "actual-outside-range", nil
	default:
		return "", fmt.Errorf("unmapped %s error: %w", op, err)
	}
}

type leanStep struct {
	Definition    string    `json:"definition,omitempty"`
	ExpectSuccess bool      `json:"expect_success,omitempty"`
	Op            string    `json:"op"`
	Time          time.Time `json:"time"`
	End           time.Time `json:"end"`
	Probes        int       `json:"probes"`
	Outputs       int       `json:"outputs"`
}

func compareLeanStep(p *leanProcess, e *occurrence.Enumerator, src leanSource, state occurrence.State, step leanStep) (occurrence.State, leanResponse, error) {
	before, _ := json.Marshal(state)
	req := leanRequest{Version: 1, Source: src, State: leanStateOf(state), Op: step.Op}
	var next occurrence.State
	var result occurrence.Result
	var superseded *occurrence.Candidate
	var err error
	switch step.Op {
	case "begin":
		req.State = nil
		req.After = leanTime(step.Time)
		next, err = e.Begin(step.Time)
	case "beginRange":
		req.State = nil
		req.Start = leanTime(step.Time)
		req.End = leanTime(step.End)
		next, err = e.BeginRange(step.Time, step.End)
	case "run":
		req.Through = leanTime(step.Time)
		req.Probes = strconv.Itoa(step.Probes)
		req.Outputs = strconv.Itoa(step.Outputs)
		result, err = e.Run(state, step.Time, occurrence.Limits{Probes: step.Probes, Outputs: step.Outputs})
		next = result.State
	case "peek":
		req.Through = leanTime(step.Time)
		req.Probes = strconv.Itoa(step.Probes)
		result, err = e.Peek(state, step.Time, step.Probes)
		next = result.State
	case "peekRange":
		req.End = leanTime(step.Time)
		req.Probes = strconv.Itoa(step.Probes)
		result, err = e.PeekRange(state, step.Time, step.Probes)
		next = result.State
	case "disposeRange":
		next, err = e.DisposeRange(state)
	case "update":
		req.At = leanTime(step.Time)
		next, superseded, err = e.Update(state, step.Time)
	case "rebaseRange":
		next, superseded, err = e.RebaseRange(state)
	default:
		return state, leanResponse{}, fmt.Errorf("unsupported Go test step %q", step.Op)
	}
	after, _ := json.Marshal(state)
	if !bytes.Equal(before, after) {
		return state, leanResponse{}, errors.New("Go mutated supplied continuation")
	}
	expected := leanResponse{Version: 1, OK: err == nil}
	if err != nil {
		if step.Op == "update" && err.Error() == "update requires a newer revision and supported time" {
			// Go combines these conditions in one error. Classify stale first,
			// matching the named wire projection when both conditions fail.
			revision, parseErr := strconv.ParseInt(src.Revision, 10, 64)
			if parseErr != nil {
				return state, expected, parseErr
			}
			if revision <= state.Revision {
				expected.Error = "stale-revision"
			} else {
				expected.Error = "input-invalid"
			}
		} else {
			expected.Error, err = leanError(err, step.Op)
			if err != nil {
				return state, expected, err
			}
		}
	} else {
		expected.State = leanStateOf(next)
		expected.Status = leanStatus(next.Terminal)
		expected.Probes = "0"
		expected.Outputs = []leanCandidate{}
		expected.Superseded = leanCandidateOf(superseded)
		if step.Op == "run" || step.Op == "peek" || step.Op == "peekRange" {
			expected.Status = leanStatus(result.Status)
			expected.Probes = strconv.Itoa(result.Probes)
			for _, c := range result.Occurrences {
				expected.Outputs = append(expected.Outputs, *leanCandidateOf(&c))
			}
		}
	}
	got, callErr := p.call(req)
	if callErr != nil {
		return state, got, callErr
	}
	// Marshal the typed observation to compare every mapped field, including exact
	// cursors, terminal cache, pending exclusions, probes and superseded identity.
	wantJSON, _ := json.Marshal(expected)
	gotJSON, _ := json.Marshal(got)
	if !bytes.Equal(wantJSON, gotJSON) {
		requestJSON, _ := json.Marshal(req)
		return state, got, fmt.Errorf("request=%s\nGo=%s\nLean=%s", requestJSON, wantJSON, gotJSON)
	}
	if step.ExpectSuccess && !got.OK {
		requestJSON, _ := json.Marshal(req)
		return state, got, fmt.Errorf("positive replay step rejected: request=%s\nGo=%s\nLean=%s", requestJSON, wantJSON, gotJSON)
	}
	if !expected.OK {
		return state, got, nil
	}
	return next, got, nil
}

func TestLeanCorrespondenceExhaustive(t *testing.T) {
	p := newLeanProcess(t)
	// Exhaust all 16 exclusion subsets, 2 fractional starts, 2 jitter settings,
	// and 3x2 bounded budgets. Every trace resumes to a cached terminal result.
	for mask := 0; mask < 16; mask++ {
		for _, fraction := range []time.Duration{0, time.Nanosecond} {
			for _, jitter := range []int{0, 1700} {
				for probes := 1; probes <= 3; probes++ {
					for outputs := 1; outputs <= 2; outputs++ {
						f := leanFixture{Start: instant().Add(fraction), Span: 4, Periods: []int{1}, JitterMS: jitter, Seed: "exhaustive", Revision: 1}
						for second := 0; second < 4; second++ {
							if mask&(1<<second) != 0 {
								f.Exclude = append(f.Exclude, second)
							}
						}
						e, src, err := f.compile()
						require.NoError(t, err)
						state, _, err := compareLeanStep(p, e, src, occurrence.State{}, leanStep{Op: "begin", Time: f.Start.Add(-time.Nanosecond)})
						require.NoError(t, err)
						done := false
						for n := 0; n < 16; n++ {
							next, response, err := compareLeanStep(p, e, src, state, leanStep{Op: "run", Time: f.Start.Add(6 * time.Second), Probes: probes, Outputs: outputs})
							require.NoError(t, err)
							state = next
							if response.Status == "impossible" || response.Status == "exhausted" {
								done = true
								break
							}
						}
						require.True(t, done, "bounded exhaustive continuation did not terminate")
						_, _, err = compareLeanStep(p, e, src, state, leanStep{Op: "run", Time: f.Start.Add(6 * time.Second), Probes: probes, Outputs: outputs})
						require.NoError(t, err)
					}
				}
			}
		}
	}
}

func TestLeanCorrespondenceRapid(t *testing.T) {
	p := newLeanProcess(t)
	rapid.Check(t, func(t *rapid.T) {
		f := leanFixture{Start: instant().Add(time.Duration(rapid.IntRange(0, 999999999).Draw(t, "fraction"))), Span: rapid.IntRange(1, 12).Draw(t, "span"), Periods: []int{rapid.IntRange(1, 7).Draw(t, "periodA"), rapid.IntRange(1, 7).Draw(t, "periodB")}, JitterMS: rapid.IntRange(0, 9000).Draw(t, "jitterMS"), Seed: rapid.StringMatching("[a-z]{1,8}").Draw(t, "seed"), Revision: 1}
		f.Exclude = rapid.SliceOfN(rapid.IntRange(0, 15), 0, 10).Draw(t, "excludedSeconds")
		trace := newLeanReplay(f)
		e, src, err := f.compile()
		if err != nil {
			leanRapidFailure(t, trace, err)
		}
		begin := leanStep{Op: "beginRange", Time: f.Start, End: f.Start.Add(time.Duration(f.Span+8) * time.Second)}
		begin = recordLeanStep(&trace, "initial", begin)
		state, _, err := compareLeanStep(p, e, src, occurrence.State{}, begin)
		if err != nil {
			leanRapidFailure(t, trace, err)
		}
		historical := rapid.Bool().Draw(t, "historical")
		for n := 0; n < 40; n++ {
			op := "run"
			if historical {
				op = "peekRange"
			}
			step := leanStep{Op: op, Time: f.Start.Add(time.Duration(f.Span+8) * time.Second), Probes: rapid.IntRange(1, 4).Draw(t, fmt.Sprintf("probes%d", n)), Outputs: rapid.IntRange(1, 3).Draw(t, fmt.Sprintf("outputs%d", n))}
			step = recordLeanStep(&trace, "initial", step)
			var response leanResponse
			state, response, err = compareLeanStep(p, e, src, state, step)
			if err != nil {
				leanRapidFailure(t, trace, err)
			}
			if historical && response.Status == "active" {
				dispose := leanStep{Op: "disposeRange"}
				dispose = recordLeanStep(&trace, "initial", dispose)
				state, _, err = compareLeanStep(p, e, src, state, dispose)
				if err != nil {
					leanRapidFailure(t, trace, err)
				}
			}
			if response.Status == "empty" || response.Status == "impossible" || response.Status == "exhausted" {
				return
			}
		}
		leanRapidFailure(t, trace, errors.New("40-call bound exceeded"))
	})
}

type leanReplay struct {
	Version     int                    `json:"version"`
	Fixture     *leanFixture           `json:"fixture,omitempty"`
	Definitions map[string]leanFixture `json:"definitions,omitempty"`
	Steps       []leanStep             `json:"steps"`
}

func newLeanReplay(initial leanFixture) leanReplay {
	return leanReplay{Version: 2, Definitions: map[string]leanFixture{"initial": initial}}
}

func recordLeanStep(trace *leanReplay, definition string, step leanStep) leanStep {
	step.Definition = definition
	step.ExpectSuccess = true
	trace.Steps = append(trace.Steps, step)
	return step
}

func leanRapidFailure(t *rapid.T, trace leanReplay, err error) {
	t.Helper()
	encoded, marshalErr := json.Marshal(trace)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	// This is a direct property assertion. Rapid shrinks failing draws and emits
	// the final concrete trace; a seed is useful but never the sole replay artifact.
	t.Fatalf("lean-replay=%s\n%v", encoded, err)
}

// Saved traces are plain JSON and replay without Rapid or generator seed recovery.
func TestLeanCorrespondenceSavedReplay(t *testing.T) {
	p := newLeanProcess(t)
	paths := []string{"testdata/lean_correspondence_v1.json", "testdata/lean_correspondence_update_v2.json", "testdata/lean_correspondence_rebase_v2.json"}
	if path := os.Getenv("SCHEDULER2_LEAN_REPLAY"); path != "" {
		paths = []string{path}
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) { replayLeanFile(t, p, path) })
	}
}

func replayLeanFile(t *testing.T, p *leanProcess, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var saved leanReplay
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&saved))
	require.ErrorIs(t, decoder.Decode(&struct{}{}), io.EOF)
	definitions := saved.Definitions
	switch saved.Version {
	case 1:
		require.NotNil(t, saved.Fixture, "v1 requires its immutable fixture")
		require.Empty(t, saved.Definitions, "v1 cannot contain replacement definitions")
		definitions = map[string]leanFixture{"": *saved.Fixture}
	case 2:
		require.Nil(t, saved.Fixture, "v2 uses explicit named definitions")
		require.NotEmpty(t, definitions)
		for name := range definitions {
			require.NotEmpty(t, name)
		}
	default:
		t.Fatalf("unsupported replay version %d", saved.Version)
	}
	type compiledDefinition struct {
		enumerator *occurrence.Enumerator
		source     leanSource
	}
	compiled := make(map[string]compiledDefinition, len(definitions))
	for name, fixture := range definitions {
		e, src, err := fixture.compile()
		require.NoError(t, err, "definition %q", name)
		compiled[name] = compiledDefinition{e, src}
	}
	var state occurrence.State
	for i, step := range saved.Steps {
		if saved.Version == 1 {
			require.Empty(t, step.Definition, "v1 step cannot select a definition")
		} else {
			require.NotEmpty(t, step.Definition, "v2 step must select a definition")
		}
		definition, ok := compiled[step.Definition]
		require.True(t, ok, "step %d references unknown definition %q", i, step.Definition)
		state, _, err = compareLeanStep(p, definition.enumerator, definition.source, state, step)
		require.NoError(t, err, "saved step %d", i)
	}
}

func TestLeanCorrespondenceControlsAndRejections(t *testing.T) {
	p := newLeanProcess(t)
	f := leanFixture{Start: instant(), Span: 4, Periods: []int{1}, JitterMS: 1700, Seed: "controls", Revision: 1}
	e, src, err := f.compile()
	require.NoError(t, err)
	state, _, err := compareLeanStep(p, e, src, occurrence.State{}, leanStep{Op: "begin", Time: f.Start.Add(-time.Nanosecond)})
	require.NoError(t, err)
	state, _, err = compareLeanStep(p, e, src, state, leanStep{Op: "peek", Time: f.Start, Probes: 1})
	require.NoError(t, err)
	require.NotNil(t, state.Pending)
	for _, op := range []string{"run", "peek", "peekRange"} {
		for _, probes := range []int{0, 1025} {
			_, r, err := compareLeanStep(p, e, src, state, leanStep{Op: op, Time: f.Start.Add(time.Second), Probes: probes, Outputs: 1})
			require.NoError(t, err)
			require.False(t, r.OK)
		}
	}
	for _, outputs := range []int{0, 129} {
		_, r, err := compareLeanStep(p, e, src, state, leanStep{Op: "run", Time: f.Start, Probes: 1, Outputs: outputs})
		require.NoError(t, err)
		require.False(t, r.OK)
	}
	for _, op := range []string{"update", "rebaseRange"} {
		_, r, err := compareLeanStep(p, e, src, state, leanStep{Op: op, Time: f.Start})
		require.NoError(t, err)
		require.Equal(t, "stale-revision", r.Error)
	}
	wrong := state
	wrong.DefinitionID = "different-definition"
	_, r, err := compareLeanStep(p, e, src, wrong, leanStep{Op: "run", Time: f.Start, Probes: 1, Outputs: 1})
	require.NoError(t, err)
	require.Equal(t, "identity-mismatch", r.Error)
	malformed := state
	malformed.Origin = state.SearchAfter.Add(time.Second)
	_, r, err = compareLeanStep(p, e, src, malformed, leanStep{Op: "peek", Time: f.Start, Probes: 1})
	require.NoError(t, err)
	require.Equal(t, "state-invalid", r.Error)

	// A genuinely different immutable definition with the SAME numeric revision
	// must not accept this continuation; this is not just a forged identity string.
	changed := f
	changed.Seed = "same-revision-other-seed"
	changedE, changedSrc, err := changed.compile()
	require.NoError(t, err)
	require.NotEqual(t, src.Identity, changedSrc.Identity)
	for _, op := range []string{"run", "peek", "disposeRange"} {
		_, r, err := compareLeanStep(p, changedE, changedSrc, state, leanStep{Op: op, Time: f.Start, Probes: 1, Outputs: 1})
		require.NoError(t, err)
		require.Equal(t, "identity-mismatch", r.Error)
	}
	f.Revision = 2
	f.Seed = "updated"
	newE, newSrc, err := f.compile()
	require.NoError(t, err)
	for _, op := range []string{"update", "rebaseRange"} {
		next, r, err := compareLeanStep(p, newE, newSrc, state, leanStep{Op: op, Time: f.Start.Add(1500 * time.Millisecond)})
		require.NoError(t, err)
		require.NotNil(t, r.Superseded)
		_, _, err = compareLeanStep(p, newE, newSrc, next, leanStep{Op: "run", Time: f.Start.Add(6 * time.Second), Probes: 10, Outputs: 10})
		require.NoError(t, err)
	}
	// Exact lower sentinel and cached Empty; no timestamp rounding in either state.
	empty := leanFixture{Start: leanEpoch.Add(time.Nanosecond), Span: 1, Revision: 1}
	emptyE, emptySrc, err := empty.compile()
	require.NoError(t, err)
	initial, _, err := compareLeanStep(p, emptyE, emptySrc, occurrence.State{}, leanStep{Op: "begin", Time: leanEpoch})
	require.NoError(t, err)
	_, r, err = compareLeanStep(p, emptyE, emptySrc, initial, leanStep{Op: "run", Time: empty.Start, Probes: 1, Outputs: 1})
	require.NoError(t, err)
	require.Equal(t, "empty", r.Status)
	// Boundary rejection must not accidentally serialize time.Duration saturation.
	_, r, err = compareLeanStep(p, e, src, state, leanStep{Op: "begin", Time: leanEpoch.Add(-time.Nanosecond)})
	require.NoError(t, err)
	require.Equal(t, "input-invalid", r.Error)
}

func TestLeanCorrespondenceExactBoundaries(t *testing.T) {
	p := newLeanProcess(t)
	f := leanFixture{Start: leanEpoch.Add(time.Nanosecond), Span: 0, Periods: []int{1}, JitterMS: 1700, Seed: "boundary", Revision: 1}
	e, src, err := f.compile()
	require.NoError(t, err)
	require.Len(t, src.Raw, 1)
	state, _, err := compareLeanStep(p, e, src, occurrence.State{}, leanStep{Op: "beginRange", Time: f.Start, End: f.Start})
	require.NoError(t, err)
	// The spec's nominal end is the first nominal, but jitter may legitimately
	// cross that end. Range exhaustion must preserve an Active pending witness.
	state, response, err := compareLeanStep(p, e, src, state, leanStep{Op: "peekRange", Time: f.Start, Probes: 1})
	require.NoError(t, err)
	require.Equal(t, "exhausted", response.Status)
	require.Equal(t, occurrence.Active, state.Terminal)
	require.NotNil(t, state.Pending)
	actual := state.Pending.Actual
	require.True(t, actual.After(f.Start))
	for _, through := range []time.Time{actual.Add(-time.Nanosecond), actual, actual.Add(time.Nanosecond)} {
		_, _, err := compareLeanStep(p, e, src, state, leanStep{Op: "run", Time: through, Probes: 1, Outputs: 1})
		require.NoError(t, err)
	}
	_, _, err = compareLeanStep(p, e, src, state, leanStep{Op: "beginRange", Time: f.Start.Add(time.Nanosecond), End: f.Start})
	require.NoError(t, err)
	// Probe-time unsupported actual, rather than eager rejection of the raw
	// source. Search a small deterministic independent fixture space for a
	// positive jitter crossing the supported maximum.
	found := false
	for seed := 0; seed < 32 && !found; seed++ {
		upperFixture := leanFixture{Start: leanUpper.Add(-6 * time.Second).Truncate(time.Second), Span: 6, Periods: []int{7}, JitterMS: 7000, Seed: strconv.Itoa(seed), Revision: 1}
		upperE, upperSrc, err := upperFixture.compile()
		require.NoError(t, err)
		bad := false
		for _, c := range upperSrc.Raw {
			ns, err := strconv.ParseInt(c.Actual, 10, 64)
			require.NoError(t, err)
			bad = bad || ns > leanUpper.Sub(leanEpoch).Nanoseconds()
		}
		if !bad {
			continue
		}
		found = true
		start, _, err := compareLeanStep(p, upperE, upperSrc, occurrence.State{}, leanStep{Op: "begin", Time: upperFixture.Start.Add(-time.Nanosecond)})
		require.NoError(t, err)
		_, r, err := compareLeanStep(p, upperE, upperSrc, start, leanStep{Op: "run", Time: leanUpper, Probes: 10, Outputs: 10})
		require.NoError(t, err)
		require.Equal(t, "actual-outside-range", r.Error)
	}
	require.True(t, found, "fixture must exercise unsupported actual at probe time")
}

func TestLeanCorrespondenceRapidWindowsAndUpdates(t *testing.T) {
	p := newLeanProcess(t)
	rapid.Check(t, func(t *rapid.T) {
		f := leanFixture{Start: instant().Add(time.Duration(rapid.IntRange(0, 999999999).Draw(t, "fraction"))), Span: 8, Periods: []int{rapid.IntRange(1, 3).Draw(t, "period")}, JitterMS: rapid.IntRange(0, 5000).Draw(t, "jitter"), Seed: "before", Revision: 1}
		f.Exclude = rapid.SliceOfN(rapid.IntRange(0, 8), 0, 5).Draw(t, "exclude")
		trace := newLeanReplay(f)
		definition := "initial"
		e, src, err := f.compile()
		if err != nil {
			leanRapidFailure(t, trace, err)
		}
		begin := recordLeanStep(&trace, definition, leanStep{Op: "beginRange", Time: f.Start, End: f.Start.Add(16 * time.Second)})
		state, _, err := compareLeanStep(p, e, src, occurrence.State{}, begin)
		if err != nil {
			leanRapidFailure(t, trace, err)
		}
		historical := rapid.Bool().Draw(t, "historical")
		for n := 0; n < 24; n++ {
			through := f.Start.Add(time.Duration(min(n, 16)) * time.Second)
			if n == 1 {
				f.Revision = 2
				f.Seed = "after"
				f.Periods = []int{rapid.IntRange(1, 3).Draw(t, "updatedPeriod")}
				definition = "replacement"
				trace.Definitions[definition] = f
				e, src, err = f.compile()
				if err != nil {
					leanRapidFailure(t, trace, err)
				}
				op := "update"
				if historical {
					op = "rebaseRange"
				}
				replacement := recordLeanStep(&trace, definition, leanStep{Op: op, Time: through})
				state, _, err = compareLeanStep(p, e, src, state, replacement)
				if err != nil {
					leanRapidFailure(t, trace, err)
				}
			}
			op := "run"
			if historical {
				op = "peek"
			}
			var r leanResponse
			step := recordLeanStep(&trace, definition, leanStep{Op: op, Time: through, Probes: rapid.IntRange(1, 3).Draw(t, fmt.Sprintf("probes%d", n)), Outputs: rapid.IntRange(1, 2).Draw(t, fmt.Sprintf("outputs%d", n))})
			state, r, err = compareLeanStep(p, e, src, state, step)
			if err != nil {
				leanRapidFailure(t, trace, err)
			}
			if historical && r.Status == "active" {
				dispose := recordLeanStep(&trace, definition, leanStep{Op: "disposeRange"})
				state, _, err = compareLeanStep(p, e, src, state, dispose)
				if err != nil {
					leanRapidFailure(t, trace, err)
				}
			}
			if n > 1 && (r.Status == "empty" || r.Status == "impossible" || r.Status == "exhausted") {
				return
			}
		}
		leanRapidFailure(t, trace, errors.New("window/update trace did not finish within 24 transactions"))
	})
}

func TestLeanCorrespondenceValidBudgetEdges(t *testing.T) {
	p := newLeanProcess(t)
	// Actual saturation, not merely accepting a large budget on a tiny source.
	f := leanFixture{Start: instant(), Span: 1030, Periods: []int{1}, Seed: "budget", Revision: 1}
	for second := 0; second < 60; second++ {
		f.Exclude = append(f.Exclude, second)
	}
	e, src, err := f.compile()
	require.NoError(t, err)
	initial, _, err := compareLeanStep(p, e, src, occurrence.State{}, leanStep{Op: "begin", Time: f.Start.Add(-time.Nanosecond)})
	require.NoError(t, err)
	for _, probes := range []int{1023, 1024} {
		for _, op := range []string{"run", "peek", "peekRange"} {
			next, r, err := compareLeanStep(p, e, src, initial, leanStep{Op: op, Time: f.Start.Add(1032 * time.Second), Probes: probes, Outputs: 128})
			require.NoError(t, err)
			require.Equal(t, "yield", r.Status)
			require.Equal(t, strconv.Itoa(probes), r.Probes)
			_, r, err = compareLeanStep(p, e, src, next, leanStep{Op: op, Time: f.Start.Add(1032 * time.Second), Probes: probes, Outputs: 128})
			require.NoError(t, err)
			require.Equal(t, "impossible", r.Status)
		}
	}
	f.Exclude = nil
	f.Span = 130
	e, src, err = f.compile()
	require.NoError(t, err)
	initial, _, err = compareLeanStep(p, e, src, occurrence.State{}, leanStep{Op: "begin", Time: f.Start.Add(-time.Nanosecond)})
	require.NoError(t, err)
	for _, outputs := range []int{127, 128} {
		next, r, err := compareLeanStep(p, e, src, initial, leanStep{Op: "run", Time: f.Start.Add(132 * time.Second), Probes: 1024, Outputs: outputs})
		require.NoError(t, err)
		require.Equal(t, "limited", r.Status)
		require.Len(t, r.Outputs, outputs)
		_, r, err = compareLeanStep(p, e, src, next, leanStep{Op: "run", Time: f.Start.Add(132 * time.Second), Probes: 1024, Outputs: outputs})
		require.NoError(t, err)
		require.Equal(t, "exhausted", r.Status)
		require.Len(t, r.Outputs, 131-outputs)
	}
}

func TestLeanCorrespondenceMalformedWire(t *testing.T) {
	p := newLeanProcess(t)
	f := leanFixture{Start: instant(), Span: 1, Periods: []int{1}, Revision: 1}
	_, src, err := f.compile()
	require.NoError(t, err)
	base := leanRequest{Version: 1, Source: src, Op: "begin", After: leanTime(f.Start)}
	valid, err := json.Marshal(base)
	require.NoError(t, err)
	cases := []struct {
		name       string
		patch      func(map[string]any)
		errorClass string
	}{
		{"unknown version", func(m map[string]any) { m["version"] = 2 }, "wire-invalid"},
		{"unknown operation", func(m map[string]any) { m["op"] = "next" }, "wire-invalid"},
		{"unknown top key", func(m map[string]any) { m["ignored"] = true }, "wire-invalid"},
		{"missing time", func(m map[string]any) { delete(m, "after") }, "wire-invalid"},
		{"numeric time", func(m map[string]any) { m["after"] = 1 }, "wire-invalid"},
		{"fraction string", func(m map[string]any) { m["after"] = "1.25" }, "wire-invalid"},
		{"bad digits", func(m map[string]any) { m["after"] = "12garbage" }, "wire-invalid"},
		{"nonnull begin state", func(m map[string]any) { m["state"] = map[string]any{} }, "wire-invalid"},
		{"unknown source key", func(m map[string]any) { m["source"].(map[string]any)["ignored"] = true }, "wire-invalid"},
		{"wrong source bool", func(m map[string]any) { m["source"].(map[string]any)["hasInclusions"] = "true" }, "wire-invalid"},
		{"unknown raw key", func(m map[string]any) {
			m["source"].(map[string]any)["raw"].([]any)[0].(map[string]any)["ignored"] = true
		}, "wire-invalid"},
		{"empty identity", func(m map[string]any) { m["source"].(map[string]any)["identity"] = "" }, "source-invalid"},
		{"wrong upper", func(m map[string]any) { m["source"].(map[string]any)["upper"] = "1" }, "source-invalid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m map[string]any
			require.NoError(t, json.Unmarshal(valid, &m))
			tc.patch(m)
			b, err := json.Marshal(m)
			require.NoError(t, err)
			r, err := p.callRaw(b)
			require.NoError(t, err)
			require.Equal(t, leanResponse{Version: 1, Error: tc.errorClass}, r)
			// Invalid messages must not kill or contaminate the long-lived process.
			recovered, err := p.call(base)
			require.NoError(t, err)
			require.True(t, recovered.OK)
		})
	}
	r, err := p.callRaw([]byte("{not json"))
	require.NoError(t, err)
	require.Equal(t, leanResponse{Version: 1, Error: "wire-invalid"}, r)
}

// Parsing signed wire quantities must not reorder the public operation's guards.
// These cases deliberately combine invalid inputs to discriminate precedence.
func TestLeanCorrespondenceNegativeInputPrecedence(t *testing.T) {
	p := newLeanProcess(t)
	f := leanFixture{Start: instant(), Span: 4, Periods: []int{1}, Seed: "negative", Revision: 1}
	e, src, err := f.compile()
	require.NoError(t, err)
	initial, err := e.Begin(f.Start.Add(-time.Nanosecond))
	require.NoError(t, err)
	newFixture := f
	newFixture.Revision = 2
	newE, newSrc, err := newFixture.compile()
	require.NoError(t, err)
	negativeTime := leanEpoch.Add(-time.Nanosecond)
	cases := []struct {
		name             string
		newDefinition    bool
		wrongIdentity    bool
		negativeRevision bool
		negativeOrigin   bool
		step             leanStep
		want             string
	}{
		{name: "identity before negative probe", wrongIdentity: true, step: leanStep{Op: "run", Time: f.Start, Probes: -1, Outputs: 1}, want: "identity-mismatch"},
		{name: "identity before negative window", wrongIdentity: true, step: leanStep{Op: "run", Time: negativeTime, Probes: 1, Outputs: 1}, want: "identity-mismatch"},
		{name: "update validates negative revision", newDefinition: true, negativeRevision: true, step: leanStep{Op: "update", Time: f.Start}, want: "state-invalid"},
		{name: "stale update before negative time", step: leanStep{Op: "update", Time: negativeTime}, want: "stale-revision"},
		{name: "negative origin is malformed state", negativeOrigin: true, step: leanStep{Op: "run", Time: f.Start, Probes: 1, Outputs: 1}, want: "state-invalid"},
		{name: "dispose identity before negative state", wrongIdentity: true, negativeOrigin: true, step: leanStep{Op: "disposeRange"}, want: "identity-mismatch"},
		{name: "malformed state before stale update and negative time", negativeOrigin: true, step: leanStep{Op: "update", Time: negativeTime}, want: "state-invalid"},
		{name: "invalid budget before negative state", negativeOrigin: true, step: leanStep{Op: "run", Time: f.Start, Probes: 0, Outputs: 1}, want: "input-invalid"},
		{name: "range end before identity", wrongIdentity: true, step: leanStep{Op: "peekRange", Time: negativeTime, Probes: 1}, want: "input-invalid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := initial
			if tc.wrongIdentity {
				state.DefinitionID = "other"
			}
			if tc.negativeRevision {
				state.Revision = -1
			}
			if tc.negativeOrigin {
				state.Origin = negativeTime
			}
			enumerator, source := e, src
			if tc.newDefinition {
				enumerator, source = newE, newSrc
			}
			_, response, err := compareLeanStep(p, enumerator, source, state, tc.step)
			require.NoError(t, err)
			require.Equal(t, leanResponse{Version: 1, Error: tc.want}, response)
		})
	}
}
