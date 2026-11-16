package occurrence_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLimitsProbeBoundaryAndResume(t *testing.T) {
	t.Parallel()
	base := instant()
	spec := intervalSpec(base, 1)
	spec.EndTime = timestamppb.New(base.Add(1030 * time.Second))
	first, last := calendar(), calendar()
	first.Minute = span(0, 16)
	last.Minute, last.Second = span(17, 17), span(0, 3)
	spec.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{first, last}
	e, err := occurrence.New(spec, "", 1)
	require.NoError(t, err)
	for _, probes := range []int{1023, 1024, 1025} {
		t.Run(fmt.Sprint(probes), func(t *testing.T) {
			state, err := e.Begin(base.Add(-time.Nanosecond))
			require.NoError(t, err)
			before := state
			got, err := e.Run(state, base.Add(1030*time.Second), occurrence.Limits{Probes: probes, Outputs: 128})
			require.Equal(t, before, state)
			if probes == 1025 {
				require.ErrorContains(t, err, "enumeration limits")
				got, err = e.Run(state, base.Add(1030*time.Second), occurrence.Limits{Probes: 1024, Outputs: 128})
				probes = 1024
			}
			require.NoError(t, err)
			require.Equal(t, occurrence.Yield, got.Status)
			require.Equal(t, probes, got.Probes)
			require.Empty(t, got.Occurrences)
			require.Equal(t, base.Add(time.Duration(probes-1)*time.Second), got.State.DisposedThrough)
			resumed, err := e.Run(got.State, base.Add(1030*time.Second), occurrence.Limits{Probes: 1024, Outputs: 128})
			require.NoError(t, err)
			require.Equal(t, occurrence.Exhausted, resumed.Status)
			expected := make([]occurrence.Candidate, 0, 7)
			for second := 1024; second <= 1030; second++ {
				at := base.Add(time.Duration(second) * time.Second)
				expected = append(expected, occurrence.Candidate{Nominal: at, Actual: at, Revision: 1})
			}
			require.Equal(t, expected, resumed.Occurrences)
		})
	}
}

func TestLimitsOutputBoundaryAndResume(t *testing.T) {
	t.Parallel()
	base := instant()
	spec := intervalSpec(base, 1)
	spec.EndTime = timestamppb.New(base.Add(129 * time.Second))
	e, err := occurrence.New(spec, "", 1)
	require.NoError(t, err)
	for _, outputs := range []int{127, 128, 129} {
		t.Run(fmt.Sprint(outputs), func(t *testing.T) {
			state, err := e.Begin(base.Add(-time.Nanosecond))
			require.NoError(t, err)
			before := state
			got, err := e.Run(state, base.Add(129*time.Second), occurrence.Limits{Probes: 1024, Outputs: outputs})
			require.Equal(t, before, state)
			if outputs == 129 {
				require.ErrorContains(t, err, "enumeration limits")
				outputs = 128
				got, err = e.Run(state, base.Add(129*time.Second), occurrence.Limits{Probes: 1024, Outputs: outputs})
			}
			require.NoError(t, err)
			require.Equal(t, occurrence.Limited, got.Status)
			require.Len(t, got.Occurrences, outputs)
			resumed, err := e.Run(got.State, base.Add(129*time.Second), occurrence.Limits{Probes: 1024, Outputs: 128})
			require.NoError(t, err)
			require.Equal(t, occurrence.Exhausted, resumed.Status)
			expected := make([]occurrence.Candidate, 0, 130)
			for second := 0; second < 130; second++ {
				at := base.Add(time.Duration(second) * time.Second)
				expected = append(expected, occurrence.Candidate{Nominal: at, Actual: at, Revision: 1})
			}
			require.Equal(t, expected, append(got.Occurrences, resumed.Occurrences...))
		})
	}
}

func TestLimitsDefinitionBoundariesAndRecovery(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"terms", "ranges", "bytes"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			limit := 64
			if kind == "ranges" {
				limit = 1024
			}
			if kind == "bytes" {
				limit = 65536
			}
			for _, size := range []int{limit - 1, limit, limit + 1} {
				t.Run(fmt.Sprint(size), func(t *testing.T) {
					base := instant()
					spec := intervalSpec(base, 1)
					spec.EndTime = timestamppb.New(base.Add(2 * time.Second))
					switch kind {
					case "terms":
						spec.Interval = nil
						for i := 0; i < size; i++ {
							spec.Interval = append(spec.Interval, &schedulepb.IntervalSpec{Interval: durationpb.New(time.Second)})
						}
					case "ranges":
						cal := calendar()
						cal.Second = nil
						for i := 0; i < size-6; i++ {
							cal.Second = append(cal.Second, &schedulepb.Range{Start: 0, End: 59, Step: 1})
						}
						spec.Interval = nil
						spec.StructuredCalendar = []*schedulepb.StructuredCalendarSpec{cal}
					case "bytes":
						// Valid unknown length-delimited protobuf data pads wire size without
						// introducing invalid calendar syntax or an unrelated timezone failure.
						baseSize := proto.Size(spec)
						for padding := size - baseSize - 10; padding <= size-baseSize; padding++ {
							wire := protowire.AppendTag(nil, 1000, protowire.BytesType)
							wire = protowire.AppendBytes(wire, make([]byte, padding))
							spec.ProtoReflect().SetUnknown(wire)
							if proto.Size(spec) == size {
								break
							}
						}
						require.Equal(t, size, proto.Size(spec))
					default:
						t.Fatalf("unknown limit fixture %q", kind)
					}
					before := proto.Clone(spec)
					e, err := occurrence.New(spec, "", 1)
					require.True(t, proto.Equal(before, spec), "constructor must not mutate rejected or accepted input")
					if size > limit {
						message := "term limit"
						if kind == "ranges" {
							message = "range limit"
						}
						if kind == "bytes" {
							message = "64 KiB"
						}
						require.ErrorContains(t, err, message)
						// A rejected construction cannot poison a later valid compilation/run.
						valid := intervalSpec(base, 1)
						valid.EndTime = timestamppb.New(base.Add(2 * time.Second))
						e, err = occurrence.New(valid, "", 1)
					}
					require.NoError(t, err)
					state, err := e.Begin(base.Add(-time.Nanosecond))
					require.NoError(t, err)
					got, err := e.Run(state, base.Add(2*time.Second), occurrence.Limits{Probes: 8, Outputs: 8})
					require.NoError(t, err)
					require.Equal(t, occurrence.Exhausted, got.Status)
					expected := []occurrence.Candidate{}
					for second := 0; second < 3; second++ {
						at := base.Add(time.Duration(second) * time.Second)
						expected = append(expected, occurrence.Candidate{Nominal: at, Actual: at, Revision: 1})
					}
					require.Equal(t, expected, got.Occurrences, "duplicate valid terms/ranges must not duplicate occurrences")
				})
			}
		})
	}
}
