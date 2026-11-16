package occurrence_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPublicDefinitionUsesIngressSizeLimits(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"terms", "ranges", "bytes"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			base := instant()
			spec := intervalSpec(base, 1)
			switch kind {
			case "terms":
				for len(spec.Interval) < 100 {
					spec.Interval = append(spec.Interval, &schedulepb.IntervalSpec{Interval: durationpb.New(time.Second)})
				}
			case "ranges":
				cal := calendar()
				cal.Second = nil
				for range 1025 {
					cal.Second = append(cal.Second, &schedulepb.Range{Start: 0, End: 59, Step: 1})
				}
				spec.Interval = nil
				spec.StructuredCalendar = []*schedulepb.StructuredCalendarSpec{cal}
			case "bytes":
				wire := protowire.AppendTag(nil, 1000, protowire.BytesType)
				spec.ProtoReflect().SetUnknown(protowire.AppendBytes(wire, make([]byte, 65537)))
			default:
				t.Fatalf("unknown definition fixture %q", kind)
			}
			before := proto.Clone(spec)
			_, err := occurrence.New(spec, "", 1)
			require.Error(t, err, "the internal verification profile retains its explicit definition bounds")
			enumerator, err := occurrence.NewPublic(spec, "", 1)
			require.NoError(t, err)
			require.True(t, proto.Equal(before, spec))
			state, err := enumerator.Begin(base.Add(-time.Nanosecond))
			require.NoError(t, err)
			result, err := enumerator.Run(state, base.Add(time.Hour), occurrence.Limits{Probes: 1, Outputs: 1})
			require.NoError(t, err)
			require.Equal(t, []occurrence.Candidate{{Nominal: base, Actual: base, Revision: 1}}, result.Occurrences)
			_, err = enumerator.Run(result.State, base.Add(time.Hour), occurrence.Limits{Probes: 1025, Outputs: 1})
			require.Error(t, err, "public definition acceptance does not enlarge scan work budgets")
		})
	}
}

func TestPublicDefinitionRetainsSemanticValidation(t *testing.T) {
	t.Parallel()
	for name, spec := range map[string]*schedulepb.ScheduleSpec{
		"negative jitter":        {Jitter: durationpb.New(-time.Second)},
		"nil interval":           {Interval: []*schedulepb.IntervalSpec{nil}},
		"invalid excluded range": {ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: -1}}}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := occurrence.NewPublic(spec, "", 1)
			require.Error(t, err)
		})
	}
}
