package occurrence

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPreparedSpecStateCodecEveryPhase(t *testing.T) {
	t.Parallel()
	cal := &schedulepb.StructuredCalendarSpec{Hour: []*schedulepb.Range{{Start: 1}}, Minute: []*schedulepb.Range{{Start: 30}}}
	h, r, z, _ := specFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, TimezoneData: preparedNewYorkTZ, Jitter: durationpb.New(time.Hour), ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Hour: []*schedulepb.Range{{Start: 2}}}}}, string(bytes.Repeat([]byte{0xff, 1, 2}, 100)))
	after := time.Date(2024, 11, 3, 5, 0, 0, 0, time.UTC)
	var state PreparedSpecState
	for i := 0; i < 10000; i++ {
		encoded, err := EncodePreparedSpecState(state)
		require.NoError(t, err)
		require.Len(t, encoded, PreparedSpecStateBytes)
		restored, err := DecodePreparedSpecState(encoded, h, z, after)
		require.NoError(t, err)
		require.Equal(t, state, restored)
		if state.Phase == specComplete {
			require.Equal(t, time.Date(2024, 11, 3, 5, 30, 0, 0, time.UTC), state.Nominal)
			return
		}
		state, _, err = AdvancePreparedSpec(restored, h, z, after, r, 1)
		require.NoError(t, err)
	}
	t.Fatal("codec trajectory did not finish")
}
func TestPreparedSpecStateCodecRejectsMalformed(t *testing.T) {
	t.Parallel()
	h, r, z, _ := specFixture(t, &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}}, "seed")
	after := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	state, _, err := AdvancePreparedSpec(PreparedSpecState{}, h, z, after, r, 1)
	require.NoError(t, err)
	encoded, err := EncodePreparedSpecState(state)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		change func([]byte) []byte
	}{
		{"truncated", func(b []byte) []byte { return b[:len(b)-1] }},
		{"extended", func(b []byte) []byte { return append(b, 0) }},
		{"version", func(b []byte) []byte { b[0] = 2; return b }},
		{"identity", func(b []byte) []byte { b[1] ^= 1; return b }},
		{"phase", func(b []byte) []byte { b[130] = 255; return b }},
		{"boolean", func(b []byte) []byte { b[282] = 2; return b }},
		{"nanoseconds", func(b []byte) []byte { binary.BigEndian.PutUint32(b[74:78], 1000000000); return b }},
		{"absent nonzero", func(b []byte) []byte { b[65] = 0; return b }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodePreparedSpecState(tc.change(bytes.Clone(encoded)), h, z, after)
			require.Error(t, err)
			require.Equal(t, PreparedSpecState{}, got)
		})
	}
}
