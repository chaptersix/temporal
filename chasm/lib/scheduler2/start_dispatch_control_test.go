package scheduler2

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
)

func TestStartDispatchControlFences(t *testing.T) {
	t.Parallel()
	zero, future := int64(0), int64(20)
	for _, tc := range []struct {
		name                        string
		before, after               *pb.StartDispatchControl
		wake                        *int64
		replace, changed, exhausted bool
	}{
		{name: "empty", before: &pb.StartDispatchControl{Token: 1}, after: &pb.StartDispatchControl{Token: 1}},
		{name: "zero is a real wake", before: &pb.StartDispatchControl{Token: 1}, wake: &zero, after: &pb.StartDispatchControl{Token: 2, Armed: true}, changed: true},
		{name: "unchanged wake", before: &pb.StartDispatchControl{Token: 2, Armed: true, At: 20}, wake: &future, after: &pb.StartDispatchControl{Token: 2, Armed: true, At: 20}},
		{name: "replacement at same time", before: &pb.StartDispatchControl{Token: 2, Armed: true, At: 20}, wake: &future, replace: true, after: &pb.StartDispatchControl{Token: 3, Armed: true, At: 20}, changed: true},
		{name: "disarm fences old task", before: &pb.StartDispatchControl{Token: 2, Armed: true, At: 20}, after: &pb.StartDispatchControl{Token: 3}, changed: true},
		{name: "exhaustion preserves source", before: &pb.StartDispatchControl{Token: math.MaxUint64, Armed: true, At: 20}, wake: &zero, after: &pb.StartDispatchControl{Token: math.MaxUint64, Armed: true, At: 20}, exhausted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			control := common.CloneProto(tc.before)
			changed, err := setStartDispatchWake(control, tc.wake, tc.replace)
			if tc.exhausted {
				require.ErrorContains(t, err, "token exhausted")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.changed, changed)
			protorequire.ProtoEqual(t, tc.after, control)
		})
	}
}
