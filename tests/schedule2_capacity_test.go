//go:build test_dep

package tests

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
)

func TestSchedule2ConfiguredCapacityNeighbors(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{2, 5} {
		t.Run(fmt.Sprintf("limit%d", limit), func(t *testing.T) {
			t.Parallel()
			tweaks := chasmscheduler.DefaultTweakables
			tweaks.MaxBufferSize = limit
			s := newScheduleEnv(t, []scheduleBackend{schedule2Backend}, testcore.WithDynamicConfig(chasmscheduler.CurrentTweakables, tweaks))
			ctx := schedule2Backend.context(t.Context())
			sid, wt := testcore.RandomizeStr("capacity"), testcore.RandomizeStr("gated-capacity")
			var runs atomic.Int32
			registerGatedWorkflow(s, wt, &runs)
			createSchedule(ctx, t, s, sid, &schedulepb.Schedule{Spec: intervalSpec(fastInterval), Action: startWorkflowAction(s, sid+"-workflow", wt), Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}})
			describe := func(at *await.T) *workflowservice.DescribeScheduleResponse {
				result, err := s.FrontendClient().DescribeSchedule(at.Context(), &workflowservice.DescribeScheduleRequest{Namespace: s.Namespace().String(), ScheduleId: sid})
				require.NoError(at, err)
				return result
			}
			await.Require(ctx, t, func(at *await.T) {
				info := describe(at).Info
				require.Positive(at, info.BufferDropped)
				require.Len(at, info.RunningWorkflows, 1)
				require.EqualValues(at, limit-1, info.BufferSize, "running tracked workflow occupies one configured slot")
			}, awaitTimeout, pollInterval)
			require.Equal(t, 1, completeRunningWorkflows(ctx, t, s, sid))
			var postCompletionDropped int64
			await.Require(ctx, t, func(at *await.T) {
				info := describe(at).Info
				require.EqualValues(at, 2, info.ActionCount)
				require.Len(at, info.RunningWorkflows, 1)
				postCompletionDropped = info.BufferDropped
			}, awaitTimeout, pollInterval)
			await.Require(ctx, t, func(at *await.T) {
				info := describe(at).Info
				require.Greater(at, info.BufferDropped, postCompletionDropped, "observe another automatic overflow after completion")
				require.EqualValues(at, limit-2, info.BufferSize, "completed and running tracked workflows both occupy configured slots")
				require.Len(at, info.RunningWorkflows, 1)
			}, awaitTimeout, pollInterval)
			completeRunningWorkflows(ctx, t, s, sid)
		})
	}
}
