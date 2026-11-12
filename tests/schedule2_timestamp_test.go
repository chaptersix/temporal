package tests

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSchedule2TimestampDomain(t *testing.T) {
	t.Parallel()
	// Cases run sequentially to bound simultaneous dedicated local clusters.
	for _, backend := range []scheduleBackend{scheduleV1Backend, schedule2Backend} {
		t.Run(backend.name, func(t *testing.T) {
			for _, kind := range []string{"Backfill1999", "Backfill0100", "Backfill0001", "Manual2500", "Manual9999"} {
				t.Run(kind, func(t *testing.T) {
					env := newScheduleEnv(t, []scheduleBackend{backend})
					ctx := backend.context(t.Context())
					sid := testcore.RandomizeStr("timestamp-domain")
					wt := testcore.RandomizeStr("timestamp-domain-workflow")
					var runs atomic.Int32
					registerCountingWorkflow(env, wt, &runs)
					spec := intervalSpec(time.Second)
					if kind == "Backfill0100" {
						spec.StartTime = timestamppb.New(time.Date(100, 1, 1, 0, 0, 0, 0, time.UTC))
					}
					if kind == "Backfill0001" {
						spec.StartTime = timestamppb.New(time.Date(1, 1, 2, 0, 0, 0, 0, time.UTC))
					}
					createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
						Spec: spec, State: &schedulepb.ScheduleState{Paused: true},
						Action: startWorkflowAction(env, "workflow-"+sid, wt),
					})
					at := time.Date(1999, 12, 31, 23, 59, 57, 0, time.UTC)
					if kind == "Backfill0100" {
						at = time.Date(100, 1, 1, 0, 0, 0, 0, time.UTC)
					}
					if kind == "Backfill0001" {
						at = time.Date(1, 1, 2, 0, 0, 0, 0, time.UTC)
					}
					patch := &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{StartTime: timestamppb.New(at), EndTime: timestamppb.New(at.Add(2 * time.Second)), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}}}
					count := 3
					if kind == "Manual2500" || kind == "Manual9999" {
						at = time.Date(2500, 1, 2, 3, 4, 5, 123456789, time.UTC)
						if kind == "Manual9999" {
							at = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
						}
						patch = &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(at), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}}
						count = 1
					}
					patchSchedule(ctx, t, env, sid, patch)
					await.Require(ctx, t, func(atest *await.T) {
						description, err := env.FrontendClient().DescribeSchedule(atest.Context(), &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
						require.NoError(atest, err)
						require.Len(atest, description.GetInfo().GetRecentActions(), count)
					}, awaitTimeout, pollInterval)
					description, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
					require.NoError(t, err)
					var observed, expected []time.Time
					for i, action := range description.Info.RecentActions {
						t.Logf("observed schedule_time=%s workflow_id=%s", action.ScheduleTime.AsTime(), action.GetStartWorkflowResult().GetWorkflowId())
						require.Equal(t, "workflow-"+sid+"-"+action.ScheduleTime.AsTime().Format(time.RFC3339), action.GetStartWorkflowResult().GetWorkflowId())
						observed = append(observed, action.ScheduleTime.AsTime())
						expected = append(expected, at.Add(time.Duration(i)*time.Second))
					}
					require.ElementsMatch(t, expected, observed)

				})
			}
		})
	}
}
