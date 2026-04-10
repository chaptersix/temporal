package tests

import (
	"context"
	"testing"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tools/schedutil"
)

func TestSchedUtil(t *testing.T) {
	t.Run("TestDedupIdempotent", testSchedUtilDedupIdempotent)
	t.Run("TestDedupRemovesDuplicates", testSchedUtilDedupRemovesDuplicates)
	t.Run("TestForceCAN", testSchedUtilForceCAN)
	t.Run("TestDedupNamespaceDryRun", testSchedUtilDedupNamespaceDryRun)
	t.Run("TestDedupNamespaceWithYes", testSchedUtilDedupNamespaceWithYes)
}

// testSchedUtilDedupIdempotent verifies that running dedup on a schedule with
// no duplicates leaves the spec unchanged.
func testSchedUtilDedupIdempotent(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	sid := "schedutil-dedup-idempotent-" + uuid.NewString()[:8]

	createSchedule(t, s, ctx, sid, "0 * * * *")

	desc, err := s.SdkClient().ScheduleClient().GetHandle(ctx, sid).Describe(ctx)
	require.NoError(t, err)
	nBefore := len(desc.Schedule.Spec.Calendars)
	require.Greater(t, nBefore, 0)

	require.NoError(t, schedutil.RunDedup(ctx, s.SdkClient(), s.Namespace().String(), sid))

	after, err := s.SdkClient().ScheduleClient().GetHandle(ctx, sid).Describe(ctx)
	require.NoError(t, err)
	require.Equal(t, nBefore, len(after.Schedule.Spec.Calendars), "dedup should be idempotent when no duplicates exist")
}

// testSchedUtilDedupRemovesDuplicates reproduces the describe-then-update
// accumulation bug: the update callback re-applies CronExpressions on top of
// the Calendars already present in the described schedule, causing the server
// to append a duplicate StructuredCalendar entry on each call. After 20 rounds
// the schedule has 21 identical entries. RunDedup is then called and must
// collapse them to 1 without relying on any server-side fix.
func testSchedUtilDedupRemovesDuplicates(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	sid := "schedutil-dedup-dups-" + uuid.NewString()[:8]
	cron := "0 * * * *"

	createSchedule(t, s, ctx, sid, cron)

	handle := s.SdkClient().ScheduleClient().GetHandle(ctx, sid)

	// Simulate the customer pattern: each update reads the described schedule
	// and re-applies CronExpressions from its own config on top of whatever
	// Calendars the describe returned. Without a server-side fix, each round
	// appends one more identical StructuredCalendar entry.
	const rounds = 20
	for i := range rounds {
		err := handle.Update(ctx, sdkclient.ScheduleUpdateOptions{
			DoUpdate: func(input sdkclient.ScheduleUpdateInput) (*sdkclient.ScheduleUpdate, error) {
				sched := input.Description.Schedule
				sched.Spec.CronExpressions = []string{cron}
				return &sdkclient.ScheduleUpdate{Schedule: &sched}, nil
			},
		})
		require.NoError(t, err, "update round %d", i+1)
	}

	// Verify duplicates have accumulated (rounds+1 entries: 1 original + 20 appended).
	desc, err := handle.Describe(ctx)
	require.NoError(t, err)
	require.Equal(t, rounds+1, len(desc.Schedule.Spec.Calendars),
		"expected %d duplicate Calendar entries after %d describe-then-update rounds", rounds+1, rounds)

	require.NoError(t, schedutil.RunDedup(ctx, s.SdkClient(), s.Namespace().String(), sid))

	after, err := handle.Describe(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(after.Schedule.Spec.Calendars),
		"RunDedup should collapse %d duplicates to 1", rounds+1)
}

// testSchedUtilForceCAN verifies that RunForceCAN signals the scheduler
// workflow without error. The workflow must exist (created by createSchedule).
func testSchedUtilForceCAN(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	sid := "schedutil-force-can-" + uuid.NewString()[:8]

	createSchedule(t, s, ctx, sid, "0 * * * *")

	require.NoError(t, schedutil.RunForceCAN(ctx, s.SdkClient(), sid))
}

// testSchedUtilDedupNamespaceDryRun verifies that omitting --yes never invokes
// the operation callback, regardless of how many schedules exist.
func testSchedUtilDedupNamespaceDryRun(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()

	createSchedule(t, s, ctx, "schedutil-ns-dry-a-"+uuid.NewString()[:8], "0 * * * *")
	createSchedule(t, s, ctx, "schedutil-ns-dry-b-"+uuid.NewString()[:8], "0 12 * * *")

	// yes=false: fn must never be called.
	require.NoError(t, schedutil.ForEachSchedule(ctx, s.SdkClient(), s.Namespace().String(), "", false, func(sid string) error {
		t.Errorf("ForEachSchedule invoked fn for %q despite yes=false", sid)
		return nil
	}))
}

// testSchedUtilDedupNamespaceWithYes accumulates duplicates on two schedules
// then runs dedup across the whole namespace, verifying both are cleaned up.
func testSchedUtilDedupNamespaceWithYes(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	cron := "0 * * * *"

	sid1 := "schedutil-ns-yes-a-" + uuid.NewString()[:8]
	sid2 := "schedutil-ns-yes-b-" + uuid.NewString()[:8]
	createSchedule(t, s, ctx, sid1, cron)
	createSchedule(t, s, ctx, sid2, cron)

	// Accumulate duplicates on both schedules.
	for _, sid := range []string{sid1, sid2} {
		handle := s.SdkClient().ScheduleClient().GetHandle(ctx, sid)
		for i := range 5 {
			err := handle.Update(ctx, sdkclient.ScheduleUpdateOptions{
				DoUpdate: func(input sdkclient.ScheduleUpdateInput) (*sdkclient.ScheduleUpdate, error) {
					sched := input.Description.Schedule
					sched.Spec.CronExpressions = []string{cron}
					return &sdkclient.ScheduleUpdate{Schedule: &sched}, nil
				},
			})
			require.NoError(t, err, "schedule %s update round %d", sid, i+1)
		}
	}

	// Verify duplicates accumulated on both.
	for _, sid := range []string{sid1, sid2} {
		desc, err := s.SdkClient().ScheduleClient().GetHandle(ctx, sid).Describe(ctx)
		require.NoError(t, err)
		require.Equal(t, 6, len(desc.Schedule.Spec.Calendars), "schedule %s should have 6 entries before dedup", sid)
	}

	// Run namespace-wide dedup.
	require.NoError(t, schedutil.ForEachSchedule(ctx, s.SdkClient(), s.Namespace().String(), "", true, func(sid string) error {
		return schedutil.RunDedup(ctx, s.SdkClient(), s.Namespace().String(), sid)
	}))

	// Both schedules should now have exactly 1 entry.
	for _, sid := range []string{sid1, sid2} {
		after, err := s.SdkClient().ScheduleClient().GetHandle(ctx, sid).Describe(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(after.Schedule.Spec.Calendars), "schedule %s should have 1 entry after dedup", sid)
	}
}

func createSchedule(t *testing.T, s *testcore.TestEnv, ctx context.Context, sid, cron string) {
	t.Helper()
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				CronString: []string{cron},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: scheduleWorkflowInfo(),
				},
			},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = s.FrontendClient().DeleteSchedule(context.Background(), &workflowservice.DeleteScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			Identity:   "test",
		})
	})
}

func scheduleWorkflowInfo() *workflowpb.NewWorkflowExecutionInfo {
	return &workflowpb.NewWorkflowExecutionInfo{
		WorkflowId:   "schedutil-child-wf",
		WorkflowType: &commonpb.WorkflowType{Name: "myworkflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "mytq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	}
}
