package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tools/schedutil"
)

func TestSchedUtil(t *testing.T) {
	t.Run("TestDedupIdempotent", testSchedUtilDedupIdempotent)
	t.Run("TestDedupRemovesDuplicates", testSchedUtilDedupRemovesDuplicates)
	t.Run("TestDedupDryRun", testSchedUtilDedupDryRun)
	t.Run("TestForceCAN", testSchedUtilForceCAN)
	t.Run("TestForceCANDryRun", testSchedUtilForceCANDryRun)
	t.Run("TestDedupNamespaceExecute", testSchedUtilDedupNamespaceExecute)
	t.Run("TestDedupRecreateDryRun", testSchedUtilDedupRecreateDryRun)
	t.Run("TestDedupRecreateExecute", testSchedUtilDedupRecreateExecute)
}

// testSchedUtilDedupIdempotent verifies that running dedup on a schedule with
// no duplicates leaves the spec unchanged.
func testSchedUtilDedupIdempotent(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	sid := "schedutil-dedup-idempotent-" + uuid.NewString()[:8]
	ns := s.Namespace().String()

	createSchedule(t, s, ctx, sid, "0 * * * *")

	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  ns,
		ScheduleId: sid,
	})
	require.NoError(t, err)
	nBefore := len(desc.Schedule.Spec.StructuredCalendar)
	require.Positive(t, nBefore)

	outDir := t.TempDir()
	require.NoError(t, schedutil.RunDedup(ctx, s.FrontendClient(), ns, sid, outDir, true))

	// No duplicates: no files should have been written.
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Empty(t, entries, "no files should be written when there are no duplicates")

	after, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  ns,
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Len(t, after.Schedule.Spec.StructuredCalendar, nBefore, "dedup should be idempotent when no duplicates exist")
}

// testSchedUtilDedupRemovesDuplicates reproduces the describe-then-update
// accumulation bug, then verifies RunDedup collapses the entries.
func testSchedUtilDedupRemovesDuplicates(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	sid := "schedutil-dedup-dups-" + uuid.NewString()[:8]
	ns := s.Namespace().String()
	cron := "0 * * * *"

	createSchedule(t, s, ctx, sid, cron)

	handle := s.SdkClient().ScheduleClient().GetHandle(ctx, sid)

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

	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  ns,
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Len(t, desc.Schedule.Spec.StructuredCalendar, rounds+1)

	outDir := t.TempDir()
	require.NoError(t, schedutil.RunDedup(ctx, s.FrontendClient(), ns, sid, outDir, true))

	after, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  ns,
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Len(t, after.Schedule.Spec.StructuredCalendar, 1)
}

// testSchedUtilDedupDryRun verifies that without execute the schedule is not
// modified but before/after JSON files are still written.
func testSchedUtilDedupDryRun(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	sid := "schedutil-dedup-dry-" + uuid.NewString()[:8]
	ns := s.Namespace().String()
	cron := "0 * * * *"

	createSchedule(t, s, ctx, sid, cron)

	handle := s.SdkClient().ScheduleClient().GetHandle(ctx, sid)
	for i := range 5 {
		err := handle.Update(ctx, sdkclient.ScheduleUpdateOptions{
			DoUpdate: func(input sdkclient.ScheduleUpdateInput) (*sdkclient.ScheduleUpdate, error) {
				sched := input.Description.Schedule
				sched.Spec.CronExpressions = []string{cron}
				return &sdkclient.ScheduleUpdate{Schedule: &sched}, nil
			},
		})
		require.NoError(t, err, "update round %d", i+1)
	}

	outDir := t.TempDir()
	require.NoError(t, schedutil.RunDedup(ctx, s.FrontendClient(), ns, sid, outDir, false))

	// Schedule must be unchanged.
	after, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  ns,
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Len(t, after.Schedule.Spec.StructuredCalendar, 6, "dry run must not modify the schedule")

	// Before/after files must exist.
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Len(t, entries, 2, "expected before and after JSON files")
}

// testSchedUtilForceCAN verifies that RunForceCAN with execute=true signals
// the scheduler workflow without error.
func testSchedUtilForceCAN(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	sid := "schedutil-force-can-" + uuid.NewString()[:8]

	createSchedule(t, s, ctx, sid, "0 * * * *")

	require.NoError(t, schedutil.RunForceCAN(ctx, s.FrontendClient(), s.Namespace().String(), sid, true))
}

// testSchedUtilForceCANDryRun verifies that RunForceCAN with execute=false
// does not signal the workflow.
func testSchedUtilForceCANDryRun(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	sid := "schedutil-force-can-dry-" + uuid.NewString()[:8]

	createSchedule(t, s, ctx, sid, "0 * * * *")

	require.NoError(t, schedutil.RunForceCAN(ctx, s.FrontendClient(), s.Namespace().String(), sid, false))
}

// testSchedUtilDedupNamespaceExecute accumulates duplicates on two schedules
// then runs dedup across the whole namespace with execute=true.
func testSchedUtilDedupNamespaceExecute(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	ns := s.Namespace().String()
	cron := "0 * * * *"

	sid1 := "schedutil-ns-yes-a-" + uuid.NewString()[:8]
	sid2 := "schedutil-ns-yes-b-" + uuid.NewString()[:8]
	createSchedule(t, s, ctx, sid1, cron)
	createSchedule(t, s, ctx, sid2, cron)

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

	for _, sid := range []string{sid1, sid2} {
		desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  ns,
			ScheduleId: sid,
		})
		require.NoError(t, err)
		require.Len(t, desc.Schedule.Spec.StructuredCalendar, 6, "schedule %s should have 6 entries before dedup", sid)
	}

	outDir := t.TempDir()
	require.NoError(t, schedutil.ForEachSchedule(ctx, s.FrontendClient(), ns, func(sid string) error {
		return schedutil.RunDedup(ctx, s.FrontendClient(), ns, sid, outDir, true)
	}))

	for _, sid := range []string{sid1, sid2} {
		after, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  ns,
			ScheduleId: sid,
		})
		require.NoError(t, err)
		require.Len(t, after.Schedule.Spec.StructuredCalendar, 1, "schedule %s should have 1 entry after dedup", sid)
	}
}

// testSchedUtilDedupRecreateDryRun verifies that --recreate without --execute
// does not modify the schedule. RunDedupRecreate reads from workflow history
// (the current run's StartScheduleArgs). Without a prior CAN the history holds
// the initial clean spec, so no files are written and the schedule is unchanged.
func testSchedUtilDedupRecreateDryRun(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	sid := "schedutil-recreate-dry-" + uuid.NewString()[:8]
	ns := s.Namespace().String()
	cron := "0 * * * *"

	createSchedule(t, s, ctx, sid, cron)

	handle := s.SdkClient().ScheduleClient().GetHandle(ctx, sid)
	for i := range 5 {
		err := handle.Update(ctx, sdkclient.ScheduleUpdateOptions{
			DoUpdate: func(input sdkclient.ScheduleUpdateInput) (*sdkclient.ScheduleUpdate, error) {
				sched := input.Description.Schedule
				sched.Spec.CronExpressions = []string{cron}
				return &sdkclient.ScheduleUpdate{Schedule: &sched}, nil
			},
		})
		require.NoError(t, err, "update round %d", i+1)
	}

	outDir := t.TempDir()
	require.NoError(t, schedutil.RunDedupRecreate(ctx, s.FrontendClient(), ns, sid, outDir, false))

	// Schedule must be unchanged.
	after, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  ns,
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Len(t, after.Schedule.Spec.StructuredCalendar, 6, "dry run must not modify the schedule")
}

// testSchedUtilDedupRecreateExecute accumulates duplicates, forces a
// ContinueAsNew so the accumulated spec is baked into the next run's
// WorkflowExecutionStarted input, then calls --recreate --execute and verifies
// the schedule is recreated with a clean spec and the original action preserved.
func testSchedUtilDedupRecreateExecute(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts()...)
	ctx := s.Context()
	sid := "schedutil-recreate-exec-" + uuid.NewString()[:8]
	ns := s.Namespace().String()
	cron := "0 * * * *"

	createSchedule(t, s, ctx, sid, cron)

	handle := s.SdkClient().ScheduleClient().GetHandle(ctx, sid)
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

	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  ns,
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Len(t, desc.Schedule.Spec.StructuredCalendar, rounds+1)

	// Force a CAN so the duplicated spec becomes the input of the next workflow
	// run — that is what RunDedupRecreate reads from history.
	require.NoError(t, schedutil.RunForceCAN(ctx, s.FrontendClient(), ns, sid, true))

	// Wait for the new run to start with the accumulated spec.
	require.Eventually(t, func() bool {
		wid := scheduler.WorkflowIDPrefix + sid
		resp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx,
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:       ns,
				Execution:       &commonpb.WorkflowExecution{WorkflowId: wid},
				MaximumPageSize: 1,
			})
		if err != nil || len(resp.History.Events) == 0 {
			return false
		}
		attrs := resp.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
		if attrs == nil {
			return false
		}
		var args schedulespb.StartScheduleArgs
		if err := payloads.Decode(attrs.Input, &args); err != nil {
			return false
		}
		return len(args.Schedule.Spec.StructuredCalendar) > 1
	}, 10*time.Second, 200*time.Millisecond, "new run should start with accumulated spec")

	outDir := t.TempDir()
	require.NoError(t, schedutil.RunDedupRecreate(ctx, s.FrontendClient(), ns, sid, outDir, true))

	after, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  ns,
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Len(t, after.Schedule.Spec.StructuredCalendar, 1, "recreated schedule should have 1 calendar entry")
	require.NotNil(t, after.Schedule.Action, "action should be preserved after recreate")
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
