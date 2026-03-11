package tests

import (
	"sync/atomic"
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
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

const signalComplete = "complete"

// TestMigrationCallbackRecordsResult validates the end-to-end flow:
//  1. Create a V1 schedule via the frontend API
//  2. Trigger it to start a target workflow that blocks on a signal
//  3. Migrate V1 -> V2 (attaches completion callback to the running workflow)
//  4. Describe V2 schedule and verify the running workflow is visible
//  5. Signal the target workflow to complete
//  6. Verify V2 records the completion
func TestMigrationCallbackRecordsResult(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, false),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-cb")
	targetWfID := testcore.RandomizeStr("sched-cb-wf")
	wt := testcore.RandomizeStr("sched-cb-wt")
	tq := testcore.RandomizeStr("sched-cb-tq")

	nsName := env.Namespace().String()
	nsID := env.NamespaceID().String()

	// Set up SDK client and worker with a workflow that blocks until signaled.
	sdkClient, err := client.Dial(client.Options{
		HostPort:  env.GetTestCluster().Host().FrontendGRPCAddress(),
		Namespace: nsName,
	})
	require.NoError(t, err)
	defer sdkClient.Close()

	var started atomic.Bool
	w := worker.New(sdkClient, tq, worker.Options{})
	w.RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			started.Store(true)
			ch := workflow.GetSignalChannel(ctx, signalComplete)
			ch.Receive(ctx, nil)
			return nil
		},
		workflow.RegisterOptions{Name: wt},
	)
	require.NoError(t, w.Start())
	defer w.Stop()

	// 1. Create V1 schedule via the frontend API.
	// EnableCHASMSchedulerCreation is false, so CreateSchedule creates a V1 schedule.
	_, err = env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(24 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   targetWfID,
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	require.NoError(t, err)

	// Wait for the V1 scheduler workflow to start processing.
	v1WorkflowID := scheduler.WorkflowIDPrefix + sid
	require.Eventually(t, func() bool {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: nsName,
			Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
		})
		if err != nil {
			return false
		}
		return desc.GetWorkflowExecutionInfo().GetHistoryLength() > 3
	}, 10*time.Second, 500*time.Millisecond, "V1 scheduler workflow did not start")

	// 2. Trigger the schedule to start the target workflow.
	_, err = env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	require.NoError(t, err)

	// Wait for the target workflow to start executing.
	require.Eventually(t, func() bool {
		return started.Load()
	}, 10*time.Second, 200*time.Millisecond, "target workflow did not start")

	// Get the real workflow ID (has timestamp appended) from the schedule description.
	descResp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
	})
	require.NoError(t, err)
	running := descResp.GetInfo().GetRunningWorkflows()
	require.Len(t, running, 1, "expected exactly one running workflow")
	realWfID := running[0].GetWorkflowId()
	realRunID := running[0].GetRunId()

	// 3. Migrate V1 -> V2.
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_CHASM,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Wait for V1 workflow to complete (migration closes it).
	require.Eventually(t, func() bool {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: nsName,
			Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
		})
		if err != nil {
			return false
		}
		return desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 10*time.Second, 500*time.Millisecond, "V1 scheduler workflow did not complete")

	// 4. Describe V2 schedule via the scheduler client and verify the running workflow.
	descV2Req := &schedulerpb.DescribeScheduleRequest{
		NamespaceId:     nsID,
		FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
	}
	var v2Desc *schedulerpb.DescribeScheduleResponse
	require.Eventually(t, func() bool {
		v2Desc, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(ctx, descV2Req)
		if err != nil {
			return false
		}
		return len(v2Desc.GetFrontendResponse().GetInfo().GetRunningWorkflows()) == 1
	}, 10*time.Second, 500*time.Millisecond, "V2 schedule should show the running workflow")
	require.Equal(t, realWfID, v2Desc.GetFrontendResponse().GetInfo().GetRunningWorkflows()[0].GetWorkflowId())

	// 5. Signal the target workflow to complete.
	err = sdkClient.SignalWorkflow(ctx, realWfID, realRunID, signalComplete, nil)
	require.NoError(t, err)

	// 6. Verify V2 records the completion.
	require.Eventually(t, func() bool {
		v2Desc, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(ctx, descV2Req)
		if err != nil {
			return false
		}
		for _, action := range v2Desc.GetFrontendResponse().GetInfo().GetRecentActions() {
			if action.GetStartWorkflowResult().GetWorkflowId() == realWfID &&
				action.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				return true
			}
		}
		return false
	}, 10*time.Second, 500*time.Millisecond, "V2 schedule did not record the workflow completion")
}
