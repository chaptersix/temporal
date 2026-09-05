package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/worker/scheduler"
)

func TestResearchMigrationSignalReplay(t *testing.T) {
	for _, boundary := range []string{"local_activity_heartbeat", "unhandled_command_retry"} {
		t.Run(boundary, func(t *testing.T) {
			h := loadHistory(t, "testdata/replay_migration_v1_to_v2.json.gz")
			scheduled := common.CloneProto(h.Events[26])
			started := common.CloneProto(h.Events[27])
			completed := common.CloneProto(h.Events[28])
			watch := common.CloneProto(h.Events[29])
			migrate := common.CloneProto(h.Events[30])
			closed := common.CloneProto(h.Events[31])
			input, err := payloads.Encode(&schedulepb.SchedulePatch{Pause: "accepted after snapshot"})
			require.NoError(t, err)
			signal := &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
					WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
						SignalName: scheduler.SignalNamePatch,
						Input:      input,
						Identity:   "research-client",
					},
				},
			}
			if boundary == "local_activity_heartbeat" {
				h.Events = h.Events[:30]
			} else {
				h.Events = h.Events[:28]
				h.Events = append(h.Events, &historypb.HistoryEvent{
					EventId: 29, EventTime: completed.EventTime,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
					Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
						WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
							ScheduledEventId: 27, StartedEventId: 28,
							Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND,
						},
					},
				})
			}
			tail := []*historypb.HistoryEvent{signal, scheduled, started, completed}
			if boundary == "unhandled_command_retry" {
				tail = append(tail, watch)
			}
			tail = append(tail, migrate, closed)
			for _, event := range tail {
				event.EventId = int64(len(h.Events) + 1)
				event.EventTime = closed.EventTime
				h.Events = append(h.Events, event)
			}
			started.GetWorkflowTaskStartedEventAttributes().ScheduledEventId = scheduled.EventId
			completed.GetWorkflowTaskCompletedEventAttributes().ScheduledEventId = scheduled.EventId
			completed.GetWorkflowTaskCompletedEventAttributes().StartedEventId = started.EventId
			if boundary == "unhandled_command_retry" {
				watch.GetMarkerRecordedEventAttributes().WorkflowTaskCompletedEventId = completed.EventId
			}
			migrate.GetMarkerRecordedEventAttributes().WorkflowTaskCompletedEventId = completed.EventId
			closed.GetWorkflowExecutionCompletedEventAttributes().WorkflowTaskCompletedEventId = completed.EventId
			var finalState *schedulespb.StartScheduleArgs
			var unhandled []string
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
				err := scheduler.SchedulerWorkflow(ctx, args)
				finalState = args
				unhandled = workflow.GetUnhandledSignalNames(ctx)
				return err
			}, workflow.RegisterOptions{Name: scheduler.WorkflowType})
			require.NoError(t, replayer.ReplayWorkflowHistory(log.NewSdkLogger(log.NewTestLogger()), h))
			require.NotNil(t, finalState)
			if boundary == "unhandled_command_retry" {
				require.True(t, finalState.Schedule.State.Paused)
				require.Empty(t, unhandled)
				return
			}
			require.False(t, finalState.Schedule.State.Paused)
			require.Equal(t, []string{scheduler.SignalNamePatch}, unhandled)
		})
	}
}
