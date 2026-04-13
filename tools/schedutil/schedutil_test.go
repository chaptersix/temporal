package schedutil

import (
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/worker/scheduler"
)

// --- helpers ----------------------------------------------------------------

func iterEvents(events ...*historypb.HistoryEvent) iter.Seq2[*historypb.HistoryEvent, error] {
	return func(yield func(*historypb.HistoryEvent, error) bool) {
		for _, e := range events {
			if !yield(e, nil) {
				return
			}
		}
	}
}

func wesEvent(typ string, args *schedulespb.StartScheduleArgs) *historypb.HistoryEvent {
	input, _ := payloads.Encode(args)
	return &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: typ},
				Input:        input,
			},
		},
	}
}

func updateEvent(sched *schedulepb.Schedule) *historypb.HistoryEvent {
	input, _ := payloads.Encode(&schedulespb.FullUpdateRequest{Schedule: sched})
	return &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: scheduler.SignalNameUpdate,
				Input:      input,
			},
		},
	}
}

func patchEvent(patch *schedulepb.SchedulePatch) *historypb.HistoryEvent {
	input, _ := payloads.Encode(patch)
	return &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: scheduler.SignalNamePatch,
				Input:      input,
			},
		},
	}
}

func otherSignalEvent(name string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: name,
			},
		},
	}
}

func sched(comment string) *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec:   &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Comment: comment}}},
		State:  &schedulepb.ScheduleState{},
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{}}},
	}
}

const wid = "temporal-sys-scheduler:my-sched"

// --- tests ------------------------------------------------------------------

func TestReplayScheduleHistory(t *testing.T) {
	tests := []struct {
		name        string
		events      []*historypb.HistoryEvent
		wantErr     string
		wantComment string
		wantPaused  bool
	}{
		{
			name:    "empty history returns error",
			wantErr: "no WorkflowExecutionStarted",
		},
		{
			name:    "wrong workflow type returns error",
			events:  []*historypb.HistoryEvent{wesEvent("other-type", &schedulespb.StartScheduleArgs{Schedule: sched("x")})},
			wantErr: "not a schedule workflow",
		},
		{
			name:        "WES only returns baseline",
			events:      []*historypb.HistoryEvent{wesEvent(scheduler.WorkflowType, &schedulespb.StartScheduleArgs{Schedule: sched("baseline")})},
			wantComment: "baseline",
		},
		{
			name: "update signal supersedes WES",
			events: []*historypb.HistoryEvent{
				wesEvent(scheduler.WorkflowType, &schedulespb.StartScheduleArgs{Schedule: sched("baseline")}),
				updateEvent(sched("updated")),
			},
			wantComment: "updated",
		},
		{
			name: "last update wins",
			events: []*historypb.HistoryEvent{
				wesEvent(scheduler.WorkflowType, &schedulespb.StartScheduleArgs{Schedule: sched("baseline")}),
				updateEvent(sched("first")),
				updateEvent(sched("second")),
			},
			wantComment: "second",
		},
		{
			name: "patch before any update is ignored",
			events: []*historypb.HistoryEvent{
				wesEvent(scheduler.WorkflowType, &schedulespb.StartScheduleArgs{Schedule: sched("baseline")}),
				patchEvent(&schedulepb.SchedulePatch{Pause: "ignored"}),
			},
			wantComment: "baseline",
			wantPaused:  false,
		},
		{
			name: "pause patch after update is applied",
			events: []*historypb.HistoryEvent{
				wesEvent(scheduler.WorkflowType, &schedulespb.StartScheduleArgs{Schedule: sched("baseline")}),
				updateEvent(sched("updated")),
				patchEvent(&schedulepb.SchedulePatch{Pause: "maintenance"}),
			},
			wantComment: "updated",
			wantPaused:  true,
		},
		{
			name: "later update resets patch queue",
			events: []*historypb.HistoryEvent{
				wesEvent(scheduler.WorkflowType, &schedulespb.StartScheduleArgs{Schedule: sched("baseline")}),
				updateEvent(sched("first")),
				patchEvent(&schedulepb.SchedulePatch{Pause: "paused"}),
				updateEvent(sched("second")),
			},
			wantComment: "second",
			wantPaused:  false,
		},
		{
			name: "unrelated signals are ignored",
			events: []*historypb.HistoryEvent{
				wesEvent(scheduler.WorkflowType, &schedulespb.StartScheduleArgs{Schedule: sched("baseline")}),
				otherSignalEvent(scheduler.SignalNameForceCAN),
			},
			wantComment: "baseline",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := replayScheduleHistory(wid, iterEvents(tc.events...))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantComment, got.Spec.StructuredCalendar[0].Comment)
			require.Equal(t, tc.wantPaused, got.State.Paused)
		})
	}
}

func TestDeduplicateStructuredCalendarsProto(t *testing.T) {
	hourly := &schedulepb.StructuredCalendarSpec{
		Second:     []*schedulepb.Range{{Start: 0}},
		Minute:     []*schedulepb.Range{{Start: 0}},
		Hour:       []*schedulepb.Range{{Start: 0, End: 23}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedulepb.Range{{}},
	}
	hourlyCommented := &schedulepb.StructuredCalendarSpec{
		Second:     []*schedulepb.Range{{Start: 0}},
		Minute:     []*schedulepb.Range{{Start: 0}},
		Hour:       []*schedulepb.Range{{Start: 0, End: 23}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedulepb.Range{{}},
		Comment:    "same schedule, different comment",
	}
	hourlyDefaulted := &schedulepb.StructuredCalendarSpec{
		Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
		Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
		Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
		DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
	}
	daily := &schedulepb.StructuredCalendarSpec{
		Second:     []*schedulepb.Range{{Start: 0}},
		Minute:     []*schedulepb.Range{{Start: 0}},
		Hour:       []*schedulepb.Range{{Start: 9}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedulepb.Range{{}},
	}

	tests := []struct {
		name  string
		input []*schedulepb.StructuredCalendarSpec
		want  int
	}{
		{name: "empty", input: nil, want: 0},
		{name: "no duplicates preserved", input: []*schedulepb.StructuredCalendarSpec{hourly, daily}, want: 2},
		{name: "all duplicates collapsed to one", input: []*schedulepb.StructuredCalendarSpec{hourly, hourly, hourly}, want: 1},
		{name: "first occurrence wins", input: []*schedulepb.StructuredCalendarSpec{hourly, daily, hourly}, want: 2},
		{name: "preserves comment differences", input: []*schedulepb.StructuredCalendarSpec{hourly, hourlyCommented}, want: 2},
		{name: "normalizes defaulted ranges", input: []*schedulepb.StructuredCalendarSpec{hourly, hourlyDefaulted}, want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Len(t, deduplicateStructuredCalendarsProto(tc.input), tc.want)
		})
	}
}
