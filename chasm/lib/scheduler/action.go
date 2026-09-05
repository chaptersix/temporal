package scheduler

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/internal"
	"go.temporal.io/server/common/resource"
)

type occurrenceContext struct {
	NominalTime, ScheduledTime time.Time
	Manual                     bool
	RequestID                  string
}

type actionMetadata struct {
	Kind                    enumspb.ExecutionType
	Type, IDBase, TaskQueue string
	SearchAttributes        *commonpb.SearchAttributes
}

type actionStartInput struct {
	Scheduler                *Scheduler
	Occurrence               *schedulespb.BufferedStart
	Callback                 *commonpb.Callback
	Previous                 *schedulerpb.LastCompletionResult
	EnableVersioningOverride bool
}

type actionClients struct {
	Frontend workflowservice.WorkflowServiceClient
	History  resource.HistoryClient
}

type actionCompletion struct {
	Result *commonpb.ActionExecutionResult
	Failed bool
}

type actionImplementation interface {
	Metadata(*schedulepb.ScheduleAction) actionMetadata
	Validate(*schedulepb.ScheduleAction) error
	Policies() *internal.PolicyRegistry
	GenerateTargetID(string, occurrenceContext) string
	Start(context.Context, actionClients, actionStartInput) (string, error)
	Cancel(context.Context, actionClients, *Scheduler, *commonpb.Execution) error
	Terminate(context.Context, actionClients, *Scheduler, *commonpb.Execution) error
	Completion(*persistencespb.ChasmNexusCompletion, *commonpb.Execution) actionCompletion
	ParticipatesInCompletionHistory() bool
}

func implementation(action *schedulepb.ScheduleAction) actionImplementation {
	if action.GetStartActivity() != nil {
		return activityAction{}
	}
	return workflowAction{}
}

func (s *Scheduler) actionMetadata() actionMetadata {
	return implementation(s.Schedule.GetAction()).Metadata(s.Schedule.GetAction())
}

func (s *Scheduler) newBufferedExecution(start *schedulespb.BufferedStart, base string) {
	action := implementation(s.Schedule.GetAction())
	id := action.GenerateTargetID(base, occurrenceContext{NominalTime: start.GetNominalTime().AsTime(), ScheduledTime: start.GetActualTime().AsTime(), Manual: start.GetManual(), RequestID: start.GetRequestId()})
	start.Execution = &commonpb.Execution{Type: action.Metadata(s.Schedule.GetAction()).Kind, BusinessId: id}
	if start.Execution.Type == enumspb.EXECUTION_TYPE_WORKFLOW {
		start.WorkflowId = id
	}
}

func (s *Scheduler) targetIDBase() string { return s.actionMetadata().IDBase }

func validateActionKind(previous, next *schedulepb.ScheduleAction) error {
	if implementation(previous).Metadata(previous).Kind != implementation(next).Metadata(next).Kind {
		return serviceerror.NewInvalidArgument("schedule action kind cannot be changed")
	}
	return nil
}
