//go:build test_dep

package scheduler2

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
)

type publicTestFrontend struct {
	engine     chasm.Engine
	visibility chasm.VisibilityManager
	mapper     *chasm.VisibilitySearchAttributesMapper
}

// NewPublicTestFrontend installs the paused public profile before a dedicated
// test cluster starts. It does not register another SchedulerService transport.
func NewPublicTestFrontend(engine chasm.Engine, visibility chasm.VisibilityManager, registry *chasm.Registry) (testhooks.ScheduleFrontend, error) {
	if err := registry.Register(NewNilLibrary()); err != nil {
		return nil, err
	}
	id, ok := registry.ComponentIDFor(&Scheduler{})
	if !ok || id == chasm.SchedulerArchetypeID {
		return nil, errors.New("independent scheduler2 registration is missing")
	}
	component, ok := registry.ComponentByID(id)
	if !ok || component.SearchAttributesMapper() == nil {
		return nil, errors.New("scheduler2 visibility mapper is missing")
	}
	return &publicTestFrontend{engine: engine, visibility: visibility, mapper: component.SearchAttributesMapper()}, nil
}

func (f *publicTestFrontend) bind(ctx context.Context) context.Context {
	return chasm.NewVisibilityManagerContext(chasm.NewEngineContext(ctx, f.engine), f.visibility)
}

func (f *publicTestFrontend) SearchAttributesMapper() *chasm.VisibilitySearchAttributesMapper {
	return f.mapper
}

func (f *publicTestFrontend) CreateSchedule(ctx context.Context, namespaceID namespace.ID, request *workflowservice.CreateScheduleRequest) (*workflowservice.CreateScheduleResponse, error) {
	result, err := chasm.StartExecution(f.bind(ctx), chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: request.ScheduleId}, NewPublicSchedule, request, chasm.WithStartRequestID(request.RequestId))
	if _, exists := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); exists {
		return nil, serviceerror.NewWorkflowExecutionAlreadyStarted(fmt.Sprintf("schedule %q is already registered", request.ScheduleId), "", "")
	}
	if err != nil {
		return nil, err
	}
	return &workflowservice.CreateScheduleResponse{ConflictToken: publicConflictToken(result.ExecutionKey.RunID, 1)}, nil
}

func (f *publicTestFrontend) DescribeSchedule(ctx context.Context, namespaceID namespace.ID, request *workflowservice.DescribeScheduleRequest) (*workflowservice.DescribeScheduleResponse, error) {
	return chasm.ReadComponent(f.bind(ctx), chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: request.ScheduleId}),
		func(s *Scheduler, cc chasm.Context, _ struct{}) (*workflowservice.DescribeScheduleResponse, error) {
			return s.DescribePublic(cc)
		}, struct{}{})
}

func (f *publicTestFrontend) DeleteSchedule(ctx context.Context, namespaceID namespace.ID, request *workflowservice.DeleteScheduleRequest) (*workflowservice.DeleteScheduleResponse, error) {
	_, _, err := chasm.UpdateComponent(f.bind(ctx), chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: request.ScheduleId}),
		func(s *Scheduler, cc chasm.MutableContext, _ struct{}) (struct{}, error) {
			if s.Closed {
				return struct{}{}, serviceerror.NewNotFound("schedule is closed")
			}
			return struct{}{}, s.Close(cc)
		}, struct{}{})
	if err != nil {
		return nil, err
	}
	return &workflowservice.DeleteScheduleResponse{}, nil
}

func (f *publicTestFrontend) ListSchedules(ctx context.Context, _ namespace.ID, request *workflowservice.ListSchedulesRequest, query string) (*workflowservice.ListSchedulesResponse, error) {
	result, err := chasm.ListExecutions[*Scheduler, *schedulepb.ScheduleListInfo](f.bind(ctx), &chasm.ListExecutionsRequest{
		NamespaceName: request.Namespace, Query: query, PageSize: int(request.MaximumPageSize), NextPageToken: request.NextPageToken,
	})
	if err != nil {
		return nil, err
	}
	response := &workflowservice.ListSchedulesResponse{NextPageToken: result.NextPageToken}
	for _, entry := range result.Executions {
		response.Schedules = append(response.Schedules, &schedulepb.ScheduleListEntry{
			ScheduleId: entry.BusinessID, Info: common.CloneProto(entry.ChasmMemo), Memo: common.CloneProto(entry.Memo),
			SearchAttributes: common.CloneProto(&commonpb.SearchAttributes{IndexedFields: entry.CustomSearchAttributes}),
		})
	}
	return response, nil
}

func (f *publicTestFrontend) CountSchedules(ctx context.Context, _ namespace.ID, request *workflowservice.CountSchedulesRequest, query string) (*workflowservice.CountSchedulesResponse, error) {
	result, err := chasm.CountExecutions[*Scheduler](f.bind(ctx), &chasm.CountExecutionsRequest{NamespaceName: request.Namespace, Query: query})
	if err != nil {
		return nil, err
	}
	response := &workflowservice.CountSchedulesResponse{Count: result.Count}
	for _, group := range result.Groups {
		response.Groups = append(response.Groups, &workflowservice.CountSchedulesResponse_AggregationGroup{GroupValues: group.Values, Count: group.Count})
	}
	return response, nil
}
