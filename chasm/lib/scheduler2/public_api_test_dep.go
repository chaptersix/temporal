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
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
)

type publicTestFrontend struct {
	engine      chasm.Engine
	visibility  chasm.VisibilityManager
	mapper      *chasm.VisibilitySearchAttributesMapper
	tweaks      func(string) schedulerconfig.Tweakables
	specBuilder *legacyscheduler.SpecBuilder
}

// NewPublicTestFrontend installs the paused public profile before a dedicated
// test cluster starts. It does not register another SchedulerService transport.
func NewPublicTestFrontend(engine chasm.Engine, visibility chasm.VisibilityManager, registry *chasm.Registry) (testhooks.ScheduleFrontend, error) {
	return newPublicTestFrontend(engine, visibility, registry, NewNilLibrary())
}

func newPublicTestFrontend(engine chasm.Engine, visibility chasm.VisibilityManager, registry *chasm.Registry, library *Library) (*publicTestFrontend, error) {
	if err := registry.Register(library); err != nil {
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
	constructor := NewPublicSchedule
	if f.tweaks != nil {
		constructor = func(cc chasm.MutableContext, r *workflowservice.CreateScheduleRequest) (*Scheduler, error) {
			return newExecutingPublicSchedule(cc, r, f.tweaks(r.Namespace))
		}
	}
	result, err := chasm.StartExecution(f.bind(ctx), chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: request.ScheduleId}, constructor, request, chasm.WithStartRequestID(request.RequestId))
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

// NewPublicExecutionTestFrontend binds side effects to actual service clients.
// Activation remains confined to explicitly constructed dedicated test clusters.
func NewPublicExecutionTestFrontend(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
	start := NewStartHandler(nil)
	start.startWorkflow = func(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
		if err := runtime.CallbackValidator.Validate(ctx, request.Namespace, request.CompletionCallbacks, callbacks.ValidatorOptions{EnabledKinds: chasmworkflow.EnabledCallbackKinds.Get(runtime.DynamicConfig)(request.Namespace)}); err != nil {
			return nil, err
		}
		if err := runtime.CallbackValidator.ValidateAdditions(request.Namespace, request.CompletionCallbacks, callbacks.CurrentCallbacksInfo{}); err != nil {
			return nil, err
		}
		response, err := runtime.FrontendClient.StartWorkflowExecution(ctx, request)
		if err != nil {
			return nil, err
		}
		if response.GetFirstExecutionRunId() == "" {
			return nil, serviceerror.NewUnavailable("workflow start response has no first execution run ID")
		}
		return response, nil
	}
	effects := NewEffectHandler(
		func(ctx context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) error {
			id, err := runtime.NamespaceRegistry.GetNamespaceID(namespace.Name(request.Namespace))
			if err != nil {
				return err
			}
			_, err = runtime.HistoryClient.RequestCancelWorkflowExecution(ctx, &historyservice.RequestCancelWorkflowExecutionRequest{NamespaceId: id.String(), CancelRequest: request})
			return err
		},
		func(ctx context.Context, request *workflowservice.TerminateWorkflowExecutionRequest) error {
			id, err := runtime.NamespaceRegistry.GetNamespaceID(namespace.Name(request.Namespace))
			if err != nil {
				return err
			}
			_, err = runtime.HistoryClient.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{NamespaceId: id.String(), TerminateRequest: request})
			return err
		},
	)
	library := NewLibrary(start, effects)
	library.tweaks = schedulerconfig.CurrentTweakables.Get(runtime.DynamicConfig)
	library.specBuilder = legacyscheduler.NewSpecBuilder(dynamicconfig.SchedulerSpecWarnIterations.Get(runtime.DynamicConfig), dynamicconfig.SchedulerSpecMaxIterations.Get(runtime.DynamicConfig))
	frontend, err := newPublicTestFrontend(runtime.Engine, runtime.Visibility, runtime.Registry, library)
	if err != nil {
		return nil, err
	}
	frontend.tweaks = schedulerconfig.CurrentTweakables.Get(runtime.DynamicConfig)
	frontend.specBuilder = library.specBuilder
	return frontend, nil
}

func (f *publicTestFrontend) UpdateSchedule(ctx context.Context, namespaceID namespace.ID, request *workflowservice.UpdateScheduleRequest) (*workflowservice.UpdateScheduleResponse, error) {
	if f.tweaks == nil {
		return nil, serviceerror.NewUnimplemented("scheduler2 public UpdateSchedule is not enabled")
	}
	_, _, err := chasm.UpdateComponent(f.bind(ctx), chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: request.ScheduleId}),
		func(s *Scheduler, cc chasm.MutableContext, r *workflowservice.UpdateScheduleRequest) (chasm.NoValue, error) {
			return nil, s.UpdatePublic(cc, r, f.tweaks(r.Namespace))
		}, request, chasm.WithRequestID(request.RequestId))
	if err != nil && !errors.Is(err, chasm.ErrRequestIDAlreadyUsed) {
		return nil, err
	}
	return &workflowservice.UpdateScheduleResponse{}, nil
}

func (f *publicTestFrontend) PatchSchedule(ctx context.Context, namespaceID namespace.ID, request *workflowservice.PatchScheduleRequest) (*workflowservice.PatchScheduleResponse, error) {
	if f.tweaks == nil {
		return nil, serviceerror.NewUnimplemented("scheduler2 public PatchSchedule is not enabled")
	}
	_, _, err := chasm.UpdateComponent(f.bind(ctx), chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: request.ScheduleId}),
		func(s *Scheduler, cc chasm.MutableContext, r *workflowservice.PatchScheduleRequest) (chasm.NoValue, error) {
			return nil, s.PatchPublic(cc, r.Patch)
		}, request, chasm.WithRequestID(request.RequestId))
	if err != nil && !errors.Is(err, chasm.ErrRequestIDAlreadyUsed) {
		return nil, err
	}
	return &workflowservice.PatchScheduleResponse{}, nil
}

func (f *publicTestFrontend) ListScheduleMatchingTimes(ctx context.Context, namespaceID namespace.ID, request *workflowservice.ListScheduleMatchingTimesRequest) (*workflowservice.ListScheduleMatchingTimesResponse, error) {
	if f.specBuilder == nil {
		return nil, serviceerror.NewUnimplemented("scheduler2 public ListScheduleMatchingTimes is not enabled")
	}
	return chasm.ReadComponent(f.bind(ctx), chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: request.ScheduleId}),
		func(s *Scheduler, cc chasm.Context, r *workflowservice.ListScheduleMatchingTimesRequest) (*workflowservice.ListScheduleMatchingTimesResponse, error) {
			return s.MatchingTimesPublic(cc, r, f.specBuilder)
		}, request)
}

// ReadCarryoverForTest returns detached logical values independent of their DATA representation.
func ReadCarryoverForTest(s *Scheduler, ctx chasm.Context) (*scheduler2pb.CarryoverState, error) {
	return s.carryoverValue(ctx)
}
