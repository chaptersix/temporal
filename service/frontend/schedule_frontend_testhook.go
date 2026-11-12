package frontend

import (
	"context"
	"fmt"
	"strings"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/worker/scheduler"
)

func (wh *WorkflowHandler) scheduleFrontend() (testhooks.ScheduleFrontend, bool) {
	return testhooks.Get(wh.testHooks, testhooks.FrontendSchedule, testhooks.GlobalScope)
}

func (wh *WorkflowHandler) createScheduleSelected(ctx context.Context, request *workflowservice.CreateScheduleRequest, frontend testhooks.ScheduleFrontend) (*workflowservice.CreateScheduleResponse, error) {
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.Namespace))
	if err != nil {
		return nil, err
	}
	if _, err := wh.validator.UnaliasedSearchAttributesFrom(request.GetSearchAttributes(), request.Namespace); err != nil {
		return nil, err
	}
	if err := wh.validateSchedulePayloadSize(ctx, request.Namespace, request.ScheduleId, request.Schedule, request.Memo, "CreateSchedule"); err != nil {
		return nil, err
	}
	return frontend.CreateSchedule(ctx, namespaceID, request)
}

func (wh *WorkflowHandler) unsupportedSelectedScheduleOperation(namespaceName, operation string) error {
	if _, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(namespaceName)); err != nil {
		return err
	}
	return serviceerror.NewUnimplemented(fmt.Sprintf("selected local schedule frontend does not support %s", operation))
}

func (wh *WorkflowHandler) prepareSelectedSchedulerQuery(query string, namespaceName namespace.Name, metricsHandler metrics.Handler, frontend testhooks.ScheduleFrontend) (string, error) {
	const baseQuery = "ExecutionStatus = 'Running'"
	if strings.TrimSpace(query) == "" {
		return baseQuery, nil
	}
	saNameType, err := wh.saProvider.GetSearchAttributes(wh.visibilityMgr.GetIndexName(), false)
	if err != nil {
		return "", serviceerror.NewUnavailablef(errUnableToGetSearchAttributesMessage, err)
	}
	if err := scheduler.ValidateIsolatedVisibilityQuery(namespaceName, saNameType, wh.saMapperProvider, frontend.SearchAttributesMapper(), query, metricsHandler, wh.logger); err != nil {
		return "", err
	}
	// Namespace custom aliases take precedence over the component's synthetic ScheduleId.
	// Preserve the predicate for the visibility converter instead of applying the V1 rewriter.
	return fmt.Sprintf("%s AND (%s)", baseQuery, query), nil
}
