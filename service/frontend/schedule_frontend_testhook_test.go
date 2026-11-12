//go:build test_dep

package frontend

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/testhooks"
	"go.uber.org/mock/gomock"
)

// Embedding the interface makes any unexpected method call fail the test immediately.
type selectedScheduleFrontend struct {
	testhooks.ScheduleFrontend
	err          error
	calls        int
	mapper       *chasm.VisibilitySearchAttributesMapper
	listResponse *workflowservice.ListSchedulesResponse
}

func (f *selectedScheduleFrontend) SearchAttributesMapper() *chasm.VisibilitySearchAttributesMapper {
	return f.mapper
}

func (f *selectedScheduleFrontend) CreateSchedule(context.Context, namespace.ID, *workflowservice.CreateScheduleRequest) (*workflowservice.CreateScheduleResponse, error) {
	f.calls++
	return nil, f.err
}
func (f *selectedScheduleFrontend) DescribeSchedule(context.Context, namespace.ID, *workflowservice.DescribeScheduleRequest) (*workflowservice.DescribeScheduleResponse, error) {
	f.calls++
	return nil, f.err
}
func (f *selectedScheduleFrontend) DeleteSchedule(context.Context, namespace.ID, *workflowservice.DeleteScheduleRequest) (*workflowservice.DeleteScheduleResponse, error) {
	f.calls++
	return nil, f.err
}
func (f *selectedScheduleFrontend) ListSchedules(context.Context, namespace.ID, *workflowservice.ListSchedulesRequest, string) (*workflowservice.ListSchedulesResponse, error) {
	f.calls++
	return f.listResponse, f.err
}
func (f *selectedScheduleFrontend) CountSchedules(context.Context, namespace.ID, *workflowservice.CountSchedulesRequest, string) (*workflowservice.CountSchedulesResponse, error) {
	f.calls++
	return nil, f.err
}

func (s *WorkflowHandlerSuite) TestSelectedScheduleFrontend_NoFallback() {
	cfg := s.newConfig()
	cfg.EnableSchedules = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	wh := s.getWorkflowHandler(cfg)
	wh.testHooks = testhooks.NewTestHooks()
	frontend := &selectedScheduleFrontend{err: serviceerror.NewNotFound("selected backend missing")}
	s.T().Cleanup(testhooks.Set(wh.testHooks, testhooks.FrontendSchedule, testhooks.ScheduleFrontend(frontend), testhooks.GlobalScope))
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Eq(s.testNamespace)).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(s.testNamespace).Return(&searchattribute.TestMapper{}, nil).AnyTimes()
	ctx := context.Background()
	ns := s.testNamespace.String()
	// NotFound is legacy-routable in the default path; selected requests must propagate it.
	_, err := wh.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{Namespace: ns, ScheduleId: "id"})
	s.ErrorIs(err, frontend.err)
	_, err = wh.DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{Namespace: ns, ScheduleId: "id"})
	s.ErrorIs(err, frontend.err)
	_, err = wh.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{Namespace: ns})
	s.ErrorIs(err, frontend.err)
	_, err = wh.CountSchedules(ctx, &workflowservice.CountSchedulesRequest{Namespace: ns})
	s.ErrorIs(err, frontend.err)
	s.Equal(4, frontend.calls)
	// Required request validation precedes dispatch.
	_, err = wh.CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{Namespace: ns, ScheduleId: "id"})
	s.Error(err)
	s.Equal(4, frontend.calls)
	_, err = wh.PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{Namespace: ns})
	var invalid *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalid)
	_, err = wh.PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{Namespace: ns, ScheduleId: "id", Patch: &schedulepb.SchedulePatch{}})
	var unsupported *serviceerror.Unimplemented
	s.ErrorAs(err, &unsupported)
	_, err = wh.UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{Namespace: ns, ScheduleId: "id", Schedule: &schedulepb.Schedule{Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "unused", WorkflowType: &commonpb.WorkflowType{Name: "unused"}, TaskQueue: &taskqueuepb.TaskQueue{Name: "unused"}}}}}})
	s.ErrorAs(err, &unsupported)
	_, err = wh.ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{Namespace: ns, ScheduleId: "id"})
	s.ErrorAs(err, &unsupported)
	s.Equal(4, frontend.calls)
}

func (s *WorkflowHandlerSuite) TestSelectedScheduleFrontend_QueryPreserved() {
	wh := s.getWorkflowHandler(s.newConfig())
	wh.saProvider = searchattribute.NewTestProvider()
	wh.saMapperProvider = searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{WithCustomScheduleID: true})
	frontend := &selectedScheduleFrontend{mapper: chasm.NewTestVisibilitySearchAttributesMapper(map[string]string{sadefs.WorkflowID: sadefs.ScheduleID}, map[string]enumspb.IndexedValueType{sadefs.WorkflowID: enumspb.INDEXED_VALUE_TYPE_KEYWORD})}
	for _, predicate := range []string{"ScheduleId = 'custom-value'", "ScheduleId IN ('a', 'b')", "ScheduleId != 'business-id'"} {
		actual, err := wh.prepareSelectedSchedulerQuery(predicate, s.testNamespace, metrics.NoopMetricsHandler, frontend)
		s.NoError(err)
		s.Equal("ExecutionStatus = 'Running' AND ("+predicate+")", actual)
	}
	_, err := wh.prepareSelectedSchedulerQuery("NamespaceDivision = 'other'", s.testNamespace, metrics.NoopMetricsHandler, frontend)
	s.Error(err)
}

func (s *WorkflowHandlerSuite) TestSelectedScheduleFrontend_ListCleansSystemAttributes() {
	cfg := s.newConfig()
	cfg.EnableSchedules = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	wh := s.getWorkflowHandler(cfg)
	wh.testHooks = testhooks.NewTestHooks()
	custom := sadefs.MustEncodeValue("custom-value", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	frontend := &selectedScheduleFrontend{listResponse: &workflowservice.ListSchedulesResponse{Schedules: []*schedulepb.ScheduleListEntry{
		{ScheduleId: "id", SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			"ScheduleId":                     custom,
			sadefs.TemporalNamespaceDivision: sadefs.MustEncodeValue("scheduler2", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			sadefs.TemporalSchedulePaused:    sadefs.MustEncodeValue(true, enumspb.INDEXED_VALUE_TYPE_BOOL),
		}}},
		{ScheduleId: "without-custom", SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			sadefs.TemporalSchedulePaused: sadefs.MustEncodeValue(true, enumspb.INDEXED_VALUE_TYPE_BOOL),
		}}},
	}, NextPageToken: []byte("opaque")}}
	s.T().Cleanup(testhooks.Set(wh.testHooks, testhooks.FrontendSchedule, testhooks.ScheduleFrontend(frontend), testhooks.GlobalScope))
	s.mockNamespaceCache.EXPECT().GetNamespaceID(s.testNamespace).Return(s.testNamespaceID, nil)
	response, err := wh.ListSchedules(s.T().Context(), &workflowservice.ListSchedulesRequest{Namespace: s.testNamespace.String()})
	s.NoError(err)
	s.Equal(map[string]*commonpb.Payload{"ScheduleId": custom}, response.Schedules[0].SearchAttributes.IndexedFields)
	s.Nil(response.Schedules[1].SearchAttributes)
	s.Equal([]byte("opaque"), response.NextPageToken)
	s.Equal(1, frontend.calls)
}
