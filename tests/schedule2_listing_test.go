package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type listingBeforeActionClient struct {
	workflowservice.WorkflowServiceClient
}

func (*listingBeforeActionClient) StartWorkflowExecution(context.Context, *workflowservice.StartWorkflowExecutionRequest, ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
	return nil, serviceerror.NewUnavailable("test holds action starts until initial listing is observed")
}

func TestSchedule2ListBeforeActionRun(t *testing.T) {
	t.Parallel()
	// The original V1 fixture disables its scheduler worker. This backend has no
	// scheduler workflow; withholding action starts establishes the public pre-run condition.
	backend := scheduleBackend{name: "scheduler2-list-before-action", context: schedule2Backend.context, assertPersisted: assertSchedule2Persisted, options: []testcore.TestOption{
		testcore.WithScheduleFrontend(func(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
			runtime.FrontendClient = &listingBeforeActionClient{WorkflowServiceClient: runtime.FrontendClient}
			return scheduler2.NewPublicExecutionTestFrontend(runtime)
		}, scheduler2.NewNilLibrary()),
	}}
	env := newScheduleEnv(t, []scheduleBackend{backend})
	sid := "sched-test-list-before-run"
	schedule := &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(3 * time.Second)}}}, Action: startWorkflowAction(env, "sched-test-list-before-run-wf", "sched-test-list-before-run-wt")}
	startTime := time.Now()
	createSchedule(t.Context(), t, env, sid, schedule)
	assertScheduleCallbackBackend(t, env, backend, sid)
	entry := getScheduleEntryFromVisibility(t, env, sid, backend.context, nil)
	require.NotNil(t, entry.Info)
	require.True(t, proto.Equal(schedule.Spec, entry.Info.Spec))
	require.Equal(t, "sched-test-list-before-run-wt", entry.Info.WorkflowType.Name)
	require.False(t, entry.Info.Paused)
	require.Greater(t, len(entry.Info.FutureActionTimes), 1)
	require.True(t, entry.Info.FutureActionTimes[0].AsTime().After(startTime))
	description, err := env.FrontendClient().DescribeSchedule(t.Context(), &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
	require.NoError(t, err)
	require.Zero(t, description.Info.ActionCount)
	require.Empty(t, description.Info.RecentActions)
}

func TestSchedule2ListingWithSearchAttributes(t *testing.T) {
	t.Parallel()
	env := newScheduleEnv(t, []scheduleBackend{schedule2Backend})
	ctx := t.Context()
	sid := "test-schedule-" + uuid.NewString()
	request := &workflowservice.CreateScheduleRequest{Namespace: env.Namespace().String(), RequestId: uuid.NewString(), ScheduleId: sid, Schedule: &schedulepb.Schedule{
		Action: startWorkflowAction(env, "test-schedule-"+uuid.NewString(), "test-workflow-type"), Policies: &schedulepb.SchedulePolicies{}, Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}},
	}}
	_, err := env.FrontendClient().CreateSchedule(ctx, request)
	require.NoError(t, err)
	assertScheduleCallbackBackend(t, env, schedule2Backend, sid)
	list := &workflowservice.ListSchedulesRequest{Namespace: env.Namespace().String(), MaximumPageSize: 1, Query: fmt.Sprintf(`%s = "%s"`, sadefs.ScheduleID, sid)}
	await.Require(ctx, t, func(at *await.T) {
		response, err := env.FrontendClient().ListSchedules(at.Context(), list)
		require.NoError(at, err)
		require.Len(at, response.Schedules, 1)
		require.Equal(at, sid, response.Schedules[0].ScheduleId)
	}, 30*time.Second, time.Second)
	list.Query = fmt.Sprintf(`%s IN ("%s", "foo", "bar")`, sadefs.ScheduleID, sid)
	response, err := env.FrontendClient().ListSchedules(ctx, list)
	require.NoError(t, err)
	require.Len(t, response.Schedules, 1)
	require.Equal(t, sid, response.Schedules[0].ScheduleId)
	_, err = env.OperatorClient().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{Namespace: env.Namespace().String(), SearchAttributes: map[string]enumspb.IndexedValueType{sadefs.ScheduleID: enumspb.INDEXED_VALUE_TYPE_KEYWORD}})
	require.NoError(t, err)
	await.Require(ctx, t, func(at *await.T) {
		attributes, err := env.OperatorClient().ListSearchAttributes(at.Context(), &operatorservice.ListSearchAttributesRequest{Namespace: env.Namespace().String()})
		require.NoError(at, err)
		require.Contains(at, attributes.CustomAttributes, sadefs.ScheduleID)
	}, 30*time.Second, time.Second)
	customID := "test-schedule-" + uuid.NewString()
	const customValue = "testScheduleId"
	request.RequestId, request.ScheduleId = uuid.NewString(), customID
	request.SearchAttributes = &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{sadefs.ScheduleID: sadefs.MustEncodeValue(customValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD)}}
	_, err = env.FrontendClient().CreateSchedule(ctx, request)
	require.NoError(t, err)
	assertScheduleCallbackBackend(t, env, schedule2Backend, customID)
	list.Query = fmt.Sprintf(`%s = "%s"`, sadefs.ScheduleID, customValue)
	await.Require(ctx, t, func(at *await.T) {
		response, err := env.FrontendClient().ListSchedules(at.Context(), list)
		require.NoError(at, err)
		require.Len(at, response.Schedules, 1)
		require.Equal(at, customID, response.Schedules[0].ScheduleId)
	}, 30*time.Second, time.Second)
	list.Query = fmt.Sprintf(`%s IN ("%s", "foo", "bar")`, sadefs.ScheduleID, customValue)
	response, err = env.FrontendClient().ListSchedules(ctx, list)
	require.NoError(t, err)
	require.Len(t, response.Schedules, 1)
	require.Equal(t, customID, response.Schedules[0].ScheduleId)
}

func TestSchedule2ReadsWithLegacyCreationDisabled(t *testing.T) {
	t.Parallel()
	env := newScheduleEnv(t, []scheduleBackend{schedule2Backend},
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, false),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerMigration, false))
	id := testcore.RandomizeStr("read-disabled-legacy")
	createSchedule(t.Context(), t, env, id, &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, Action: startWorkflowAction(env, id+"-workflow", "unused"), State: &schedulepb.ScheduleState{Paused: true}})
	assertScheduleCallbackBackend(t, env, schedule2Backend, id)
	response, err := env.FrontendClient().DescribeSchedule(t.Context(), &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: id})
	require.NoError(t, err)
	require.NotNil(t, response.Schedule)
	require.True(t, response.Schedule.State.Paused)
	entry := getScheduleEntryFromVisibility(t, env, id, schedule2Backend.context, nil)
	require.Equal(t, id, entry.ScheduleId)
}
