package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	oldscheduler "go.temporal.io/server/chasm/lib/scheduler"
	oldschedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type observedScheduleFrontend struct {
	testhooks.ScheduleFrontend
	creates atomic.Int64
}

func (f *observedScheduleFrontend) CreateSchedule(ctx context.Context, id namespace.ID, request *workflowservice.CreateScheduleRequest) (*workflowservice.CreateScheduleResponse, error) {
	f.creates.Add(1)
	return f.ScheduleFrontend.CreateSchedule(ctx, id, request)
}

// This is a public persistence/visibility slice, not the shared execution suite.
func TestSchedule2PublicPausedProfile(t *testing.T) {
	var adapter *observedScheduleFrontend
	var archetypeID uint32
	backend := scheduleBackend{name: "scheduler2", context: func(ctx context.Context) context.Context { return ctx }, options: []testcore.TestOption{
		testcore.WithScheduleFrontend(func(engine chasm.Engine, visibility chasm.VisibilityManager, registry *chasm.Registry) (testhooks.ScheduleFrontend, error) {
			frontend, err := scheduler2.NewPublicTestFrontend(engine, visibility, registry)
			if err != nil {
				return nil, err
			}
			archetypeID, _ = registry.ComponentIDFor(&scheduler2.Scheduler{})
			adapter = &observedScheduleFrontend{ScheduleFrontend: frontend}
			return adapter, nil
		}, scheduler2.NewNilLibrary()),
	}}
	env := newScheduleEnv(t, []scheduleBackend{backend})
	ctx := t.Context()
	client := env.FrontendClient()
	sid := "scheduler2-public-" + uuid.NewString()
	request := &workflowservice.CreateScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid, RequestId: uuid.NewString(),
		Schedule: &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, Action: startWorkflowAction(env, "inert-"+sid, "inert"), State: &schedulepb.ScheduleState{Paused: true, Notes: "original"}},
		Memo:     &commonpb.Memo{Fields: map[string]*commonpb.Payload{"custom": payloads.EncodeString("memo").Payloads[0]}},
	}
	request.Schedule.Action.GetStartWorkflow().UserMetadata = &sdkpb.UserMetadata{Summary: payloads.EncodeString("action summary").Payloads[0], Details: payloads.EncodeString("action details").Payloads[0]}
	created, err := client.CreateSchedule(ctx, request)
	require.NoError(t, err)
	require.NotEmpty(t, created.ConflictToken)
	describe := func() (*workflowservice.DescribeScheduleResponse, error) {
		return client.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{Namespace: request.Namespace, ScheduleId: sid})
	}
	description, err := describe()
	require.NoError(t, err)
	require.Equal(t, created.ConflictToken, description.ConflictToken)
	require.Equal(t, "original", description.Schedule.State.Notes)
	require.True(t, description.Schedule.State.Paused)
	require.True(t, proto.Equal(request.Schedule.Action, description.Schedule.Action))
	require.True(t, proto.Equal(request.Memo, description.Memo))
	require.Positive(t, description.Info.StateSizeBytes)
	require.Empty(t, description.Info.FutureActionTimes)
	require.Zero(t, description.Info.ActionCount)

	// Rejected creation must not leave a current execution in either backend.
	for _, tc := range []struct {
		name    string
		change  func(*workflowservice.CreateScheduleRequest)
		invalid bool
	}{
		{"missing-action", func(r *workflowservice.CreateScheduleRequest) { r.Schedule.Action = nil }, true},
		{"unpaused", func(r *workflowservice.CreateScheduleRequest) { r.Schedule.State.Paused = false }, false},
		{"interval", func(r *workflowservice.CreateScheduleRequest) {
			r.Schedule.Spec.Interval = []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}
		}, false},
		{"initial-patch", func(r *workflowservice.CreateScheduleRequest) {
			r.InitialPatch = &schedulepb.SchedulePatch{Unpause: "start"}
		}, false},
	} {
		r := common.CloneProto(request)
		r.ScheduleId, r.RequestId = sid+"-"+tc.name, uuid.NewString()
		tc.change(r)
		_, err := client.CreateSchedule(ctx, r)
		if tc.invalid {
			var invalid *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalid, tc.name)
		} else {
			var unsupported *serviceerror.Unimplemented
			require.ErrorAs(t, err, &unsupported, tc.name)
		}
		_, err = client.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{Namespace: request.Namespace, ScheduleId: r.ScheduleId})
		var missing *serviceerror.NotFound
		require.ErrorAs(t, err, &missing, tc.name)
	}

	changed := common.CloneProto(request)
	changed.Schedule.State.Notes = "ignored replay body"
	replayed, err := client.CreateSchedule(ctx, changed)
	require.NoError(t, err)
	require.Equal(t, created.ConflictToken, replayed.ConflictToken)
	description, err = describe()
	require.NoError(t, err)
	require.Equal(t, "original", description.Schedule.State.Notes)
	changed.RequestId = uuid.NewString()
	_, err = client.CreateSchedule(ctx, changed)
	var duplicate *serviceerror.WorkflowExecutionAlreadyStarted
	require.ErrorAs(t, err, &duplicate)

	engineContext, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)
	key := chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: sid}
	runID, err := chasm.ReadComponent(engineContext, chasm.NewComponentRef[*scheduler2.Scheduler](key),
		func(s *scheduler2.Scheduler, cc chasm.Context, _ struct{}) (string, error) {
			require.Equal(t, cc.ExecutionKey().RunID, s.Incarnation)
			return s.Incarnation, nil
		}, struct{}{})
	require.NoError(t, err)
	require.NotEmpty(t, runID)
	require.NotEqual(t, chasm.SchedulerArchetypeID, archetypeID)
	persisted, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{Namespace: request.Namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: sid, RunId: runID}, ArchetypeId: archetypeID})
	require.NoError(t, err)
	require.NotNil(t, persisted.DatabaseMutableState)
	storedID, err := chasm.ExecutionArchetypeID(persisted.DatabaseMutableState)
	require.NoError(t, err)
	require.Equal(t, archetypeID, storedID)
	for _, old := range []struct {
		id        string
		archetype uint32
	}{{sid, chasm.SchedulerArchetypeID}, {scheduler.WorkflowIDPrefix + sid, chasm.WorkflowArchetypeID}} {
		_, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{Namespace: request.Namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: old.id}, ArchetypeId: old.archetype})
		var missing *serviceerror.NotFound
		require.ErrorAs(t, err, &missing, "no old scheduler or sentinel may be created")
	}

	// Seed a real old-archetype row with the same business ID. Public queries and
	// deletion must continue to address scheduler2 alone.
	_, err = chasm.StartExecution(engineContext, key, oldscheduler.CreateScheduler, &oldschedulerpb.CreateScheduleRequest{NamespaceId: env.NamespaceID().String(), FrontendRequest: common.CloneProto(request)})
	require.NoError(t, err)
	await.Require(ctx, t, func(t *await.T) {
		count, err := chasm.CountExecutions[*oldscheduler.Scheduler](engineContext, &chasm.CountExecutionsRequest{NamespaceName: request.Namespace, Query: "ExecutionStatus = 'Running'"})
		require.NoError(t, err)
		require.EqualValues(t, 1, count.Count)
	}, 20*time.Second, 100*time.Millisecond)

	query := "ScheduleId = '" + sid + "'"
	waitCount := func(want int64) {
		await.Require(ctx, t, func(t *await.T) {
			count, err := client.CountSchedules(t.Context(), &workflowservice.CountSchedulesRequest{Namespace: request.Namespace, Query: query})
			require.NoError(t, err)
			require.Equal(t, want, count.Count)
		}, 20*time.Second, 100*time.Millisecond)
	}
	waitCount(1)
	listed, err := client.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{Namespace: request.Namespace, Query: query, MaximumPageSize: 1})
	require.NoError(t, err)
	require.Len(t, listed.Schedules, 1)
	require.Equal(t, sid, listed.Schedules[0].ScheduleId)
	require.Equal(t, "original", listed.Schedules[0].Info.Notes)
	require.True(t, proto.Equal(request.Memo, listed.Schedules[0].Memo))

	second := common.CloneProto(request)
	second.ScheduleId, second.RequestId = sid+"-second", uuid.NewString()
	_, err = client.CreateSchedule(ctx, second)
	require.NoError(t, err)
	pageQuery := "ScheduleId IN ('" + sid + "', '" + second.ScheduleId + "')"
	await.Require(ctx, t, func(t *await.T) {
		count, err := client.CountSchedules(t.Context(), &workflowservice.CountSchedulesRequest{Namespace: request.Namespace, Query: pageQuery})
		require.NoError(t, err)
		require.EqualValues(t, 2, count.Count)
	}, 20*time.Second, 100*time.Millisecond)
	var token []byte
	var ids []string
	for pages := 0; ; pages++ {
		require.Less(t, pages, 4, "pagination must terminate")
		page, err := client.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{Namespace: request.Namespace, Query: pageQuery, MaximumPageSize: 1, NextPageToken: token})
		require.NoError(t, err)
		require.LessOrEqual(t, len(page.Schedules), 1)
		for _, entry := range page.Schedules {
			ids = append(ids, entry.ScheduleId)
		}
		token = page.NextPageToken
		if len(token) == 0 {
			break
		}
	}
	require.ElementsMatch(t, []string{sid, second.ScheduleId}, ids)
	negative, err := client.CountSchedules(ctx, &workflowservice.CountSchedulesRequest{Namespace: request.Namespace, Query: pageQuery + " AND ScheduleId != '" + sid + "'"})
	require.NoError(t, err)
	require.EqualValues(t, 1, negative.Count)

	for _, predicate := range []string{"NamespaceDivision = 'TemporalScheduler'", "TemporalNamespaceDivision = 'TemporalScheduler'", "NOT (NamespaceDivision = 'other')"} {
		_, err := client.CountSchedules(ctx, &workflowservice.CountSchedulesRequest{Namespace: request.Namespace, Query: predicate})
		var invalid *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalid)
		_, err = client.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{Namespace: request.Namespace, Query: predicate})
		require.ErrorAs(t, err, &invalid)
	}
	_, err = client.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{Namespace: request.Namespace, Query: query, NextPageToken: []byte("invalid token")})
	require.Error(t, err)

	env.SetOnAuthorize(func(_ context.Context, _ *authorization.Claims, target *authorization.CallTarget) (authorization.Result, error) {
		if create, ok := target.Request.(*workflowservice.CreateScheduleRequest); ok && create.ScheduleId == "denied" {
			return authorization.Result{Decision: authorization.DecisionDeny}, nil
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	})
	before := adapter.creates.Load()
	denied := common.CloneProto(request)
	denied.ScheduleId, denied.RequestId = "denied", uuid.NewString()
	_, err = client.CreateSchedule(ctx, denied)
	var permission *serviceerror.PermissionDenied
	require.ErrorAs(t, err, &permission)
	require.Equal(t, before, adapter.creates.Load(), "authorization must precede adapter dispatch")

	_, err = client.DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{Namespace: request.Namespace, ScheduleId: sid})
	require.NoError(t, err)
	_, err = describe()
	var missing *serviceerror.NotFound
	require.ErrorAs(t, err, &missing)
	_, err = client.DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{Namespace: request.Namespace, ScheduleId: sid})
	require.ErrorAs(t, err, &missing)
	waitCount(0)
	request.RequestId = uuid.NewString()
	recreated, err := client.CreateSchedule(ctx, request)
	require.NoError(t, err)
	require.NotEqual(t, created.ConflictToken, recreated.ConflictToken)
	key.RunID = runID
	closed, err := chasm.ReadComponent(engineContext, chasm.NewComponentRef[*scheduler2.Scheduler](key), func(s *scheduler2.Scheduler, _ chasm.Context, _ struct{}) (bool, error) { return s.Closed, nil }, struct{}{})
	require.NoError(t, err)
	require.True(t, closed, "old run remains fenced after same-ID recreation")
	waitCount(1)
	oldClosed, err := chasm.ReadComponent(engineContext, chasm.NewComponentRef[*oldscheduler.Scheduler](chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: sid}), func(s *oldscheduler.Scheduler, _ chasm.Context, _ struct{}) (bool, error) { return s.Closed, nil }, struct{}{})
	require.NoError(t, err)
	require.False(t, oldClosed, "scheduler2 deletion must not close the colliding old component")
	_, _, err = chasm.UpdateComponent(engineContext, chasm.NewComponentRef[*oldscheduler.Scheduler](chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: sid}), (*oldscheduler.Scheduler).Delete, &oldschedulerpb.DeleteScheduleRequest{})
	require.NoError(t, err)

	// Namespace-defined aliases take precedence over the synthetic business ID.
	_, err = env.OperatorClient().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{Namespace: request.Namespace, SearchAttributes: map[string]enumspb.IndexedValueType{"ScheduleId": enumspb.INDEXED_VALUE_TYPE_KEYWORD}})
	require.NoError(t, err)
	custom := common.CloneProto(request)
	custom.ScheduleId, custom.RequestId = sid+"-custom", uuid.NewString()
	custom.SearchAttributes = &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{"ScheduleId": sadefs.MustEncodeValue("custom-value", enumspb.INDEXED_VALUE_TYPE_KEYWORD)}}
	_, err = client.CreateSchedule(ctx, custom)
	require.NoError(t, err)
	await.Require(ctx, t, func(t *await.T) {
		count, err := client.CountSchedules(t.Context(), &workflowservice.CountSchedulesRequest{Namespace: request.Namespace, Query: "ScheduleId = 'custom-value'"})
		require.NoError(t, err)
		require.EqualValues(t, 1, count.Count)
	}, 20*time.Second, 100*time.Millisecond)
	customList, err := client.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{Namespace: request.Namespace, Query: "ScheduleId = 'custom-value'"})
	require.NoError(t, err)
	require.Len(t, customList.Schedules, 1)
	require.Equal(t, custom.ScheduleId, customList.Schedules[0].ScheduleId)
	customDescription, err := client.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{Namespace: request.Namespace, ScheduleId: custom.ScheduleId})
	require.NoError(t, err)
	for _, attributes := range []*commonpb.SearchAttributes{customDescription.SearchAttributes, customList.Schedules[0].SearchAttributes} {
		require.Len(t, attributes.GetIndexedFields(), 1)
		value, err := sadefs.DecodeValue(attributes.IndexedFields["ScheduleId"], enumspb.INDEXED_VALUE_TYPE_KEYWORD, false)
		require.NoError(t, err)
		require.Equal(t, "custom-value", value)
	}
	byBusinessID, err := client.CountSchedules(ctx, &workflowservice.CountSchedulesRequest{Namespace: request.Namespace, Query: "ScheduleId = '" + custom.ScheduleId + "'"})
	require.NoError(t, err)
	require.Zero(t, byBusinessID.Count)

}
