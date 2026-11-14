package scheduler2

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func stagedPublicTestRequest() *workflowservice.CreateScheduleRequest {
	return &workflowservice.CreateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: &schedulepb.Schedule{
		Spec:   &schedulepb.ScheduleSpec{TimezoneName: "Fixture/Zone", Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Minute)}, {Interval: durationpb.New(time.Hour)}}},
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "workflow", WorkflowType: &commonpb.WorkflowType{Name: "type"}}}},
		State:  &schedulepb.ScheduleState{LimitedActions: true, RemainingActions: 2, Notes: "notes"},
	}, InitialPatch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{}}}
}
func TestStagedPublicDetachedCanonicalEnvelopeAndFence(t *testing.T) {
	t.Parallel()
	service, wall, reads, _ := preparationServiceFixture(t)
	request := stagedPublicTestRequest()
	original := common.CloneProto(request)
	stage, err := service.stagePublicCreate(request, "namespace-id", 1)
	require.NoError(t, err)
	require.True(t, proto.Equal(original, request))
	require.Equal(t, 1, *reads)
	require.Len(t, stage.definition.static.Spec.Interval, 1)
	require.Len(t, stage.definition.schedule.Spec.Interval, 2)
	require.Equal(t, int64(1), stage.definition.definition.revision)
	require.NoError(t, stage.definition.validateBinding("namespace-id", "schedule", 0))
	wall.Advance(24 * time.Hour)
	for _, binding := range []struct {
		namespace, id string
		revision      int64
	}{{"other", "schedule", 0}, {"namespace-id", "other", 0}, {"namespace-id", "schedule", 1}} {
		require.Error(t, stage.definition.validateBinding(binding.namespace, binding.id, binding.revision))
	}
	require.Equal(t, 1, *reads, "fixed binding must not resolve source even after cache expiry")
	request.Schedule.Action.GetStartWorkflow().WorkflowType.Name = "changed"
	request.Schedule.State.Notes = "changed"
	request.InitialPatch.TriggerImmediately = nil
	require.Equal(t, "type", stage.request.Schedule.Action.GetStartWorkflow().WorkflowType.Name)
	require.Equal(t, "notes", stage.request.Schedule.State.Notes)
	require.NotNil(t, stage.request.InitialPatch.TriggerImmediately)
	updateRequest := &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: original.Schedule, ConflictToken: []byte("token")}
	update, err := service.stagePublicUpdate(updateRequest, "namespace-id", 7, 2)
	require.NoError(t, err)
	require.Equal(t, int64(8), update.definition.definition.revision)
	require.NoError(t, update.definition.validateBinding("namespace-id", "schedule", 7))
	require.Error(t, update.definition.validateBinding("namespace-id", "schedule", 8))
	updateRequest.ConflictToken[0] = 'X'
	require.Equal(t, []byte("token"), update.request.ConflictToken)
}
func TestStagedPublicRejectsInvalidIngress(t *testing.T) {
	t.Parallel()
	service, _, _, _ := preparationServiceFixture(t)
	_, err := service.stagePublicCreate(nil, "ns", 1)
	require.Error(t, err)
	for _, tc := range []struct {
		name   string
		mutate func(*workflowservice.CreateScheduleRequest)
	}{
		{"nil schedule", func(r *workflowservice.CreateScheduleRequest) { r.Schedule = nil }},
		{"nil action", func(r *workflowservice.CreateScheduleRequest) { r.Schedule.Action = nil }},
		{"negative remaining", func(r *workflowservice.CreateScheduleRequest) { r.Schedule.State.RemainingActions = -1 }},
		{"bad policy", func(r *workflowservice.CreateScheduleRequest) {
			r.Schedule.Policies = &schedulepb.SchedulePolicies{OverlapPolicy: 99}
		}},
		{"bad zone", func(r *workflowservice.CreateScheduleRequest) { r.Schedule.Spec.TimezoneName = "missing" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := stagedPublicTestRequest()
			tc.mutate(r)
			_, err := service.stagePublicCreate(r, "ns", 1)
			require.Error(t, err)
		})
	}
	_, err = service.stagePublicCreate(stagedPublicTestRequest(), "ns", -1)
	require.Error(t, err)
	for _, revision := range []int64{0, -1, math.MaxInt64} {
		_, err = service.stagePublicUpdate(&workflowservice.UpdateScheduleRequest{Schedule: stagedPublicTestRequest().Schedule}, "ns", revision, 1)
		require.Error(t, err)
	}
	request := stagedPublicTestRequest()
	request.Schedule.Spec = nil
	request.Schedule.State = nil
	request.Schedule.Policies = nil
	stage, err := service.stagePublicCreate(request, "ns", 0)
	require.NoError(t, err)
	require.NotNil(t, stage.request.Schedule.Spec)
	require.NotNil(t, stage.request.Schedule.State)
	require.NotNil(t, stage.request.Schedule.Policies)
	require.Nil(t, request.Schedule.Spec)
}
func TestStagedPublicListRemainsUnpublishedUntilWindow(t *testing.T) {
	t.Parallel()
	service, _, reads, _ := preparationServiceFixture(t)
	stage, err := service.stagePublicCreate(stagedPublicTestRequest(), "ns", 1)
	require.NoError(t, err)
	h := newTreeHarness(t)
	s, raw := h.root(true)
	ctx, err := s.reserveMemoVersion(raw.(chasm.MutableContext))
	require.NoError(t, err)
	plan, preview, err := s.prepareStagedPublicList(ctx, stage.definition, time.Unix(0, h.now), nil)
	require.NoError(t, err)
	require.Equal(t, uint32(0), plan.control.PublishedSlot)
	require.Equal(t, uint32(2), preview.TargetCount)
	s.publishListRefresh(ctx, plan)
	h.commit(true)
	s, readCtx := h.root(false)
	memo, err := s.listMemo(readCtx)
	require.NoError(t, err)
	require.Nil(t, memo)
	require.Equal(t, 1, *reads)
	finishListTest(t, h, stage.definition.definition.owner, preview.Stamp, true)
	s, readCtx = h.root(false)
	memo, err = s.listMemo(readCtx)
	require.NoError(t, err)
	require.NotNil(t, memo)
	require.Equal(t, "notes", memo.Notes)
	require.Len(t, memo.Spec.Interval, 1)
}
