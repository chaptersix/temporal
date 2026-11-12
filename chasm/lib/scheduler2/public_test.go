package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func publicTree(t *testing.T, request *workflowservice.CreateScheduleRequest) *treeHarness {
	t.Helper()
	request.Schedule.Action = &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "inert", WorkflowType: &commonpb.WorkflowType{Name: "inert"}}}}
	h := newTreeHarness(t)
	h.now = generationStart().UnixNano()
	h.resetBackend()
	h.node = chasm.NewEmptyTree(h.registry, h.backend, chasm.DefaultPathEncoder, testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly), metrics.NoopMetricsHandler)
	s, err := NewPublicSchedule(chasm.NewMutableContext(t.Context(), h.node), request)
	require.NoError(t, err)
	require.NoError(t, h.node.SetRootComponent(s))
	h.commit(true)
	return h
}

func TestPublicPausedDefinitionRoundTripAndOwnership(t *testing.T) {
	t.Parallel()
	request := &workflowservice.CreateScheduleRequest{Schedule: &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{}, State: &schedulepb.ScheduleState{Paused: true, Notes: "original"},
	}, Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{"custom": payloads.EncodeString("value").Payloads[0]}}}
	h := publicTree(t, request)
	request.Schedule.State.Notes = "caller mutation"
	request.Memo.Fields["custom"].Data = []byte("caller mutation")
	s, ctx := h.root(false)
	description, err := s.DescribePublic(ctx)
	require.NoError(t, err)
	require.Equal(t, "original", description.Schedule.State.Notes)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, description.Schedule.Policies.OverlapPolicy)
	require.Equal(t, generationStart(), description.Info.CreateTime.AsTime())
	require.Zero(t, description.Info.ActionCount)
	require.Equal(t, []byte("incarnation:1"), description.ConflictToken)
	require.True(t, proto.Equal(payloads.EncodeString("value").Payloads[0], description.Memo.Fields["custom"]))
	description.Schedule.State.Notes = "reader mutation"
	description.Memo.Fields["custom"].Data = []byte("reader mutation")
	h.reload()
	s, ctx = h.root(false)
	description, err = s.DescribePublic(ctx)
	require.NoError(t, err)
	require.Equal(t, "original", description.Schedule.State.Notes)
	require.True(t, proto.Equal(payloads.EncodeString("value").Payloads[0], description.Memo.Fields["custom"]))
	s, ctx = h.root(true)
	require.NoError(t, s.Close(ctx.(chasm.MutableContext)))
	h.commit(true)
	s, ctx = h.root(false)
	_, err = s.DescribePublic(ctx)
	require.ErrorContains(t, err, "closed")
}

func TestPublicVisibilityAvoidsDefinitionPayload(t *testing.T) {
	t.Parallel()
	h := publicTree(t, &workflowservice.CreateScheduleRequest{Schedule: &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{}, State: &schedulepb.ScheduleState{Paused: true, Notes: "compact"},
	}})
	require.NotNil(t, h.durable["PublicSchedule"].Data)
	h.durable["PublicSchedule"].Data.Data = []byte{0xff}
	h.reload()
	s, ctx := h.root(false)
	info := s.Memo(ctx).(*schedulepb.ScheduleListInfo)
	require.Equal(t, "compact", info.Notes)
	require.True(t, info.Paused)
	require.NotContains(t, h.node.MaterializedPathsForTest(), "PublicSchedule")
	require.NotContains(t, h.node.MaterializedPathsForTest(), "PublicInfo")
	info.Notes = "changed"
	require.Equal(t, "compact", s.Memo(ctx).(*schedulepb.ScheduleListInfo).Notes)
}

func TestPublicProfileRejectsUnwiredWork(t *testing.T) {
	t.Parallel()
	for _, request := range []*workflowservice.CreateScheduleRequest{
		{},
		{Schedule: &schedulepb.Schedule{}},
		{Schedule: &schedulepb.Schedule{State: &schedulepb.ScheduleState{Paused: true}, Spec: generationSpec(generationStart(), generationStart(), time.Second)}},
		{Schedule: &schedulepb.Schedule{State: &schedulepb.ScheduleState{Paused: true}}, InitialPatch: &schedulepb.SchedulePatch{Unpause: "start"}},
	} {
		if request.Schedule != nil {
			request.Schedule.Action = &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "inert"}}}
		}
		h := newTreeHarness(t)
		before := cloneNodes(h.durable)
		_, ctx := h.root(true)
		_, err := NewPublicSchedule(ctx.(chasm.MutableContext), request)
		require.Error(t, err)
		h.commit(true)
		// No new root or DATA nodes can be published by rejected creation.
		require.Len(t, h.durable, len(before))
		require.NotContains(t, h.durable, "PublicSchedule")
	}
}

func TestPublicCatchupWindowDefaults(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name               string
		supplied, expected time.Duration
	}{
		{"default", 0, 365 * 24 * time.Hour},
		{"minimum", time.Second, 10 * time.Second},
		{"explicit", time.Hour, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := publicTree(t, &workflowservice.CreateScheduleRequest{Schedule: &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, State: &schedulepb.ScheduleState{Paused: true}, Policies: &schedulepb.SchedulePolicies{CatchupWindow: durationpb.New(tc.supplied)}}})
			s, ctx := h.root(false)
			got, err := s.DescribePublic(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got.Schedule.Policies.CatchupWindow.AsDuration())
		})
	}
}
