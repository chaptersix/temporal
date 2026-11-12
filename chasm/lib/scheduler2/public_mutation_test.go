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
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/types/known/durationpb"
)

func executingPublicTree(t *testing.T, patch *schedulepb.SchedulePatch) *treeHarness {
	t.Helper()
	h := newTreeHarness(t)
	h.now = generationStart().UnixNano()
	h.resetBackend()
	h.node = chasm.NewEmptyTree(h.registry, h.backend, chasm.DefaultPathEncoder, testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly), metrics.NoopMetricsHandler)
	request := &workflowservice.CreateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: &schedulepb.Schedule{
		Spec:   generationSpec(generationStart(), generationStart().Add(time.Hour), time.Minute),
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "action", WorkflowType: &commonpb.WorkflowType{Name: "workflow"}}}},
	}, InitialPatch: patch}
	s, err := newExecutingPublicSchedule(chasm.NewMutableContext(t.Context(), h.node), request, schedulerconfig.DefaultTweakables)
	require.NoError(t, err)
	require.NoError(t, h.node.SetRootComponent(s))
	h.commit(true)
	return h
}

func TestExecutingPublicCreationInitialPatch(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, &schedulepb.SchedulePatch{Pause: "initial pause", TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{}})
	s, ctx := h.root(false)
	require.True(t, s.HasGenerator)
	require.True(t, s.Paused)
	require.Len(t, s.Manuals, 1)
	require.EqualValues(t, 1, s.PublicRevision)
	require.Equal(t, "initial pause", s.PublicList.Get(ctx).Notes)
	require.Equal(t, "initial pause", s.PublicSchedule.Get(ctx).State.Notes)
	require.Equal(t, int64(schedulerconfig.DefaultTweakables.IdleTime), s.IdleDuration)
}

func TestPublicMutationTokensAndDefinitionRevision(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, ctx := h.root(true)
	originalRevision := s.Revision
	require.NoError(t, s.PatchPublic(ctx.(chasm.MutableContext), &schedulepb.SchedulePatch{Pause: "pause"}))
	h.commit(true)
	s, ctx = h.root(false)
	require.EqualValues(t, 2, s.PublicRevision)
	require.Equal(t, originalRevision, s.Revision)
	definition := common.CloneProto(s.PublicSchedule.Get(ctx))
	definition.State.Notes = "replaced"
	request := &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: definition, ConflictToken: publicConflictToken(s.Incarnation, 1)}
	s, ctx = h.root(true)
	require.ErrorContains(t, s.UpdatePublic(ctx.(chasm.MutableContext), request, schedulerconfig.DefaultTweakables), "mismatched conflict token")
	h.reload()
	s, ctx = h.root(true)
	request.ConflictToken = publicConflictToken(s.Incarnation, 2)
	require.NoError(t, s.UpdatePublic(ctx.(chasm.MutableContext), request, schedulerconfig.DefaultTweakables))
	h.commit(true)
	s, ctx = h.root(false)
	require.EqualValues(t, 3, s.PublicRevision)
	require.Equal(t, originalRevision+1, s.Revision)
	require.Equal(t, "replaced", s.PublicList.Get(ctx).Notes)
	description, err := s.DescribePublic(ctx)
	require.NoError(t, err)
	require.Equal(t, publicConflictToken(s.Incarnation, 3), description.ConflictToken)
}

func TestPublicMutationCounterExhaustion(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, _ := h.root(true)
	s.PublicRevision = math.MaxUint64
	h.commit(true)
	s, ctx := h.root(true)
	require.ErrorContains(t, s.PatchPublic(ctx.(chasm.MutableContext), &schedulepb.SchedulePatch{Pause: "must not apply"}), "exhausted")
	require.False(t, s.Paused)
	require.Empty(t, s.PublicSchedule.Get(ctx).State.Notes)
}

// The option-aware engine exercises actual CHASM request-ID state and rolls back
// the tree on a failed transition or failed publication; it is not History I/O.
func TestPublicMutationTransactionRollbackAndDedup(t *testing.T) {
	t.Parallel()
	e := newManualCommandEngine(t)
	e.h = executingPublicTree(t, nil)
	applyPatch := func(id string, patch *schedulepb.SchedulePatch) error {
		_, _, err := chasm.UpdateComponent(e.ctx, e.ref, func(s *Scheduler, cc chasm.MutableContext, patch *schedulepb.SchedulePatch) (chasm.NoValue, error) {
			return nil, s.PatchPublic(cc, patch)
		}, patch, chasm.WithRequestID(id))
		return err
	}
	before := cloneNodes(e.h.durable)
	// Pause and manual ingress succeed in memory before the malformed final range.
	bad := &schedulepb.SchedulePatch{Pause: "must roll back", TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{}, BackfillRequest: []*schedulepb.BackfillRequest{{}}}
	require.Error(t, applyPatch("patch", bad))
	require.Equal(t, before, e.h.durable)
	used, err := e.h.node.HasRequestID("patch", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
	good := &schedulepb.SchedulePatch{Pause: "accepted", TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{}}
	e.failPublication = true
	require.ErrorIs(t, applyPatch("patch", good), errManualPublication)
	require.Equal(t, before, e.h.durable)
	e.failPublication = false
	require.NoError(t, applyPatch("patch", good))
	committed := cloneNodes(e.h.durable)
	require.ErrorIs(t, applyPatch("patch", bad), chasm.ErrRequestIDAlreadyUsed)
	require.Equal(t, committed, e.h.durable)
	s, ctx := e.h.root(false)
	require.EqualValues(t, 2, s.PublicRevision)
	require.Len(t, s.Manuals, 1)
	definition := common.CloneProto(s.PublicSchedule.Get(ctx))
	definition.State.Notes = "must not publish"
	update := &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: definition,
		SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{"TemporalNamespaceDivision": {Data: []byte("reserved")}}}}
	_, _, err = chasm.UpdateComponent(e.ctx, e.ref, func(s *Scheduler, cc chasm.MutableContext, r *workflowservice.UpdateScheduleRequest) (chasm.NoValue, error) {
		return nil, s.UpdatePublic(cc, r, schedulerconfig.DefaultTweakables)
	}, update, chasm.WithRequestID("update"))
	require.Error(t, err)
	require.Equal(t, committed, e.h.durable, "late visibility rejection must roll back generation and public data")
	used, err = e.h.node.HasRequestID("update", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
}

func TestPublicDescribeUsesFrozenConfiguredCatchup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		requested time.Duration
		want      time.Duration
	}{
		{"configured default", 0, time.Hour},
		{"configured minimum", time.Second, 2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := executingPublicTree(t, nil)
			s, ctx := h.root(true)
			definition := common.CloneProto(s.PublicSchedule.Get(ctx))
			definition.Policies.CatchupWindow = durationpb.New(tc.requested)
			tweaks := schedulerconfig.DefaultTweakables
			tweaks.DefaultCatchupWindow, tweaks.MinCatchupWindow = time.Hour, 2*time.Minute
			require.NoError(t, s.UpdatePublic(ctx.(chasm.MutableContext), &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: definition}, tweaks))
			h.commit(true)
			h.reload()
			s, ctx = h.root(false)
			description, err := s.DescribePublic(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.want, description.Schedule.Policies.CatchupWindow.AsDuration())
		})
	}
}

func TestPublicInitialClockConfigurationAppliedOnce(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	h.now = generationStart().UnixNano()
	h.resetBackend()
	calls := 0
	configuredAt := generationStart().Add(time.Hour)
	h.backend.HandleSetTimeSkippingConfig = func(*commonpb.TimeSkippingConfig) { calls++; h.now = configuredAt.UnixNano() }
	h.node = chasm.NewEmptyTree(h.registry, h.backend, chasm.DefaultPathEncoder, testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly), metrics.NoopMetricsHandler)
	request := &workflowservice.CreateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{}, Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "workflow"}}},
	}, InitialPatch: &schedulepb.SchedulePatch{Pause: "initial", TimeSkippingConfig: &commonpb.TimeSkippingConfig{}}}
	ctx := chasm.NewMutableContext(t.Context(), h.node)
	s, err := newExecutingPublicSchedule(ctx, request, schedulerconfig.DefaultTweakables)
	require.NoError(t, err)
	require.NoError(t, h.node.SetRootComponent(s))
	require.Equal(t, 1, calls)
	// CHASM captures the transaction clock when constructing the context.
	require.Equal(t, generationStart(), s.PublicInfo.Get(ctx).CreateTime.AsTime())
	require.True(t, s.Paused)
	require.NotNil(t, request.InitialPatch.TimeSkippingConfig, "caller-owned initial patch remains intact")
}

func TestPublicPatchGenerationTokenExhaustion(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, ctx := h.root(true)
	s.Generator.Get(ctx).Token = math.MaxUint64
	h.commit(true)
	before := cloneNodes(h.durable)
	s, ctx = h.root(true)
	require.ErrorContains(t, s.PatchPublic(ctx.(chasm.MutableContext), &schedulepb.SchedulePatch{Pause: "must not apply", TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{}}), "generation token exhausted")
	require.False(t, s.Paused)
	require.Empty(t, s.Manuals)
	h.commit(true)
	requireCallbackNodesEqual(t, before, h.durable)
}
