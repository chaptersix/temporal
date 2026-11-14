//go:build test_dep

package scheduler2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func boundedCommandPatch(count int) *schedulepb.SchedulePatch {
	patch := &schedulepb.SchedulePatch{}
	for range count {
		patch.BackfillRequest = append(patch.BackfillRequest, &schedulepb.BackfillRequest{StartTime: timestamppb.New(generationStart()), EndTime: timestamppb.New(generationStart().Add(2 * time.Second)), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL})
	}
	return patch
}
func newBoundedRuntimeTree(t *testing.T, initial int, constructorQuanta bool) (*treeHarness, map[string]int64) {
	t.Helper()
	h := newTreeHarness(t)
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newBoundedLibrary(NewStartHandler(nil), nil)))
	h.registry = registry
	h.now = generationStart().Add(10 * time.Second).UnixNano()
	h.transition = 1
	h.physical = nil
	h.resetBackend()
	h.node = chasm.NewEmptyTree(registry, h.backend, chasm.DefaultPathEncoder, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	counts := h.node.BeginWorkInspectionForTest()
	request := &workflowservice.CreateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: &schedulepb.Schedule{
		Spec: generationSpec(generationStart(), generationStart().Add(time.Hour), time.Second), State: &schedulepb.ScheduleState{Paused: true}, Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "bounded", WorkflowType: &commonpb.WorkflowType{Name: "workflow"}}}},
	}, InitialPatch: boundedCommandPatch(initial), Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{"memo": {Data: []byte("memo")}}}, SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{"CustomKeywordField": {Data: []byte("value")}}}}
	ctx := chasm.NewMutableContext(t.Context(), h.node)
	if constructorQuanta {
		ctx = chasm.ContextWithValue(ctx, workQuantaKey{}, coordinatorTransactionQuanta)
	}
	s, err := newAddressedPublicSchedule(ctx, request, schedulerconfig.DefaultTweakables)
	require.NoError(t, err)
	require.NoError(t, h.node.SetRootComponent(s))
	h.commit(true)
	return h, counts.Snapshot()
}
func assertBoundedTaskCounts(t *testing.T, h *treeHarness, counts map[string]int64) {
	t.Helper()
	require.LessOrEqual(t, counts["task_regular_enqueues"], int64(29))
	require.LessOrEqual(t, counts["task_immediate_enqueues"], int64(103))
	require.Equal(t, counts["task_immediate_enqueues"], counts["task_immediate_attempts"])
	root := h.durable[""].Metadata.GetComponentAttributes()
	require.LessOrEqual(t, len(root.PureTasks), 3)
	require.LessOrEqual(t, len(root.SideEffectTasks), 3)
	visibility := h.durable["Visibility"].Metadata.GetComponentAttributes()
	require.Empty(t, visibility.PureTasks)
	require.LessOrEqual(t, len(visibility.SideEffectTasks), 1)
	generator := h.durable["Generator"].Metadata.GetComponentAttributes()
	require.Empty(t, generator.PureTasks)
	require.Empty(t, generator.SideEffectTasks)
	t.Logf("regular=%d immediate_enqueues=%d immediate_attempts=%d logical_root_pure=%d logical_root_side=%d visibility=%d", counts["task_regular_enqueues"], counts["task_immediate_enqueues"], counts["task_immediate_attempts"], len(root.PureTasks), len(root.SideEffectTasks), len(visibility.SideEffectTasks))
}
func TestBoundedRuntimeHundredInitialCommands(t *testing.T) {
	t.Parallel()
	for _, quanta := range []bool{false, true} {
		t.Run(fmt.Sprint(quanta), func(t *testing.T) {
			h, counts := newBoundedRuntimeTree(t, 100, quanta)
			assertBoundedTaskCounts(t, h, counts)
			s, read := h.root(false)
			require.EqualValues(t, 4, s.Visibility.Get(read).Data.TransitionCount)
			require.Len(t, s.Backfills, 100)
			if quanta {
				require.EqualValues(t, 103, counts["task_immediate_attempts"])
				require.LessOrEqual(t, s.OperationStore.OperationCount, int64(1))
			}
			// Offline decoding uses precisely the same layout, even without live handlers.
			registry := chasm.NewRegistry(log.NewNoopLogger())
			require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
			require.NoError(t, registry.Register(newBoundedNilLibrary()))
			h.registry = registry
			h.reload()
			s, _ = h.root(false)
			require.Len(t, s.Backfills, 100)
		})
	}
}
func TestBoundedRuntimeHundredPatchedCommands(t *testing.T) {
	t.Parallel()
	h, _ := newBoundedRuntimeTree(t, 0, false)
	counts := h.node.BeginWorkInspectionForTest()
	s, raw := h.root(true)
	require.NoError(t, s.PatchPublic(raw.(chasm.MutableContext), boundedCommandPatch(100)))
	h.commit(true)
	observed := counts.Snapshot()
	assertBoundedTaskCounts(t, h, observed)
	require.EqualValues(t, 102, observed["task_immediate_attempts"])
	s, _ = h.root(false)
	require.Len(t, s.Backfills, 100)
	require.LessOrEqual(t, s.OperationStore.OperationCount, int64(1))
	require.True(t, s.Coordination.Armed)
	before := s.Coordination.Token
	counts = h.node.BeginWorkInspectionForTest()
	executed, err := deliverGeneration(h, time.Unix(0, h.now))
	require.NoError(t, err)
	require.Positive(t, executed)
	commitGeneration(h, true)
	assertBoundedTaskCounts(t, h, counts.Snapshot())
	s, _ = h.root(false)
	require.Equal(t, before+2, s.Coordination.Token, "one physical step and one immediate successor")
}

func TestBoundedRuntimeCombinedWorkAndClaimRecovery(t *testing.T) {
	t.Parallel()
	h, _ := newBoundedRuntimeTree(t, 0, false)
	s, raw := h.root(true)
	require.NoError(t, s.PatchPublic(raw.(chasm.MutableContext), boundedCommandPatch(100)))
	h.commit(true)
	s, raw = h.root(true)
	require.EqualValues(t, 1, s.OperationStore.PendingCount)
	dispatch := s.OperationStore.StartDispatch
	claim, err := s.claimStart(raw.(chasm.MutableContext), startClaimInput{task: &pb.StartDispatchTask{Token: dispatch.Token, WakeTime: dispatch.At}, deadline: time.Unix(0, h.now).Add(time.Second)})
	require.NoError(t, err)
	require.NotNil(t, claim.claim)
	h.commit(true)
	s, _ = h.root(false)
	require.True(t, s.Coordination.Armed)
	require.True(t, s.OperationStore.StartDispatch.Armed)
	h.now = claim.claim.Expiry
	counts := h.node.BeginWorkInspectionForTest()
	executed, err := deliverGeneration(h, time.Unix(0, h.now))
	require.NoError(t, err)
	require.Equal(t, 2, executed, "physical Work and Recovery share this transaction")
	commitGeneration(h, true)
	assertBoundedTaskCounts(t, h, counts.Snapshot())
	s, _ = h.root(false)
	require.Nil(t, s.OperationStore.StartDispatch.Claim)
	require.Positive(t, s.OperationStore.PendingCount)
}

func TestBoundedRuntimeEffectBindingAndVisibility(t *testing.T) {
	t.Parallel()
	h, _ := newBoundedRuntimeTree(t, 0, false)
	trigger := &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER}}
	s, raw := h.root(true)
	require.NoError(t, s.PatchPublic(raw.(chasm.MutableContext), trigger))
	h.commit(true)
	s, _ = h.root(false)
	for step := 0; s.OperationStore.OperationCount == 0 && step < 4; step++ {
		executed, err := deliverGeneration(h, time.Unix(0, h.now))
		require.NoError(t, err)
		require.Positive(t, executed)
		commitGeneration(h, true)
		s, _ = h.root(false)
	}
	require.EqualValues(t, 1, s.OperationStore.OperationCount)
	id := s.OperationId
	s, raw = h.root(true)
	require.NoError(t, s.PatchPublic(raw.(chasm.MutableContext), trigger))
	h.commit(true)
	s, _ = h.root(false)
	require.Len(t, s.Effects, 1)
	require.Equal(t, pb.EFFECT_PHASE_AWAITING_TARGET, s.Effects[0].Phase)
	counts := h.node.BeginWorkInspectionForTest()
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: s.Incarnation, OperationID: id, ChainID: "chain", PublicRunID: "run", StartObserved: true, StartObservedAt: h.now}, true)
	assertBoundedTaskCounts(t, h, counts.Snapshot())
	s, raw = h.root(false)
	require.Equal(t, pb.EFFECT_PHASE_PENDING, s.Effects[0].Phase)
	require.True(t, s.CancelRequest.IsSet())
	require.Empty(t, s.EffectRequests)
	request, err := s.effectRequest(raw, s.Effects[0].Id)
	require.NoError(t, err)
	require.Equal(t, "chain", request.Cancel.FirstExecutionRunId)
	// Updating both custom fields creates at most two explicit visibility tasks
	// and one final versioned refresh, still retaining one persisted obligation.
	counts = h.node.BeginWorkInspectionForTest()
	s, raw = h.root(true)
	visibilityBefore := s.Visibility.Get(raw).Data.TransitionCount
	definition, err := s.publicDefinition(raw)
	require.NoError(t, err)
	require.NoError(t, s.UpdatePublic(raw.(chasm.MutableContext), &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: definition, Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{"new": {Data: []byte("new")}}}, SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{"CustomKeywordField": {Data: []byte("changed")}}}}, schedulerconfig.DefaultTweakables))
	h.commit(true)
	assertBoundedTaskCounts(t, h, counts.Snapshot())
	s, raw = h.root(false)
	require.Equal(t, visibilityBefore+3, s.Visibility.Get(raw).Data.TransitionCount)
}

func TestBoundedRuntimeRejectsLegacyMapInsertion(t *testing.T) {
	t.Parallel()
	h, _ := newBoundedRuntimeTree(t, 0, false)
	before := cloneNodes(h.durable)
	counts := h.node.BeginWorkInspectionForTest()
	s, raw := h.root(true)
	s.Operations["forbidden"] = chasm.NewDataField(raw.(chasm.MutableContext), &pb.OperationState{Id: "forbidden"})
	mutation, err := h.node.CloseTransaction(t.Context(), true)
	require.ErrorContains(t, err, "required-empty map")
	require.Empty(t, mutation.UpdatedNodes)
	require.Empty(t, mutation.DeletedNodes)
	require.Zero(t, counts.Snapshot()["map_key_copies"])
	h.reload()
	require.Equal(t, before, h.durable)
}
