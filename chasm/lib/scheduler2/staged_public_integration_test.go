//go:build test_dep

package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func stagedPublicIntegrationTree(t *testing.T) (*treeHarness, *PreparationService, *int, *stagedPublicCreate) {
	t.Helper()
	service, _, reads, _ := preparationServiceFixture(t)
	request := stagedPublicTestRequest()
	request.InitialPatch.Pause = "initial pause"
	request.InitialPatch.BackfillRequest = []*schedulepb.BackfillRequest{{StartTime: timestamppb.New(generationStart()), EndTime: timestamppb.New(generationStart().Add(time.Minute))}}
	stage, err := service.stagePublicCreate(request, "namespace", 1)
	require.NoError(t, err)
	h, _ := newBoundedRuntimeTree(t, 0, false)
	h.transition = 1
	h.physical = nil
	h.resetBackend()
	h.node = chasm.NewEmptyTree(h.registry, h.backend, chasm.DefaultPathEncoder, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	beforeReads := *reads
	s, err := newPreparedPublicSchedule(chasm.NewMutableContext(t.Context(), h.node), stage, schedulerconfig.DefaultTweakables)
	require.NoError(t, err)
	require.NoError(t, h.node.SetRootComponent(s))
	h.commit(true)
	require.Equal(t, beforeReads, *reads, "constructor must consume preparation without source resolution")
	return h, service, reads, stage
}
func TestStagedPublicIntegrationInitialPatchAndColdPatch(t *testing.T) {
	t.Parallel()
	h, _, reads, _ := stagedPublicIntegrationTree(t)
	s, ctx := h.root(false)
	require.True(t, s.Paused)
	require.EqualValues(t, 1, s.ManualSequence)
	require.EqualValues(t, 1, s.OperationStore.OperationCount)
	require.Len(t, s.Backfills, 1)
	require.True(t, s.sourcePreparationPending(ctx))
	require.False(t, s.PublicList.IsSet())
	require.Nil(t, s.Memo(ctx))
	require.Equal(t, "initial pause", string(s.ListNotes.Get(ctx).Data))
	// The legacy list is absent. Full definition/source corruption must not be
	// consulted by the selected metadata-only pause path.
	for _, path := range []string{"PublicSchedule", "Generator$PreparedDefinition$Source"} {
		h.durable[path].Data.Data = []byte{0xff}
	}
	h.reload()
	beforeReads := *reads
	s, ctx = h.root(true)
	require.NoError(t, s.PatchPublic(ctx.(chasm.MutableContext), &schedulepb.SchedulePatch{Unpause: "resume"}))
	require.False(t, s.Paused)
	require.Equal(t, "resume", string(s.ListNotes.Get(ctx).Data))
	require.Equal(t, beforeReads, *reads)
	require.NotContains(t, h.node.MaterializedPathsForTest(), "PublicSchedule")
	require.NotContains(t, h.node.MaterializedPathsForTest(), "Generator/PreparedDefinition/Source")
	h.commit(true)
}
func TestStagedPublicIntegrationReplacementCoherenceAndStaleFence(t *testing.T) {
	t.Parallel()
	h, service, reads, created := stagedPublicIntegrationTree(t)
	s, ctx := h.root(false)
	first := common.CloneProto(s.ListPreview.Get(ctx))
	finishListTest(t, h, created.definition.definition.owner, first.Stamp, true)
	s, ctx = h.root(false)
	previous := common.CloneProto(s.Memo(ctx).(*schedulepb.ScheduleListInfo))
	request := &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: common.CloneProto(created.request.Schedule)}
	request.Schedule.Action.GetStartWorkflow().WorkflowType.Name = "replacement"
	request.Schedule.State.Notes = "replacement notes"
	stage, err := service.stagePublicUpdate(request, "namespace", s.Revision, 2)
	require.NoError(t, err)
	beforeReads := *reads
	h.durable["Generator$PreparedDefinition$Source"].Data.Data = []byte{0xff}
	h.reload()
	s, ctx = h.root(true)
	require.NoError(t, s.UpdatePreparedPublic(ctx.(chasm.MutableContext), stage, schedulerconfig.DefaultTweakables))
	require.Equal(t, beforeReads, *reads)
	require.NotContains(t, h.node.MaterializedPathsForTest(), "Generator/PreparedDefinition/Source")
	h.commit(true)
	s, ctx = h.root(false)
	pending := s.Memo(ctx).(*schedulepb.ScheduleListInfo)
	require.True(t, proto.Equal(previous.Spec, pending.Spec))
	require.True(t, proto.Equal(previous.WorkflowType, pending.WorkflowType))
	require.True(t, proto.Equal(&schedulepb.ScheduleListInfo{FutureActionTimes: previous.FutureActionTimes}, &schedulepb.ScheduleListInfo{FutureActionTimes: pending.FutureActionTimes}))
	require.Equal(t, "replacement notes", pending.Notes)
	require.Equal(t, "replacement", s.PublicMetadata.Get(ctx).WorkflowType)
	before, physical := cloneNodes(h.durable), len(h.physical)
	s, ctx = h.root(true)
	require.ErrorContains(t, s.UpdatePreparedPublic(ctx.(chasm.MutableContext), stage, schedulerconfig.DefaultTweakables), "binding")
	requireRejectedCoordinatorUnchanged(h, before, physical)
	s, ctx = h.root(false)
	preview := s.ListPreview.Get(ctx)
	finishListTest(t, h, stage.definition.definition.owner, preview.Stamp, true)
	s, ctx = h.root(false)
	require.Equal(t, "replacement", s.Memo(ctx).(*schedulepb.ScheduleListInfo).WorkflowType.Name)
	require.Len(t, s.Memo(ctx).(*schedulepb.ScheduleListInfo).Spec.Interval, 2)
}
