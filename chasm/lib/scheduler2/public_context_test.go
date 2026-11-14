package scheduler2

import (
	"testing"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/contextutil"
)

func TestContextMetadataUsesActionSkeleton(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, ctx := h.root(false)
	s.PublicActionRef = "unavailable-payload-owner"
	require.Equal(t, map[string]string{contextutil.MetadataKeyWorkflowType: "workflow"}, s.ContextMetadata(ctx))
	s.Closed = true
	require.Equal(t, map[string]string{contextutil.MetadataKeyWorkflowType: "workflow"}, s.ContextMetadata(ctx))
}

func TestContextMetadataWithoutPublicAction(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	s, ctx := h.root(true)
	require.Nil(t, s.ContextMetadata(ctx))
	s.PublicSchedule = chasm.NewDataField(ctx.(chasm.MutableContext), &schedulepb.Schedule{})
	require.Nil(t, s.ContextMetadata(ctx))
}

func TestAddressedContextMetadataDoesNotReadDefinition(t *testing.T) {
	t.Parallel()
	h := newAddressedTaskEngine(t, nil).h
	s, ctx := h.root(false)
	definition, err := s.publicDefinition(ctx)
	require.NoError(t, err)
	action := definition.GetAction().GetStartWorkflow()
	action.WorkflowType.Name = "updated-workflow"
	action.TaskQueue = &taskqueuepb.TaskQueue{Name: "updated-queue"}
	s, ctx = h.root(true)
	require.NoError(t, s.UpdatePublic(ctx.(chasm.MutableContext), &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: definition}, schedulerconfig.DefaultTweakables))
	h.commit(true)
	// Poison the detached definition after the real update has committed. Metadata
	// reads must work on a cold tree without decoding the specification or action.
	h.durable["PublicSchedule"].Data.Data = []byte{0xff}
	h.reload()
	s, ctx = h.root(false)
	require.Equal(t, map[string]string{contextutil.MetadataKeyWorkflowType: "updated-workflow", contextutil.MetadataKeyWorkflowTaskQueue: "updated-queue"}, s.ContextMetadata(ctx))
	require.NotContains(t, h.node.MaterializedPathsForTest(), "PublicSchedule")
	require.Contains(t, h.node.MaterializedPathsForTest(), "PublicMetadata")
	s.Closed = true
	require.Equal(t, map[string]string{contextutil.MetadataKeyWorkflowType: "updated-workflow", contextutil.MetadataKeyWorkflowTaskQueue: "updated-queue"}, s.ContextMetadata(ctx))
}
