package scheduler2

import (
	"bytes"
	"math"
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
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
)

// newBlobHarness selects the complete blob owner path during fresh creation.
// The production constructor remains on its existing profile pending review.
func newBlobHarness(t *testing.T) *treeHarness {
	t.Helper()
	h := newTreeHarness(t)
	h.now = generationStart().UnixNano()
	h.resetBackend()
	h.node = chasm.NewEmptyTree(h.registry, h.backend, chasm.DefaultPathEncoder, testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly), metrics.NoopMetricsHandler)
	request := &workflowservice.CreateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: &schedulepb.Schedule{
		Spec:   generationSpec(generationStart(), generationStart().Add(time.Hour), time.Minute),
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "action", WorkflowType: &commonpb.WorkflowType{Name: "workflow"}}}},
	}}
	s, err := newExecutingPublicScheduleWithPayloadStore(chasm.NewMutableContext(t.Context(), h.node), request, schedulerconfig.DefaultTweakables, true)
	require.NoError(t, err)
	require.NoError(t, h.node.SetRootComponent(s))
	h.commit(true)
	return h
}

func TestBlobOwnersAcrossDefinitionAndRequestVersions(t *testing.T) {
	t.Parallel()
	h := newBlobHarness(t)
	firstInput := bytes.Repeat([]byte("first"), 1000)
	replaceBundleInput(h, firstInput)
	first := admitBundle(h)
	s, ctx := h.root(false)
	oldRequest, err := s.requiredRequestRecord(ctx, first)
	require.NoError(t, err)
	oldRef := oldRequest.ActionRef
	count, ok := s.BlobReferences.Get(ctx, oldRef)
	require.True(t, ok)
	require.EqualValues(t, 3, count.OwnerCount)
	replaceBundleInput(h, []byte("next"))
	second := admitBundle(h)
	s, ctx = h.root(false)
	nextRequest, err := s.requiredRequestRecord(ctx, second)
	require.NoError(t, err)
	require.NotEqual(t, oldRef, nextRequest.ActionRef)
	count, ok = s.BlobReferences.Get(ctx, oldRef)
	require.True(t, ok)
	require.EqualValues(t, 1, count.OwnerCount)
	assembled, err := s.assembleRequest(ctx, oldRequest)
	require.NoError(t, err)
	require.Equal(t, firstInput, assembled.Input.Payloads[0].Data)
	// Releasing the final owner must not decode old immutable content.
	require.Contains(t, h.durable, "BlobRecords#"+oldRef)
	h.durable["BlobRecords#"+oldRef].Data.Data = []byte{0xff}
	h.reload()
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: s.Incarnation, OperationID: first, ChainID: "chain"}, true)
	s, ctx = h.root(false)
	_, ok = s.BlobReferences.Get(ctx, oldRef)
	require.False(t, ok)
	_, ok = s.BlobRecords.Get(ctx, oldRef)
	require.False(t, ok)
	count, ok = s.BlobReferences.Get(ctx, nextRequest.ActionRef)
	require.True(t, ok)
	require.EqualValues(t, 3, count.OwnerCount)
	require.Empty(t, s.PayloadBlobs)
}

func TestBlobDeltaCoalescesOwnersAndRollback(t *testing.T) {
	t.Parallel()
	h := newBlobHarness(t)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	id := s.PublicActionRef
	before := cloneNodes(h.durable)
	owners := []blobOwnerChange{{owner: "old", before: [3]string{id, id}}, {owner: "new", after: [3]string{id, id, id}}}
	plan, err := s.prepareBlobDelta(ctx, owners, nil)
	require.NoError(t, err)
	require.Empty(t, plan.counts)
	require.Empty(t, plan.prepared)
	s.publishBlobDelta(ctx, plan)
	h.commit(false)
	require.Equal(t, before, h.durable)
	s, raw = h.root(true)
	ctx = raw.(chasm.MutableContext)
	wire := []byte("new payload")
	added := blobIdentity("result", wire)
	plan, err = s.prepareBlobDelta(ctx, []blobOwnerChange{{owner: "new", after: [3]string{added, added}}}, payloadPreparation{added: wire, blobIdentity("action", []byte("unused")): []byte("unused")})
	require.NoError(t, err)
	require.Len(t, plan.counts, 1)
	require.Len(t, plan.prepared, 1)
	wire[0] = 'X'
	s.publishBlobDelta(ctx, plan)
	h.commit(false)
	require.Equal(t, before, h.durable)
	s, raw = h.root(true)
	ctx = raw.(chasm.MutableContext)
	s.publishBlobDelta(ctx, plan)
	h.commit(true)
	s, raw = h.root(false)
	record, ok := s.BlobRecords.Get(raw, added)
	require.True(t, ok)
	require.Equal(t, "new payload", string(record.Data))
	count, ok := s.BlobReferences.Get(raw, added)
	require.True(t, ok)
	require.EqualValues(t, 1, count.OwnerCount)
}

func TestBlobDeltaRejectsCorruptOwnershipWithoutWrites(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"missing count", "zero count", "underflow", "overflow", "missing content", "forged content", "duplicate owner", "owner budget"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newBlobHarness(t)
			s, raw := h.root(true)
			ctx := raw.(chasm.MutableContext)
			id := s.PublicActionRef
			content, ok := s.BlobRecords.Get(ctx, id)
			require.True(t, ok)
			owners := []blobOwnerChange{{owner: "old", before: [3]string{id}}}
			prepared := payloadPreparation{}
			switch name {
			case "missing count":
				s.BlobReferences.Delete(ctx, id)
			case "zero count":
				s.BlobReferences.Put(ctx, id, &pb.BlobReference{})
			case "underflow":
				owners = append(owners, blobOwnerChange{owner: "old2", before: [3]string{id}}, blobOwnerChange{owner: "old3", before: [3]string{id}})
			case "overflow":
				s.BlobReferences.Put(ctx, id, &pb.BlobReference{OwnerCount: math.MaxUint64})
				owners = []blobOwnerChange{{owner: "new", after: [3]string{id}}}
			case "missing content":
				s.BlobRecords.Delete(ctx, id)
				owners = []blobOwnerChange{{owner: "new", after: [3]string{id}}}
				prepared[id] = content.Data
			case "forged content":
				owners = []blobOwnerChange{{owner: "new", after: [3]string{id}}}
				prepared[id] = []byte("forged")
			case "duplicate owner":
				owners = append(owners, owners[0])
			case "owner budget":
				for len(owners) < 7 {
					owners = append(owners, owners[0])
				}
			default:
				t.Fatal("unknown fixture")
			}
			h.commit(true)
			before := cloneNodes(h.durable)
			s, raw = h.root(true)
			ctx = raw.(chasm.MutableContext)
			root := common.CloneProto(s.RootState)
			_, err := s.prepareBlobDelta(ctx, owners, prepared)
			require.Error(t, err)
			require.Equal(t, root, s.RootState)
			h.commit(false)
			require.Equal(t, before, h.durable)
		})
	}
}

func TestBlobCarryoverAndDuplicateTerminalOwnership(t *testing.T) {
	t.Parallel()
	h := newBlobHarness(t)
	first := admitBundle(h)
	sequentialComplete(h, first, &commonpb.Payload{Data: []byte("first result")})
	s, ctx := h.root(false)
	old := s.Carryover.Get(ctx).ResultRef
	count, ok := s.BlobReferences.Get(ctx, old)
	require.True(t, ok)
	require.EqualValues(t, 1, count.OwnerCount)
	second := admitBundle(h)
	s, ctx = h.root(false)
	count, ok = s.BlobReferences.Get(ctx, old)
	require.True(t, ok)
	require.EqualValues(t, 2, count.OwnerCount)
	request, err := s.requiredRequestRecord(ctx, second)
	require.NoError(t, err)
	require.Equal(t, old, request.ResultRef)
	sequentialComplete(h, second, &commonpb.Payload{Data: []byte("next result")})
	s, ctx = h.root(false)
	_, ok = s.BlobReferences.Get(ctx, old)
	require.False(t, ok)
	current := s.Carryover.Get(ctx).ResultRef
	count, ok = s.BlobReferences.Get(ctx, current)
	require.True(t, ok)
	require.EqualValues(t, 1, count.OwnerCount)
	sequentialComplete(h, second, &commonpb.Payload{Data: []byte("late result")})
	s, ctx = h.root(false)
	require.Equal(t, current, s.Carryover.Get(ctx).ResultRef)
	count, ok = s.BlobReferences.Get(ctx, current)
	require.True(t, ok)
	require.EqualValues(t, 1, count.OwnerCount)
	s, ctx = h.root(true)
	require.NoError(t, s.Close(ctx.(chasm.MutableContext)))
	h.commit(true)
	s, ctx = h.root(false)
	count, ok = s.BlobReferences.Get(ctx, current)
	require.True(t, ok)
	require.EqualValues(t, 1, count.OwnerCount)
}

func TestBlobCompletionRetentionSelfEviction(t *testing.T) {
	t.Parallel()
	h := newBlobHarness(t)
	var id string
	for i := 0; i < 11; i++ {
		id = admitBundle(h)
		closeTime := time.Unix(int64(1000+i), 0)
		if i == 10 {
			closeTime = time.Unix(1, 0)
		}
		h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: id + "/chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, Result: completionWire(t, &commonpb.Payload{Data: []byte{byte(i)}}), PublicCloseTime: closeTime}, true)
	}
	s, ctx := h.root(false)
	_, ok, readErr := s.operationRecord(ctx, id)
	require.NoError(t, readErr)
	require.False(t, ok)
	_, ok, readErr = s.requestRecord(ctx, id)
	require.NoError(t, readErr)
	require.False(t, ok)
	require.Len(t, s.PublicRuntime.Get(ctx).CompletedOperations, 10)
	resultRef := s.Carryover.Get(ctx).ResultRef
	count, ok := s.BlobReferences.Get(ctx, resultRef)
	require.True(t, ok)
	require.EqualValues(t, 1, count.OwnerCount)
	count, ok = s.BlobReferences.Get(ctx, s.PublicActionRef)
	require.True(t, ok)
	require.EqualValues(t, 2, count.OwnerCount)
}

func TestBlobOwnerFailurePrecedesDefinitionPublication(t *testing.T) {
	t.Parallel()
	h := newBlobHarness(t)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	definition, err := s.publicDefinition(ctx)
	require.NoError(t, err)
	definition.Action.GetStartWorkflow().Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("new definition")}}}
	s.BlobReferences.Delete(ctx, s.PublicActionRef)
	h.commit(true)
	s, raw = h.root(true)
	ctx = raw.(chasm.MutableContext)
	root := common.CloneProto(s.RootState)
	generator := common.CloneProto(s.Generator.Get(ctx).GeneratorState)
	oldDefinition := common.CloneProto(s.Generator.Get(ctx).Definition.Get(ctx))
	before := cloneNodes(h.durable)
	err = s.UpdatePublic(ctx, &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: definition}, schedulerconfig.DefaultTweakables)
	require.ErrorContains(t, err, "underflow")
	require.Equal(t, root, s.RootState)
	require.Equal(t, generator, s.Generator.Get(ctx).GeneratorState)
	require.Equal(t, oldDefinition, s.Generator.Get(ctx).Definition.Get(ctx))
	h.commit(false)
	require.Equal(t, before, h.durable)
}
