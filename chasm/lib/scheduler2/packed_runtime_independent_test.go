//go:build test_dep

package scheduler2

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
)

func TestPackedIndependentCallbackCleanupAvoidsImmutableRequest(t *testing.T) {
	t.Parallel()
	e := newSelectedTaskEngine(t, true, nil)
	h := e.h
	s, raw := h.root(true)
	id := s.OperationId
	view, err := s.newPackedView(raw)
	require.NoError(t, err)
	identity, ok, err := view.identity(id)
	require.NoError(t, err)
	require.True(t, ok)
	view.close()
	key := packedRequestAddress(identity.locator)
	// A valid outer protobuf with corrupt packed issuance bytes. Metadata release
	// must never open Q, while issuance would fail decoding this same value.
	s.PackedRequests.Put(raw.(chasm.MutableContext), key, &pb.Payload{Data: []byte{0xff}})
	h.commit(true)
	s, read := h.root(false)
	_, _, err = s.requestRecord(read, id)
	require.Error(t, err)
	h.reload()
	s, raw = h.root(true)
	require.NoError(t, s.Apply(raw.(chasm.MutableContext), decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: id, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}))
	for _, path := range h.node.MaterializedPathsForTest() {
		require.False(t, strings.HasPrefix(path, "PackedRequests/"), path)
	}
	h.commit(true)
	s, read = h.root(false)
	op, ok, err := s.operationRecord(read, id)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, pb.PHASE_COMPLETED, op.Phase)
	_, ok = s.PackedRequests.Get(read, key)
	require.False(t, ok)
	require.Zero(t, s.OperationStore.RequestCount)
	action, ok, err := s.actionRecord(read, id)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, action.OperationOwned)
}

func TestPackedIndependentEditorPoisonPublishesNothing(t *testing.T) {
	t.Parallel()
	e := newSelectedTaskEngine(t, true, nil)
	h := e.h
	before, physical := cloneNodes(h.durable), len(h.physical)
	s, raw := h.root(true)
	editor, err := s.newPackedEditor(raw)
	require.NoError(t, err)
	defer editor.abort()
	op, ok, err := editor.operation(s.OperationId)
	require.NoError(t, err)
	require.True(t, ok)
	op.RetryAt++
	require.NoError(t, editor.setOperation(op.Id, op))
	require.Error(t, editor.setOperation("wrong", op))
	_, err = editor.finish(s.OperationStore, s.ProjectionStore)
	require.Error(t, err)
	requireRejectedCoordinatorUnchanged(h, before, physical)
}

func TestPackedIndependentCorruptionNeverFallsBack(t *testing.T) {
	t.Parallel()
	e := newSelectedTaskEngine(t, true, nil)
	h := e.h
	s, raw := h.root(true)
	id := s.OperationId
	op, ok, err := s.operationRecord(raw, id)
	require.NoError(t, err)
	require.True(t, ok)
	// Even a valid legacy record cannot rescue corrupt selected packed storage.
	s.OperationRecords.Put(raw.(chasm.MutableContext), id, common.CloneProto(op))
	s.PackedPages.Put(raw.(chasm.MutableContext), packedTemplateKey(packedOperationKind), &commonpb.Payload{Metadata: map[string][]byte{"invalid": {1}}})
	h.commit(true)
	s, read := h.root(false)
	_, _, err = s.operationRecord(read, id)
	require.ErrorContains(t, err, "invalid packed page envelope")
	require.NotContains(t, h.node.MaterializedPathsForTest(), "OperationRecords/"+id)
	h.reload()
	before, physical := cloneNodes(h.durable), len(h.physical)
	s, raw = h.root(true)
	require.Error(t, s.Apply(raw.(chasm.MutableContext), decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: id, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}))
	requireRejectedCoordinatorUnchanged(h, before, physical)
}

func TestPackedIndependentQueueDrainAndAdmissionNamespaces(t *testing.T) {
	t.Parallel()
	e := newSelectedTaskEngine(t, true, nil)
	h := e.h
	s, raw := h.root(true)
	first := s.OperationId
	require.NoError(t, s.AcceptManual(raw.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}))
	h.commit(true)
	for quantum := 0; quantum < 10; quantum++ {
		s, _ = h.root(false)
		total, err := queueTotal(queueCounts(s.IndexedQueue))
		require.NoError(t, err)
		if total == 1 {
			break
		}
		require.EqualValues(t, 0, total)
		if s.Coordination.Armed && s.Coordination.At > h.now {
			h.now = s.Coordination.At
		}
		ran, err := deliverGeneration(h, time.Unix(0, h.now))
		require.NoError(t, err)
		require.Positive(t, ran)
		commitGeneration(h, true)
	}
	s, raw = h.root(true)
	require.EqualValues(t, 1, s.IndexedQueue.ExplicitOrdinary)
	require.NoError(t, s.Apply(raw.(chasm.MutableContext), decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: first, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}))
	h.commit(true)
	for quantum := 0; quantum < 10; quantum++ {
		s, read := h.root(false)
		if s.OperationStore.OperationCount == 2 {
			view, err := s.newPackedView(read)
			require.NoError(t, err)
			t.Cleanup(view.close)
			old, ok, err := view.identity(first)
			require.NoError(t, err)
			require.True(t, ok)
			current, ok, err := view.identity(s.OperationId)
			require.NoError(t, err)
			require.True(t, ok)
			require.NotEqual(t, old.locator, current.locator)
			require.Equal(t, byte(packedOperationOwner|packedActionOwner), current.owners)
			total, err := queueTotal(queueCounts(s.IndexedQueue))
			require.NoError(t, err)
			require.Zero(t, total)
			require.EqualValues(t, 1, s.OperationStore.RequestCount)
			require.EqualValues(t, 1, s.OperationStore.PendingCount)
			return
		}
		if s.Coordination.Armed && s.Coordination.At > h.now {
			h.now = s.Coordination.At
		}
		ran, err := deliverGeneration(h, time.Unix(0, h.now))
		require.NoError(t, err)
		require.Positive(t, ran)
		commitGeneration(h, true)
	}
	t.Fatal("packed drain did not admit second operation")
}
