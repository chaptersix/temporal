package scheduler2

import (
	"encoding/base64"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
)

func newReadinessHarness(t *testing.T) *treeHarness {
	t.Helper()
	h := newRetirementHarness(t)
	s, raw := h.root(true)
	s.OperationStore.NextReadySequence = 1
	s.OperationStore.StartDispatch = &pb.StartDispatchControl{Token: 1}
	s.ReadyRecords = chasm.NewDataMap[*pb.ReadyRecord](raw.(chasm.MutableContext))
	h.commit(true)
	return h
}
func readinessOperation(s *Scheduler, id uint64, wake int64) *pb.OperationState {
	op := retirementOperation(s.Incarnation, int64(id), id, 0, nil)
	op.Phase = pb.PHASE_PENDING
	op.RetryAt = wake
	op.Deadline = 1000
	return op
}
func stageReadinessChange(t *testing.T, h *treeHarness, op *pb.OperationState, id string, publish bool) *pb.OperationState {
	t.Helper()
	s, raw := h.root(true)
	plan, err := s.prepareOperationRecords(raw, []operationRecordChange{{id: id, operation: op}})
	require.NoError(t, err)
	s.publishOperationRecords(raw.(chasm.MutableContext), plan)
	h.commit(publish)
	return common.CloneProto(plan.changes[0].operation)
}

func claimReadiness(t *testing.T, h *treeHarness, now, expiry int64, publish bool) *operationReadyEntry {
	t.Helper()
	s, raw := h.root(true)
	control := common.CloneProto(s.OperationStore)
	e, err := s.newOperationReadinessEditor(raw, control)
	require.NoError(t, err)
	defer e.abort()
	entry, err := e.minimum()
	require.NoError(t, err)
	require.NoError(t, e.claim(entry, now, expiry))
	changed, err := prepareStartDispatch(e, true)
	require.NoError(t, err)
	plan, err := e.finish()
	require.NoError(t, err)
	s.publishOperationReadiness(raw.(chasm.MutableContext), plan)
	s.OperationStore = control
	s.publishStartDispatch(raw.(chasm.MutableContext), changed)
	h.commit(publish)
	return entry
}

func TestOperationReadinessExactOrdering(t *testing.T) {
	t.Parallel()
	h := newReadinessHarness(t)
	s, _ := h.root(false)
	var operations []*pb.OperationState
	for i, wake := range []int64{20, -5, 20, math.MinInt64, 0, 10} {
		op := readinessOperation(s, uint64(i+1), wake)
		operations = append(operations, stageReadinessChange(t, h, op, op.Id, true))
	}
	slices.SortFunc(operations, func(a, b *pb.OperationState) int {
		if a.RetryAt < b.RetryAt {
			return -1
		}
		if a.RetryAt > b.RetryAt {
			return 1
		}
		if a.ReadySequence < b.ReadySequence {
			return -1
		}
		return 1
	})
	for _, expected := range operations {
		s, raw := h.root(false)
		entry, err := s.readOperationReady(raw)
		require.NoError(t, err)
		require.Equal(t, expected.Id, entry.Operation.Id)
		require.Equal(t, expected.ReadySequence, entry.Sequence)
		require.Equal(t, expected.RetryAt, entry.ReadyAt)
		done := common.CloneProto(expected)
		done.Phase = pb.PHASE_COMPLETED
		stageReadinessChange(t, h, done, done.Id, true)
	}
	s, raw := h.root(false)
	entry, err := s.readOperationReady(raw)
	require.NoError(t, err)
	require.Nil(t, entry)
	require.Zero(t, s.OperationStore.PendingCount)
}

func TestOperationReadinessClaimRecovery(t *testing.T) {
	t.Parallel()
	h := newReadinessHarness(t)
	s, _ := h.root(false)
	var ops []*pb.OperationState
	for i := range 3 {
		op := readinessOperation(s, uint64(i+1), int64(10*(i+1)))
		ops = append(ops, stageReadinessChange(t, h, op, op.Id, true))
	}
	before := cloneNodes(h.durable)
	selected := claimReadiness(t, h, 10, 50, false)
	require.Equal(t, before, h.durable)
	require.Equal(t, ops[0].Id, selected.Operation.Id)
	claimReadiness(t, h, 10, 50, true)
	s, raw := h.root(false)
	entry, err := s.readOperationReady(raw)
	require.NoError(t, err)
	require.Equal(t, ops[1].Id, entry.Operation.Id)
	require.Equal(t, int64(3), s.OperationStore.PendingCount)
	before = cloneNodes(h.durable)
	for _, publish := range []bool{false, true} {
		s, raw = h.root(true)
		control := common.CloneProto(s.OperationStore)
		token := control.StartDispatch.Token
		e, err := s.newOperationReadinessEditor(raw, control)
		require.NoError(t, err)
		recovered, err := e.recover(50)
		require.NoError(t, err)
		require.Equal(t, ops[0].RetryAt, recovered.RetryAt)
		require.Equal(t, ops[0].Deadline, recovered.Deadline)
		require.Greater(t, recovered.ReadySequence, ops[2].ReadySequence)
		require.Equal(t, token, control.StartDispatch.Token)
		changed, err := prepareStartDispatch(e, false)
		require.NoError(t, err)
		plan, err := e.finish()
		require.NoError(t, err)
		require.True(t, plan.ClaimReleased)
		require.Len(t, plan.recovered, 1)
		s.publishOperationReadiness(raw.(chasm.MutableContext), plan)
		s.OperationStore = control
		s.publishStartDispatch(raw.(chasm.MutableContext), changed)
		e.abort()
		h.commit(publish)
		if !publish {
			require.Equal(t, before, h.durable)
		}
	}
	s, raw = h.root(false)
	entry, err = s.readOperationReady(raw)
	require.NoError(t, err)
	require.Equal(t, ops[1].Id, entry.Operation.Id)
	recovered, ok, readErr := s.operationRecord(raw, ops[0].Id)
	require.NoError(t, readErr)
	require.True(t, ok)
	locator, ok := s.ReadyRecords.Get(raw, readyRecordKey(recovered.ReadySequence))
	require.True(t, ok)
	require.Equal(t, int64(50), locator.ReadyAt)
	require.Equal(t, int64(10), locator.LogicalWake)
}

func TestOperationReadinessOtherClaimPreserved(t *testing.T) {
	t.Parallel()
	h := newReadinessHarness(t)
	s, _ := h.root(false)
	first := readinessOperation(s, 1, 10)
	first = stageReadinessChange(t, h, first, first.Id, true)
	second := readinessOperation(s, 2, 20)
	second = stageReadinessChange(t, h, second, second.Id, true)
	claimReadiness(t, h, 10, 50, true)
	s, _ = h.root(false)
	dispatch := common.CloneProto(s.OperationStore.StartDispatch)
	nextSequence := s.OperationStore.NextReadySequence
	// Unchanged pending metadata and a positive result for another operation
	// preserve the active claim token, expiry and selected sequence.
	second.WorkflowId = "unchanged-wake"
	second = stageReadinessChange(t, h, second, second.Id, true)
	s, _ = h.root(false)
	protorequire.ProtoEqual(t, dispatch, s.OperationStore.StartDispatch)
	require.Equal(t, nextSequence, s.OperationStore.NextReadySequence)
	second.Phase = pb.PHASE_STARTED
	stageReadinessChange(t, h, second, second.Id, true)
	s, _ = h.root(false)
	protorequire.ProtoEqual(t, dispatch, s.OperationStore.StartDispatch)
	require.Equal(t, int64(1), s.OperationStore.PendingCount)
	first.Phase = pb.PHASE_STARTED
	stageReadinessChange(t, h, first, first.Id, true)
	s, raw := h.root(false)
	require.Nil(t, s.OperationStore.StartDispatch.Claim)
	require.Equal(t, dispatch.Token+1, s.OperationStore.StartDispatch.Token)
	entry, err := s.readOperationReady(raw)
	require.NoError(t, err)
	require.Nil(t, entry)
}

func TestOperationReadinessCorruption(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"root", "locator", "record", "logical wake", "sequence", "count", "claim underflow", "missing dispatch"} {
		t.Run(kind, func(t *testing.T) {
			h := newReadinessHarness(t)
			s, _ := h.root(false)
			op := readinessOperation(s, 1, 10)
			op = stageReadinessChange(t, h, op, op.Id, true)
			s, raw := h.root(true)
			ctx := raw.(chasm.MutableContext)
			switch kind {
			case "root":
				s.OperationPages.Delete(ctx, "sequence-root/"+base64.RawURLEncoding.EncodeToString(operationReadyIndex[:]))
			case "locator":
				s.ReadyRecords.Delete(ctx, readyRecordKey(op.ReadySequence))
			case "record":
				s.OperationRecords.Delete(ctx, op.Id)
			case "logical wake":
				record, ok := s.ReadyRecords.Get(raw, readyRecordKey(op.ReadySequence))
				require.True(t, ok)
				record.LogicalWake++
				s.ReadyRecords.Put(ctx, readyRecordKey(op.ReadySequence), record)
			case "sequence":
				op.ReadySequence++
				s.OperationRecords.Put(ctx, op.Id, op)
			case "count":
				s.OperationStore.PendingCount = 0
			case "claim underflow":
				s.OperationStore.PendingCount = 0
				s.OperationStore.StartDispatch.Claim = &pb.StartClaim{OperationId: op.Id, ReadySequence: op.ReadySequence, LogicalWake: 10, Expiry: 50}
			case "missing dispatch":
				s.OperationStore.StartDispatch = nil
			default:
				t.Fatal("unknown corruption")
			}
			h.commit(true)
			s, raw = h.root(false)
			before := cloneNodes(h.durable)
			_, err := s.readOperationReady(raw)
			require.Error(t, err)
			h.commit(false)
			require.Equal(t, before, h.durable)
		})
	}
}

func TestOperationReadinessExhaustionAndExpiry(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"sequence", "future claim", "expired recovery"} {
		t.Run(kind, func(t *testing.T) {
			h := newReadinessHarness(t)
			s, _ := h.root(false)
			op := readinessOperation(s, 1, 10)
			stageReadinessChange(t, h, op, op.Id, true)
			if kind == "expired recovery" {
				claimReadiness(t, h, 10, 50, true)
			}
			s, raw := h.root(true)
			control := common.CloneProto(s.OperationStore)
			if kind == "sequence" {
				s.OperationStore.NextReadySequence = math.MaxUint64
				control.NextReadySequence = math.MaxUint64
				control.OperationCount++
			}
			e, err := s.newOperationReadinessEditor(raw, control)
			require.NoError(t, err)
			before := cloneNodes(h.durable)
			switch kind {
			case "sequence":
				next := readinessOperation(s, 2, 20)
				err = e.change(operationRecordChange{id: next.Id, operation: next})
			case "future claim":
				entry, queryErr := e.minimum()
				require.NoError(t, queryErr)
				err = e.claim(entry, 9, 50)
			case "expired recovery":
				_, err = e.recover(1000)
				require.ErrorContains(t, err, "operation expiry")
				require.Empty(t, e.records)
				require.Empty(t, e.metadata)
			default:
				t.Fatal("unknown case")
			}
			require.Error(t, err)
			_, err = e.finish()
			require.Error(t, err)
			e.abort()
			h.commit(false)
			require.Equal(t, before, h.durable)
		})
	}
}

func TestOperationReadinessRetirementCaptureDisjoint(t *testing.T) {
	t.Parallel()
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			h := newReadinessHarness(t)
			s, _ := h.root(false)
			op := readinessOperation(s, 1, 10)
			op = stageReadinessChange(t, h, op, op.Id, true)
			s, raw := h.root(true)
			ctx := raw.(chasm.MutableContext)
			op.Phase = pb.PHASE_START_ONLY
			recordPlan, err := s.prepareOperationRecords(raw, []operationRecordChange{{id: op.Id, operation: op}})
			require.NoError(t, err)
			retirement, readiness, control := recordPlan.indexes, recordPlan.readiness, recordPlan.control
			op = recordPlan.changes[0].operation
			keys := make(map[string]bool)
			for _, change := range retirement.changes {
				require.True(t, strings.HasPrefix(change.Key, "retirement"))
				keys[change.Key] = true
			}
			require.NotEmpty(t, keys)
			require.NotEmpty(t, readiness.indexes.changes)
			for _, change := range readiness.indexes.changes {
				require.False(t, keys[change.Key])
				require.False(t, strings.HasPrefix(change.Key, "retirement"))
			}
			if reverse {
				s.publishOperationReadiness(ctx, readiness)
				s.publishOperationIndexes(ctx, retirement)
			} else {
				s.publishOperationIndexes(ctx, retirement)
				s.publishOperationReadiness(ctx, readiness)
			}
			s.OperationRecords.Put(ctx, op.Id, op)
			s.OperationStore = control
			s.publishStartDispatch(ctx, recordPlan.dispatchChanged)
			h.commit(true)
			s, raw = h.root(false)
			entry, err := s.readOperationReady(raw)
			require.NoError(t, err)
			require.Nil(t, entry)
			require.Zero(t, s.OperationStore.PendingCount)
			require.Equal(t, int64(1), s.OperationStore.RetirementCount)
			batch := queueindex.NewCountStoreWithBackend(&indexBackend{ctx: raw, pages: s.OperationPages}).Begin()
			key, present, err := batch.RetirementMinimum(operationRetirementIndex)
			require.NoError(t, err)
			require.True(t, present)
			expected, err := s.operationRetirementKey(op)
			require.NoError(t, err)
			require.Equal(t, expected, key)
			batch.Abort()
		})
	}
}
