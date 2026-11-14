package scheduler2

import (
	"encoding/base64"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newRetirementHarness(t *testing.T) *treeHarness {
	t.Helper()
	h := newTreeHarness(t)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	s.PublicRevision = 1
	s.OperationStore = &pb.OperationStoreControl{FormatVersion: operationStoreVersion}
	s.OperationRecords = chasm.NewDataMap[*pb.OperationState](ctx)
	s.RequestRecords = chasm.NewDataMap[*pb.Payload](ctx)
	s.OperationPages = chasm.NewDataMap[*commonpb.Payload](ctx)
	h.commit(true)
	return h
}
func retirementOperation(incarnation string, nominal int64, manual, backfill uint64, wide *timestamppb.Timestamp) *pb.OperationState {
	occurrence := fmt.Sprintf("automatic/%d", nominal)
	if manual != 0 {
		occurrence = fmt.Sprintf("manual/%d", manual)
	}
	if backfill != 0 {
		occurrence = fmt.Sprintf("backfill/%d/%s", backfill, decodeDecisionTime(wide).Key(nominal))
	}
	action := incarnation + "/" + occurrence + "/action"
	return &pb.OperationState{Id: action + "/start", ActionId: action, OccurrenceId: occurrence, Nominal: nominal, ManualId: manual, BackfillId: backfill, NominalTime: wide, Phase: pb.PHASE_START_ONLY}
}
func putRetirementOperation(t *testing.T, h *treeHarness, op *pb.OperationState) {
	t.Helper()
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	plan, err := s.prepareOperationRecords(ctx, []operationRecordChange{{id: op.Id, operation: op}})
	require.NoError(t, err)
	s.publishOperationRecords(ctx, plan)
	h.commit(true)
}
func retirementCandidate(s *Scheduler) decision.State {
	return decision.State{Operation: &decision.Operation{ID: s.Incarnation + "/new"}}
}

func TestOperationRetirementReference(t *testing.T) {
	t.Parallel()
	h := newRetirementHarness(t)
	s, _ := h.root(false)
	incarnation := s.Incarnation
	var records []*pb.OperationState
	for i, nominal := range []int64{math.MaxInt64, 0, math.MinInt64, -1, -1, -1, 0, 1} {
		op := retirementOperation(incarnation, nominal, uint64(i+1), 0, nil)
		records = append(records, op)
		putRetirementOperation(t, h, op)
	}
	// Wide timestamps remain ordered by the legacy Nominal value, then ID.
	wide := retirementOperation(incarnation, 0, 0, math.MaxUint64, timestamppb.New(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)))
	records = append(records, wide)
	putRetirementOperation(t, h, wide)
	pending := retirementOperation(incarnation, math.MinInt64, 100, 0, nil)
	pending.Phase = pb.PHASE_PENDING
	putRetirementOperation(t, h, pending)
	for len(records) > 0 {
		s, raw := h.root(true)
		var expected *pb.OperationState
		for _, op := range records {
			if expected == nil || op.Nominal < expected.Nominal || op.Nominal == expected.Nominal && op.Id < expected.Id {
				expected = op
			}
		}
		id, err := s.retentionEviction(raw, decision.State{}, retirementCandidate(s))
		require.NoError(t, err)
		if s.OperationStore.OperationCount < 4 {
			require.Empty(t, id)
			break
		}
		require.Equal(t, expected.Id, id)
		plan, err := s.prepareOperationRecords(raw, []operationRecordChange{{id: id, changeRequest: true}})
		require.NoError(t, err)
		s.publishOperationRecords(raw.(chasm.MutableContext), plan)
		h.commit(true)
		for i, op := range records {
			if op.Id == id {
				records = append(records[:i], records[i+1:]...)
				break
			}
		}
	}
}

func TestOperationRetirementPublicationAndPhase(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"preflight", "indexes", "record", "complete"} {
		t.Run(stage, func(t *testing.T) {
			h := newRetirementHarness(t)
			s, _ := h.root(false)
			op := retirementOperation(s.Incarnation, -10, 1, 0, nil)
			op.Phase = pb.PHASE_PENDING
			putRetirementOperation(t, h, op)
			s, raw := h.root(true)
			ctx := raw.(chasm.MutableContext)
			before := cloneNodes(h.durable)
			op.Phase = pb.PHASE_START_ONLY
			plan, err := s.prepareOperationRecords(ctx, []operationRecordChange{{id: op.Id, operation: op}})
			require.NoError(t, err)
			require.Zero(t, s.OperationStore.RetirementCount)
			if stage == "indexes" {
				s.publishOperationIndexes(ctx, plan.indexes)
			}
			if stage == "record" {
				s.OperationRecords.Put(ctx, op.Id, op)
				s.OperationStore = common.CloneProto(plan.control)
			}
			if stage == "complete" {
				s.publishOperationRecords(ctx, plan)
			}
			h.commit(false)
			require.Equal(t, before, h.durable)
			s, raw = h.root(true)
			ctx = raw.(chasm.MutableContext)
			plan, err = s.prepareOperationRecords(ctx, []operationRecordChange{{id: op.Id, operation: op}})
			require.NoError(t, err)
			s.publishOperationRecords(ctx, plan)
			h.commit(true)
			s, raw = h.root(true)
			require.Equal(t, int64(1), s.OperationStore.RetirementCount)
			// Repeated metadata publication retains one membership; completion removes it.
			plan, err = s.prepareOperationRecords(raw, []operationRecordChange{{id: op.Id, operation: op}})
			require.NoError(t, err)
			s.publishOperationRecords(raw.(chasm.MutableContext), plan)
			h.commit(true)
			s, raw = h.root(true)
			op.Phase = pb.PHASE_COMPLETED
			plan, err = s.prepareOperationRecords(raw, []operationRecordChange{{id: op.Id, operation: op}})
			require.NoError(t, err)
			s.publishOperationRecords(raw.(chasm.MutableContext), plan)
			h.commit(true)
			s, raw = h.root(false)
			require.Zero(t, s.OperationStore.RetirementCount)
			b := queueindex.NewCountStoreWithBackend(&indexBackend{ctx: raw, pages: s.OperationPages}).Begin()
			count, err := b.RetirementCount(operationRetirementIndex)
			require.NoError(t, err)
			require.Zero(t, count)
			b.Abort()
		})
	}
}

func TestOperationRetirementCorruption(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"root", "record", "phase", "nominal", "identity", "summary"} {
		t.Run(kind, func(t *testing.T) {
			h := newRetirementHarness(t)
			s, _ := h.root(false)
			var first *pb.OperationState
			for i := range 4 {
				op := retirementOperation(s.Incarnation, int64(i), uint64(i+1), 0, nil)
				if i == 0 {
					first = op
				}
				putRetirementOperation(t, h, op)
			}
			s, raw := h.root(true)
			ctx := raw.(chasm.MutableContext)
			switch kind {
			case "root":
				s.OperationPages.Delete(ctx, "retirement-root/"+base64.RawURLEncoding.EncodeToString(operationRetirementIndex[:]))
			case "record":
				s.OperationRecords.Delete(ctx, first.Id)
			case "phase":
				first.Phase = pb.PHASE_STARTED
				s.OperationRecords.Put(ctx, first.Id, first)
			case "nominal":
				first.Nominal--
				s.OperationRecords.Put(ctx, first.Id, first)
			case "identity":
				first.ActionId = "foreign"
				s.OperationRecords.Put(ctx, first.Id, first)
			case "summary":
				s.OperationStore.RetirementCount++
			default:
				t.Fatal("unknown corruption")
			}
			h.commit(true)
			s, raw = h.root(true)
			before := cloneNodes(h.durable)
			_, err := s.retentionEviction(raw, decision.State{}, retirementCandidate(s))
			require.Error(t, err)
			h.commit(false)
			require.Equal(t, before, h.durable)
		})
	}
}

func TestOperationRetirementIdentityAndAdmissionGate(t *testing.T) {
	t.Parallel()
	h := newRetirementHarness(t)
	s, _ := h.root(false)
	valid := retirementOperation(s.Incarnation, -1, 0, 0, nil)
	for _, mutate := range []func(*pb.OperationState){func(op *pb.OperationState) { op.Id = "foreign/" + op.Id }, func(op *pb.OperationState) { op.OccurrenceId = "manual/1" }, func(op *pb.OperationState) { op.NominalTime = &timestamppb.Timestamp{Seconds: math.MaxInt64} }} {
		op := common.CloneProto(valid)
		mutate(op)
		s, raw := h.root(true)
		_, err := s.prepareOperationRecords(raw, []operationRecordChange{{id: op.Id, operation: op}})
		require.Error(t, err)
		h.commit(false)
	}
	for i := range 4 {
		putRetirementOperation(t, h, retirementOperation(s.Incarnation, int64(i), uint64(i+1), 0, nil))
	}
	s, raw := h.root(false)
	next := retirementCandidate(s)
	id, err := s.retentionEviction(raw, next, next)
	require.NoError(t, err)
	require.Empty(t, id)
	id, err = s.retentionEviction(raw, decision.State{}, decision.State{})
	require.NoError(t, err)
	require.Empty(t, id)
}

func TestOperationRetirementNoEligibleRecord(t *testing.T) {
	t.Parallel()
	h := newRetirementHarness(t)
	s, _ := h.root(false)
	for i := range 4 {
		op := retirementOperation(s.Incarnation, int64(i), uint64(i+1), 0, nil)
		op.Phase = pb.PHASE_PENDING
		putRetirementOperation(t, h, op)
	}
	s, ctx := h.root(false)
	id, err := s.retentionEviction(ctx, decision.State{}, retirementCandidate(s))
	require.NoError(t, err)
	require.Empty(t, id)
	require.Zero(t, s.OperationStore.RetirementCount)
}

func TestOperationRetirementReadOnlyPreparation(t *testing.T) {
	t.Parallel()
	h := newRetirementHarness(t)
	s, _ := h.root(false)
	op := retirementOperation(s.Incarnation, -5, 1, 0, nil)
	op.Phase = pb.PHASE_PENDING
	putRetirementOperation(t, h, op)
	before := cloneNodes(h.durable)
	s, ctx := h.root(false)
	op.Phase = pb.PHASE_START_ONLY
	plan, err := s.prepareOperationRecords(ctx, []operationRecordChange{{id: op.Id, operation: op}})
	require.NoError(t, err)
	require.NotEmpty(t, plan.indexes.changes)
	h.commit(true)
	require.Equal(t, before, h.durable)
	s, ctx = h.root(true)
	s.publishOperationRecords(ctx.(chasm.MutableContext), plan)
	h.commit(true)
	s, _ = h.root(false)
	require.Equal(t, int64(1), s.OperationStore.RetirementCount)
}
