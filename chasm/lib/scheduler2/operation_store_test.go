package scheduler2

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
)

func TestOperationStoreAddressedPublication(t *testing.T) {
	t.Parallel()
	for _, keyed := range []bool{false, true} {
		name := "finite"
		if keyed {
			name = "addressed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			s, raw := h.root(true)
			ctx := raw.(chasm.MutableContext)
			if keyed {
				s.OperationStore = &pb.OperationStoreControl{FormatVersion: operationStoreVersion}
				s.OperationRecords = chasm.NewDataMap[*pb.OperationState](ctx)
				s.RequestRecords = chasm.NewDataMap[*pb.Payload](ctx)
			}
			h.commit(true)
			s, raw = h.root(true)
			ctx = raw.(chasm.MutableContext)
			operation := &pb.OperationState{Id: "retained", Phase: pb.PHASE_PENDING}
			request := &pb.Payload{Data: []byte("immutable request"), ActionRef: "old-action"}
			plan, err := s.prepareOperationRecords(ctx, []operationRecordChange{{id: operation.Id, operation: operation, request: request, changeRequest: true}})
			require.NoError(t, err)
			operation.Phase = pb.PHASE_COMPLETED
			request.Data[0] = 'X'
			before := cloneNodes(h.durable)
			s.publishOperationRecords(ctx, plan)
			h.commit(false)
			require.Equal(t, before, h.durable)
			s, raw = h.root(true)
			ctx = raw.(chasm.MutableContext)
			s.publishOperationRecords(ctx, plan)
			h.commit(true)
			s, raw = h.root(true)
			ctx = raw.(chasm.MutableContext)
			got, ok := s.operationRecord(ctx, "retained")
			require.True(t, ok)
			require.Equal(t, pb.PHASE_PENDING, got.Phase)
			frozen, err := s.requiredRequestRecord(ctx, "retained")
			require.NoError(t, err)
			require.Equal(t, "immutable request", string(frozen.Data))
			got.Phase = pb.PHASE_STARTED
			frozen.Data[0] = 'Y'
			_, err = s.prepareOperationRecords(ctx, []operationRecordChange{{id: "retained", operation: got, request: frozen, changeRequest: true}})
			require.ErrorContains(t, err, "immutable")
			plan, err = s.prepareOperationRecords(ctx, []operationRecordChange{{id: "retained", operation: got}})
			require.NoError(t, err)
			s.publishOperationRecords(ctx, plan)
			h.commit(true)
			s, raw = h.root(true)
			ctx = raw.(chasm.MutableContext)
			frozen, err = s.requiredRequestRecord(ctx, "retained")
			require.NoError(t, err)
			require.Equal(t, "immutable request", string(frozen.Data))
			if keyed {
				require.EqualValues(t, 1, s.OperationStore.OperationCount)
				require.EqualValues(t, 1, s.OperationStore.RequestCount)
				require.Empty(t, s.Operations)
				require.Empty(t, s.Requests)
				before := cloneNodes(h.durable)
				require.ErrorContains(t, s.Apply(ctx, decision.Event{Kind: decision.Update, Incarnation: s.Incarnation}), "integration is incomplete")
				h.commit(false)
				require.Equal(t, before, h.durable)
				s, raw = h.root(true)
				ctx = raw.(chasm.MutableContext)
			}
			plan, err = s.prepareOperationRecords(ctx, []operationRecordChange{{id: "retained", changeRequest: true}})
			require.NoError(t, err)
			s.publishOperationRecords(ctx, plan)
			h.commit(true)
			s, raw = h.root(false)
			_, ok = s.operationRecord(raw, "retained")
			require.False(t, ok)
			_, err = s.requiredRequestRecord(raw, "retained")
			require.ErrorContains(t, err, "missing")
			if keyed {
				require.Zero(t, s.OperationStore.OperationCount)
				require.Zero(t, s.OperationStore.RequestCount)
			}
		})
	}
}

func TestOperationStoreRejectsInvalidSummary(t *testing.T) {
	t.Parallel()
	for _, control := range []*pb.OperationStoreControl{
		{FormatVersion: 99}, {FormatVersion: 1, OperationCount: -1}, {FormatVersion: 1, RequestCount: 1},
	} {
		h := newTreeHarness(t)
		s, ctx := h.root(true)
		s.OperationStore = control
		_, err := s.state(ctx)
		require.ErrorContains(t, err, "operation store summary")
	}
	h := newTreeHarness(t)
	s, ctx := h.root(true)
	s.OperationStore = &pb.OperationStoreControl{FormatVersion: 1}
	s.Operations["legacy"] = chasm.NewDataField(ctx.(chasm.MutableContext), &pb.OperationState{Id: "legacy"})
	_, err := s.state(ctx)
	require.ErrorContains(t, err, "mixed operation")
}

func TestOperationStorePreflightRejectsWithoutWrites(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"overflow", "underflow", "duplicate", "budget", "retained request"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			s, raw := h.root(true)
			ctx := raw.(chasm.MutableContext)
			s.OperationStore = &pb.OperationStoreControl{FormatVersion: operationStoreVersion}
			s.OperationRecords = chasm.NewDataMap[*pb.OperationState](ctx)
			s.RequestRecords = chasm.NewDataMap[*pb.Payload](ctx)
			changes := []operationRecordChange{{id: "new", operation: &pb.OperationState{Id: "new"}}}
			switch name {
			case "overflow":
				s.OperationStore.OperationCount = math.MaxInt64
			case "underflow":
				s.OperationRecords.Put(ctx, "old", &pb.OperationState{Id: "old"})
				changes = []operationRecordChange{{id: "old", changeRequest: true}}
			case "duplicate":
				changes = append(changes, changes[0])
			case "budget":
				changes = append(changes, changes[0], changes[0], changes[0])
			case "retained request":
				s.OperationRecords.Put(ctx, "old", &pb.OperationState{Id: "old"})
				s.RequestRecords.Put(ctx, "old", &pb.Payload{Data: []byte("frozen")})
				s.OperationStore.OperationCount = 1
				s.OperationStore.RequestCount = 1
				changes = []operationRecordChange{{id: "old"}}
			default:
				t.Fatal("unknown fixture")
			}
			h.commit(true)
			before := cloneNodes(h.durable)
			s, raw = h.root(true)
			ctx = raw.(chasm.MutableContext)
			control := common.CloneProto(s.OperationStore)
			_, err := s.prepareOperationRecords(ctx, changes)
			require.Error(t, err)
			require.Equal(t, control, s.OperationStore)
			h.commit(true)
			// Opening a mutable component advances CHASM transition metadata even
			// without a semantic write. Compare every persisted DATA value and key.
			require.Len(t, h.durable, len(before))
			for key, node := range before {
				require.Contains(t, h.durable, key)
				require.Equal(t, node.Data, h.durable[key].Data)
			}
		})
	}
}

func TestOperationStoreDescribeRejectsInvalidRepresentation(t *testing.T) {
	t.Parallel()
	for _, mixed := range []bool{false, true} {
		name := "unknown format"
		if mixed {
			name = "mixed records"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			limit := 5
			h := publicCapacityHarness(t, &limit, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			publicProjectionAdmission(h)
			s, ctx := h.root(true)
			s.OperationStore = &pb.OperationStoreControl{FormatVersion: 99}
			if mixed {
				s.OperationStore.FormatVersion = operationStoreVersion
			}
			description, err := s.DescribePublic(ctx)
			require.Error(t, err)
			require.Nil(t, description)
		})
	}
}
