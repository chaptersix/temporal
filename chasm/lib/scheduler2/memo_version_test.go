package scheduler2

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

func TestMemoVersionLegacyBaselineAndNestedReservation(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	require.Zero(t, s.MemoRevision)
	require.Equal(t, uint64(1), s.MemoVersion(nil))
	s.PublicRevision = 900
	reserved, err := s.reserveMemoVersion(ctx)
	require.NoError(t, err)
	require.Zero(t, s.MemoRevision)
	// ContextWithValue retains the concrete context expected by addressed DATA.
	s.QueuePages = chasm.NewDataMap[*commonpb.Payload](reserved)
	s.QueuePages.Put(reserved, "witness", &commonpb.Payload{Data: []byte("value")})
	s.publishMemoVersion(reserved)
	require.Equal(t, uint64(2), s.MemoRevision)
	nested, err := s.reserveMemoVersion(reserved)
	require.NoError(t, err)
	s.publishMemoVersion(nested)
	require.Equal(t, uint64(2), s.MemoRevision)
	require.Equal(t, uint64(900), s.PublicRevision)
	h.commit(true)
	s, raw = h.root(false)
	require.Equal(t, uint64(2), s.MemoVersion(raw))
	value, ok := s.QueuePages.Get(raw, "witness")
	require.True(t, ok)
	require.Equal(t, "value", string(value.Data))
}

func TestMemoVersionReservationLimits(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	s.MemoRevision = math.MaxUint64 - 1
	reserved, err := s.reserveMemoVersion(ctx)
	require.NoError(t, err)
	s.publishMemoVersion(reserved)
	require.Equal(t, uint64(math.MaxUint64), s.MemoRevision)
	nested, err := s.reserveMemoVersion(reserved)
	require.NoError(t, err)
	s.publishMemoVersion(nested)
	require.Equal(t, uint64(math.MaxUint64), s.MemoRevision)
	_, err = s.reserveMemoVersion(ctx)
	require.ErrorContains(t, err, "memo revision exhausted")
	other := New("other", 1)
	require.Panics(t, func() { other.publishMemoVersion(reserved) })
	s.MemoRevision = 5
	_, err = s.reserveMemoVersion(reserved)
	require.ErrorContains(t, err, "stale")
	require.Panics(t, func() { s.publishMemoVersion(reserved) })
	require.Panics(t, func() { s.publishMemoVersion(ctx) })
}

func TestMemoVersionAcknowledgmentAndCallback(t *testing.T) {
	t.Parallel()
	for _, callbackFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(callbackFirst), func(t *testing.T) {
			h := executingPublicTree(t, nil)
			id := publicProjectionAdmission(h)
			s, ctx := h.root(false)
			conflict := s.PublicRevision
			version := s.MemoVersion(ctx)
			memo := common.CloneProto(s.Memo(ctx))
			before := cloneNodes(h.durable)
			ack := decision.Event{Kind: decision.Acknowledged, Incarnation: s.Incarnation, OperationID: id, ChainID: "chain", PublicRunID: "run", StartObserved: true, StartObservedAt: h.now + 1}
			callback := decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: id, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}
			if callbackFirst {
				h.apply(callback, true)
				s, ctx = h.root(false)
				require.Greater(t, s.MemoVersion(ctx), version)
				require.Equal(t, conflict, s.PublicRevision)
				version = s.MemoVersion(ctx)
				memo = common.CloneProto(s.Memo(ctx))
				before = cloneNodes(h.durable)
			}
			h.apply(ack, false)
			require.Equal(t, before, h.durable)
			s, ctx = h.root(false)
			require.Equal(t, version, s.MemoVersion(ctx))
			h.apply(ack, true)
			s, ctx = h.root(false)
			require.Greater(t, s.MemoVersion(ctx), version)
			require.Equal(t, conflict, s.PublicRevision)
			require.False(t, proto.Equal(memo, s.Memo(ctx)))
			if !callbackFirst {
				version = s.MemoVersion(ctx)
				memo = common.CloneProto(s.Memo(ctx))
				h.apply(callback, true)
				s, ctx = h.root(false)
				require.Greater(t, s.MemoVersion(ctx), version)
				require.Equal(t, conflict, s.PublicRevision)
				require.False(t, proto.Equal(memo, s.Memo(ctx)))
			}
		})
	}
}

func TestMemoVersionOverflowBeforePublicWrites(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, _ := h.root(true)
	s.MemoRevision = math.MaxUint64
	h.commit(true)
	before, physical := cloneNodes(h.durable), len(h.physical)
	s, raw := h.root(true)
	err := s.PatchPublic(raw.(chasm.MutableContext), &schedulepb.SchedulePatch{Pause: "must not publish"})
	require.ErrorContains(t, err, "memo revision exhausted")
	require.False(t, s.Paused)
	requireRejectedCoordinatorUnchanged(h, before, physical)
}

func TestMemoVersionPauseUsesOneReservation(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	id := publicProjectionAdmission(h)
	s, _ := h.root(true)
	s.PauseOnFailure = true
	s.MemoRevision = math.MaxUint64 - 1
	conflict := s.PublicRevision
	h.commit(true)
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: id, ChainID: "chain", Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED}, true)
	s, ctx := h.root(false)
	require.Equal(t, uint64(math.MaxUint64), s.MemoVersion(ctx))
	require.Equal(t, conflict+1, s.PublicRevision)
	memo := s.Memo(ctx).(*schedulepb.ScheduleListInfo)
	require.True(t, memo.Paused)
	require.Contains(t, memo.Notes, "failed")
}

func TestMemoVersionGenerationPausedOverride(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	version, conflict := s.MemoVersion(ctx), s.PublicRevision
	config := UpdateConfig{Generation: publicGeneration(ctx, "namespace", "schedule", s.PublicSchedule.Get(ctx), s.Revision+1, schedulerconfig.DefaultTweakables), Paused: true, Limited: s.Limited, Remaining: s.Remaining, PauseOnFailure: s.PauseOnFailure}
	require.NoError(t, s.UpdateConfiguration(ctx, config))
	h.commit(true)
	s, raw = h.root(false)
	require.Greater(t, s.MemoVersion(raw), version)
	require.Equal(t, conflict, s.PublicRevision)
	require.False(t, s.PublicList.Get(raw).Paused)
	require.True(t, s.Memo(raw).(*schedulepb.ScheduleListInfo).Paused)
}

func TestMemoVersionDoesNotReadList(t *testing.T) {
	t.Parallel()
	s := New("incarnation", 1)
	s.MemoRevision = 17
	// An invalid staged list would fail if MemoVersion attempted Memo assembly.
	s.PublicList = chasm.NewDataField(nil, (*schedulepb.ScheduleListInfo)(nil))
	require.Equal(t, uint64(17), s.MemoVersion(nil))
	require.Panics(t, func() { s.Memo(nil) })
}

func TestMemoVersionLegacyPublicRecord(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, _ := h.root(true)
	require.Equal(t, uint64(1), s.MemoVersion(nil))
	s.MemoRevision = 0
	h.commit(true)
	s, raw := h.root(true)
	require.Zero(t, s.MemoRevision)
	require.NoError(t, s.PatchPublic(raw.(chasm.MutableContext), &schedulepb.SchedulePatch{Pause: "legacy pause"}))
	h.commit(true)
	s, raw = h.root(false)
	require.Equal(t, uint64(2), s.MemoRevision)
	require.True(t, s.Memo(raw).(*schedulepb.ScheduleListInfo).Paused)
}

func TestMemoVersionOverflowBeforeTransition(t *testing.T) {
	t.Parallel()
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			h := executingPublicTree(t, nil)
			id := publicProjectionAdmission(h)
			s, _ := h.root(true)
			s.MemoRevision = math.MaxUint64
			s.PauseOnFailure = true
			h.commit(true)
			before, physical := cloneNodes(h.durable), len(h.physical)
			s, raw := h.root(true)
			event := decision.Event{Kind: decision.Acknowledged, Incarnation: s.Incarnation, OperationID: id, ChainID: "chain", PublicRunID: "run"}
			if failure {
				event.Kind = decision.Callback
				event.Outcome = decision.Failure
				event.Status = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
			}
			err := s.Apply(raw.(chasm.MutableContext), event)
			require.ErrorContains(t, err, "memo revision exhausted")
			requireRejectedCoordinatorUnchanged(h, before, physical)
		})
	}
}

func TestMemoVersionPrivatePausedProjection(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	s, raw := h.root(true)
	ctx, err := s.reserveMemoVersion(raw.(chasm.MutableContext))
	require.NoError(t, err)
	s.PublicList = chasm.NewDataField(ctx, &schedulepb.ScheduleListInfo{})
	s.publishMemoVersion(ctx)
	h.commit(true)
	s, raw = h.root(false)
	before := s.MemoVersion(raw)
	require.Zero(t, s.PublicRevision)
	h.apply(decision.Event{Kind: decision.Update, Incarnation: s.Incarnation, Revision: s.Revision + 1, Paused: true, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}, true)
	s, raw = h.root(false)
	require.Equal(t, before+1, s.MemoVersion(raw))
	require.Zero(t, s.PublicRevision)
	require.True(t, s.Memo(raw).(*schedulepb.ScheduleListInfo).Paused)
}
