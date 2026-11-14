//go:build test_dep

package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

func TestPreparedPendingTransferIndependentOfTimezoneInstall(t *testing.T) {
	t.Parallel()
	h := preparedRuntimeTree(t)
	h.now += int64(5 * time.Second)
	for n := 0; n < 40; n++ {
		s, raw := h.root(true)
		ctx := chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
		require.NoError(t, s.scanAutomatic(ctx))
		pending := s.Generator.Get(ctx).Continuation.Pending != nil
		h.commit(true)
		if pending {
			break
		}
		require.Less(t, n, 39)
	}
	s, read := h.root(false)
	g := s.Generator.Get(read)
	pending := g.Continuation.Pending
	require.NotNil(t, pending)
	p := g.PreparedDefinition.Get(read)
	owner := [32]byte(p.OwnerId)
	// A source refresh retains the definition and completed candidate while its
	// replacement fixed timezone pages are still being installed.
	data := largeTimezoneFixture()
	resolvedAt := time.Unix(0, h.now)
	zone, err := occurrence.PrepareTimezone(data, resolvedAt)
	require.NoError(t, err)
	stage, err := stageTimezoneForDefinition(owner, "UTC", data, resolvedAt, 2, false, zone)
	require.NoError(t, err)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	p = s.Generator.Get(ctx).PreparedDefinition.Get(ctx)
	require.NoError(t, p.BindTimezone(ctx, stage))
	require.Positive(t, p.Timezone.Get(ctx).TotalPages)
	require.Zero(t, p.Timezone.Get(ctx).InstalledPages)
	h.commit(true)
	s, raw = h.root(true)
	ctx = chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
	require.NoError(t, s.transferAutomatic(ctx), "completed pending authority must not depend on installing a later timezone snapshot")
	require.Nil(t, s.Generator.Get(ctx).Continuation.Pending)
	require.EqualValues(t, 1, s.OperationStore.OperationCount)
	h.commit(true)
}

func TestPreparedTransferValidatesDispositionBeforeAdmission(t *testing.T) {
	t.Parallel()
	h := preparedRuntimeTree(t)
	h.now += int64(5 * time.Second)
	for n := 0; n < 40; n++ {
		s, raw := h.root(true)
		ctx := chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
		require.NoError(t, s.scanAutomatic(ctx))
		pending := s.Generator.Get(ctx).Continuation.Pending != nil
		h.commit(true)
		if pending {
			break
		}
		require.Less(t, n, 39)
	}
	s, raw := h.root(true)
	ctx := chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
	g := s.Generator.Get(ctx)
	require.NotNil(t, g.Continuation.Pending)
	g.Continuation.Pending.Revision++
	beforeRoot := common.CloneProto(s.RootState)
	beforeCursor := common.CloneProto(g.Continuation)
	require.Error(t, s.transferAutomatic(ctx))
	require.True(t, proto.Equal(beforeRoot, s.RootState), "rejected disposition must not admit operation or modify owners")
	require.True(t, proto.Equal(beforeCursor, g.Continuation))
}
