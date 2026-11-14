package scheduler2

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

func TestDefinitionReplacementPreflightAndPublication(t *testing.T) {
	t.Parallel()
	initial := stagedInterval(t, 1, "old", 1)
	replacement := stagedInterval(t, 2, "new", 2)
	h := newDefinitionHarness(t, initial)
	installDefinition(t, h, initial)
	p, ctx := definitionRoot(t, h, true)
	before := common.CloneProto(p.DefinitionPreparationState)
	sourceBefore, _, _, err := p.SourceSnapshot(ctx)
	require.NoError(t, err)
	plan, err := p.prepareDefinitionReplacement(ctx, replacement)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, p.DefinitionPreparationState))
	sourceAfter, _, _, err := p.SourceSnapshot(ctx)
	require.NoError(t, err)
	require.True(t, proto.Equal(sourceBefore, sourceAfter))
	_, exists := p.Superseded.Get(ctx, plan.cleanupKey)
	require.False(t, exists)
	expectedSource := common.CloneProto(replacement.source)
	replacement.source.Seed[0] = 'x'
	p.publishDefinitionReplacement(ctx.(chasm.MutableContext), plan)
	require.Equal(t, int64(2), p.Revision)
	require.Equal(t, uint64(1), p.CleanupCount)
	published, _, _, err := p.SourceSnapshot(ctx)
	require.NoError(t, err)
	require.True(t, proto.Equal(expectedSource, published))
	retired, exists := p.Superseded.Get(ctx, plan.cleanupKey)
	require.True(t, exists)
	require.Equal(t, initial.owner[:], retired.OwnerId)
	require.Equal(t, uint64(len(initial.pages)), retired.InstalledPages)
	h.commit(true)
}
func TestDefinitionReplacementRejectionPrecedesPublication(t *testing.T) {
	t.Parallel()
	initial := stagedInterval(t, 1, "old", 1)
	h := newDefinitionHarness(t, initial)
	p, ctx := definitionRoot(t, h, true)
	before := common.CloneProto(p.DefinitionPreparationState)
	plan, err := p.prepareDefinitionReplacement(ctx, initial)
	require.ErrorContains(t, err, "newer")
	require.Nil(t, plan)
	require.True(t, proto.Equal(before, p.DefinitionPreparationState))
	_, exists := p.Superseded.Get(ctx, "0")
	require.False(t, exists)
	h.commit(false)
}
