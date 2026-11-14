//go:build test_dep

package scheduler2

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

func stagedUpdateConfig(t *testing.T, h *treeHarness) UpdateConfig {
	t.Helper()
	s, _ := h.root(false)
	spec := generationSpec(generationStart(), generationStart().Add(time.Hour), 3*time.Second)
	stage, err := StageDefinitionSource(spec, "replacement seed", s.Revision+1)
	require.NoError(t, err)
	request, _ := frozenRequest(t)
	request.LastCompletionResult = nil
	return UpdateConfig{Generation: GenerationConfig{prepared: stage, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, Revision: stage.revision, Seed: "replacement seed", Spec: spec, CatchupWindow: time.Hour, Request: request}, Paused: true}
}

func TestStagedGenerationReplacementFencesOldTask(t *testing.T) {
	t.Parallel()
	e := newSourceTaskHarness(t, 1000, "UTC")
	require.NoError(t, e.execute(e.task(t)))
	oldTask := e.task(t)
	oldRequest := e.request(t)
	h := e.h
	s, read := h.root(false)
	old := common.CloneProto(s.Generator.Get(read).PreparedDefinition.Get(read).DefinitionPreparationState)
	require.Positive(t, old.InstalledPages)
	config := stagedUpdateConfig(t, h)
	for _, path := range []string{"Generator$Definition", "Generator$PreparedDefinition$Source"} {
		h.durable[path].Data.Data = []byte{0xff}
	}
	h.reload()
	s, raw := h.root(true)
	ctx := chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
	require.NoError(t, s.UpdateConfiguration(ctx, config))
	for _, path := range []string{"Generator/Definition", "Generator/PreparedDefinition/Source"} {
		require.NotContains(t, h.node.MaterializedPathsForTest(), path)
	}
	h.commit(true)
	current := e.request(t)
	require.Equal(t, sourceCheckAutomatic, current.CheckKind)
	require.Greater(t, current.Token, oldRequest.Token)
	require.NotEqual(t, oldRequest.DefinitionOwnerId, current.DefinitionOwnerId)
	s, read = h.root(false)
	p := s.Generator.Get(read).PreparedDefinition.Get(read)
	require.EqualValues(t, 1, p.CleanupCount)
	require.Zero(t, p.InstalledPages)
	require.Equal(t, config.Generation.Revision, p.Revision)
	beforeUpdates := e.updates
	require.ErrorContains(t, e.execute(oldTask), "task is no longer valid")
	require.Equal(t, beforeUpdates, e.updates)
	e.finish(t)
	s, read = h.root(false)
	require.False(t, s.sourcePreparationPending(read))
	require.Equal(t, config.Generation.Revision, s.Generator.Get(read).Continuation.Revision)
	require.NotNil(t, s.Generator.Get(read).Continuation.Pending, "installation services its first lookup without another source request")
}

func TestStagedGenerationRejectsBeforePublication(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"source token", "cleanup token", "binding", "missing stage"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			e := newSourceTaskHarness(t, 1000, "UTC")
			h := e.h
			config := stagedUpdateConfig(t, h)
			s, raw := h.root(true)
			switch failure {
			case "source token":
				r := common.CloneProto(s.SourceRequest.Get(raw))
				r.Token = math.MaxUint64
				s.SourceRequest = chasm.NewDataField(raw.(chasm.MutableContext), r)
			case "cleanup token":
				p := s.Generator.Get(raw).PreparedDefinition.Get(raw)
				p.CleanupHead, p.CleanupTail = math.MaxUint64, math.MaxUint64
			case "binding":
				config.Generation.Seed = "wrong"
			case "missing stage":
				config.Generation.prepared = nil
			default:
				t.Fatalf("unknown failure %q", failure)
			}
			h.commit(true)
			s, raw = h.root(true)
			before := common.CloneProto(s.RootState)
			p := common.CloneProto(s.Generator.Get(raw).PreparedDefinition.Get(raw).DefinitionPreparationState)
			request := common.CloneProto(s.SourceRequest.Get(raw))
			require.Error(t, s.UpdateConfiguration(raw.(chasm.MutableContext), config))
			require.True(t, proto.Equal(before, s.RootState))
			require.True(t, proto.Equal(p, s.Generator.Get(raw).PreparedDefinition.Get(raw).DefinitionPreparationState))
			require.True(t, proto.Equal(request, s.SourceRequest.Get(raw)))
			h.commit(false)
		})
	}
}
