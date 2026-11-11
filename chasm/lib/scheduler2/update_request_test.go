package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

func requestUpdateConfig(t *testing.T, revision int64) UpdateConfig {
	t.Helper()
	request, _ := frozenRequest(t)
	request.WorkflowId = "replacement-template"
	return UpdateConfig{Generation: GenerationConfig{
		Spec: &schedulepb.ScheduleSpec{}, Seed: "replacement-seed", Revision: revision,
		Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, CatchupWindow: 2 * time.Minute, Request: request,
	}, Paused: true, Limited: true, Remaining: 7, PauseOnFailure: true}
}

// This exercises real Node request-ID DATA and serialized publication under an
// option-aware mock engine. It does not establish History storage or API conflict
// tokens; deduplication is bounded by the framework retention configuration.
func TestUpdateRequestDedupAndAtomicPublication(t *testing.T) {
	t.Parallel()
	e := newManualCommandEngine(t)
	before := cloneNodes(e.h.durable)
	config := requestUpdateConfig(t, 2)
	e.failPublication = true
	require.ErrorIs(t, Update(e.ctx, e.ref, "update", config), errManualPublication)
	require.Equal(t, before, e.h.durable, "failed replacement must preserve all definition, root and task nodes")
	used, err := e.h.node.HasRequestID("update", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
	e.failPublication = false
	require.NoError(t, Update(e.ctx, e.ref, "update", config))
	used, err = e.h.node.HasRequestID("update", e.maxAge)
	require.NoError(t, err)
	require.True(t, used)
	s, ctx := e.h.root(false)
	require.Equal(t, int64(2), s.Revision)
	require.True(t, s.Paused)
	require.True(t, s.Limited)
	require.True(t, s.PauseOnFailure)
	require.Equal(t, int64(7), s.Remaining)
	require.Equal(t, config.Generation.Policy, s.Policy)
	g := s.Generator.Get(ctx)
	require.Equal(t, int64(2*time.Minute), g.CatchupWindow)
	definition := g.Definition.Get(ctx)
	require.Equal(t, "replacement-seed", definition.Seed)
	var template workflowservice.StartWorkflowExecutionRequest
	require.NoError(t, proto.Unmarshal(definition.Request, &template))
	protorequire.ProtoEqual(t, config.Generation.Request, &template)
	var spec schedulepb.ScheduleSpec
	require.NoError(t, proto.Unmarshal(definition.Spec, &spec))
	protorequire.ProtoEqual(t, config.Generation.Spec, &spec)
	committed := cloneNodes(e.h.durable)
	e.h.now += int64(time.Second)
	require.NoError(t, Update(e.ctx, e.ref, "update", UpdateConfig{}), "retained success wins over invalid changed body")
	require.NoError(t, Update(e.ctx, e.ref, "update", requestUpdateConfig(t, 3)), "same request ID cannot apply a new revision")
	require.Equal(t, committed, e.h.durable)
	err = Update(e.ctx, e.ref, "stale", config)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
	used, err = e.h.node.HasRequestID("stale", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
	require.Equal(t, committed, e.h.durable)
	s, ctx = e.h.root(true)
	require.NoError(t, s.Close(ctx.(chasm.MutableContext)))
	commitGeneration(e.h, true)
	closed := cloneNodes(e.h.durable)
	require.NoError(t, Update(e.ctx, e.ref, "update", UpdateConfig{}))
	err = Update(e.ctx, e.ref, "closed-new", requestUpdateConfig(t, 3))
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	used, err = e.h.node.HasRequestID("closed-new", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
	require.Equal(t, closed, e.h.durable)
}

func TestUpdateRequestDedupBoundedRetention(t *testing.T) {
	t.Parallel()
	e := newManualCommandEngine(t)
	e.maxCount = 1
	first := requestUpdateConfig(t, 2)
	require.NoError(t, Update(e.ctx, e.ref, "first", first))
	e.h.now += int64(time.Second)
	require.NoError(t, Update(e.ctx, e.ref, "second", requestUpdateConfig(t, 3)))
	used, err := e.h.node.HasRequestID("first", e.maxAge)
	require.NoError(t, err)
	require.False(t, used, "evicted IDs no longer return cached success")
	before := cloneNodes(e.h.durable)
	err = Update(e.ctx, e.ref, "first", first)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition), "revision fencing still prevents reapplying an evicted update")
	require.Equal(t, before, e.h.durable)
	used, err = e.h.node.HasRequestID("first", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
}
