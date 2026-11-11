package scheduler2

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

func newEffectsEngine(t *testing.T, policy enumspb.ScheduleOverlapPolicy, handler *EffectHandler) (*taskEngine, string, *tasks.ChasmTask) {
	t.Helper()
	e := newTaskEngine(t, func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		return "chain", nil
	})
	h := e.h
	sequentialBudget(h, 10)
	h.now = generationStart().UnixNano()
	s, _ := h.root(false)
	target := s.OperationId
	request, _ := frozenRequest(t)
	configureBuffer(h, &schedulepb.ScheduleSpec{}, s.Revision+1, policy, time.Hour, request)
	fireGeneration(h, true)
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewLibrary(nil, handler)))
	h.registry = registry
	h.reload()
	allowDue(h, true)
	s, _ = h.root(false)
	require.Len(t, s.Effects, 1)
	require.Equal(t, decision.AwaitingTarget, decision.EffectPhase(s.Effects[0].Phase))
	allowAck(h, target)
	s, _ = h.root(false)
	return e, s.Effects[0].Id, h.physical[len(h.physical)-1]
}

func TestEffectsTaskLostAckFrozenRetryAndNotFound(t *testing.T) {
	t.Parallel()
	var observed []*workflowservice.RequestCancelWorkflowExecutionRequest
	handler := NewEffectHandler(func(_ context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) error {
		observed = append(observed, proto.Clone(request).(*workflowservice.RequestCancelWorkflowExecutionRequest))
		if len(observed) == 1 {
			return errors.New("remote accepted; acknowledgment lost")
		}
		return serviceerror.NewNotFound("chain already absent")
	}, nil)
	e, id, physical := newEffectsEngine(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, handler)
	require.NoError(t, e.execute(e.ctx, e.h.node, physical))
	assertEffectsPhase(e.h, 0, decision.EffectPending)
	var successor *tasks.ChasmTask
	for _, batch := range e.h.backend.TasksByCategory {
		for _, task := range batch {
			if candidate, ok := task.(*tasks.ChasmTask); ok {
				successor = candidate
			}
		}
	}
	require.NotNil(t, successor)
	s, _ := e.h.root(false)
	at := s.Effects[0].RetryAt
	replacement, _ := frozenRequest(t)
	replacement.Namespace = "changed"
	replacement.WorkflowId = "changed"
	configureBuffer(e.h, &schedulepb.ScheduleSpec{}, s.Revision+1, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, time.Hour, replacement)
	fireGeneration(e.h, true)
	e.h.now = at
	require.NoError(t, e.execute(e.ctx, e.h.node, successor))
	require.Len(t, observed, 2)
	protorequire.ProtoEqual(t, observed[0], observed[1])
	assertEffectsPhase(e.h, 0, decision.EffectAbsent)
	s, _ = e.h.root(false)
	require.Equal(t, int64(1), s.Unfinished)
	require.NotEmpty(t, s.BlockingOperationId)
	require.Len(t, s.Queue, 1)
	require.Nil(t, effectSnapshot(e.h, id))
}

func TestEffectsTaskTerminateOverInFlightCancel(t *testing.T) {
	t.Parallel()
	var e *taskEngine
	var terminated *workflowservice.TerminateWorkflowExecutionRequest
	handler := NewEffectHandler(func(context.Context, *workflowservice.RequestCancelWorkflowExecutionRequest) error {
		allowUpdate(e.h, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER)
		allowDue(e.h, true)
		return nil
	}, func(_ context.Context, request *workflowservice.TerminateWorkflowExecutionRequest) error {
		terminated = proto.Clone(request).(*workflowservice.TerminateWorkflowExecutionRequest)
		return nil
	})
	var physical *tasks.ChasmTask
	e, _, physical = newEffectsEngine(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, handler)
	require.NoError(t, e.execute(e.ctx, e.h.node, physical))
	assertEffectsPhase(e.h, 0, decision.EffectSuperseded)
	assertEffectsPhase(e.h, 1, decision.EffectPending)
	terminateTask := e.h.physical[len(e.h.physical)-1]
	require.NoError(t, e.execute(e.ctx, e.h.node, terminateTask))
	require.NotNil(t, terminated)
	require.NotEmpty(t, terminated.FirstExecutionRunId)
	require.Empty(t, terminated.WorkflowExecution.RunId)
	assertEffectsPhase(e.h, 1, decision.EffectAccepted)
	s, _ := e.h.root(false)
	require.Equal(t, int64(1), s.Unfinished)
	require.Len(t, s.Queue, 2)
}

func TestEffectsTaskPermitAndDetachedRequest(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	handler := NewEffectHandler(func(_ context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) error {
		entered <- struct{}{}
		<-release
		request.WorkflowExecution.WorkflowId = "mutated by client"
		return nil
	}, nil)
	e, id, physical := newEffectsEngine(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, handler)
	expected := effectSnapshot(e.h, id).Cancel
	node := e.h.node
	first := make(chan error, 1)
	go func() { first <- e.execute(e.ctx, node, physical) }()
	select {
	case <-entered:
	case <-t.Context().Done():
		t.Fatal("RPC did not start")
	}
	cancelled, cancel := context.WithCancel(e.ctx)
	second := make(chan error, 1)
	copyTask := *physical
	go func() { second <- e.execute(cancelled, node, &copyTask) }()
	cancel()
	select {
	case err := <-second:
		require.ErrorIs(t, err, context.Canceled)
	case <-t.Context().Done():
		t.Fatal("permit waiter did not cancel")
	}
	select {
	case <-entered:
		t.Fatal("two concurrent effects exceeded permit")
	default:
	}
	close(release)
	select {
	case err := <-first:
		require.NoError(t, err)
	case <-t.Context().Done():
		t.Fatal("RPC did not finish")
	}
	s, ctx := e.h.root(false)
	stored := &workflowservice.RequestCancelWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(s.EffectRequests[id].Get(ctx).Data, stored))
	protorequire.ProtoEqual(t, expected, stored)
	assertEffectsPhase(e.h, 0, decision.EffectAccepted)
}

func TestEffectsTaskCancellationAfterRemoteAcceptance(t *testing.T) {
	t.Parallel()
	var cancel context.CancelFunc
	handler := NewEffectHandler(func(context.Context, *workflowservice.RequestCancelWorkflowExecutionRequest) error {
		cancel()
		return nil
	}, nil)
	e, id, physical := newEffectsEngine(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, handler)
	ctx, cancelFn := context.WithCancel(e.ctx)
	cancel = cancelFn
	defer cancel()
	require.ErrorIs(t, e.execute(ctx, e.h.node, physical), context.Canceled)
	assertEffectsPhase(e.h, 0, decision.EffectPending)
	require.NotNil(t, effectSnapshot(e.h, id))
	require.Equal(t, 1, logicalCount(e.h.durable))
}
