package scheduler2

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
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
	var releaseOnce sync.Once
	releaseRPC := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseRPC()
	handler := NewEffectHandler(func(_ context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) error {
		entered <- struct{}{}
		<-release
		request.WorkflowExecution.WorkflowId = "mutated by client"
		return nil
	}, nil)
	e, id, physical := newEffectsEngine(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, handler)
	expected := effectSnapshot(e.h, id).Cancel
	initialRoot, _ := e.h.root(false)
	initialWake := initialRoot.Effects[0].RetryAt
	node := e.h.node
	first := make(chan error, 1)
	go func() { first <- e.execute(e.ctx, node, physical) }()
	select {
	case <-entered:
	case <-t.Context().Done():
		t.Fatal("RPC did not start")
	}
	cancelled, cancel := context.WithCancel(e.ctx)
	defer cancel()
	reached := make(chan struct{})
	checked := &effectsPermitContext{Context: cancelled, reached: reached}
	second := make(chan error, 1)
	go func() {
		second <- handler.Execute(checked, chasm.NewComponentRef[*Scheduler](e.key), chasm.TaskAttributes{}, &scheduler2pb.EffectTask{EffectId: id, WakeTime: initialWake})
	}()
	select {
	case <-reached: // Done() was evaluated by the occupied permit select.
	case <-entered:
		t.Fatal("second RPC bypassed occupied permit")
	case err := <-second:
		t.Fatalf("second delivery returned before cancellation: %v", err)
	case <-t.Context().Done():
		t.Fatal("second delivery did not reach permit select")
	}
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
	releaseRPC()
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

func TestEffectsTaskDeadlineBoundary(t *testing.T) {
	t.Parallel()
	for _, offset := range []int64{-1, 0, 1} {
		name := "exact"
		if offset < 0 {
			name = "one_nanosecond_before"
		} else if offset > 0 {
			name = "one_nanosecond_after"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			handler := NewEffectHandler(func(context.Context, *workflowservice.RequestCancelWorkflowExecutionRequest) error {
				calls++
				return nil
			}, nil)
			e, _, physical := newEffectsEngine(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, handler)
			s, _ := e.h.root(false)
			blocker, remaining, started := s.BlockingOperationId, s.Remaining, s.Started
			queue := proto.Clone(s.RootState).(*scheduler2pb.RootState).Queue
			e.h.now = s.Effects[0].Deadline + offset
			require.NoError(t, e.execute(e.ctx, e.h.node, physical))
			expectedCalls := 0
			expectedPhase := decision.EffectAmbiguous
			if offset < 0 {
				expectedCalls = 1
				expectedPhase = decision.EffectAccepted
			}
			require.Equal(t, expectedCalls, calls)
			assertEffectsPhase(e.h, 0, expectedPhase)
			s, _ = e.h.root(false)
			require.Equal(t, blocker, s.BlockingOperationId)
			require.Equal(t, remaining, s.Remaining)
			require.Equal(t, started, s.Started)
			protorequire.ProtoEqual(t, &scheduler2pb.RootState{Queue: queue}, &scheduler2pb.RootState{Queue: s.Queue})
			require.Equal(t, int64(1), s.Unfinished)
			require.False(t, s.Coordination.Armed)
		})
	}
}

func TestEffectsTaskOriginalPhysicalInvalidation(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"retry", "completion", "terminate"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			calls := 0
			handler := NewEffectHandler(func(context.Context, *workflowservice.RequestCancelWorkflowExecutionRequest) error {
				calls++
				return nil
			}, nil)
			e, id, physical := newEffectsEngine(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, handler)
			_, valid, err := e.h.node.ValidateSideEffectTask(t.Context(), physical)
			require.NoError(t, err)
			require.True(t, valid)
			s, _ := e.h.root(false)
			switch change {
			case "retry":
				effectsResult(e.h, id, decision.EffectFailed, true)
			case "completion":
				sequentialComplete(e.h, s.BlockingOperationId, nil)
			case "terminate":
				allowUpdate(e.h, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER)
				allowDue(e.h, true)
			default:
				t.Fatal("unsupported invalidation")
			}
			_, valid, err = e.h.node.ValidateSideEffectTask(t.Context(), physical)
			require.NoError(t, err)
			require.False(t, valid)
			// The actual dispatch boundary also refuses the stale envelope before RPC.
			err = e.execute(e.ctx, e.h.node, physical)
			require.Zero(t, calls)
			require.Error(t, err)
		})
	}
}

// Done is evaluated on entry to the handler's permit select, before cancellation.
// Calling the handler directly avoids unrelated framework context probes.
type effectsPermitContext struct {
	context.Context
	reached chan struct{}
	once    sync.Once
}

func (c *effectsPermitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.reached) })
	return c.Context.Done()
}
