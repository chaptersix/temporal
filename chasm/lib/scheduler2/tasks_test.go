package scheduler2

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

// taskEngine preserves the framework's reference validation at every locked
// access while exercising the real side-effect dispatch path outside that lock.
type taskEngine struct {
	mu  sync.Mutex
	h   *treeHarness
	ctx context.Context
	key chasm.ExecutionKey
}

func newTaskEngine(t *testing.T, start func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error)) *taskEngine {
	t.Helper()
	h := newTreeHarness(t)
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewLibrary(NewStartHandler(start))))
	h.registry = registry
	h.reload()
	h.admit()
	e := &taskEngine{h: h, key: chasm.ExecutionKey{NamespaceID: "namespace", BusinessID: "schedule", RunID: "incarnation"}}
	mock := chasm.NewMockEngine(gomock.NewController(t))
	mock.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, ref chasm.ComponentRef, read func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
			e.mu.Lock()
			defer e.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return err
			}
			cc := chasm.NewContext(ctx, h.node)
			component, err := h.node.Component(cc, ref)
			if err != nil {
				return err
			}
			return read(cc, component)
		})
	mock.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, ref chasm.ComponentRef, update func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
			e.mu.Lock()
			defer e.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			cc := chasm.NewMutableContext(ctx, h.node)
			component, err := h.node.Component(cc, ref)
			if err != nil {
				return nil, err
			}
			if err := update(cc, component); err != nil {
				return nil, err
			}
			if _, err := h.node.CloseTransaction(ctx, true); err != nil {
				return nil, err
			}
			h.durable = cloneNodes(h.node.Snapshot(nil).Nodes)
			h.transition++
			return nil, nil
		})
	e.ctx = chasm.NewEngineContext(t.Context(), mock)
	return e
}

func (e *taskEngine) execute(ctx context.Context, node *chasm.Node, task *tasks.ChasmTask) error {
	return node.ExecuteSideEffectTask(ctx, e.key, task, func(chasm.NodeBackend, chasm.Context, chasm.Component) error { return nil })
}

func TestTaskLateAcknowledgmentAfterExpiry(t *testing.T) {
	t.Parallel()
	var e *taskEngine
	e = newTaskEngine(t, func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		// Expiry commits while the remote RPC is outstanding, invalidating its task.
		s, ctx := e.h.root(false)
		deadline := s.Operations[s.OperationId].Get(ctx).Deadline
		e.h.now = deadline
		e.h.apply(decision.Event{Kind: decision.Expire, Incarnation: "incarnation", OperationID: s.OperationId}, true)
		return "chain", nil
	})
	require.NoError(t, e.execute(e.ctx, e.h.node, e.h.physical[0]))
	s, ctx := e.h.root(false)
	require.Equal(t, int64(1), s.Started)
	require.Equal(t, decision.Started, decision.Phase(s.Operations[s.OperationId].Get(ctx).Phase))
}

func TestTaskRollbackKeepsPhysicalDeliveryRetryable(t *testing.T) {
	t.Parallel()
	calls := 0
	e := newTaskEngine(t, func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		calls++
		return "chain", nil
	})
	physical := e.h.physical[0]
	e.h.now--
	require.ErrorContains(t, e.execute(e.ctx, e.h.node, physical), "not due")
	require.Zero(t, calls)
	require.Equal(t, 1, logicalCount(e.h.durable))
	e.h.now++
	require.NoError(t, e.execute(e.ctx, e.h.node, physical))
	require.Equal(t, 1, calls)
}

func TestTaskCancellationAndConcurrencyBound(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	e := newTaskEngine(t, func(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		entered <- struct{}{}
		request.Input.Payloads[0].Data[0] = 'X'
		select {
		case <-release:
			return "chain", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	node, physical := e.h.node, e.h.physical[0]
	firstResult := make(chan error, 1)
	go func() { firstResult <- e.execute(e.ctx, node, physical) }()
	select {
	case <-entered:
	case err := <-firstResult:
		t.Fatalf("first delivery ended before RPC: %v", err)
	case <-t.Context().Done():
		t.Fatal("first RPC did not start")
	}
	// A second delivery cannot enter the RPC while the first holds the permit.
	// Cancellation must release its wait without requiring the first RPC to finish.
	secondCtx, cancel := context.WithCancel(e.ctx)
	secondTask := *physical
	secondResult := make(chan error, 1)
	go func() { secondResult <- e.execute(secondCtx, node, &secondTask) }()
	cancel()
	select {
	case err := <-secondResult:
		require.ErrorIs(t, err, context.Canceled)
	case <-t.Context().Done():
		t.Fatal("cancelled delivery did not finish")
	}
	select {
	case <-entered:
		t.Fatal("concurrent remote issuance exceeded one")
	default:
	}
	close(release)
	select {
	case err := <-firstResult:
		require.NoError(t, err)
	case <-t.Context().Done():
		t.Fatal("first delivery did not finish")
	}
	s, ctx := e.h.root(false)
	require.Equal(t, int64(1), s.Started)
	stored := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(s.Operations[s.OperationId].Get(ctx).Request.Get(ctx).Data, stored))
	expected, _ := frozenRequest(t)
	require.True(t, proto.Equal(expected, stored), "RPC must not alias the complete persisted request")
}

func TestTaskAcceptedCancellationRetainsIntent(t *testing.T) {
	t.Parallel()
	var cancel context.CancelFunc
	e := newTaskEngine(t, func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		cancel() // The remote accepted before local cancellation; acknowledgment is lost.
		return "", context.Canceled
	})
	ctx, cancelFn := context.WithCancel(e.ctx)
	cancel = cancelFn
	defer cancel()
	require.ErrorIs(t, e.execute(ctx, e.h.node, e.h.physical[0]), context.Canceled)
	require.Equal(t, 1, logicalCount(e.h.durable))
	s, cc := e.h.root(false)
	require.Equal(t, int64(0), s.Started)
	require.Equal(t, decision.Pending, decision.Phase(s.Operations[s.OperationId].Get(cc).Phase))
}

func TestTaskRetrySchedulesSuccessor(t *testing.T) {
	t.Parallel()
	e := newTaskEngine(t, func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		return "", errors.New("response lost")
	})
	require.NoError(t, e.execute(e.ctx, e.h.node, e.h.physical[0]))
	s, ctx := e.h.root(false)
	require.Equal(t, e.h.now+int64(time.Second), s.Operations[s.OperationId].Get(ctx).RetryAt)
	require.Equal(t, 1, logicalCount(e.h.durable))
	generated := 0
	for _, batch := range e.h.backend.TasksByCategory {
		generated += len(batch)
	}
	require.Positive(t, generated, "retry must publish a physical successor")
}

func TestTaskRollbackDuringRPCPublishesSuccessor(t *testing.T) {
	t.Parallel()
	var e *taskEngine
	e = newTaskEngine(t, func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		e.h.now -= int64(time.Second)
		return "", errors.New("response lost after clock rollback")
	})
	originalWake := e.h.now
	require.NoError(t, e.execute(e.ctx, e.h.node, e.h.physical[0]))
	s, ctx := e.h.root(false)
	require.Equal(t, originalWake+int64(time.Second), s.Operations[s.OperationId].Get(ctx).RetryAt)
	generated := 0
	for _, batch := range e.h.backend.TasksByCategory {
		generated += len(batch)
	}
	require.Positive(t, generated, "successful delivery must leave a physical successor after rollback")
}
