package scheduler2

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tasks"
)

func newAddressedTaskEngine(t *testing.T, rpc func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error)) *taskEngine {
	return newAddressedTaskEngineWithAdmission(t, rpc, false)
}
func newAddressedTaskEngineWithAdmission(t *testing.T, rpc func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error), automatic bool) *taskEngine {
	t.Helper()
	// Reuse the locked mock-engine adapter, then discard its entire setup tree.
	// The selected execution below is created fresh, never converted from it.
	e := newTaskEngine(t, nil)
	h := e.h
	h.now = time.Now().UnixNano()
	h.transition = 1
	h.physical = nil
	h.durable = nil
	h.resetBackend()
	starter := NewStartHandler(nil)
	starter.startWorkflow = rpc
	starter.wallNow = func() time.Time { return time.Unix(0, h.now) }
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewLibrary(starter, nil)))
	h.registry = registry
	h.node = chasm.NewEmptyTree(registry, h.backend, chasm.DefaultPathEncoder, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	request := &workflowservice.CreateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{}, State: &schedulepb.ScheduleState{Paused: true},
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "addressed", WorkflowType: &commonpb.WorkflowType{Name: "workflow"}, Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("immutable")}}}}}},
	}, InitialPatch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{}}}
	if automatic {
		request.Schedule.State.Paused = false
		request.InitialPatch = nil
		request.Schedule.Spec = generationSpec(time.Unix(0, h.now), time.Unix(0, h.now).Add(time.Hour), time.Second)
	}
	s, err := newAddressedPublicSchedule(chasm.NewMutableContext(t.Context(), h.node), request, schedulerconfig.DefaultTweakables)
	require.NoError(t, err)
	require.NoError(t, h.node.SetRootComponent(s))
	h.commit(true)
	if automatic {
		h.now += int64(time.Second)
	}
	for step := 0; step < 8; step++ {
		s, _ = h.root(false)
		if s.OperationStore.OperationCount == 1 {
			break
		}
		executed, err := deliverGeneration(h, time.Unix(0, h.now))
		require.NoError(t, err)
		require.Positive(t, executed)
		commitGeneration(h, true)
	}
	s, _ = h.root(false)
	require.EqualValues(t, 1, s.OperationStore.OperationCount)
	require.Empty(t, s.Manuals)
	require.EqualValues(t, 1, s.OperationStore.PendingCount)
	require.Empty(t, s.PendingStarts)
	require.Empty(t, s.Operations)
	require.Empty(t, s.Requests)
	require.Empty(t, s.PayloadBlobs)
	require.NotEmpty(t, h.physical)
	return e
}
func newestAddressedPhysical(t *testing.T, h *treeHarness) *tasks.ChasmTask {
	t.Helper()
	var latest *tasks.ChasmTask
	kind, ok := h.registry.TaskIDFor(&pb.StartDispatchTask{})
	require.True(t, ok)
	for _, task := range h.physical {
		if task.Info.TypeId == kind {
			latest = task
		}
	}
	for _, batch := range h.backend.TasksByCategory {
		for _, task := range batch {
			if physical, ok := task.(*tasks.ChasmTask); ok && physical.Info.TypeId == kind {
				latest = physical
			}
		}
	}
	require.NotNil(t, latest)
	return latest
}
func assertAddressedOwners(t *testing.T, h *treeHarness, id string, phase pb.Phase, pending, requests, running int64) {
	t.Helper()
	s, ctx := h.root(false)
	op, ok := s.operationRecord(ctx, id)
	require.True(t, ok)
	require.Equal(t, phase, op.Phase)
	require.Equal(t, pending, s.OperationStore.PendingCount)
	require.Equal(t, requests, s.OperationStore.RequestCount)
	_, requestPresent := s.requestRecord(ctx, id)
	require.Equal(t, requests != 0, requestPresent)
	require.Equal(t, running, s.ProjectionStore.RunningCount)
	count, ok := s.BlobReferences.Get(ctx, s.PublicActionRef)
	require.True(t, ok)
	require.EqualValues(t, 2+requests, count.OwnerCount)
	require.Empty(t, s.Operations)
	require.Empty(t, s.Requests)
	require.Empty(t, s.PayloadBlobs)
	require.Empty(t, s.PendingStarts)
	require.Empty(t, s.PublicRuntime.Get(ctx).Actions)
}
func TestAddressedRuntimePhysicalSuccessAndCallbackOrdering(t *testing.T) {
	t.Parallel()
	for _, early := range []bool{false, true} {
		t.Run(fmt.Sprint(early), func(t *testing.T) {
			var e *taskEngine
			calls := 0
			e = newAddressedTaskEngine(t, func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
				calls++
				s, ctx := e.h.root(false)
				require.NotNil(t, s.OperationStore.StartDispatch.Claim)
				stored, err := s.requiredRequestRecord(ctx, request.RequestId)
				require.NoError(t, err)
				frozen, err := s.assembleRequest(ctx, stored)
				require.NoError(t, err)
				protorequire.ProtoEqual(t, frozen.Input, request.Input)
				if early {
					e.h.apply(decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: request.RequestId, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, true)
				}
				request.Input.Payloads[0].Data = []byte("RPC mutation")
				return &workflowservice.StartWorkflowExecutionResponse{RunId: "run", FirstExecutionRunId: "chain"}, nil
			})
			s, _ := e.h.root(false)
			id := s.OperationId
			physical := newestAddressedPhysical(t, e.h)
			ctx, cancel := context.WithTimeout(e.ctx, time.Minute)
			defer cancel()
			require.NoError(t, e.execute(ctx, e.h.node, physical))
			phase := pb.PHASE_STARTED
			running := int64(1)
			if early {
				phase = pb.PHASE_COMPLETED
				running = 0
			}
			requests := int64(1)
			if early {
				requests = 0
			}
			assertAddressedOwners(t, e.h, id, phase, 0, requests, running)
			e.h.reload()
			s, raw := e.h.root(false)
			description, err := s.DescribePublic(raw)
			require.NoError(t, err)
			require.Len(t, description.Info.RecentActions, 1)
			require.Equal(t, "run", description.Info.RecentActions[0].StartWorkflowResult.RunId)
			require.Len(t, description.Info.RunningWorkflows, int(running))
			require.NoError(t, e.execute(ctx, e.h.node, physical))
			require.Equal(t, 1, calls)
			if !early {
				e.h.apply(decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: id, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, true)
			}
			assertAddressedOwners(t, e.h, id, pb.PHASE_COMPLETED, 0, 0, 0)
		})
	}
}
func TestAddressedRuntimePhysicalRetryKeepsFrozenRequest(t *testing.T) {
	t.Parallel()
	var observed []*workflowservice.StartWorkflowExecutionRequest
	e := newAddressedTaskEngine(t, func(_ context.Context, r *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
		observed = append(observed, common.CloneProto(r))
		if len(observed) == 1 {
			return nil, errors.New("accepted response lost")
		}
		return &workflowservice.StartWorkflowExecutionResponse{RunId: "run", FirstExecutionRunId: "chain"}, nil
	})
	s, raw := e.h.root(false)
	id := s.OperationId
	before, err := s.requiredRequestRecord(raw, id)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(e.ctx, time.Minute)
	defer cancel()
	require.NoError(t, e.execute(ctx, e.h.node, newestAddressedPhysical(t, e.h)))
	assertAddressedOwners(t, e.h, id, pb.PHASE_PENDING, 1, 1, 0)
	next := newestAddressedPhysical(t, e.h)
	s, raw = e.h.root(false)
	after, err := s.requiredRequestRecord(raw, id)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, before, after)
	op, ok := s.operationRecord(raw, id)
	require.True(t, ok)
	e.h.now = op.RetryAt
	e.h.reload()
	require.NoError(t, e.execute(ctx, e.h.node, next))
	require.Len(t, observed, 2)
	protorequire.ProtoEqual(t, observed[0], observed[1])
	assertAddressedOwners(t, e.h, id, pb.PHASE_STARTED, 0, 1, 1)
}

func TestAddressedRuntimePhysicalClaimCrashAndRecovery(t *testing.T) {
	t.Parallel()
	var cancel context.CancelFunc
	calls := 0
	e := newAddressedTaskEngine(t, func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
		calls++
		if calls == 1 {
			cancel()
			return nil, context.Canceled
		}
		return &workflowservice.StartWorkflowExecutionResponse{RunId: "recovered", FirstExecutionRunId: "chain"}, nil
	})
	s, raw := e.h.root(false)
	id := s.OperationId
	request, err := s.requiredRequestRecord(raw, id)
	require.NoError(t, err)
	ctx, cancelFn := context.WithTimeout(e.ctx, 10*time.Second)
	cancel = cancelFn
	defer cancel()
	old := newestAddressedPhysical(t, e.h)
	require.ErrorIs(t, e.execute(ctx, e.h.node, old), context.Canceled)
	e.h.reload()
	s, raw = e.h.root(false)
	claim := common.CloneProto(s.OperationStore.StartDispatch.Claim)
	require.NotNil(t, claim)
	pending, ok := s.operationRecord(raw, id)
	require.True(t, ok)
	e.h.now = claim.Expiry
	before := cloneNodes(e.h.durable)
	executed, err := deliverGeneration(e.h, time.Unix(0, e.h.now))
	require.NoError(t, err)
	require.Positive(t, executed)
	commitGeneration(e.h, false)
	require.Equal(t, before, e.h.durable)
	executed, err = deliverGeneration(e.h, time.Unix(0, e.h.now))
	require.NoError(t, err)
	require.Positive(t, executed)
	commitGeneration(e.h, true)
	s, raw = e.h.root(false)
	require.Nil(t, s.OperationStore.StartDispatch.Claim)
	next, ok := s.operationRecord(raw, id)
	require.True(t, ok)
	require.Equal(t, pending.RetryAt, next.RetryAt)
	require.Equal(t, pending.Deadline, next.Deadline)
	require.Greater(t, next.ReadySequence, pending.ReadySequence)
	frozen, err := s.requiredRequestRecord(raw, id)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, request, frozen)
	resumed, resumeCancel := context.WithTimeout(e.ctx, time.Minute)
	defer resumeCancel()
	require.NoError(t, e.execute(resumed, e.h.node, old))
	require.Equal(t, 1, calls)
	require.NoError(t, e.execute(resumed, e.h.node, newestAddressedPhysical(t, e.h)))
	require.Equal(t, 2, calls)
	assertAddressedOwners(t, e.h, id, pb.PHASE_STARTED, 0, 1, 1)
}

func TestAddressedRuntimeCloseDuringPhysicalRPC(t *testing.T) {
	t.Parallel()
	var e *taskEngine
	calls := 0
	e = newAddressedTaskEngine(t, func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
		calls++
		s, ctx := e.h.root(true)
		require.NotNil(t, s.OperationStore.StartDispatch.Claim)
		require.NoError(t, s.Close(ctx.(chasm.MutableContext)))
		e.h.commit(true)
		return &workflowservice.StartWorkflowExecutionResponse{RunId: "closed", FirstExecutionRunId: "chain"}, nil
	})
	s, _ := e.h.root(false)
	id := s.OperationId
	physical := newestAddressedPhysical(t, e.h)
	ctx, cancel := context.WithTimeout(e.ctx, time.Minute)
	defer cancel()
	require.NoError(t, e.execute(ctx, e.h.node, physical))
	e.h.reload()
	s, _ = e.h.root(false)
	require.True(t, s.Closed)
	require.False(t, s.OperationStore.StartDispatch.Armed)
	require.NotNil(t, s.OperationStore.StartDispatch.Claim)
	require.False(t, s.validStartDispatch(s.OperationStore.StartDispatch.Token, s.OperationStore.StartDispatch.At, true))
	assertAddressedOwners(t, e.h, id, pb.PHASE_PENDING, 1, 1, 0)
	require.NoError(t, e.execute(ctx, e.h.node, physical))
	require.Equal(t, 1, calls)
}

func TestAddressedRuntimeAutomaticCoordinatorPhysicalStart(t *testing.T) {
	t.Parallel()
	var observed *workflowservice.StartWorkflowExecutionRequest
	e := newAddressedTaskEngineWithAdmission(t, func(_ context.Context, r *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
		observed = common.CloneProto(r)
		return &workflowservice.StartWorkflowExecutionResponse{RunId: "automatic", FirstExecutionRunId: "chain"}, nil
	}, true)
	s, _ := e.h.root(false)
	id := s.OperationId
	require.Contains(t, id, "/automatic/")
	ctx, cancel := context.WithTimeout(e.ctx, time.Minute)
	defer cancel()
	require.NoError(t, e.execute(ctx, e.h.node, newestAddressedPhysical(t, e.h)))
	require.NotNil(t, observed)
	require.Equal(t, id, observed.RequestId)
	assertAddressedOwners(t, e.h, id, pb.PHASE_STARTED, 0, 1, 1)
}
