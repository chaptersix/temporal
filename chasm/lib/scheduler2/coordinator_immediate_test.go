package scheduler2

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCoordinatorImmediateBudgetFallbackRollbackAndFence(t *testing.T) {
	t.Parallel()
	tweaks := schedulerconfig.DefaultTweakables
	h := publicBatchHarness(t, 3, &tweaks)
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly))
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	rpcCalls := 0
	library := NewLibrary(NewStartHandler(func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		rpcCalls++
		return "unused", nil
	}), nil)
	require.NoError(t, registry.Register(library))
	h.registry = registry
	h.reload()
	s, ctx := h.root(true)
	token := s.Coordination.Token
	before := cloneNodes(h.durable)
	physical := len(h.physical)
	request := &schedulepb.BackfillRequest{StartTime: timestamppb.New(generationStart()), EndTime: timestamppb.New(generationStart().Add(2 * time.Second)), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}
	require.NoError(t, s.AcceptBackfill(ctx.(chasm.MutableContext), request))
	commitGeneration(h, false)
	require.Equal(t, before, h.durable)
	require.Len(t, h.physical, physical)
	coordinatorBackfill(h, generationStart(), generationStart().Add(2*time.Second), enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	s, _ = h.root(false)
	require.Zero(t, rpcCalls, "pure transaction pump must never issue external RPCs")
	require.Len(t, s.Operations, 1)
	require.Len(t, s.Backfills, 1, "two quanta cannot scan and dispose the entire range")
	require.Equal(t, generationStart().UnixNano(), s.Backfills[0].Continuation.DisposedThrough)
	require.Nil(t, s.Backfills[0].Continuation.Pending)
	require.Equal(t, token+3, s.Coordination.Token, "one initial obligation plus two successful quanta")
	require.True(t, s.Coordination.Armed)
	require.Equal(t, h.now, s.Coordination.At)
	workType, ok := h.registry.TaskIDFor(&scheduler2pb.WorkTask{})
	require.True(t, ok)
	fallbacks := 0
	for _, task := range h.durable[""].GetMetadata().GetComponentAttributes().GetPureTasks() {
		if task.TypeId != workType {
			continue
		}
		fallbacks++
		payload := &scheduler2pb.WorkTask{}
		require.NoError(t, proto.Unmarshal(task.Data.Data, payload))
		require.Equal(t, s.Coordination.Token, payload.Token)
		require.Zero(t, payload.RemainingQuanta)
		require.Equal(t, s.Coordination.At, task.ScheduledTime.AsTime().UnixNano())
		require.Positive(t, task.PhysicalTaskStatus)
	}
	require.Equal(t, 1, fallbacks, "budget exhaustion must publish one physical coordinator continuation")
	// The physical continuation consumes one quantum, then one immediate successor.
	coordinatorStep(h, true)
	s, ctx = h.root(false)
	require.Len(t, s.Backfills, 1)
	require.Equal(t, generationStart().Add(time.Second).UnixNano(), s.Backfills[0].Continuation.DisposedThrough)
	require.Equal(t, token+5, s.Coordination.Token)
	state := proto.Clone(s.RootState)
	for _, bad := range []*scheduler2pb.WorkTask{{Token: token + 1, RemainingQuanta: 2}, {Token: s.Coordination.Token}, {Token: s.Coordination.Token, RemainingQuanta: 3}} {
		valid, err := (&WorkHandler{}).Validate(ctx, s, chasm.TaskInvocation{}, bad)
		require.NoError(t, err)
		require.False(t, valid)
		s, ctx = h.root(true)
		require.NoError(t, (&WorkHandler{}).Execute(ctx.(chasm.MutableContext), s, chasm.TaskAttributes{}, bad))
		require.True(t, proto.Equal(state, s.RootState))
	}
	require.Zero(t, rpcCalls)
}

func TestCoordinatorDueStartUsesTransferAndRetryUsesTimer(t *testing.T) {
	t.Parallel()
	tweaks := schedulerconfig.DefaultTweakables
	h := publicBatchHarness(t, 1, &tweaks)
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly))
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewLibrary(NewStartHandler(nil), nil)))
	h.registry = registry
	h.reload()
	coordinatorBackfill(h, generationStart(), generationStart(), enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	s, _ := h.root(false)
	id := s.OperationId
	require.NotEmpty(t, id)
	startType, ok := h.registry.TaskIDFor(&scheduler2pb.StartTask{})
	require.True(t, ok)
	var initial *tasks.ChasmTask
	for _, task := range h.physical {
		if task.Info.TypeId == startType {
			initial = task
		}
	}
	require.NotNil(t, initial)
	require.Equal(t, tasks.CategoryTransfer, initial.GetCategory())
	payload := &scheduler2pb.StartTask{}
	require.NoError(t, proto.Unmarshal(initial.Info.Data.Data, payload))
	require.Equal(t, s.PendingStarts[id], payload.WakeTime)
	beforeRetry := len(h.physical)
	h.apply(decision.Event{Kind: decision.RetryableFailure, Incarnation: s.Incarnation, OperationID: id}, true)
	s, cc := h.root(false)
	var retries []*tasks.ChasmTask
	for _, task := range h.physical[beforeRetry:] {
		if task.Info.TypeId == startType {
			retries = append(retries, task)
		}
	}
	require.Len(t, retries, 1)
	retry := retries[0]
	require.Equal(t, startType, retry.Info.TypeId)
	require.Equal(t, tasks.CategoryTimer, retry.GetCategory())
	require.NoError(t, proto.Unmarshal(retry.Info.Data.Data, payload))
	require.Equal(t, s.PendingStarts[id], payload.WakeTime)
	require.Greater(t, payload.WakeTime, h.now)
	stale := &scheduler2pb.StartTask{}
	require.NoError(t, proto.Unmarshal(initial.Info.Data.Data, stale))
	valid, err := (&StartHandler{}).Validate(cc, s, chasm.TaskInvocation{}, stale)
	require.NoError(t, err)
	require.False(t, valid)
}
