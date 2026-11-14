package scheduler2

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func newDispatchHarness(t *testing.T) (*treeHarness, *pb.StartDispatchTask) {
	t.Helper()
	h := newReadinessHarness(t)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	s.PublicRevision = 0
	op := readinessOperation(s, 1, h.now)
	op.Deadline = h.now + int64(30*time.Second)
	request, _ := frozenRequest(t)
	request.RequestId = op.Id
	encoded, err := proto.Marshal(request)
	require.NoError(t, err)
	plan, err := s.prepareOperationRecords(ctx, []operationRecordChange{{id: op.Id, operation: op, request: &pb.Payload{Data: encoded}, changeRequest: true}})
	require.NoError(t, err)
	s.publishOperationRecords(ctx, plan)
	s.OperationId = op.Id
	h.commit(true)
	s, _ = h.root(false)
	c := s.OperationStore.StartDispatch
	return h, &pb.StartDispatchTask{Token: c.Token, WakeTime: c.At}
}

func TestStartClaimPublicationAndRecovery(t *testing.T) {
	t.Parallel()
	h, task := newDispatchHarness(t)
	before := cloneNodes(h.durable)
	s, raw := h.root(true)
	result, err := s.claimStart(raw.(chasm.MutableContext), startClaimInput{task: task, deadline: time.Unix(0, h.now+int64(time.Second))})
	require.NoError(t, err)
	require.NotNil(t, result.snapshot.request)
	require.Equal(t, s.OperationId, result.snapshot.request.RequestId)
	require.False(t, s.validStartDispatch(task.Token, task.WakeTime, false))
	require.True(t, s.validStartDispatch(result.token, result.claim.Expiry, true))
	h.commit(false)
	require.Equal(t, before, h.durable)
	s, raw = h.root(true)
	result, err = s.claimStart(raw.(chasm.MutableContext), startClaimInput{task: task, deadline: time.Unix(0, h.now+int64(time.Second))})
	require.NoError(t, err)
	h.commit(true)
	h.now = result.claim.Expiry
	s, raw = h.root(true)
	old, ok, readErr := s.operationRecord(raw, result.claim.OperationId)
	require.NoError(t, readErr)
	require.True(t, ok)
	require.NoError(t, (&StartRecoveryHandler{}).Execute(raw.(chasm.MutableContext), s, chasm.TaskAttributes{}, &pb.StartRecoveryTask{Token: result.token, WakeTime: result.claim.Expiry}))
	h.commit(true)
	s, raw = h.root(false)
	next, err := s.readOperationReady(raw)
	require.NoError(t, err)
	require.Equal(t, old.RetryAt, next.Operation.RetryAt)
	require.Equal(t, old.Deadline, next.Operation.Deadline)
	require.Greater(t, next.Sequence, result.claim.ReadySequence)
	require.Equal(t, h.now, next.ReadyAt)
	require.Nil(t, s.OperationStore.StartDispatch.Claim)
	require.Equal(t, int64(1), s.OperationStore.PendingCount)
	require.Greater(t, s.OperationStore.StartDispatch.Token, result.token)
}

func TestStartClaimDeadlineDoesNotExtendAfterLockWait(t *testing.T) {
	t.Parallel()
	h, task := newDispatchHarness(t)
	capturedDeadline := time.Unix(0, h.now+int64(time.Second))
	h.now += int64(2 * time.Second)
	s, raw := h.root(true)
	before := common.CloneProto(s.OperationStore)
	result, err := s.claimStart(raw.(chasm.MutableContext), startClaimInput{task: task, deadline: capturedDeadline})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, result.snapshot.request)
	protorequire.ProtoEqual(t, before, s.OperationStore)
}

func TestStartDispatchDoesNotIssueWithoutCommittedLiveClaim(t *testing.T) {
	t.Parallel()
	for _, commit := range []bool{false, true} {
		name := "failed publication"
		if commit {
			name = "cancel after publication"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h, task := newDispatchHarness(t)
			calls := 0
			handler := &StartDispatchHandler{start: NewStartHandler(func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error) {
				calls++
				return "chain", nil
			})}
			engine := chasm.NewMockEngine(gomock.NewController(t))
			ctx, cancel := context.WithTimeout(chasm.NewEngineContext(t.Context(), engine), time.Minute)
			defer cancel()
			publicationErr := errors.New("injected claim publication failure")
			engine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ chasm.ComponentRef, update func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
					s, _ := h.root(true)
					require.NoError(t, update(chasm.NewMutableContext(ctx, h.node), s))
					require.NotNil(t, s.OperationStore.StartDispatch.Claim)
					if !commit {
						h.reload()
						return nil, publicationErr
					}
					h.commit(true)
					cancel()
					return nil, nil
				})
			ref := chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{NamespaceID: "namespace", BusinessID: "schedule", RunID: "incarnation"})
			err := handler.Execute(ctx, ref, chasm.TaskAttributes{}, task)
			if commit {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, publicationErr)
			}
			require.Zero(t, calls)
			s, _ := h.root(false)
			if commit {
				require.NotNil(t, s.OperationStore.StartDispatch.Claim)
			} else {
				require.Nil(t, s.OperationStore.StartDispatch.Claim)
			}
		})
	}
}

func TestStartRecoveryRejectsMismatchedExpiry(t *testing.T) {
	t.Parallel()
	h, task := newDispatchHarness(t)
	s, raw := h.root(true)
	result, err := s.claimStart(raw.(chasm.MutableContext), startClaimInput{task: task, deadline: time.Unix(0, h.now+int64(time.Second))})
	require.NoError(t, err)
	s.OperationStore.StartDispatch.At++
	valid, err := (&StartRecoveryHandler{}).Validate(raw, s, chasm.TaskInvocation{}, &pb.StartRecoveryTask{Token: result.token, WakeTime: s.OperationStore.StartDispatch.At})
	require.NoError(t, err)
	require.False(t, valid)
}

func TestStartClaimUsesIssuanceClockSnapshot(t *testing.T) {
	t.Parallel()
	h, task := newDispatchHarness(t)
	physicalDeadline := time.Unix(0, h.now+int64(time.Second))
	h.now += int64(17 * time.Second)
	info := &persistencespb.WorkflowExecutionInfo{TimeSkippingInfo: &persistencespb.TimeSkippingInfo{Config: &commonpb.TimeSkippingConfig{Enabled: true}, AccumulatedSkippedDuration: durationpb.New(17 * time.Second)}}
	h.backend.HandleGetExecutionInfo = func() *persistencespb.WorkflowExecutionInfo { return info }
	s, raw := h.root(true)
	issued, err := s.claimStart(raw.(chasm.MutableContext), startClaimInput{task: task, deadline: physicalDeadline})
	require.NoError(t, err)
	require.Equal(t, physicalDeadline.Add(17*time.Second).UnixNano(), issued.claim.Expiry)
	require.Equal(t, 17*time.Second, issued.snapshot.skipped)
	require.Equal(t, 17*time.Second, issued.snapshot.request.TimeSkippingStatePropagation.InitialSkippedDuration.AsDuration())
	// A future change to execution clock configuration cannot rewrite the issued snapshot.
	info.TimeSkippingInfo.AccumulatedSkippedDuration = durationpb.New(time.Hour)
	require.Equal(t, 17*time.Second, issued.snapshot.skipped)
	require.Equal(t, 17*time.Second, issued.snapshot.request.TimeSkippingStatePropagation.InitialSkippedDuration.AsDuration())
}

func TestStartDispatchSharesFinitePermit(t *testing.T) {
	t.Parallel()
	start := NewStartHandler(func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		t.Fatal("RPC entered while finite permit held")
		return "", nil
	})
	start.permit <- struct{}{}
	defer func() { <-start.permit }()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := (&StartDispatchHandler{start: start}).Execute(ctx, chasm.ComponentRef{}, chasm.TaskAttributes{}, &pb.StartDispatchTask{})
	require.ErrorIs(t, err, context.Canceled)
}
