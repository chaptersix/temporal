package scheduler2

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func manualHarness(t *testing.T) *treeHarness {
	t.Helper()
	h := newTreeHarness(t)
	h.now = generationStart().UnixNano()
	request, _ := frozenRequest(t)
	configureBuffer(h, &schedulepb.ScheduleSpec{}, 1, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, time.Minute, request)
	fireGeneration(h, true)
	return h
}
func acceptManual(h *treeHarness, request *schedulepb.TriggerImmediatelyRequest, publish bool) {
	h.t.Helper()
	s, ctx := h.root(true)
	require.NoError(h.t, s.AcceptManual(ctx.(chasm.MutableContext), request))
	commitGeneration(h, publish)
}
func transferManual(h *treeHarness, publish bool) {
	h.t.Helper()
	s, ctx := h.root(true)
	attrs := chasm.TaskAttributes{ScheduledTime: time.Unix(0, s.ManualAt)}
	task := &scheduler2pb.ManualTask{Token: s.ManualToken}
	valid, err := (&ManualHandler{}).Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: attrs}, task)
	require.NoError(h.t, err)
	require.True(h.t, valid)
	require.NoError(h.t, (&ManualHandler{}).Execute(ctx.(chasm.MutableContext), s, attrs, task))
	commitGeneration(h, publish)
}

// manualCommandEngine honors actual framework transition options and persists the
// real Node request-ID data child. It does not model History leases or storage.
type manualCommandEngine struct {
	h               *treeHarness
	ctx             context.Context
	ref             chasm.ComponentRef
	failPublication bool
	maxCount        int
	maxAge          time.Duration
	mu              sync.Mutex
}

var errManualPublication = errors.New("manual test publication rejected")

func newManualCommandEngine(t *testing.T) *manualCommandEngine {
	t.Helper()
	e := &manualCommandEngine{h: manualHarness(t), maxCount: 100, maxAge: time.Hour}
	e.ref = chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{NamespaceID: "namespace", BusinessID: "schedule", RunID: "incarnation"})
	engine := chasm.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, ref chasm.ComponentRef, update func(chasm.MutableContext, chasm.Component) error, opts ...chasm.TransitionOption) ([]byte, error) {
		e.mu.Lock()
		defer e.mu.Unlock()
		options := chasm.TransitionOptions{}
		for _, option := range opts {
			option(&options)
		}
		if options.RequestID != "" {
			used, err := e.h.node.HasRequestID(options.RequestID, e.maxAge)
			if err != nil {
				return nil, err
			}
			if used {
				return nil, chasm.ErrRequestIDAlreadyUsed
			}
		}
		cc := chasm.NewMutableContext(ctx, e.h.node)
		component, err := e.h.node.Component(cc, ref)
		if err != nil {
			return nil, err
		}
		if err = update(cc, component); err != nil {
			e.h.reload()
			return nil, err
		}
		if options.RequestID != "" {
			if err = e.h.node.AttachRequestID(options.RequestID, e.maxCount, e.maxAge); err != nil {
				e.h.reload()
				return nil, err
			}
		}
		commitGeneration(e.h, !e.failPublication)
		if e.failPublication {
			return nil, errManualPublication
		}
		return nil, nil
	})
	e.ctx = chasm.NewEngineContext(t.Context(), engine)
	return e
}

func TestManualPublicDedupAtomicityAndChangedBody(t *testing.T) {
	t.Parallel()
	e := newManualCommandEngine(t)
	original := &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(generationStart().Add(time.Hour)), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}
	e.failPublication = true
	require.ErrorIs(t, Trigger(e.ctx, e.ref, "command", original), errManualPublication)
	s, _ := e.h.root(false)
	require.Zero(t, s.ManualSequence)
	require.Empty(t, s.Manuals)
	used, err := e.h.node.HasRequestID("command", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
	e.failPublication = false
	require.NoError(t, Trigger(e.ctx, e.ref, "command", original))
	original.ScheduledTime.Seconds++ // accepted metadata owns its timestamp
	changed := &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}
	require.NoError(t, Trigger(e.ctx, e.ref, "command", changed))
	s, _ = e.h.root(false)
	require.Equal(t, uint64(1), s.ManualSequence)
	require.Len(t, s.Manuals, 1)
	require.Equal(t, generationStart().Add(time.Hour).UnixNano(), s.Manuals[0].Nominal)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, s.Manuals[0].Policy)
	for i := 0; i < 3; i++ {
		require.NoError(t, Trigger(e.ctx, e.ref, "", changed))
	}
	require.Error(t, Trigger(e.ctx, e.ref, "over-capacity", changed))
	used, err = e.h.node.HasRequestID("over-capacity", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
	transferManual(e.h, true)
	require.NoError(t, Trigger(e.ctx, e.ref, "over-capacity", changed))
	used, err = e.h.node.HasRequestID("over-capacity", e.maxAge)
	require.NoError(t, err)
	require.True(t, used)
}

func TestManualSameTimeBypassesPauseBudgetCursor(t *testing.T) {
	t.Parallel()
	h := manualHarness(t)
	s, _ := h.root(true)
	s.Paused = true
	s.Remaining = 0
	cursor := s.Cursor
	h.commit(true)
	nominal := generationStart().Add(24 * time.Hour)
	for i := 0; i < 2; i++ {
		acceptManual(h, &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(nominal), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}, true)
		fireGeneration(h, true)
	}
	ids := allowIDs(h)
	require.Len(t, ids, 2)
	require.NotEqual(t, ids[0], ids[1])
	for _, id := range ids {
		request := allowRequest(h, id)
		require.Equal(t, "workflow-"+nominal.Format(time.RFC3339), request.WorkflowId)
		require.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, request.WorkflowIdReusePolicy)
		require.Nil(t, request.LastCompletionResult)
	}
	s, ctx := h.root(false)
	require.Equal(t, cursor, s.Cursor)
	require.Zero(t, s.Remaining)
	require.True(t, s.Paused)
	require.Equal(t, uint64(2), s.ManualSequence)
	require.Empty(t, s.Manuals)
	for _, field := range s.Operations {
		op := field.Get(ctx)
		require.Equal(t, h.now+decision.RetryHorizon, op.Deadline)
	}
}

func TestManualQueueWaitPolicyResolutionAndCapacityRecovery(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 5)
	acceptManual(h, &schedulepb.TriggerImmediatelyRequest{}, true)
	s, _ := h.root(false)
	require.False(t, s.ManualArmed)
	require.Len(t, s.Manuals, 1)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	s, _ = h.root(false)
	require.False(t, s.ManualArmed, "full queue waits even ALLOW_ALL")
	sequentialComplete(h, first.RequestId, nil)
	fireGeneration(h, true)
	s, _ = h.root(false)
	require.True(t, s.ManualArmed)
	require.Len(t, s.Queue, 3)
	transferManual(h, false)
	s, _ = h.root(false)
	require.Len(t, s.Manuals, 1)
	require.True(t, s.ManualArmed)
	transferManual(h, true)
	s, ctx := h.root(false)
	require.Empty(t, s.Manuals)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, s.Operations[s.OperationId].Get(ctx).Policy)
	require.Equal(t, uint64(1), s.Operations[s.OperationId].Get(ctx).ManualId)
	require.Equal(t, int64(8), s.Remaining)
}

func TestManualAcceptancePayloadIsolationAndOverflow(t *testing.T) {
	t.Parallel()
	h := manualHarness(t)
	poisoned := 0
	for path, node := range h.durable {
		if node.Data != nil && (strings.Contains(path, "Definition") || path == "Diagnostics") {
			node.Data.Data = []byte{0xff}
			poisoned++
		}
	}
	require.Equal(t, 2, poisoned)
	h.reload()
	acceptManual(h, &schedulepb.TriggerImmediatelyRequest{}, true)
	s, ctx := h.root(false)
	require.Len(t, s.Manuals, 1)
	require.True(t, s.ManualArmed)
	require.Panics(t, func() { s.Generator.Get(ctx).Definition.Get(ctx) })
	for _, boundary := range []string{"sequence", "token"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			h := manualHarness(t)
			s, _ := h.root(true)
			if boundary == "sequence" {
				s.ManualSequence = math.MaxUint64
			} else {
				s.ManualToken = math.MaxUint64
			}
			h.commit(true)
			snapshot := cloneNodes(h.durable)
			s, ctx := h.root(true)
			require.Error(t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{}))
			h.reload()
			require.Equal(t, snapshot, h.durable)
		})
	}
}

func TestManualDedupRetentionIndependentOfOperationRetention(t *testing.T) {
	t.Parallel()
	e := newManualCommandEngine(t)
	command := &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		require.NoError(t, Trigger(e.ctx, e.ref, id, command))
		transferManual(e.h, true)
		s, _ := e.h.root(false)
		allowAck(e.h, s.OperationId)
		e.h.now += int64(time.Second)
	}
	s, _ := e.h.root(false)
	require.Len(t, s.Operations, 4)
	require.Equal(t, uint64(6), s.ManualSequence)
	require.NoError(t, Trigger(e.ctx, e.ref, "a", &schedulepb.TriggerImmediatelyRequest{}))
	s, _ = e.h.root(false)
	require.Equal(t, uint64(6), s.ManualSequence)
	// Deliberately small framework retention demonstrates the bounded guarantee.
	e.maxAge = time.Second
	e.h.now += int64(2 * time.Second)
	require.NoError(t, Trigger(e.ctx, e.ref, "a", command))
	s, _ = e.h.root(false)
	require.Equal(t, uint64(7), s.ManualSequence)
}

func TestManualTimestampDefaultsAndInvalidInputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		scheduled *timestamppb.Timestamp
		invalid   bool
	}{
		{"default", nil, false}, {"past", timestamppb.New(generationStart().Add(-time.Hour)), false},
		{"invalid_nanos", &timestamppb.Timestamp{Seconds: 1, Nanos: 1_000_000_000}, true},
		{"outside_nanosecond_range", &timestamppb.Timestamp{Seconds: 253402300799}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := manualHarness(t)
			s, ctx := h.root(true)
			err := s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{ScheduledTime: tc.scheduled})
			if tc.invalid {
				require.Error(t, err)
				h.reload()
				s, _ = h.root(false)
				require.Zero(t, s.ManualSequence)
				return
			}
			require.NoError(t, err)
			commitGeneration(h, true)
			s, _ = h.root(false)
			nominal := h.now
			if tc.scheduled != nil {
				nominal = tc.scheduled.AsTime().UnixNano()
			}
			require.Equal(t, nominal, s.Manuals[0].Nominal)
			require.Equal(t, h.now, s.ManualAt)
		})
	}
}

func TestManualDiscardAvoidsDefinitionPayload(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			count := 1
			if policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE {
				count = 2
			}
			h, _ := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, count)
			acceptManual(h, &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: policy}, true)
			poisoned := 0
			for path, node := range h.durable {
				if node.Data != nil && strings.Contains(path, "Definition") {
					node.Data.Data = []byte{0xff}
					poisoned++
				}
			}
			require.Equal(t, 1, poisoned)
			h.reload()
			transferManual(h, true)
			s, ctx := h.root(false)
			require.Empty(t, s.Manuals)
			require.Len(t, s.Queue, count-1)
			require.Equal(t, int64(9), s.Remaining)
			require.Panics(t, func() { s.Generator.Get(ctx).Definition.Get(ctx) })
		})
	}
}

func TestManualFullUnfinishedWaitAndSameTimeEviction(t *testing.T) {
	t.Parallel()
	h := manualHarness(t)
	nominal := timestamppb.New(generationStart())
	command := &schedulepb.TriggerImmediatelyRequest{ScheduledTime: nominal, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}
	for i := 0; i < 4; i++ {
		acceptManual(h, command, true)
		transferManual(h, true)
	}
	ids := allowIDs(h)
	require.Len(t, ids, 4)
	acceptManual(h, command, true)
	s, _ := h.root(false)
	require.False(t, s.ManualArmed)
	require.Len(t, s.Manuals, 1)
	require.Zero(t, pureObligations(h))
	allowAck(h, ids[1])
	allowAck(h, ids[0])
	s, _ = h.root(false)
	require.True(t, s.ManualArmed)
	transferManual(h, true)
	s, _ = h.root(false)
	require.Len(t, s.Operations, 4)
	require.NotContains(t, s.Operations, ids[0])
	require.Contains(t, s.Operations, ids[1])
	require.Empty(t, s.Manuals)
	require.Equal(t, int64(3), s.Unfinished)
}

func TestManualExplicitOverlapOverrides(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 1)
			acceptManual(h, &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: policy}, true)
			transferManual(h, true)
			s, ctx := h.root(false)
			require.Empty(t, s.Manuals)
			require.Equal(t, int64(9), s.Remaining)
			switch policy {
			case enumspb.SCHEDULE_OVERLAP_POLICY_SKIP:
				require.Empty(t, s.Queue)
				require.Len(t, s.Operations, 1)
			case enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL:
				require.Len(t, s.Operations, 2)
				require.Equal(t, policy, s.Operations[s.OperationId].Get(ctx).Policy)
				require.Equal(t, first.RequestId, s.BlockingOperationId)
			default:
				require.Len(t, s.Queue, 1)
				require.Equal(t, policy, s.Queue[0].Policy)
				require.Equal(t, uint64(1), s.Queue[0].ManualId)
			}
		})
	}
}

func TestManualQueuedIdentityCurrentPayloadAndImmutableRetry(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 2)
	nominal := generationStart().Add(-24 * time.Hour)
	acceptManual(h, &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(nominal), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}, true)
	transferManual(h, true)
	s, _ := h.root(false)
	require.Len(t, s.Queue, 2)
	require.Equal(t, "workflow-"+nominal.Format(time.RFC3339), s.Queue[1].WorkflowId)
	updated, _ := frozenRequest(t)
	updated.WorkflowId = "new-prefix"
	updated.Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("current template")}}}
	h.now++
	configureBuffer(h, &schedulepb.ScheduleSpec{}, s.Revision+1, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0, updated)
	fireGeneration(h, true)
	s, _ = h.root(true)
	s.Paused = true
	s.Remaining = 0
	h.commit(true)
	result := &commonpb.Payload{Data: []byte("current carryover")}
	sequentialComplete(h, first.RequestId, result)
	s, _ = h.root(false)
	cursor := s.Cursor
	fireGeneration(h, true)
	s, ctx := h.root(false)
	require.Empty(t, s.Queue, "paused/expired automatic waiter disposed; manual remains eligible")
	require.Zero(t, s.Remaining)
	require.Equal(t, cursor, s.Cursor)
	op := s.Operations[s.OperationId].Get(ctx)
	require.Equal(t, uint64(1), op.ManualId)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, op.Policy)
	actual := h.request()
	expected := proto.Clone(updated).(*workflowservice.StartWorkflowExecutionRequest)
	expected.WorkflowId = "workflow-" + nominal.Format(time.RFC3339)
	expected.RequestId = s.OperationId
	expected.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	expected.LastCompletionResult = &commonpb.Payloads{Payloads: []*commonpb.Payload{result}}
	protorequire.ProtoEqual(t, expected, actual)
	updated.Input.Payloads[0].Data = []byte("later template")
	configureBuffer(h, &schedulepb.ScheduleSpec{}, s.Revision+1, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, 0, updated)
	fireGeneration(h, true)
	h.apply(decision.Event{Kind: decision.RetryableFailure, Incarnation: "incarnation", OperationID: actual.RequestId}, true)
	s, ctx = h.root(false)
	h.now = s.Operations[actual.RequestId].Get(ctx).RetryAt
	protorequire.ProtoEqual(t, expected, allowRequest(h, actual.RequestId))
}

func TestManualStoredTaskRollbackAndStaleToken(t *testing.T) {
	t.Parallel()
	h := manualHarness(t)
	command := &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}
	acceptManual(h, command, true)
	acceptManual(h, command, true)
	var original *scheduler2pb.ManualTask
	var attrs chasm.TaskAttributes
	require.NoError(t, h.node.EachPureTask(time.Unix(0, h.now), func(_ chasm.NodePureTask, a chasm.TaskAttributes, task any) (bool, error) {
		if manual, ok := task.(*scheduler2pb.ManualTask); ok {
			original = proto.Clone(manual).(*scheduler2pb.ManualTask)
			attrs = a
		}
		return false, nil
	}))
	require.NotNil(t, original)
	durable := cloneNodes(h.durable)
	h.now--
	s, ctx := h.root(true)
	require.ErrorContains(t, (&ManualHandler{}).Execute(ctx.(chasm.MutableContext), s, attrs, original), "not due")
	h.reload()
	require.Equal(t, durable, h.durable)
	require.Equal(t, 1, pureObligations(h))
	h.now++
	transferManual(h, true)
	s, ctx = h.root(false)
	require.Len(t, s.Manuals, 1)
	require.Greater(t, s.ManualToken, original.Token)
	valid, err := (&ManualHandler{}).Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: attrs}, original)
	require.NoError(t, err)
	require.False(t, valid)
	before := proto.Clone(s.RootState)
	s, ctx = h.root(true)
	require.NoError(t, (&ManualHandler{}).Execute(ctx.(chasm.MutableContext), s, attrs, original))
	commitGeneration(h, true)
	s, _ = h.root(false)
	protorequire.ProtoEqual(t, before, s.RootState)
	transferManual(h, true)
	s, ctx = h.root(false)
	valid, err = (&ManualHandler{}).Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: attrs}, original)
	require.NoError(t, err)
	require.False(t, valid)
	require.Empty(t, s.Manuals)
	require.Zero(t, pureObligations(h))
}
