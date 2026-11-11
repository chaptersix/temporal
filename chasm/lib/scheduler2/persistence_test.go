package scheduler2

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

// treeHarness publishes serialized nodes and physical tasks together. Its store
// models database atomicity; the real CHASM tree performs serialization and task
// validation. A failed publication discards the entire tentative tree/backend.
type treeHarness struct {
	t          *testing.T
	registry   *chasm.Registry
	node       *chasm.Node
	backend    *chasm.MockNodeBackend
	now        int64
	transition int64
	durable    map[string]*persistencespb.ChasmNode
	physical   []*tasks.ChasmTask
}

func newTreeHarness(t *testing.T) *treeHarness {
	t.Helper()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	h := &treeHarness{t: t, registry: registry, now: int64(time.Second), transition: 1}
	h.resetBackend()
	h.node = chasm.NewEmptyTree(registry, h.backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	s := New("incarnation", 1)
	s.Diagnostics = chasm.NewDataField(chasm.NewMutableContext(t.Context(), h.node), &scheduler2pb.Payload{Data: bytes.Repeat([]byte("d"), 1<<20)})
	require.NoError(t, h.node.SetRootComponent(s))
	h.commit(true)
	return h
}
func (h *treeHarness) resetBackend() {
	tv := testvars.New(h.t)
	h.backend = &chasm.MockNodeBackend{
		HandleNow:                 func() time.Time { return time.Unix(0, h.now) },
		HandleNextTransitionCount: func() int64 { return h.transition + 1 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetNamespaceEntry:   tv.Namespace,
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey("namespace", "schedule", "incarnation")
		},
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: h.transition}
		},
	}
}
func cloneNodes(nodes map[string]*persistencespb.ChasmNode) map[string]*persistencespb.ChasmNode {
	result := make(map[string]*persistencespb.ChasmNode, len(nodes))
	for key, node := range nodes {
		result[key] = proto.Clone(node).(*persistencespb.ChasmNode)
	}
	return result
}
func (h *treeHarness) reload() {
	h.t.Helper()
	h.resetBackend()
	var err error
	h.node, err = chasm.NewTreeFromDB(cloneNodes(h.durable), h.registry, h.backend, chasm.DefaultPathEncoder, testlogger.NewTestLogger(h.t, testlogger.FailOnExpectedErrorOnly), metrics.NoopMetricsHandler)
	require.NoError(h.t, err)
}
func (h *treeHarness) root(mutable bool) (*Scheduler, chasm.Context) {
	h.t.Helper()
	var ctx chasm.Context = chasm.NewContext(h.t.Context(), h.node)
	if mutable {
		ctx = chasm.NewMutableContext(h.t.Context(), h.node)
	}
	component, err := h.node.Component(ctx, chasm.ComponentRef{})
	require.NoError(h.t, err)
	return component.(*Scheduler), ctx
}
func (h *treeHarness) apply(event decision.Event, publish bool) chasm.NodesMutation {
	h.t.Helper()
	s, ctx := h.root(true)
	require.NoError(h.t, s.Apply(ctx.(chasm.MutableContext), event))
	return h.commit(publish)
}
func (h *treeHarness) commit(publish bool) chasm.NodesMutation {
	h.t.Helper()
	mutation, err := h.node.CloseTransaction(h.t.Context(), true)
	require.NoError(h.t, err)
	if publish {
		h.durable = cloneNodes(h.node.Snapshot(nil).Nodes)
		for _, batch := range h.backend.TasksByCategory {
			for _, task := range batch {
				if physical, ok := task.(*tasks.ChasmTask); ok {
					copy := *physical
					copy.Info = proto.Clone(physical.Info).(*persistencespb.ChasmTaskInfo)
					h.physical = append(h.physical, &copy)
				}
			}
		}
		h.transition++
	}
	h.reload()
	return mutation
}
func frozenRequest(t *testing.T) (*workflowservice.StartWorkflowExecutionRequest, []byte) {
	t.Helper()
	request := &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace", WorkflowId: "workflow", WorkflowType: &commonpb.WorkflowType{Name: "type"}, TaskQueue: &taskqueuepb.TaskQueue{Name: "queue"}, Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{"encoding": []byte("binary/plain")}, Data: []byte("frozen")}}}, LastCompletionResult: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("carryover")}}}}
	encoded, err := proto.Marshal(request)
	require.NoError(t, err)
	request.RequestId = "incarnation/automatic/1000000000/action/start"
	return request, encoded
}
func (h *treeHarness) admit() *workflowservice.StartWorkflowExecutionRequest {
	expected, encoded := frozenRequest(h.t)
	h.apply(decision.Event{Kind: decision.Due, Incarnation: "incarnation", Nominal: h.now, CatchupDeadline: h.now, Request: encoded}, true)
	return expected
}
func (h *treeHarness) request() *workflowservice.StartWorkflowExecutionRequest {
	h.t.Helper()
	s, ctx := h.root(false)
	request, err := s.request(ctx, s.OperationId)
	require.NoError(h.t, err)
	return request
}
func logicalCount(nodes map[string]*persistencespb.ChasmNode) int {
	count := 0
	for _, node := range nodes {
		count += len(node.GetMetadata().GetComponentAttributes().GetSideEffectTasks())
	}
	return count
}
func TestLostAcknowledgmentFreshTree(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	expected := h.admit()
	require.Equal(t, 1, logicalCount(h.durable))
	require.Len(t, h.physical, 1)
	issued := h.request()
	require.True(t, proto.Equal(expected, issued))
	// The remote acceptance survives every local crash and failed publication.
	accepted := proto.Clone(issued).(*workflowservice.StartWorkflowExecutionRequest)
	issued.Input.Payloads[0].Data[0] = 'X'
	old, _ := h.root(false)
	h.reload()
	old.Remaining = 999
	old.Operations = nil
	require.True(t, proto.Equal(accepted, h.request()))
	h.apply(decision.Event{Kind: decision.Update, Incarnation: "incarnation", Revision: 2, Limited: true, Remaining: 0, Request: []byte("new definition")}, true)
	h.apply(decision.Event{Kind: decision.RetryableFailure, Incarnation: "incarnation", OperationID: expected.RequestId}, true)
	h.now += int64(time.Second)
	require.True(t, proto.Equal(accepted, h.request()))
	physical := h.physical[len(h.physical)-1]
	exists, valid, err := h.node.ValidateSideEffectTask(t.Context(), physical)
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, valid)
	h.apply(decision.Event{Kind: decision.Complete, Incarnation: "incarnation", OperationID: expected.RequestId, ChainID: "chain"}, true)
	for _, kind := range []decision.Kind{decision.Acknowledged, decision.RetryableFailure, decision.Complete} {
		h.apply(decision.Event{Kind: kind, Incarnation: "incarnation", OperationID: expected.RequestId, ChainID: "chain"}, true)
	}
	s, ctx := h.root(false)
	require.Equal(t, int64(0), s.Remaining)
	require.Equal(t, int64(1), s.Started)
	require.Equal(t, scheduler2pb.Phase(decision.Completed), s.Operations[s.OperationId].Get(ctx).Phase)
	require.Nil(t, h.request())
	require.Zero(t, logicalCount(h.durable))
	exists, valid, err = h.node.ValidateSideEffectTask(t.Context(), physical)
	require.NoError(t, err)
	require.False(t, exists)
	require.False(t, valid)
}
func TestFailedPublicationRetainsPriorStateAndTasks(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	before := cloneNodes(h.durable)
	_, encoded := frozenRequest(t)
	event := decision.Event{Kind: decision.Due, Incarnation: "incarnation", Nominal: h.now, CatchupDeadline: h.now, Request: encoded}
	h.apply(event, false)
	require.Equal(t, before, h.durable)
	require.Empty(t, h.physical)
	require.Zero(t, logicalCount(h.durable))
	s, _ := h.root(false)
	require.Equal(t, int64(1), s.Remaining)
	require.Empty(t, s.OperationId)
	h.apply(event, true)
	require.Equal(t, 1, logicalCount(h.durable))
	require.Len(t, h.physical, 1)
	committed := cloneNodes(h.durable)
	h.apply(decision.Event{Kind: decision.Complete, Incarnation: "incarnation", OperationID: h.request().RequestId, ChainID: "chain"}, false)
	require.Equal(t, committed, h.durable)
	exists, valid, err := h.node.ValidateSideEffectTask(t.Context(), h.physical[0])
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, valid)
}
func TestMetadataTransitionsAvoidPayloadDeserialization(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	expected := h.admit()
	poisoned := 0
	for path, node := range h.durable {
		if strings.Contains(path, "Request") || strings.Contains(path, "Diagnostics") {
			node.Data.Data = []byte{0xff}
			poisoned++
		}
	}
	require.Equal(t, 2, poisoned, "fixture must poison both separately persisted payload nodes")
	h.reload()
	mutation := h.apply(decision.Event{Kind: decision.Complete, Incarnation: "incarnation", OperationID: expected.RequestId, ChainID: "chain"}, true)
	for path := range mutation.UpdatedNodes {
		require.NotContains(t, path, "Request")
		require.NotContains(t, path, "Diagnostics")
	}
	t.Logf("metadata completion: %d payload nodes unread; %d nodes written; no database-read reduction claimed", poisoned, len(mutation.UpdatedNodes))
	s, ctx := h.root(false)
	require.Equal(t, int64(1), s.Started)
	// The poison is discriminating: explicitly loading it fails deserialization.
	require.Panics(t, func() { s.Diagnostics.Get(ctx) })
}
func TestDroppedLogicalTaskIsDetected(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	h.admit()
	for _, node := range h.durable {
		if attrs := node.Metadata.GetComponentAttributes(); attrs != nil {
			attrs.SideEffectTasks = nil
		}
	}
	h.reload()
	exists, valid, err := h.node.ValidateSideEffectTask(context.Background(), h.physical[0])
	require.NoError(t, err)
	require.False(t, exists)
	require.False(t, valid)
	s, _ := h.root(false)
	require.NotEmpty(t, s.OperationId, "pending intent survives the injected lost-task defect")
	require.Zero(t, logicalCount(h.durable), "mutation must remove the pending obligation")
}

func TestIndependentNilLibraryRestoresPersistedComponent(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	h.admit()
	typeID := h.durable[""].GetMetadata().GetComponentAttributes().GetTypeId()
	fqn, ok := h.registry.ComponentFqnByID(typeID)
	require.True(t, ok)
	require.Equal(t, "scheduler2.scheduler", fqn)
	require.NotEqual(t, chasm.GenerateTypeID("scheduler.scheduler"), typeID)
	require.NotZero(t, h.physical[0].Info.TypeId)
	_, ok = h.registry.TaskByID(h.physical[0].Info.TypeId)
	require.True(t, ok, "nil library must register its persisted task type")
	h.reload()
	s, ctx := h.root(false)
	require.Equal(t, "incarnation", s.Incarnation)
	require.Equal(t, int64(0), s.Remaining)
	require.Equal(t, scheduler2pb.Phase(decision.Pending), s.Operations[s.OperationId].Get(ctx).Phase)
}
