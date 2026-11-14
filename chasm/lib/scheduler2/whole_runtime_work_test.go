//go:build test_dep

package scheduler2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
)

// wholeRuntimeCommit preserves real CHASM publication while avoiding the generic
// fixture's complete Snapshot/reload per setup item. Only returned deltas are
// copied into its simulated durable store; each next operation uses a new lease.
func wholeRuntimeCommit(t *testing.T, h *treeHarness) chasm.NodesMutation {
	t.Helper()
	mutation, err := h.node.CloseTransaction(t.Context(), true)
	require.NoError(t, err)
	if h.durable == nil {
		h.durable = make(map[string]*persistencespb.ChasmNode)
	}
	for key := range mutation.DeletedNodes {
		delete(h.durable, key)
	}
	for key, node := range mutation.UpdatedNodes {
		h.durable[key] = common.CloneProto(node)
	}
	h.transition++
	clear(h.backend.TasksByCategory)
	return mutation
}
func wholeRuntimeTree(t *testing.T, policy enumspb.ScheduleOverlapPolicy) (*treeHarness, []byte) {
	return selectedWholeRuntimeTree(t, policy, false)
}

func selectedWholeRuntimeTree(t *testing.T, policy enumspb.ScheduleOverlapPolicy, packed bool) (*treeHarness, []byte) {
	t.Helper()
	h := &treeHarness{t: t, now: generationStart().UnixNano(), transition: 1}
	h.resetBackend()
	library := newBoundedLibrary(NewStartHandler(nil), nil)
	tweaks := schedulerconfig.DefaultTweakables
	tweaks.MaxBufferSize = 0
	library.tweaks = func(string) schedulerconfig.Tweakables { return tweaks }
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(library))
	h.registry = registry
	h.node = chasm.NewEmptyTree(registry, h.backend, chasm.DefaultPathEncoder, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	request := &workflowservice.CreateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: &schedulepb.Schedule{Spec: generationSpec(generationStart().Add(time.Hour), generationStart().Add(24*time.Hour), time.Hour), Policies: &schedulepb.SchedulePolicies{OverlapPolicy: policy}, Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "workflow-" + strings.Repeat("w", 55), WorkflowType: &commonpb.WorkflowType{Name: "workflow"}, Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: bytes.Repeat([]byte{7}, 1024)}}}}}}}}
	ctx := chasm.ContextWithValue(chasm.NewMutableContext(t.Context(), h.node), workQuantaKey{}, 0)
	constructor := newAddressedPublicSchedule
	if packed {
		constructor = newPackedAddressedPublicSchedule
	}
	s, err := constructor(ctx, request, tweaks)
	require.NoError(t, err)
	require.NoError(t, h.node.SetRootComponent(s))
	wholeRuntimeCommit(t, h)
	s, raw := h.root(false)
	wire, err := s.generationRequest(raw)
	require.NoError(t, err)
	return h, wire
}
func wholeRuntimeDue(t *testing.T, h *treeHarness, wire []byte) {
	t.Helper()
	h.now++
	s, raw := h.root(true)
	ctx := chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
	require.NoError(t, s.applyTransition(ctx, decision.Event{Kind: decision.Due, Incarnation: s.Incarnation, Nominal: h.now, Actual: h.now, CatchupDeadline: h.now, WorkflowID: "workflow-" + strings.Repeat("w", 55), Request: wire}))
	wholeRuntimeCommit(t, h)
}

type wholeRuntimeObservedBackend struct {
	*chasm.MockNodeBackend
	counts *chasm.WorkCountersForTest
}

func (b *wholeRuntimeObservedBackend) ChasmWorkCountersForTest() *chasm.WorkCountersForTest {
	return b.counts
}
func wholeRuntimeCold(t *testing.T, h *treeHarness) {
	t.Helper()
	counts := &chasm.WorkCountersForTest{}
	backend := &wholeRuntimeObservedBackend{MockNodeBackend: h.backend, counts: counts}
	copyStarted := time.Now()
	copied := cloneNodes(h.durable)
	t.Logf("COLD_INPUT_COPY duration=%s nodes=%d", time.Since(copyStarted), len(copied))
	started := time.Now()
	node, err := chasm.NewTreeFromDB(copied, h.registry, backend, chasm.DefaultPathEncoder, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	h.node = node
	encoded, err := json.Marshal(counts.Snapshot())
	require.NoError(t, err)
	t.Logf("COLD duration=%s counters=%s", time.Since(started), encoded)
}
func wholeRuntimeStorage(t *testing.T, h *treeHarness, label string) {
	t.Helper()
	groups := make(map[string][3]int)
	for path, node := range h.durable {
		name := strings.SplitN(path, "#", 2)[0]
		group := groups[name]
		group[0]++
		group[1] += proto.Size(node)
		group[2] += len(path)
		groups[name] = group
	}
	framed := proto.Size(&persistencespb.WorkflowMutableState{ChasmNodes: h.durable})
	info, state := h.backend.GetExecutionInfo(), h.backend.GetExecutionState()
	withExecution := proto.Size(&persistencespb.WorkflowMutableState{ChasmNodes: h.durable, ExecutionInfo: info, ExecutionState: state})
	encoded, err := json.Marshal(groups)
	require.NoError(t, err)
	t.Logf("STORAGE phase=%s nodes=%d chasm_map_framed=%d with_fixture_execution=%d fixture_execution_info_bytes=%d fixture_execution_state_bytes=%d eight_mib=%d groups_node_count_proto_bytes_path_bytes=%s", label, len(h.durable), framed, withExecution, proto.Size(info), proto.Size(state), 8<<20, encoded)
}
func wholeRuntimeMeasure(t *testing.T, h *treeHarness, label string, run func()) {
	t.Helper()
	counts := h.node.BeginWorkInspectionForTest()
	started := time.Now()
	run()
	mutation, err := h.node.CloseTransaction(t.Context(), true)
	require.NoError(t, err)
	elapsed := time.Since(started)
	measured := counts.Snapshot()
	for _, key := range []string{"tree_visits", "map_hydration_keys", "map_sync_keys", "map_key_copies", "cold_nodes"} {
		require.Zero(t, measured[key], key)
	}
	encoded, err := json.Marshal(measured)
	require.NoError(t, err)
	updatedBytes := 0
	for _, node := range mutation.UpdatedNodes {
		updatedBytes += proto.Size(node)
	}
	t.Logf("HOT phase=%s duration=%s updated=%d deleted=%d updated_node_proto_bytes=%d counters=%s", label, elapsed, len(mutation.UpdatedNodes), len(mutation.DeletedNodes), updatedBytes, encoded)
	// Store merging is deliberately outside the measured handler+Close boundary.
	for key := range mutation.DeletedNodes {
		delete(h.durable, key)
	}
	for key, node := range mutation.UpdatedNodes {
		h.durable[key] = common.CloneProto(node)
	}
	h.transition++
	clear(h.backend.TasksByCategory)
}

func TestSelectedRuntimeWorkAndStorage(t *testing.T) {
	testSelectedRuntimeWorkAndStorage(t, false)
}

func TestPackedRuntimeWorkAndStorage(t *testing.T) {
	testSelectedRuntimeWorkAndStorage(t, true)
}

func testSelectedRuntimeWorkAndStorage(t *testing.T, packed bool) {
	sizes := []int{4, 5}
	if os.Getenv("SCHEDULER_WHOLE_RUNTIME_LARGE") == "1" {
		sizes = append(sizes, 1000, 10000)
	}
	for _, family := range []string{"pending", "queue"} {
		t.Run(family, func(t *testing.T) {
			for _, n := range sizes {
				t.Run(fmt.Sprint(n), func(t *testing.T) {
					policy := enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
					if family == "queue" {
						policy = enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
					}
					started := time.Now()
					h, wire := selectedWholeRuntimeTree(t, policy, packed)
					if family == "queue" {
						wholeRuntimeDue(t, h, wire)
					}
					blocker := ""
					if family == "queue" {
						s, _ := h.root(false)
						blocker = s.OperationId
					}
					for range n {
						wholeRuntimeDue(t, h, wire)
					}
					t.Logf("SETUP family=%s N=%d duration=%s", family, n, time.Since(started))
					s, raw := h.root(false)
					if family == "pending" {
						require.EqualValues(t, n, s.OperationStore.OperationCount)
						require.EqualValues(t, n, s.OperationStore.PendingCount)
						require.EqualValues(t, n, s.OperationStore.RequestCount)
					} else {
						require.EqualValues(t, n, s.queueLength())
						require.EqualValues(t, 1, s.OperationStore.OperationCount)
					}
					wholeRuntimeStorage(t, h, "initial")
					if family == "queue" {
						s, raw = h.root(true)
						ctx := chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
						require.NoError(t, s.Apply(ctx, decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: blocker, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}))
						wholeRuntimeCommit(t, h)
					}
					wholeRuntimeCold(t, h)
					if family == "pending" {
						var claimed claimedStart
						wholeRuntimeMeasure(t, h, "first_after_cold_claim", func() {
							s, raw = h.root(true)
							c := s.OperationStore.StartDispatch
							var err error
							claimed, err = s.claimStart(raw.(chasm.MutableContext), startClaimInput{task: &pb.StartDispatchTask{Token: c.Token, WakeTime: c.At}, deadline: time.Unix(0, h.now).Add(time.Second)})
							require.NoError(t, err)
							require.NotNil(t, claimed.snapshot.request)
						})
						wholeRuntimeMeasure(t, h, "second_hot_ack", func() {
							s, raw = h.root(true)
							_, err := s.mergeStartClaim(raw.(chasm.MutableContext), startClaimResult{issuance: claimed, event: decision.Event{Kind: decision.Acknowledged, Incarnation: s.Incarnation, OperationID: claimed.claim.OperationId, ChainID: "chain", PublicRunID: "run", StartObserved: true, StartObservedAt: h.now}})
							require.NoError(t, err)
						})
						s, raw = h.root(false)
						require.EqualValues(t, n-1, s.OperationStore.PendingCount)
						require.EqualValues(t, n-1, s.OperationStore.RequestCount)
						count, ok := s.BlobReferences.Get(raw, s.PublicActionRef)
						require.True(t, ok)
						require.EqualValues(t, n+1, count.OwnerCount)
					} else {
						for _, phase := range []string{"first_after_cold_drain", "second_hot_maintenance"} {
							wholeRuntimeMeasure(t, h, phase, func() {
								executed, err := deliverGeneration(h, time.Unix(0, h.now))
								require.NoError(t, err)
								require.Positive(t, executed)
							})
						}
						s, _ = h.root(false)
						require.Less(t, s.queueLength(), int64(n))
					}
					wholeRuntimeStorage(t, h, "after_handlers")
				})
			}
		})
	}
}
