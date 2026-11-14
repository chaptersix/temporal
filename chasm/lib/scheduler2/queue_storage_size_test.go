package scheduler2

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
)

func measureQueueStorage(t *testing.T, h *treeHarness, label string, n int, s *Scheduler) {
	t.Helper()
	var indexNodes, indexBytes, recordNodes, recordBytes, otherBytes, paths int
	for path, node := range h.durable {
		size := proto.Size(node)
		paths += len(path)
		switch {
		case strings.HasPrefix(path, "QueuePages#"):
			indexNodes++
			indexBytes += size
		case strings.HasPrefix(path, "QueueRecords#"):
			recordNodes++
			recordBytes += size
		default:
			otherBytes += size
		}
	}
	logical, err := queueTotal(queueCounts(s.IndexedQueue))
	require.NoError(t, err)
	remaining := int64(0)
	if d := s.IndexedQueue.CompletedDrain; d != nil {
		remaining = d.Remaining
	}
	framed := proto.Size(&persistencespb.WorkflowMutableState{ChasmNodes: h.durable})
	t.Logf("%s N=%d chasm_map_framed_bytes=%d logical=%d source_remaining=%d index_nodes=%d index_node_protobuf=%d record_nodes=%d record_node_protobuf=%d other_node_protobuf=%d encoded_path_bytes=%d total=%d", label, n, framed, logical, remaining, indexNodes, indexBytes, recordNodes, recordBytes, otherBytes, paths, indexBytes+recordBytes+otherBytes+paths)
}

func TestQueueStorageSixClassesAndTwoGenerations(t *testing.T) {
	t.Parallel()
	for _, n := range []int{4, 5, 1000, 10000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			t.Parallel()
			h := newQueueHarness(t, nil)
			s, ctx, _ := queueHarnessState(t, h)
			// Exclude the generic harness's unrelated1MiB diagnostic payload. Whole
			// queue/control nodes remain included; operations/request payloads are absent.
			s.Diagnostics = chasm.Field[*pb.Payload]{}
			var records []decision.Queued
			policies := [3]enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}
			for i := range n {
				class := i % 6
				at := int64(uint64(i) * 11400714819323198485)
				q := queueFixtureRecord(i+1, at, policies[class%3], class >= 3)
				q.Nominal = at
				q.WorkflowID = fmt.Sprintf("workflow-%055d", i)
				require.Len(t, q.WorkflowID, 64)
				if class >= 3 && i%7 == 0 {
					q.NominalTime = decision.Timestamp{Present: true, Seconds: 253402300798, Nanos: 999999999}
					q.ActualTime = decision.Timestamp{Present: true, Seconds: 253402300799, Nanos: 999999999}
				}
				records = append(records, q)
				require.NoError(t, s.stageQueueAppend(ctx, q))
			}
			h.commit(true)
			s, _, _ = queueHarnessState(t, h)
			measureQueueStorage(t, h, "actual_scheduler_initial", n, s)
			// The public Scheduler has not opted into bounded framework traversal yet.
			// Use the identical facade with a fixed-layout owner to construct the large
			// partially cleaned image without repeating known default full-tree walks.
			h = newBoundedQueueStorageHarness(t, records)
			s, ctx = queueStorageRoot(t, h)
			measureQueueStorage(t, h, "bounded_fixture_initial", n, s)
			state, err := s.state(ctx)
			require.NoError(t, err)
			plan, err := s.planQueueDrain(ctx, state, 500, 100)
			require.NoError(t, err)
			require.NotNil(t, plan)
			require.NoError(t, s.stageQueueDrain(ctx, *plan, state))
			h.commit(true)
			s, ctx = queueStorageRoot(t, h)
			// This batches fixture setup to measure the exact partially cleaned image.
			// Runtime maintenance still handles one source record per invocation.
			for range n / 2 {
				progressed, err := s.maintainQueue(ctx)
				require.NoError(t, err)
				require.True(t, progressed)
			}
			for i := range max(1, n/8) {
				q := queueFixtureRecord(n+i+1, 100, policies[0], true)
				q.WorkflowID = fmt.Sprintf("workflow-%055d", n+i)
				require.NoError(t, s.stageQueueAppend(ctx, q))
			}
			h.commit(true)
			s, _ = queueStorageRoot(t, h)
			measureQueueStorage(t, h, "bounded_fixture_two_generations", n, s)
			require.EqualValues(t, n-n/2, s.IndexedQueue.CompletedDrain.Remaining)
		})
	}
}

// queueStorageFixture uses the production RootState and exact queue field names,
// page/record types and facade. Its fixed layout is a test-only owner; Scheduler
// activation still requires conversion of its other raw maps and task sets.
type queueStorageFixture struct {
	chasm.UnimplementedComponent
	*pb.RootState
	QueueRecords chasm.DataMap[*pb.QueuedOccurrence]
	QueuePages   chasm.DataMap[*commonpb.Payload]
}

func (*queueStorageFixture) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
func (*queueStorageFixture) ContextMetadata(chasm.Context) map[string]string { return nil }
func (*queueStorageFixture) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, nil
}

type queueStorageLibrary struct{ chasm.UnimplementedLibrary }

func (*queueStorageLibrary) Name() string { return "queuestoragefixture" }
func (*queueStorageLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{chasm.NewRegistrableComponent[*queueStorageFixture]("root", chasm.WithBoundedDataLayout(chasm.BoundedDataLayout{Components: []chasm.BoundedComponentSlot{chasm.BoundedComponent[*queueStorageFixture](nil, chasm.BoundedTaskLimits{})}}))}
}
func newBoundedQueueStorageHarness(t *testing.T, records []decision.Queued) *treeHarness {
	t.Helper()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&queueStorageLibrary{}))
	h := &treeHarness{t: t, registry: registry, transition: 1}
	h.resetBackend()
	h.node = chasm.NewEmptyTree(registry, h.backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(t.Context(), h.node)
	root := &queueStorageFixture{RootState: &pb.RootState{Incarnation: "incarnation", IndexedQueue: newQueueState()}, QueueRecords: chasm.NewDataMap[*pb.QueuedOccurrence](ctx), QueuePages: chasm.NewDataMap[*commonpb.Payload](ctx)}
	require.NoError(t, h.node.SetRootComponent(root))
	h.commit(true)
	s, ctx := queueStorageRoot(t, h)
	for _, q := range records {
		require.NoError(t, s.stageQueueAppend(ctx, q))
	}
	h.commit(true)
	return h
}
func queueStorageRoot(t *testing.T, h *treeHarness) (*Scheduler, chasm.MutableContext) {
	t.Helper()
	ctx := chasm.NewMutableContext(t.Context(), h.node)
	root, err := h.node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	fixture := root.(*queueStorageFixture)
	return &Scheduler{RootState: fixture.RootState, QueueRecords: fixture.QueueRecords, QueuePages: fixture.QueuePages}, ctx
}
