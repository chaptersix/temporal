package scheduler2

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"
)

// Counts are boundary observations, not a global decode profiler. Two poisoned
// DATA leaves prove avoided materialization; successful output reads witness named
// prepared values. Node bytes exclude map keys/storage envelopes; task bytes use
// the persistence serializer and exclude database framing.
func TestInstrumentationTransitionBoundaries(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"keyed completion", "automatic scan", "automatic handoff", "full update"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			base := generationStart()
			h := coordinatorHarness(t, generationSpec(base, base.Add(time.Second), time.Second), base.Add(-time.Nanosecond), base)
			if name == "automatic handoff" || name == "keyed completion" {
				coordinatorStep(h, true)
			}
			if name == "keyed completion" {
				coordinatorStep(h, true)
			}
			s, ctx := h.root(true)
			s.Requests["unrelated"] = chasm.NewDataField(ctx.(chasm.MutableContext), &scheduler2pb.Payload{Data: []byte("unrelated")})
			commitGeneration(h, true)
			avoided := 0
			for path, node := range h.durable {
				if node.Data != nil && (path == "Diagnostics" || strings.Contains(path, "unrelated")) {
					node.Data.Data = []byte{0xff}
					avoided++
				}
			}
			require.Equal(t, 2, avoided)
			h.reload()
			materializedBefore := h.node.MaterializedPathsForTest()
			require.Equal(t, []string{""}, materializedBefore, "tree reload prepares root visibility metadata")
			s, ctx = h.root(true)
			switch name {
			case "keyed completion":
				require.NotEmpty(t, s.OperationId)
				require.NoError(t, s.Apply(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Complete, Incarnation: s.Incarnation, OperationID: s.OperationId, ChainID: "observed-chain"}))
			case "automatic scan", "automatic handoff":
				n, err := deliverGeneration(h, time.Unix(0, h.now))
				require.NoError(t, err)
				require.Equal(t, 1, n)
			case "full update":
				require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), requestUpdateConfig(t, 2)))
			default:
				t.Fatal("unknown measurement")
			}
			mutation, err := h.node.CloseTransaction(t.Context(), true)
			require.NoError(t, err)
			materializedAfter := h.node.MaterializedPathsForTest()
			require.Contains(t, materializedAfter, "")
			require.NotContains(t, materializedAfter, "Diagnostics")
			for _, path := range materializedAfter {
				require.NotContains(t, path, "unrelated")
			}
			nodeBytes, taskBytes, taskCount := 0, 0, 0
			for path, node := range mutation.UpdatedNodes {
				require.NotContains(t, path, "unrelated")
				require.NotEqual(t, "Diagnostics", path)
				nodeBytes += proto.Size(node)
			}
			serializer := serialization.NewSerializer()
			for _, batch := range h.backend.TasksByCategory {
				for _, task := range batch {
					blob, err := serializer.SerializeTask(task)
					require.NoError(t, err)
					taskBytes += len(blob.Data)
					taskCount++
				}
			}
			require.Positive(t, nodeBytes)
			h.durable = cloneNodes(h.node.Snapshot(nil).Nodes)
			h.transition++
			h.reload()
			s, ctx = h.root(false)
			// Each named value is decoded successfully from the committed snapshot. This
			// is not a claim that exactly one value decoded during transition execution.
			switch name {
			case "keyed completion":
				require.Equal(t, scheduler2pb.Phase(decision.StartOnly), s.Operations[s.OperationId].Get(ctx).Phase)
			case "automatic scan":
				require.NotNil(t, s.Generator.Get(ctx).Continuation.Pending)
			case "automatic handoff":
				require.NotEmpty(t, s.OperationId)
				require.NotEmpty(t, s.Requests[s.OperationId].Get(ctx).Data)
			case "full update":
				require.Equal(t, "replacement-seed", s.Generator.Get(ctx).Definition.Get(ctx).Seed)
			default:
				t.Fatal("unknown measurement")
			}
			require.Panics(t, func() { s.Diagnostics.Get(ctx) })
			require.Panics(t, func() { s.Requests["unrelated"].Get(ctx) })
			t.Logf("P3.3 materialized_before=%d materialized_after=%d paths=%q", len(materializedBefore), len(materializedAfter), materializedAfter)
			t.Logf("P3.3 boundary=%q avoided_DATA_values=%d successful_named_output_values=1 modified_nodes=%d deleted_nodes=%d node_proto_bytes=%d published_tasks=%d serialized_task_data_bytes=%d persistence_reads=unmeasured_Node_store", name, avoided, len(mutation.UpdatedNodes), len(mutation.DeletedNodes), nodeBytes, taskCount, taskBytes)
		})
	}
}
