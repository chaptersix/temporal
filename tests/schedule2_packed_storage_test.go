package tests

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
)

// This is a storage/admission witness, not a throughput benchmark. Every admission
// has its own actual History transaction; persisted execution fields are not mocked.
func TestSchedule2PackedPreparedPersistenceStorage(t *testing.T) {
	sizes := []int{4}
	if os.Getenv("SCHEDULER_PACKED_PERSISTENCE_LARGE") == "1" {
		sizes = []int{1000, 10000}
	}
	for _, payloadBytes := range []int{1024, 1280 * 1024} {
		t.Run(fmt.Sprint(payloadBytes), func(t *testing.T) {
			backend := scheduleBackend{name: "scheduler2-packed-storage", context: func(ctx context.Context) context.Context { return ctx }, assertPersisted: assertSchedule2PreparedPersisted, options: []testcore.TestOption{
				testcore.WithScheduleFrontend(func(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
					runtime.FrontendClient = &listingBeforeActionClient{WorkflowServiceClient: runtime.FrontendClient}
					return scheduler2.NewPackedPreparedPublicExecutionTestFrontend(runtime)
				}, scheduler2.NewBoundedPublicTestNilLibrary()),
			}}
			tweaks := schedulerconfig.DefaultTweakables
			tweaks.MaxBufferSize = 0 // Exercise the supported unlimited capacity, not a raised storage limit.
			env := newScheduleEnv(t, []scheduleBackend{backend}, testcore.WithDynamicConfig(schedulerconfig.CurrentTweakables, tweaks))
			for _, n := range sizes {
				t.Run(fmt.Sprint(n), func(t *testing.T) { packedPersistenceCase(t, env, n, payloadBytes) })
			}
		})
	}
}

func packedPersistenceCase(t *testing.T, env *testcore.TestEnv, n, payloadBytes int) {
	t.Helper()
	id := fmt.Sprintf("packed-storage-%d-%d", payloadBytes, n)
	action := startWorkflowAction(env, "workflow-"+id, "storage-retry")
	action.GetStartWorkflow().Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: bytes.Repeat([]byte{7}, payloadBytes)}}}
	createSchedule(t.Context(), t, env, id, &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{TimezoneName: "America/New_York"}, Action: action, Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}})
	ctx, err := env.GetTestCluster().Host().ChasmContext(t.Context())
	require.NoError(t, err)
	ref := chasm.NewComponentRef[*scheduler2.Scheduler](chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: id})
	await.Require(t.Context(), t, func(at *await.T) {
		ready, readErr := chasm.ReadComponent(ctx, ref, func(s *scheduler2.Scheduler, cc chasm.Context, _ struct{}) ([3]bool, error) {
			return [3]bool{s.PackedStorage != nil, !s.SourceRequest.Get(cc).Pending, s.ListControl.Get(cc).PublishedStamp != 0}, nil
		}, struct{}{})
		require.NoError(at, readErr)
		require.Equal(at, [3]bool{true, true, true}, ready)
	}, 30*time.Second, time.Second)
	accepted := 0
	started := time.Now()
	for range n {
		_, _, err = chasm.UpdateComponent(ctx, ref, func(s *scheduler2.Scheduler, cc chasm.MutableContext, _ struct{}) (chasm.NoValue, error) {
			return nil, s.AdmitPackedMeasurementForTest(cc, cc.Now(s))
		}, struct{}{})
		if err != nil {
			t.Errorf("ADMISSION_REJECT target=%d accepted=%d error=%T %v", n, accepted, err, err)
			break
		}
		accepted++
	}
	t.Logf("ADMISSIONS target=%d accepted=%d payload=%d duration=%s", n, accepted, payloadBytes, time.Since(started))
	require.Positive(t, accepted)
	summary, err := chasm.ReadComponent(ctx, ref, func(s *scheduler2.Scheduler, cc chasm.Context, _ struct{}) (packedPersistenceSummary, error) {
		serialized, refErr := cc.Ref(s)
		return packedPersistenceSummary{ref: serialized, packed: s.PackedStorage != nil, counts: [3]int64{s.OperationStore.OperationCount, s.OperationStore.PendingCount, s.OperationStore.RequestCount}}, refErr
	}, struct{}{})
	require.NoError(t, err)
	concrete, err := chasm.DeserializeComponentRef(summary.ref)
	require.NoError(t, err)
	archetype, err := concrete.ArchetypeID(nil)
	require.NoError(t, err)
	persisted, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(t.Context(), &persistence.GetWorkflowExecutionRequest{
		ShardID: common.WorkflowIDToHistoryShard(concrete.NamespaceID, concrete.BusinessID, env.GetTestClusterConfig().HistoryConfig.NumHistoryShards), NamespaceID: concrete.NamespaceID, WorkflowID: concrete.BusinessID, RunID: concrete.RunID, ArchetypeID: archetype,
	})
	require.NoError(t, err)
	stats, err := json.Marshal(persisted.MutableStateStats)
	require.NoError(t, err)
	t.Logf("PERSISTENCE accepted=%d target=%d payload=%d mutable_state_proto_bytes=%d chasm_map_framed=%d execution_info=%d execution_state=%d nodes=%d db_record_version=%d stats=%s", accepted, n, payloadBytes, proto.Size(persisted.State), proto.Size(&persistencespb.WorkflowMutableState{ChasmNodes: persisted.State.ChasmNodes}), proto.Size(persisted.State.ExecutionInfo), proto.Size(persisted.State.ExecutionState), len(persisted.State.ChasmNodes), persisted.DBRecordVersion, stats)
	groups := make(map[string][3]int)
	for path, node := range persisted.State.ChasmNodes {
		name := strings.SplitN(path, "#", 2)[0]
		group := groups[name]
		group[0]++
		group[1] += proto.Size(node)
		group[2] += len(path)
		groups[name] = group
	}
	encodedGroups, err := json.Marshal(groups)
	require.NoError(t, err)
	t.Logf("PERSISTENCE_GROUPS nodes_proto_path_bytes=%s", encodedGroups)
	if dir := os.Getenv("SCHEDULER_PACKED_PERSISTENCE_SNAPSHOTS"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0755))
		file, err := os.Create(filepath.Join(dir, fmt.Sprintf("state-%d-%d.pb.gz", payloadBytes, n)))
		require.NoError(t, err)
		out := gzip.NewWriter(file)
		wire, err := proto.Marshal(persisted.State)
		require.NoError(t, err)
		_, err = out.Write(wire)
		require.NoError(t, err)
		require.NoError(t, out.Close())
		require.NoError(t, file.Close())
	}

	require.True(t, summary.packed)
	require.Equal(t, [3]int64{int64(accepted), int64(accepted), int64(accepted)}, summary.counts)

}

type packedPersistenceSummary struct {
	ref    []byte
	packed bool
	counts [3]int64
}
