//go:build test_dep

package chasm

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
)

type workFixture struct {
	UnimplementedComponent
	State   *persistencespb.WorkflowExecutionState
	Records Map[int, *persistencespb.WorkflowExecutionState]
}

func (*workFixture) LifecycleState(Context) LifecycleState     { return LifecycleStateRunning }
func (*workFixture) ContextMetadata(Context) map[string]string { return nil }
func (*workFixture) Terminate(MutableContext, TerminateComponentRequest) (TerminateComponentResponse, error) {
	return TerminateComponentResponse{}, nil
}

type workFixtureLibrary struct{ UnimplementedLibrary }

func (*workFixtureLibrary) Name() string { return "workbaseline" }
func (*workFixtureLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{NewRegistrableComponent[*workFixture]("root")}
}
func (*workFixtureLibrary) Tasks() []*RegistrableTask {
	return []*RegistrableTask{NewRegistrablePureTask("future", &workFixtureTask{})}
}

type workFixtureTask struct {
	PureTaskHandlerBase
}

func (*workFixtureTask) Validate(Context, *workFixture, TaskInvocation, *commonpb.Payload) (bool, error) {
	return true, nil
}
func (*workFixtureTask) Execute(MutableContext, *workFixture, TaskAttributes, *commonpb.Payload) error {
	return nil
}

type workFixtureBackend struct {
	*MockNodeBackend
	counts *WorkCountersForTest
}

func (b *workFixtureBackend) ChasmWorkCountersForTest() *WorkCountersForTest { return b.counts }

// TestWorkObservationOrdinaryMapBaseline is a baseline witness, not the final
// bounded-work gate. It demonstrates hot O(N) traversal without unrelated DATA decoding.
func TestWorkObservationOrdinaryMapBaseline(t *testing.T) {
	t.Parallel()
	for _, size := range []int{4, 5, 1000} {
		t.Run(fmt.Sprintf("records%d", size), func(t *testing.T) {
			t.Parallel()
			counts := &WorkCountersForTest{}
			backend := &workFixtureBackend{MockNodeBackend: &MockNodeBackend{HandleGetCurrentVersion: func() int64 { return 1 }, HandleNextTransitionCount: func() int64 { return 1 }}, counts: counts}
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			registry := NewRegistry(logger)
			require.NoError(t, registry.Register(&CoreLibrary{}))
			require.NoError(t, registry.Register(&workFixtureLibrary{}))
			root := NewEmptyTree(registry, backend, DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
			cc := NewMutableContext(t.Context(), root)
			value := &workFixture{State: &persistencespb.WorkflowExecutionState{RunId: "initial"}, Records: make(Map[int, *persistencespb.WorkflowExecutionState])}
			for i := range size {
				value.Records[i] = NewDataField(cc, &persistencespb.WorkflowExecutionState{RunId: fmt.Sprintf("unrelated-%d", i)})
			}
			require.NoError(t, root.SetRootComponent(value))
			cc.AddTask(value, TaskAttributes{ScheduledTime: time.Unix(2000000000, 0)}, &commonpb.Payload{})
			_, err := root.CloseTransaction(t.Context(), true)
			require.NoError(t, err)
			saved := root.Snapshot(nil).Nodes
			persisted := make(map[string]*persistencespb.ChasmNode, len(saved))
			for key, node := range saved {
				persisted[key] = proto.Clone(node).(*persistencespb.ChasmNode)
			}
			counts.Reset()
			root, err = NewTreeFromDB(persisted, registry, backend, DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
			require.NoError(t, err)
			cold := counts.Snapshot()
			require.EqualValues(t, len(persisted), cold["cold_nodes"])
			require.EqualValues(t, size, cold["map_hydration_keys"])
			require.Zero(t, cold["data_decode_calls"])
			emitWorkCounts(t, size, "cold", cold)
			for step := range 2 {
				counts.Reset()
				component, err := root.Component(NewMutableContext(t.Context(), root), ComponentRef{})
				require.NoError(t, err)
				current, ok := component.(*workFixture)
				require.True(t, ok)
				current.State.RunId = fmt.Sprintf("hot-%d", step)
				_, err = root.CloseTransaction(t.Context(), true)
				require.NoError(t, err)
				hot := counts.Snapshot()
				require.Zero(t, hot["cold_nodes"], "ordinary mutations must not be classified as cold reconstruction")
				require.EqualValues(t, size, hot["map_sync_keys"])
				require.GreaterOrEqual(t, hot["map_key_copies"], int64(2*size))
				require.Greater(t, hot["tree_visits"], int64(size))
				require.Positive(t, hot["sync_visits"])
				require.Zero(t, hot["data_decode_calls"], "unrelated payload isolation does not imply bounded metadata work")
				require.Positive(t, hot["serialize_calls"])
				require.Positive(t, hot["task_validations"])
				emitWorkCounts(t, size, fmt.Sprintf("hot%d", step), hot)
			}
			counts.Reset()
			readContext := NewContext(t.Context(), root)
			component, err := root.Component(readContext, ComponentRef{})
			require.NoError(t, err)
			current, ok := component.(*workFixture)
			require.True(t, ok)
			require.Equal(t, "unrelated-0", current.Records[0].Get(readContext).RunId)
			point := counts.Snapshot()
			require.EqualValues(t, 1, point["data_decode_calls"])
			require.Positive(t, point["decode_data_bytes"])
			require.Zero(t, point["map_hydration_keys"])
			emitWorkCounts(t, size, "point-read", point)

		})
	}
}

func emitWorkCounts(t *testing.T, size int, phase string, counts map[string]int64) {
	t.Helper()
	encoded, err := json.Marshal(struct {
		Size   int              `json:"size"`
		Phase  string           `json:"phase"`
		Counts map[string]int64 `json:"counts"`
	}{size, phase, counts})
	require.NoError(t, err)
	t.Logf("WORK_COUNTS %s", encoded)
}
