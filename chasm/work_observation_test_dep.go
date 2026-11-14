//go:build test_dep

package chasm

import (
	"reflect"

	"google.golang.org/protobuf/proto"
)

// WorkCountersForTest counts operation-site work while the caller holds the
// execution lock. It is deliberately not thread-safe and never walks the tree.
// Counts include failed attempts; Reset marks an observation boundary, not rollback.
type WorkCountersForTest struct{ values [workCounterCount]int64 }

type workObservation struct{ counters *WorkCountersForTest }

type workCounterProvider interface{ ChasmWorkCountersForTest() *WorkCountersForTest }

func newWorkObservation(backend NodeBackend) workObservation {
	if provider, ok := backend.(workCounterProvider); ok {
		return workObservation{counters: provider.ChasmWorkCountersForTest()}
	}
	return workObservation{}
}

// BeginWorkInspectionForTest attaches a fresh observer to this tree. To include
// true cold construction, supply a backend implementing ChasmWorkCountersForTest.
func (n *Node) BeginWorkInspectionForTest() *WorkCountersForTest {
	counters := &WorkCountersForTest{}
	n.observation.counters = counters
	return counters
}

func (c *WorkCountersForTest) Reset() { c.values = [workCounterCount]int64{} }

func (c *WorkCountersForTest) Snapshot() map[string]int64 {
	names := []string{"cold_nodes", "tree_visits", "sync_visits", "map_hydration_keys", "map_sync_keys", "map_key_copies", "delete_child_visits", "decode_calls", "decode_data_bytes", "data_decode_calls", "serialize_calls", "serialize_data_bytes", "task_validations", "serialized_node_proto_bytes", "workset_enqueues", "workset_drains", "slot_visits", "path_inspections", "task_regular_enqueues", "task_immediate_enqueues", "task_immediate_attempts"}
	result := make(map[string]int64, len(names))
	for i, name := range names {
		result[name] = c.values[i]
	}
	return result
}

func (n *Node) noteWork(counter workCounter, amount int) {
	if n != nil && n.nodeBase != nil && n.observation.counters != nil {
		n.observation.counters.values[counter] += int64(amount)
	}
}
func (n *Node) noteDecode() {
	if n == nil || n.nodeBase == nil || n.observation.counters == nil {
		return
	}
	n.noteWork(workDecodeCalls, 1)
	n.noteWork(workDecodeBytes, len(n.serializedNode.GetData().GetData()))
	if n.isData() {
		n.noteWork(workDataDecodeCalls, 1)
	}
}
func (n *Node) noteSerialize() {
	if n == nil || n.nodeBase == nil || n.observation.counters == nil {
		return
	}
	n.noteWork(workSerializeCalls, 1)
	n.noteWork(workSerializeBytes, len(n.serializedNode.GetData().GetData()))
	n.noteWork(workSerializedNodeProtoBytes, proto.Size(n.serializedNode))
}

func (n *Node) noteMapKeyCopy(value reflect.Value) { n.noteWork(workMapKeyCopies, value.Len()) }
