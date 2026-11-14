//go:build test_dep

package scheduler2

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

type compactPrototypeCollection struct {
	template []byte
	records  [][]byte
}

func (c *compactPrototypeCollection) add(t *testing.T, wire []byte) {
	t.Helper()
	if c.template == nil {
		c.template = append([]byte{}, wire...)
	}
	encoded, err := compactRequest(c.template, wire)
	require.NoError(t, err)
	decoded, err := expandCompactRequest(c.template, encoded)
	require.NoError(t, err)
	require.Equal(t, wire, decoded)
	c.records = append(c.records, encoded)
}
func compactMarshal(t *testing.T, value proto.Message) []byte {
	t.Helper()
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(value)
	require.NoError(t, err)
	return wire
}

// This is an exact read-only encoding prototype, not a mutable packed-index adapter.
// Index key directory bytes are included; stable mutable split/locator allocation
// is deliberately not inferred from a successful static round trip or size result.
func TestCompactStoragePrototypeMeasurement(t *testing.T) {
	sizes := []int{4}
	if os.Getenv("SCHEDULER_COMPACT_LARGE") == "1" {
		sizes = []int{1000, 10000}
	}
	for _, n := range sizes {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			h, wire := wholeRuntimeTree(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			for range n {
				wholeRuntimeDue(t, h, wire)
			}
			s, ctx := h.root(false)
			groups := map[string]*compactPrototypeCollection{}
			add := func(name string, wire []byte) {
				c := groups[name]
				if c == nil {
					c = &compactPrototypeCollection{}
					groups[name] = c
				}
				c.add(t, wire)
			}
			var firstNode *persistencespb.ChasmNode
			retained := map[string]*persistencespb.ChasmNode{}
			indexPaths := []string{}
			for path, node := range h.durable {
				group := strings.SplitN(path, "#", 2)[0]
				switch group {
				case "OperationRecords", "RequestRecords", "ActionRecords", "ReadyRecords":
					if node.GetMetadata().GetDataAttributes() != nil {
						firstNode = node
					}
				case "OperationPages":
					if node.GetMetadata().GetDataAttributes() != nil {
						indexPaths = append(indexPaths, path)
					}
				default:
					retained[path] = common.CloneProto(node)
				}
			}
			require.NotNil(t, firstNode)
			for sequence := uint64(1); sequence <= uint64(n); sequence++ {
				ready, ok := s.ReadyRecords.Get(ctx, readyRecordKey(sequence))
				require.True(t, ok)
				op, ok := s.OperationRecords.Get(ctx, ready.OperationId)
				require.True(t, ok)
				stored, ok := s.RequestRecords.Get(ctx, ready.OperationId)
				require.True(t, ok)
				action, ok := s.ActionRecords.Get(ctx, ready.OperationId)
				require.True(t, ok)
				encodedOp, err := compactOperation(s.Incarnation, op)
				require.NoError(t, err)
				restored, err := expandCompactOperation(s.Incarnation, encodedOp)
				require.NoError(t, err)
				require.True(t, proto.Equal(op, restored))
				// Wrap the mode and operation bytes in one proto field for exact field-delta encoding.
				add("O", protowireBytes(encodedOp))
				// Request reconstruction checks Data and every reference. No current definition is read.
				add("Q", stored.Data)
				refs := common.CloneProto(stored)
				refs.Data = nil
				add("E", compactMarshal(t, refs))
				readyCopy := common.CloneProto(ready)
				readyCopy.OperationId = ""
				add("R", compactMarshal(t, readyCopy))
				actionCopy := common.CloneProto(action)
				actionCopy.Action.OperationId = ""
				add("A", compactMarshal(t, actionCopy))
				// Exact external identity directory is charged, not omitted as an implicit map.
				add("I", protowireBytes(compactBlob(binary.AppendUvarint(nil, sequence), []byte(op.Id))))
				readyCopy.OperationId = op.Id
				actionCopy.Action.OperationId = op.Id
				require.True(t, proto.Equal(ready, readyCopy))
				require.True(t, proto.Equal(action, actionCopy))
			}
			slices.Sort(indexPaths)
			for _, path := range indexPaths {
				node := h.durable[path]
				payload := &commonpb.Payload{}
				require.NoError(t, proto.Unmarshal(node.Data.Data, payload))
				// Preserve exact source key and index value; no uncharged relocation directory.
				add("X", compactMarshal(t, &commonpb.Payload{Metadata: map[string][]byte{"key": []byte(strings.TrimPrefix(path, "OperationPages#"))}, Data: payload.Data}))
			}
			breakdown := map[string][4]int{}
			for name, c := range groups {
				collection := common.CloneProto(firstNode)
				collection.Data = nil
				collection.Metadata.Attributes = &persistencespb.ChasmNodeMetadata_CollectionAttributes{CollectionAttributes: &persistencespb.ChasmCollectionAttributes{}}
				retained[name] = collection
				templateNode := common.CloneProto(firstNode)
				templateNode.Data.Data = compactMarshal(t, &commonpb.Payload{Data: c.template})
				retained[name+"T"] = templateNode
				totalData := 0
				pages := 0
				for offset := 0; offset < len(c.records); offset += 16 {
					var slots [16][]byte
					for j := 0; j < 16 && offset+j < len(c.records); j++ {
						slots[j] = c.records[offset+j]
					}
					packed := compactPage(slots)
					decoded, err := expandCompactPage(packed)
					require.NoError(t, err)
					require.Equal(t, slots, decoded)
					page := common.CloneProto(firstNode)
					page.Data.Data = compactMarshal(t, &commonpb.Payload{Data: packed})
					retained[fmt.Sprintf("%s#%x", name, offset/16)] = page
					pages++
					totalData += len(packed)
				}
				// Count/control is explicit and survives even an empty collection in a live design.
				control := common.CloneProto(firstNode)
				control.Data.Data = compactMarshal(t, &commonpb.Payload{Data: binary.AppendUvarint(nil, uint64(len(c.records)))})
				retained[name+"C"] = control
				breakdown[name] = [4]int{len(c.records), pages, totalData, len(c.template)}
			}
			before := proto.Size(&persistencespb.WorkflowMutableState{ChasmNodes: h.durable})
			after := proto.Size(&persistencespb.WorkflowMutableState{ChasmNodes: retained})
			encoded, err := json.Marshal(breakdown)
			require.NoError(t, err)
			t.Logf("COMPACT N=%d original_framed=%d prototype_framed=%d nodes=%d groups_records_pages_data_template=%s", n, before, after, len(retained), encoded)
			// No claim that the prototype is a registered/valid reconstructed CHASM tree.
			// Fixed parent collection records and runtime adapter are still integration work.
		})
	}
}
func protowireBytes(value []byte) []byte { return compactBlob([]byte{10}, value) }
