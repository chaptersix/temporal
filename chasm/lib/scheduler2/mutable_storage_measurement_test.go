//go:build test_dep

package scheduler2

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/packedstore"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

type mutableStorageRow struct {
	store      packedstore.ID
	key, value []byte
}
type mutableStorageBackend struct {
	data                                                     map[string][]byte
	exemplar                                                 *persistencespb.ChasmNode
	framed, peak, maxBatchBytes, maxChanges, reads, maxReads int
}

func (b *mutableStorageBackend) Read(k string) ([]byte, bool, error) {
	b.reads++
	v, ok := b.data[k]
	return bytes.Clone(v), ok, nil
}
func (b *mutableStorageBackend) framedEntry(k string, v []byte) int {
	node := common.CloneProto(b.exemplar)
	node.Data.Data = compactMarshalRaw(&commonpb.Payload{Data: v})
	return proto.Size(&persistencespb.WorkflowMutableState{ChasmNodes: map[string]*persistencespb.ChasmNode{"PackedPages#" + k: node}})
}
func compactMarshalRaw(value proto.Message) []byte {
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(value)
	if err != nil {
		panic(err)
	}
	return wire
}
func (b *mutableStorageBackend) Apply(changes []queueindex.IndexChange) error {
	batchBytes := 0
	for _, c := range changes {
		if old, ok := b.data[c.Key]; ok {
			b.framed -= b.framedEntry(c.Key, old)
		}
		if c.Delete {
			delete(b.data, c.Key)
		} else {
			b.data[c.Key] = bytes.Clone(c.Value)
			size := b.framedEntry(c.Key, c.Value)
			b.framed += size
			batchBytes += size
		}
	}
	b.peak = max(b.peak, b.framed)
	b.maxBatchBytes = max(b.maxBatchBytes, batchBytes)
	b.maxChanges = max(b.maxChanges, len(changes))
	b.maxReads = max(b.maxReads, b.reads)
	b.reads = 0
	return nil
}
func mutableStorageRows(t *testing.T, h *treeHarness) ([]mutableStorageRow, map[string][]byte, int, *persistencespb.ChasmNode) {
	t.Helper()
	s, ctx := h.root(false)
	templates := map[string][]byte{}
	rows := []mutableStorageRow{}
	add := func(name byte, k, wire []byte, templated bool) {
		if templated {
			tag := string(name)
			base, ok := templates[tag]
			if !ok {
				base = bytes.Clone(wire)
				templates[tag] = base
			}
			delta, err := compactRequest(base, wire)
			require.NoError(t, err)
			restored, err := expandCompactRequest(base, delta)
			require.NoError(t, err)
			require.Equal(t, wire, restored)
			wire = delta
		}
		rows = append(rows, mutableStorageRow{store: packedstore.ID{name}, key: bytes.Clone(k), value: bytes.Clone(wire)})
	}
	for seq := uint64(1); seq <= uint64(s.OperationStore.PendingCount); seq++ {
		k := binary.BigEndian.AppendUint64(nil, seq)
		ready, ok := s.ReadyRecords.Get(ctx, readyRecordKey(seq))
		require.True(t, ok)
		op, ok := s.OperationRecords.Get(ctx, ready.OperationId)
		require.True(t, ok)
		request, ok := s.RequestRecords.Get(ctx, ready.OperationId)
		require.True(t, ok)
		action, ok := s.ActionRecords.Get(ctx, ready.OperationId)
		require.True(t, ok)
		operation, err := compactOperation(s.Incarnation, op)
		require.NoError(t, err)
		restored, err := expandCompactOperation(s.Incarnation, operation)
		require.NoError(t, err)
		require.True(t, proto.Equal(op, restored))
		add('O', k, protowireBytes(operation), true)
		add('Q', k, request.Data, true)
		request.Data = nil
		add('E', k, compactMarshal(t, request), true)
		ready.OperationId = ""
		add('R', k, compactMarshal(t, ready), true)
		action.Action.OperationId = ""
		add('A', k, compactMarshal(t, action), true)
		add('I', []byte(op.Id), k, false)
	}
	fixed := map[string]*persistencespb.ChasmNode{}
	var exemplar *persistencespb.ChasmNode
	paths := []string{}
	for path := range h.durable {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	for _, path := range paths {
		node := h.durable[path]
		group := strings.SplitN(path, "#", 2)[0]
		switch group {
		case "OperationRecords", "RequestRecords", "ActionRecords", "ReadyRecords":
			if node.GetMetadata().GetDataAttributes() != nil {
				exemplar = node
			}
		case "OperationPages":
			if node.GetMetadata().GetDataAttributes() != nil {
				payload := &commonpb.Payload{}
				require.NoError(t, proto.Unmarshal(node.Data.Data, payload))
				add('X', []byte(strings.TrimPrefix(path, "OperationPages#")), payload.Data, false)
			}
		default:
			fixed[path] = node
		}
	}
	require.NotNil(t, exemplar)
	for tag, wire := range templates {
		node := common.CloneProto(exemplar)
		node.Data.Data = compactMarshal(t, &commonpb.Payload{Data: wire})
		fixed["template"+tag] = node
	}
	// One actual DATA-map collection owns all opaque namespaced packed pages.
	collection := common.CloneProto(exemplar)
	collection.Data = nil
	collection.Metadata.Attributes = &persistencespb.ChasmNodeMetadata_CollectionAttributes{CollectionAttributes: &persistencespb.ChasmCollectionAttributes{}}
	fixed["PackedPages"] = collection
	return rows, templates, proto.Size(&persistencespb.WorkflowMutableState{ChasmNodes: fixed}), exemplar
}
func TestMutablePackedStorageMeasurement(t *testing.T) {
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
			rows, _, fixed, exemplar := mutableStorageRows(t, h)
			for _, isolated := range []bool{false, true} {
				measureMutableRows(t, h, rows, fixed, exemplar, n, isolated, false)
			}
			fullRows := slices.Clone(rows)
			for i := range fullRows {
				for j := 1; j < 16; j++ {
					fullRows[i].store[j] = byte(j*17) ^ fullRows[i].store[0]
				}
			}
			measureMutableRows(t, h, fullRows, fixed, exemplar, n, true, true)

		})
	}
}

func measureMutableRows(t *testing.T, h *treeHarness, rows []mutableStorageRow, fixed int, exemplar *persistencespb.ChasmNode, n int, isolated, fullNamespace bool) {
	t.Helper()
	b := &mutableStorageBackend{data: map[string][]byte{}, exemplar: exemplar}
	apply := func(row mutableStorageRow, remove bool) {
		if isolated && row.store[0] == 'Q' {
			// Immutable issuance deletion addresses one DATA node without reading its bytes.
			require.NoError(t, b.Apply([]queueindex.IndexChange{{Key: immutableRequestAddress(binary.BigEndian.Uint64(row.key)), Value: row.value, Delete: remove}}))
			return
		}
		batch := packedstore.NewBatch(b)
		var err error
		if remove {
			err = batch.Delete(row.store, row.key)
		} else {
			err = batch.Put(row.store, row.key, row.value)
		}
		require.NoError(t, err)
		require.NoError(t, batch.Commit())
	}
	for _, row := range rows {
		apply(row, false)
	}
	initial := b.framed
	// Delete and reinsert every fourth exact row in deterministic mixed order;
	// this exercises merges, borrowed separators, fresh page IDs and new splits.
	order := rand.New(rand.NewSource(31)).Perm(len(rows))
	for i := 0; i < len(order); i += 4 {
		apply(rows[order[i]], true)
	}
	trough := b.framed
	for i := len(order) - 1; i >= 0; i-- {
		if i%4 == 0 {
			apply(rows[order[i]], false)
		}
	}
	readsBeforeVerify := b.reads
	for _, row := range rows {
		batch := packedstore.NewBatch(b)
		var got []byte
		var ok bool
		var err error
		if isolated && row.store[0] == 'Q' {
			got, ok, err = b.Read(immutableRequestAddress(binary.BigEndian.Uint64(row.key)))
		} else {
			got, ok, err = batch.Get(row.store, row.key)
		}
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, row.value, got)
	}
	t.Logf("MUTABLE isolated_issuance=%t full_namespace=%t N=%d original=%d initial=%d after_delete=%d final=%d peak_committed=%d nodes=%d max_transaction_updated_framed_bytes=%d max_changed_pages=%d max_backend_reads=%d verification_reads=%d fixed=%d", isolated, fullNamespace, n, proto.Size(&persistencespb.WorkflowMutableState{ChasmNodes: h.durable}), initial+fixed, trough+fixed, b.framed+fixed, b.peak+fixed, len(b.data), b.maxBatchBytes, b.maxChanges, b.maxReads, b.reads-readsBeforeVerify, fixed)
}

func immutableRequestAddress(sequence uint64) string {
	return "q/" + base64.RawURLEncoding.EncodeToString(binary.BigEndian.AppendUint64(nil, sequence))
}
func TestImmutableRequestAddress(t *testing.T) {
	seen := map[string]bool{}
	for _, sequence := range []uint64{0, 1, 255, 256, 1 << 63, ^uint64(0)} {
		address := immutableRequestAddress(sequence)
		require.False(t, seen[address])
		seen[address] = true
		wire, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(address, "q/"))
		require.NoError(t, err)
		require.Len(t, wire, 8)
		require.Equal(t, sequence, binary.BigEndian.Uint64(wire))
		require.Len(t, address, 13)
	}
}
