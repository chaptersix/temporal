//go:build test_dep

package scheduler2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackedRuntimePhysicalSuccessAndCallbackOrdering(t *testing.T) {
	testSelectedRuntimePhysicalSuccessAndCallbackOrdering(t, true)
}
func TestPackedRuntimePhysicalRetryKeepsFrozenRequest(t *testing.T) {
	testSelectedRuntimePhysicalRetryKeepsFrozenRequest(t, true)
}
func TestPackedRuntimePhysicalClaimCrashAndRecovery(t *testing.T) {
	testSelectedRuntimePhysicalClaimCrashAndRecovery(t, true)
}
func TestPackedRuntimeCloseDuringPhysicalRPC(t *testing.T) {
	testSelectedRuntimeCloseDuringPhysicalRPC(t, true)
}
func TestPackedRuntimeAutomaticCoordinatorPhysicalStart(t *testing.T) {
	testSelectedRuntimeAutomaticCoordinatorPhysicalStart(t, true)
}

// Cached DATA payloads still require packed page and record decoding per read.
func TestPackedRuntimeCachedPayloadObservations(t *testing.T) {
	e := newSelectedTaskEngine(t, true, nil)
	s, ctx := e.h.root(false)
	id := s.OperationId
	_, ok, err := s.operationRecord(ctx, id)
	require.NoError(t, err)
	require.True(t, ok)
	counts := e.h.node.BeginWorkInspectionForTest()
	_, ok, err = s.operationRecord(ctx, id)
	require.NoError(t, err)
	require.True(t, ok)
	first := counts.Snapshot()
	require.Zero(t, first["data_decode_calls"])
	for _, name := range []string{"packed_read_calls", "packed_read_entries", "packed_record_decode_calls", "packed_backend_calls"} {
		require.Positive(t, first[name], name)
	}
	counts.Reset()
	_, ok, err = s.operationRecord(ctx, id)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, first, counts.Snapshot())
}
