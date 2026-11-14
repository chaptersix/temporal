//go:build test_dep

package scheduler2

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCompactStorageExactWire(t *testing.T) {
	template := append(protowireBytes(bytes.Repeat([]byte{7}, 80)), []byte{16, 1}...)
	cases := [][]byte{template, append(protowireBytes(bytes.Repeat([]byte{8}, 80)), []byte{16, 2}...), {10, 0, 10, 0, 16, 129, 0}, {24, 3}, nil}
	for _, wire := range cases {
		delta, err := compactRequest(template, wire)
		require.NoError(t, err)
		restored, err := expandCompactRequest(template, delta)
		require.NoError(t, err)
		require.True(t, bytes.Equal(wire, restored))
	}
	_, err := compactRequest(template, []byte{10, 255})
	require.Error(t, err)
	for _, wire := range [][]byte{nil, {2}, {1}, {1, 255}, {1, 127, 127, 0}, {1, 0, 0, 127}} {
		_, err := expandCompactRequest(template, wire)
		require.Error(t, err)
	}
}
func TestCompactStorageIdentityFallback(t *testing.T) {
	for _, op := range []*pb.OperationState{
		{Nominal: -1},
		{Nominal: 1, ManualId: 7},
		{Nominal: 2, BackfillId: 3},
		{Nominal: 2, BackfillId: 3, NominalTime: timestamppb.New(time.Date(2500, 1, 1, 0, 0, 0, 3, time.UTC))},
	} {
		op.OccurrenceId = compactOccurrence(op)
		op.ActionId = "inc/" + op.OccurrenceId + "/action"
		op.Id = op.ActionId + "/start"
		for _, literal := range []bool{false, true} {
			if literal {
				op.Id = "future/grammar/exact"
			}
			wire, err := compactOperation("inc", op)
			require.NoError(t, err)
			if literal {
				require.Zero(t, wire[0])
			} else {
				require.Equal(t, byte(1), wire[0])
			}
			restored, err := expandCompactOperation("inc", wire)
			require.NoError(t, err)
			require.True(t, proto.Equal(op, restored))
		}
	}
}
func TestCompactStoragePageIsolation(t *testing.T) {
	var metadata [16][]byte
	for i := range metadata {
		metadata[i] = []byte{byte(i + 1)}
	}
	page := compactPage(metadata)
	restored, err := expandCompactPage(page)
	require.NoError(t, err)
	require.Equal(t, metadata, restored)
	// Corrupt every request byte independently: metadata decoding takes only the
	// metadata page, so no issuance page is loaded, parsed or required at all.
	request := bytes.Repeat([]byte{255}, 1<<20)
	request[0] = 0
	_, err = expandCompactPage(request)
	require.Error(t, err)
	again, err := expandCompactPage(page)
	require.NoError(t, err)
	require.Equal(t, metadata, again)
	require.Len(t, again, 16)
	for _, wire := range [][]byte{nil, page[:64], page[:len(page)-1], append(bytes.Clone(page), 0)} {
		_, err := expandCompactPage(wire)
		require.Error(t, err)
	}
}
