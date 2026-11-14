package scheduler2

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTimezoneSourceRecordBounds(t *testing.T) {
	t.Parallel()
	id := bytes.Repeat([]byte{0xff}, 32)
	header := bytes.Repeat([]byte{0xff}, 89)
	page := &scheduler2pb.TimezoneSourcePage{SourceId: id, Ordinal: ^uint64(0), Encoded: bytes.Repeat([]byte{1}, timezoneSourcePageBytes)}
	require.Equal(t, 1072, proto.Size(page))
	// Independent field maxima conservatively bound normal writers, even though
	// some combinations (native fields with a TZif header) cannot occur together.
	pending := &scheduler2pb.TimezonePendingSource{DefinitionSourceId: id, SourceId: id, SemanticFingerprint: id, Generation: ^uint64(0), Header: header, TotalPages: ^uint32(0), InstalledPages: ^uint32(0), SourceBytes: ^uint64(0), InstalledSourcePages: ^uint64(0), SourceHash: id, ResolvedAt: &timestamppb.Timestamp{Seconds: -62135596800, Nanos: 999999999}, NativeInitializationYear: -1, SourceKind: 2, NativeWordBits: 64}
	require.Equal(t, 306, proto.Size(pending))
	control := &scheduler2pb.TimezonePreparationState{FormatVersion: 2, DefinitionSourceId: id, SourceId: id, SemanticFingerprint: id, Generation: ^uint64(0), Header: header, TotalPages: ^uint32(0), InstalledPages: ^uint32(0), CleanupHead: ^uint64(0), CleanupTail: ^uint64(0), CleanupCount: ^uint64(0), LastGeneration: ^uint64(0), SourceBytes: ^uint64(0), SourceHash: id, Pending: pending}
	require.Equal(t, 616, proto.Size(control))
	cleanup := &scheduler2pb.TimezoneSupersededSource{SourceId: id, Header: header, InstalledPages: ^uint32(0), Cursor: ^uint32(0), SourceBytes: ^uint64(0), InstalledSourcePages: ^uint64(0), SourceCursor: ^uint64(0)}
	require.Equal(t, 170, proto.Size(cleanup))
	task := &scheduler2pb.SourcePreparationTask{DefinitionOwnerId: id, Token: ^uint64(0)}
	require.Equal(t, 45, proto.Size(task))
}
