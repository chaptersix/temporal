package scheduler2

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

func TestTimezonePendingBatchFaultsAndCleanup(t *testing.T) {
	t.Parallel()
	service, wall, _, data := preparationServiceFixture(t)
	definition, snapshot, err := service.PrepareDefinition(&schedulepb.ScheduleSpec{TimezoneName: strings.Repeat("q", 20000), TimezoneData: data}, "seed", 1)
	require.NoError(t, err)
	candidate, err := service.PrepareTimezone(definition, snapshot, 1)
	require.NoError(t, err)
	h := newDefinitionHarness(t, definition)
	root, ctx := definitionRoot(t, h, true)
	require.NoError(t, root.BeginTimezoneCandidate(ctx.(chasm.MutableContext), candidate.Descriptor()))
	h.commit(true)
	// Fault every record in a full raw batch, then every record in a batch that
	// crosses the raw/compiled boundary. No earlier record may be published.
	for quantum := 0; quantum < 2; quantum++ {
		zone, _ := pendingTimezoneRoot(t, h, false)
		_, source, trans, _, err := zone.PendingTimezone()
		require.NoError(t, err)
		batch, err := candidate.InstallBatch(source, trans, 16)
		require.NoError(t, err)
		require.Equal(t, uint8(16), batch.count)
		for i := uint8(0); i < batch.count; i++ {
			zone, ctx := pendingTimezoneRoot(t, h, true)
			before := common.CloneProto(zone.TimezonePreparationState)
			good := batch.pages[i].encoded
			batch.pages[i].encoded = nil
			_, err := zone.InstallCandidateBatch(ctx.(chasm.MutableContext), batch)
			require.Error(t, err)
			require.True(t, proto.Equal(before, zone.TimezonePreparationState))
			batch.pages[i].encoded = good
			for j := uint8(0); j < batch.count; j++ {
				page := batch.pages[j]
				if page.source {
					_, exists := zone.SourcePages.Get(ctx, timezoneSourcePageKey(candidate.descriptor.ownerID[:], page.ordinal))
					require.False(t, exists)
				} else {
					_, exists := zone.Pages.Get(ctx, timezonePageKey(candidate.descriptor.ownerID[:], uint32(page.ordinal)))
					require.False(t, exists)
				}
			}
			h.commit(false)
		}
		zone, ctx := pendingTimezoneRoot(t, h, true)
		_, err = zone.InstallCandidateBatch(ctx.(chasm.MutableContext), batch)
		require.NoError(t, err)
		h.commit(true)
	}
	installTimezoneCandidate(t, h, candidate, 16)
	wall.Advance(time.Second)
	snapshot, err = service.ResolveDefinition(definition)
	require.NoError(t, err)
	replacement, err := service.PrepareTimezone(definition, snapshot, 2)
	require.NoError(t, err)
	zone, ctx := pendingTimezoneRoot(t, h, true)
	require.NoError(t, zone.BeginCandidate(ctx.(chasm.MutableContext), replacement.Descriptor()))
	h.commit(true)
	installTimezoneCandidate(t, h, replacement, 16)
	// Reach a cleanup quantum containing both compiled and raw pages.
	for {
		zone, ctx = pendingTimezoneRoot(t, h, true)
		old, ok := zone.Superseded.Get(ctx, strconv.FormatUint(zone.CleanupHead, 10))
		require.True(t, ok)
		if old.InstalledPages-old.Cursor < 16 {
			h.commit(false)
			break
		}
		_, err = zone.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
		require.NoError(t, err)
		h.commit(true)
	}
	zone, ctx = pendingTimezoneRoot(t, h, true)
	old, ok := zone.Superseded.Get(ctx, strconv.FormatUint(zone.CleanupHead, 10))
	require.True(t, ok)
	raw, ok := zone.SourcePages.Get(ctx, timezoneSourcePageKey(old.SourceId, old.SourceCursor))
	require.True(t, ok)
	bad := common.CloneProto(raw)
	bad.Encoded = append(bad.Encoded, 0)
	zone.SourcePages.Put(ctx.(chasm.MutableContext), timezoneSourcePageKey(old.SourceId, old.SourceCursor), bad)
	h.commit(true)
	zone, ctx = pendingTimezoneRoot(t, h, true)
	before := common.CloneProto(zone.TimezonePreparationState)
	oldBefore := common.CloneProto(old)
	_, err = zone.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
	require.Error(t, err)
	require.True(t, proto.Equal(before, zone.TimezonePreparationState))
	current, ok := zone.Superseded.Get(ctx, strconv.FormatUint(zone.CleanupHead, 10))
	require.True(t, ok)
	require.True(t, proto.Equal(oldBefore, current))
	h.commit(false)
	zone, ctx = pendingTimezoneRoot(t, h, true)
	zone.SourcePages.Put(ctx.(chasm.MutableContext), timezoneSourcePageKey(old.SourceId, old.SourceCursor), raw)
	h.commit(true)
	for {
		zone, ctx = pendingTimezoneRoot(t, h, true)
		done, err := zone.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
		require.NoError(t, err)
		h.commit(true)
		if done {
			break
		}
	}
	zone, ctx = pendingTimezoneRoot(t, h, false)
	require.Equal(t, replacement.descriptor.ownerID[:], zone.SourceId)
	for ordinal := uint64(0); ordinal < candidate.descriptor.SourcePageCount(); ordinal++ {
		_, ok := zone.SourcePages.Get(ctx, timezoneSourcePageKey(candidate.descriptor.ownerID[:], ordinal))
		require.False(t, ok)
	}
	for ordinal := uint32(0); ordinal < candidate.descriptor.totalPages; ordinal++ {
		_, ok := zone.Pages.Get(ctx, timezonePageKey(candidate.descriptor.ownerID[:], ordinal))
		require.False(t, ok)
	}
}

func TestTimezonePendingSupersessionPreservesActiveAndPrefixes(t *testing.T) {
	t.Parallel()
	service, wall, _, data := preparationServiceFixture(t)
	definition, snapshot, err := service.PrepareDefinition(&schedulepb.ScheduleSpec{TimezoneName: strings.Repeat("r", 20000), TimezoneData: data}, "seed", 1)
	require.NoError(t, err)
	initial, err := service.PrepareTimezone(definition, snapshot, 1)
	require.NoError(t, err)
	h := newDefinitionHarness(t, definition)
	p, ctx := definitionRoot(t, h, true)
	require.NoError(t, p.BeginTimezoneCandidate(ctx.(chasm.MutableContext), initial.Descriptor()))
	h.commit(true)
	installTimezoneCandidate(t, h, initial, 16)
	wall.Advance(time.Second)
	snapshot, err = service.ResolveDefinition(definition)
	require.NoError(t, err)
	second, err := service.PrepareTimezone(definition, snapshot, 2)
	require.NoError(t, err)
	zone, ctx := pendingTimezoneRoot(t, h, true)
	require.NoError(t, zone.BeginCandidate(ctx.(chasm.MutableContext), second.Descriptor()))
	batch, err := second.InstallBatch(0, 0, 8)
	require.NoError(t, err)
	_, err = zone.InstallCandidateBatch(ctx.(chasm.MutableContext), batch)
	require.NoError(t, err)
	h.commit(true)
	third, err := service.PrepareTimezone(definition, snapshot, 3)
	require.NoError(t, err)
	zone, ctx = pendingTimezoneRoot(t, h, true)
	require.NoError(t, zone.BeginCandidate(ctx.(chasm.MutableContext), third.Descriptor()))
	require.Equal(t, initial.descriptor.ownerID[:], zone.SourceId)
	require.Equal(t, uint64(1), zone.CleanupCount)
	require.Equal(t, uint64(0), zone.Pending.InstalledSourcePages)
	before := common.CloneProto(zone.TimezonePreparationState)
	_, err = zone.InstallCandidateBatch(ctx.(chasm.MutableContext), batch)
	require.ErrorContains(t, err, "owner")
	require.True(t, proto.Equal(before, zone.TimezonePreparationState))
	h.commit(true)
	zone, ctx = pendingTimezoneRoot(t, h, true)
	done, err := zone.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, initial.descriptor.ownerID[:], zone.SourceId)
	require.Equal(t, third.descriptor.ownerID[:], zone.Pending.SourceId)
	h.commit(true)
	// Same candidate publication cannot reset an already installed prefix.
	zone, ctx = pendingTimezoneRoot(t, h, true)
	batch, err = third.InstallBatch(0, 0, 1)
	require.NoError(t, err)
	_, err = zone.InstallCandidateBatch(ctx.(chasm.MutableContext), batch)
	require.NoError(t, err)
	require.NoError(t, zone.BeginCandidate(ctx.(chasm.MutableContext), third.Descriptor()))
	require.Equal(t, uint64(1), zone.Pending.InstalledSourcePages)
	h.commit(true)
	zone, ctx = pendingTimezoneRoot(t, h, true)
	page, ok := zone.SourcePages.Get(ctx, timezoneSourcePageKey(initial.descriptor.ownerID[:], 0))
	require.True(t, ok)
	bad := common.CloneProto(page)
	bad.Encoded = bytes.Clone(bad.Encoded)
	bad.Encoded[0] ^= 1
	zone.SourcePages.Put(ctx.(chasm.MutableContext), timezoneSourcePageKey(initial.descriptor.ownerID[:], 0), bad)
	h.commit(true)
	zone, ctx = pendingTimezoneRoot(t, h, false)
	_, err = zone.PreparedTimezone(ctx)
	require.NoError(t, err)
	_, _, _, err = zone.SourceSnapshotChecked(ctx)
	require.ErrorContains(t, err, "hash")
}
