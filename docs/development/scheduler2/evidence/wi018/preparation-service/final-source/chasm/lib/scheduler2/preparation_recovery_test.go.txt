package scheduler2

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/proto"
)

func TestPreparationRecoveryProgressAcrossCacheCapacity(t *testing.T) {
	t.Parallel()
	service, wall, _, data := preparationServiceFixture(t)
	definition, snapshot, err := service.PrepareDefinition(&schedulepb.ScheduleSpec{TimezoneName: strings.Repeat("s", 20000), TimezoneData: data}, "seed", 1)
	require.NoError(t, err)
	candidate, err := service.PrepareTimezone(definition, snapshot, 1)
	require.NoError(t, err)
	h := newDefinitionHarness(t, definition)
	p, ctx := definitionRoot(t, h, true)
	require.NoError(t, p.BeginTimezoneCandidate(ctx.(chasm.MutableContext), candidate.Descriptor()))
	h.commit(true)
	for quantum := 0; quantum < 10; quantum++ {
		// Real service admission of1001 distinct owners evicts both target stages.
		for i := 0; i < 1001; i++ {
			_, _, err := service.PrepareDefinition(&schedulepb.ScheduleSpec{TimezoneName: "UTC"}, "evict", int64(100+quantum*2000+i))
			require.NoError(t, err)
		}
		_, hit := service.Definition(definition.owner)
		require.False(t, hit)
		_, hit = service.Timezone(candidate.descriptor.ownerID)
		require.False(t, hit)
		wall.Advance(time.Second)
		p, ctx = definitionRoot(t, h, false)
		source, revision, _, err := p.SourceSnapshot(ctx)
		require.NoError(t, err)
		restoredDefinition, err := service.RestoreDefinition(source, revision, definition.owner)
		require.NoError(t, err)
		zone := p.Timezone.Get(ctx)
		descriptor, rawFrontier, transitionFrontier, pending, err := zone.PendingTimezone()
		require.NoError(t, err)
		require.True(t, pending)
		var recovered *StagedTimezoneCandidate
		if rawFrontier < descriptor.SourcePageCount() {
			var matched bool
			recovered, matched, err = service.RecoverTimezone(restoredDefinition, descriptor)
			require.NoError(t, err)
			require.True(t, matched)
		} else {
			raw, generation, _, err := zone.PendingSourceSnapshot(ctx)
			require.NoError(t, err)
			recovered, err = service.RestoreTimezone(restoredDefinition, raw, generation, descriptor.OwnerID())
			require.NoError(t, err)
		}
		require.Equal(t, candidate.descriptor, recovered.descriptor)
		batch, err := recovered.InstallBatch(rawFrontier, transitionFrontier, 8)
		require.NoError(t, err)
		zone, ctx = pendingTimezoneRoot(t, h, true)
		done, err := zone.InstallCandidateBatch(ctx.(chasm.MutableContext), batch)
		require.NoError(t, err)
		if !done {
			require.Greater(t, zone.Pending.InstalledSourcePages+uint64(zone.Pending.InstalledPages), rawFrontier+uint64(transitionFrontier))
		}
		h.commit(true)
		if done {
			zone, ctx = pendingTimezoneRoot(t, h, false)
			require.Equal(t, candidate.descriptor.ownerID[:], zone.SourceId)
			source, _, _, err := zone.SourceSnapshotChecked(ctx)
			require.NoError(t, err)
			require.Equal(t, snapshot.ResolvedAt(), source.ResolvedAt.AsTime())
			return
		}
	}
	t.Fatal("stable source did not progress under cache eviction")
}

func TestPreparationRecoveryRejectsChangedSourceWithoutWrites(t *testing.T) {
	t.Parallel()
	ny, err := os.ReadFile("internal/occurrence/testdata/America_New_York.tzif")
	require.NoError(t, err)
	london, err := os.ReadFile("internal/occurrence/testdata/Europe_London.tzif")
	require.NoError(t, err)
	current := ny
	wall := clock.NewEventTimeSource().Update(time.Unix(1704067200, 0).UTC())
	env := timezonesource.NewEnvironment(timezonesource.SourceConfig{Clock: wall, LookupEnv: func(string) (string, bool) { return "", false }, Embedded: func(string) ([]byte, error) { return bytes.Clone(current), nil }})
	service, err := NewPreparationService(env, wall)
	require.NoError(t, err)
	definition, snapshot, err := service.PrepareDefinition(&schedulepb.ScheduleSpec{TimezoneName: "Fixture/Changing"}, "seed", 1)
	require.NoError(t, err)
	candidate, err := service.PrepareTimezone(definition, snapshot, 1)
	require.NoError(t, err)
	h := newDefinitionHarness(t, definition)
	p, ctx := definitionRoot(t, h, true)
	require.NoError(t, p.BeginTimezoneCandidate(ctx.(chasm.MutableContext), candidate.Descriptor()))
	h.commit(true)
	zone, ctx := pendingTimezoneRoot(t, h, true)
	batch, err := candidate.InstallBatch(0, 0, 1)
	require.NoError(t, err)
	_, err = zone.InstallCandidateBatch(ctx.(chasm.MutableContext), batch)
	require.NoError(t, err)
	h.commit(true)
	zone, _ = pendingTimezoneRoot(t, h, false)
	before := common.CloneProto(zone.TimezonePreparationState)
	current = london
	wall.Advance(25 * time.Hour)
	got, matched, err := service.RecoverTimezone(definition, candidate.Descriptor())
	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, got)
	require.True(t, proto.Equal(before, zone.TimezonePreparationState))
	snapshot, err = service.ResolveDefinition(definition)
	require.NoError(t, err)
	next, err := service.PrepareTimezone(definition, snapshot, 2)
	require.NoError(t, err)
	zone, ctx = pendingTimezoneRoot(t, h, true)
	require.NoError(t, zone.BeginCandidate(ctx.(chasm.MutableContext), next.Descriptor()))
	require.Equal(t, uint64(1), zone.CleanupCount)
	require.Zero(t, zone.Pending.InstalledSourcePages)
	h.commit(true)
}

func TestPreparationRecoveryPreservesNativeWordWidthAndYear(t *testing.T) {
	t.Parallel()
	makeService := func(year int, bits uint8) *PreparationService {
		wall := clock.NewEventTimeSource().Update(time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC))
		env := timezonesource.NewEnvironment(timezonesource.SourceConfig{Clock: wall, WindowsLocal: func() (occurrence.WindowsTimezoneSource, error) {
			return occurrence.WindowsTimezoneSource{WordBits: bits, Bias: 2147483647}, nil
		}})
		service, err := NewPreparationService(env, wall)
		require.NoError(t, err)
		return service
	}
	old := makeService(2024, 32)
	definition, snapshot, err := old.PrepareDefinition(&schedulepb.ScheduleSpec{TimezoneName: "Local"}, "seed", 1)
	require.NoError(t, err)
	candidate, err := old.PrepareTimezone(definition, snapshot, 1)
	require.NoError(t, err)
	fresh := makeService(2025, 64)
	recovered, matched, err := fresh.RecoverTimezone(definition, candidate.Descriptor())
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, candidate.descriptor, recovered.descriptor)
	require.Equal(t, int64(2024), recovered.descriptor.nativeYear)
	require.Equal(t, uint8(32), recovered.descriptor.nativeWordBits)
}
