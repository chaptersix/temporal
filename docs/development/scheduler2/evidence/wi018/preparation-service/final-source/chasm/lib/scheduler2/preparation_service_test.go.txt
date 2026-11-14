package scheduler2

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func preparationServiceFixture(t *testing.T) (*PreparationService, *clock.EventTimeSource, *int, []byte) {
	t.Helper()
	data, err := os.ReadFile("internal/occurrence/testdata/America_New_York.tzif")
	require.NoError(t, err)
	wall := clock.NewEventTimeSource().Update(time.Unix(1704067200, 0).UTC())
	reads := new(int)
	env := timezonesource.NewEnvironment(timezonesource.SourceConfig{Clock: wall, LookupEnv: func(string) (string, bool) { return "", false }, Read: func(string, string) ([]byte, error) { return nil, os.ErrNotExist }, Embedded: func(name string) ([]byte, error) {
		*reads++
		if name != "Fixture/Zone" {
			return nil, errors.New("missing zone")
		}
		return bytes.Clone(data), nil
	}})
	service, err := NewPreparationService(env, wall)
	require.NoError(t, err)
	return service, wall, reads, data
}
func TestPreparationServiceWallCacheAndDefinitionRestore(t *testing.T) {
	t.Parallel()
	service, wall, reads, _ := preparationServiceFixture(t)
	spec := &schedulepb.ScheduleSpec{TimezoneName: "Fixture/Zone", Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Minute)}}}
	definition, snapshot, err := service.PrepareDefinition(spec, "seed", 1)
	require.NoError(t, err)
	require.Equal(t, 1, *reads)
	wall.Advance(24 * time.Hour)
	same, err := service.ResolveDefinition(definition)
	require.NoError(t, err)
	require.Same(t, snapshot, same)
	require.Equal(t, 1, *reads)
	wall.Advance(time.Nanosecond)
	next, err := service.ResolveDefinition(definition)
	require.NoError(t, err)
	require.NotSame(t, snapshot, next)
	require.Equal(t, 2, *reads)
	_, hit := service.Definition(definition.owner)
	require.False(t, hit)
	rebuilt, err := service.RestoreDefinition(common.CloneProto(definition.source), definition.revision, definition.owner)
	require.NoError(t, err)
	require.Equal(t, definition.owner, rebuilt.owner)
	wrong := definition.owner
	wrong[0] ^= 1
	_, err = service.RestoreDefinition(definition.source, definition.revision, wrong)
	require.ErrorContains(t, err, "owner")
	batch, hit, err := service.DefinitionBatch(definition.owner, 0, 16)
	require.NoError(t, err)
	require.True(t, hit)
	h := newDefinitionHarness(t, definition)
	p, ctx := definitionRoot(t, h, true)
	done, err := p.InstallBatch(ctx.(chasm.MutableContext), batch)
	require.NoError(t, err)
	require.True(t, done)
	h.commit(true)
	p, ctx = definitionRoot(t, h, true)
	before := common.CloneProto(p.DefinitionPreparationState)
	_, err = p.InstallBatch(ctx.(chasm.MutableContext), batch)
	require.ErrorContains(t, err, "frontier")
	require.True(t, proto.Equal(before, p.DefinitionPreparationState))
	h.commit(false)
	require.Equal(t, 2, *reads)
}

func pendingTimezoneRoot(t *testing.T, h *treeHarness, mutable bool) (*TimezonePreparation, chasm.Context) {
	t.Helper()
	p, ctx := definitionRoot(t, h, mutable)
	return p.Timezone.Get(ctx), ctx
}
func installTimezoneCandidate(t *testing.T, h *treeHarness, candidate *StagedTimezoneCandidate, limit int) {
	t.Helper()
	for {
		p, ctx := pendingTimezoneRoot(t, h, true)
		_, source, trans, pending, err := p.PendingTimezone()
		require.NoError(t, err)
		if !pending {
			h.commit(false)
			return
		}
		batch, err := candidate.InstallBatch(source, trans, limit)
		require.NoError(t, err)
		done, err := p.InstallCandidateBatch(ctx.(chasm.MutableContext), batch)
		require.NoError(t, err)
		h.commit(true)
		if done {
			return
		}
	}
}

func TestPreparationServicePagedSourceReloadAndActivation(t *testing.T) {
	t.Parallel()
	service, wall, _, data := preparationServiceFixture(t)
	// An accepted long custom-zone name makes raw source exceed one batch without
	// changing transition semantics or imposing an artificial TZif count limit.
	spec := &schedulepb.ScheduleSpec{TimezoneName: strings.Repeat("z", 20000), TimezoneData: data, Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Minute)}}}
	definition, snapshot, err := service.PrepareDefinition(spec, "seed", 1)
	require.NoError(t, err)
	candidate, err := service.PrepareTimezone(definition, snapshot, 1)
	require.NoError(t, err)
	require.Greater(t, candidate.descriptor.SourcePageCount(), uint64(16))
	h := newDefinitionHarness(t, definition)
	p, ctx := definitionRoot(t, h, true)
	require.NoError(t, p.BeginTimezoneCandidate(ctx.(chasm.MutableContext), candidate.Descriptor()))
	h.commit(true)
	zone, ctx := pendingTimezoneRoot(t, h, true)
	_, err = zone.PreparedTimezone(ctx)
	require.Error(t, err)
	first, err := candidate.InstallBatch(0, 0, 8)
	require.NoError(t, err)
	done, err := zone.InstallCandidateBatch(ctx.(chasm.MutableContext), first)
	require.NoError(t, err)
	require.False(t, done)
	h.commit(true)
	zone, ctx = pendingTimezoneRoot(t, h, false)
	_, _, _, err = zone.PendingSourceSnapshot(ctx)
	require.ErrorContains(t, err, "incomplete")
	// Cache loss can produce the same rules at a different preparation instant;
	// the exact source bytes must have a different candidate owner.
	wall.Advance(time.Second)
	freshSnapshot, err := service.ResolveDefinition(definition)
	require.NoError(t, err)
	freshSameGeneration, err := service.PrepareTimezone(definition, freshSnapshot, 1)
	require.NoError(t, err)
	require.Equal(t, candidate.descriptor.fingerprint, freshSameGeneration.descriptor.fingerprint)
	require.NotEqual(t, candidate.descriptor.ownerID, freshSameGeneration.descriptor.ownerID)
	zone, ctx = pendingTimezoneRoot(t, h, true)
	require.ErrorContains(t, zone.BeginCandidate(ctx.(chasm.MutableContext), freshSameGeneration.Descriptor()), "newer generation")
	h.commit(false)
	// Complete the original raw prefix and reconstruct it using only durable bytes.
	zone, ctx = pendingTimezoneRoot(t, h, true)
	remaining := int(candidate.descriptor.SourcePageCount() - 8)
	require.LessOrEqual(t, remaining, 16)
	batch, err := candidate.InstallBatch(8, 0, remaining)
	require.NoError(t, err)
	done, err = zone.InstallCandidateBatch(ctx.(chasm.MutableContext), batch)
	require.NoError(t, err)
	require.False(t, done)
	h.commit(true)
	zone, ctx = pendingTimezoneRoot(t, h, false)
	source, generation, binding, err := zone.PendingSourceSnapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, definition.owner[:], binding)
	restored, err := service.RestoreTimezone(definition, source, generation, candidate.descriptor.ownerID)
	require.NoError(t, err)
	require.Equal(t, candidate.descriptor, restored.descriptor)
	installTimezoneCandidate(t, h, restored, 3)
	zone, ctx = pendingTimezoneRoot(t, h, false)
	active, err := zone.PreparedTimezone(ctx)
	require.NoError(t, err)
	require.Equal(t, candidate.descriptor.fingerprint, active.Fingerprint())
	require.Nil(t, zone.Pending)
	source, generation, binding, err = zone.SourceSnapshotChecked(ctx)
	require.NoError(t, err)
	require.Equal(t, candidate.descriptor.generation, generation)
	require.Equal(t, definition.owner[:], binding)
	require.True(t, proto.Equal(candidateSource(t, candidate), source))
	_, legacyPresent := zone.Source.TryGet(ctx)
	require.False(t, legacyPresent)
	// A replacement stays unselected until both its prefixes complete.
	replacement, err := service.PrepareTimezone(definition, freshSnapshot, 2)
	require.NoError(t, err)
	zone, ctx = pendingTimezoneRoot(t, h, true)
	require.NoError(t, zone.BeginCandidate(ctx.(chasm.MutableContext), replacement.Descriptor()))
	active, err = zone.PreparedTimezone(ctx)
	require.NoError(t, err)
	require.Equal(t, candidate.descriptor.fingerprint, active.Fingerprint())
	require.Equal(t, uint64(1), zone.Generation)
	h.commit(true)
	installTimezoneCandidate(t, h, replacement, 16)
	zone, _ = pendingTimezoneRoot(t, h, true)
	require.Equal(t, uint64(2), zone.Generation)
	require.Equal(t, uint64(1), zone.CleanupCount)
	h.commit(false)
}
func candidateSource(t *testing.T, candidate *StagedTimezoneCandidate) *scheduler2pb.TimezonePreparationSource {
	t.Helper()
	var source scheduler2pb.TimezonePreparationSource
	require.NoError(t, proto.Unmarshal(candidate.source, &source))
	return &source
}
