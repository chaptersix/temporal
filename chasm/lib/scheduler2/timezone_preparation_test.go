package scheduler2

import (
	"bytes"
	_ "embed"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:embed internal/occurrence/testdata/America_New_York.tzif
var persistedNewYorkTZ []byte

func largeTimezoneFixture() []byte {
	const count = 600
	data := make([]byte, 44)
	copy(data, "TZif")
	binary.BigEndian.PutUint32(data[32:36], count)
	binary.BigEndian.PutUint32(data[36:40], 1)
	binary.BigEndian.PutUint32(data[40:44], 4)
	for i := range count {
		data = binary.BigEndian.AppendUint32(data, uint32(100000+i*1000))
	}
	data = append(data, make([]byte, count)...)
	data = append(data, 0, 0, 0, 0, 0, 0, 'U', 'T', 'C', 0)
	return data
}
func bindTimezone(t *testing.T, h *treeHarness, stage *StagedTimezoneSource) {
	t.Helper()
	p, ctx := preparationRoot(t, h, true)
	require.NoError(t, p.BindTimezone(ctx.(chasm.MutableContext), stage))
	h.commit(true)
}
func timezoneRoot(t *testing.T, h *treeHarness, mutable bool) (*TimezonePreparation, chasm.Context) {
	t.Helper()
	p, ctx := preparationRoot(t, h, mutable)
	return p.Timezone.Get(ctx), ctx
}
func installTimezone(t *testing.T, h *treeHarness, stage *StagedTimezoneSource) {
	t.Helper()
	for {
		p, ctx := timezoneRoot(t, h, true)
		done, err := p.InstallNextPages(ctx.(chasm.MutableContext), stage, 16)
		require.NoError(t, err)
		h.commit(true)
		if done {
			return
		}
	}
}
func TestTimezonePreparationReloadAndDST(t *testing.T) {
	t.Parallel()
	calendar := &schedulepb.StructuredCalendarSpec{Second: []*schedulepb.Range{{Start: 0}}, Minute: []*schedulepb.Range{{Start: 15}}, Hour: []*schedulepb.Range{{Start: 1}}, DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}}, Month: []*schedulepb.Range{{Start: 1, End: 12, Step: 1}}, DayOfWeek: []*schedulepb.Range{{Start: 0, End: 6, Step: 1}}}
	stage, err := StageCalendarSource(calendar, 1)
	require.NoError(t, err)
	zoneStage, err := StageCalendarTimezone(stage, "America/New_York", persistedNewYorkTZ, time.Date(2024, 1, 1, 0, 0, 0, 123, time.UTC), 1)
	require.NoError(t, err)
	h := newPreparationHarness(t, stage)
	bindTimezone(t, h, zoneStage)
	installTimezone(t, h, zoneStage)
	installPreparation(t, h, stage)
	p, ctx := timezoneRoot(t, h, false)
	snapshot, generation, binding := p.SourceSnapshot(ctx)
	require.Equal(t, stage.sourceID[:], binding)
	rebuilt, err := StageCalendarTimezone(stage, snapshot.Name, snapshot.TzifData, snapshot.ResolvedAt.AsTime(), generation)
	require.NoError(t, err)
	require.Equal(t, zoneStage.ownerID, rebuilt.ownerID)
	require.Equal(t, zoneStage.header, rebuilt.header)
	// Loading the fixed capability must never decode the original input blob.
	require.NotNil(t, h.durable["Timezone$Source"])
	h.durable["Timezone$Source"].Data.Data = []byte{0xff}
	h.reload()
	for {
		cal, ctx := preparationRoot(t, h, true)
		_, err := cal.Advance(ctx.(chasm.MutableContext), 64)
		require.NoError(t, err)
		_, done, err := cal.PreparedMasks()
		require.NoError(t, err)
		h.commit(true)
		if done {
			break
		}
	}
	for _, tc := range []struct{ after, want time.Time }{
		{time.Date(2024, 11, 3, 4, 0, 0, 0, time.UTC), time.Date(2024, 11, 3, 5, 15, 0, 0, time.UTC)},
		{time.Date(2024, 11, 3, 5, 15, 0, 0, time.UTC), time.Date(2024, 11, 3, 6, 15, 0, 0, time.UTC)},
	} {
		var cursor occurrence.CalendarNextState
		for calls := 0; ; calls++ {
			require.Less(t, calls, 10000)
			counts := h.node.BeginWorkInspectionForTest()
			cal, ctx := preparationRoot(t, h, false)
			zone, err := cal.PreparedTimezone(ctx)
			require.NoError(t, err)
			require.Zero(t, counts.Snapshot()["data_decode_calls"])
			location, err := occurrence.NewResolvedLocation(zone)
			require.NoError(t, err)
			masks, done, err := cal.PreparedMasks()
			require.NoError(t, err)
			require.True(t, done)
			var stats occurrence.CalendarNextStats
			cursor, stats, err = occurrence.AdvancePreparedCalendar(cursor, occurrence.PreparedCalendar{SourceID: stage.sourceID, Masks: masks}, location, tc.after, 1)
			require.NoError(t, err)
			require.LessOrEqual(t, counts.Snapshot()["data_decode_calls"], int64(stats.TimezonePageReads))
			candidate, found, done := cursor.Result()
			h.reload()
			if done {
				require.True(t, found)
				require.Equal(t, tc.want, candidate)
				break
			}
		}
	}
}
func TestTimezonePreparationInstallPreflightAndPublication(t *testing.T) {
	t.Parallel()
	calendar, err := StageCalendarSource(preparationCalendar(1), 1)
	require.NoError(t, err)
	stage, err := StageCalendarTimezone(calendar, "many", largeTimezoneFixture(), time.Unix(0, 0), 1)
	require.NoError(t, err)
	require.Greater(t, len(stage.pages), 32)
	h := newPreparationHarness(t, calendar)
	bindTimezone(t, h, stage)
	p, ctx := timezoneRoot(t, h, true)
	_, err = p.PreparedTimezone(ctx)
	require.ErrorContains(t, err, "not installed")
	bad := *stage
	bad.pages = append([][]byte(nil), stage.pages...)
	bad.pages[1] = nil
	_, err = p.InstallNextPages(ctx.(chasm.MutableContext), &bad, 16)
	require.Error(t, err)
	require.Zero(t, p.InstalledPages)
	_, exists := p.Pages.Get(ctx, timezonePageKey(p.SourceId, 0))
	require.False(t, exists)
	_, err = p.InstallNextPages(ctx.(chasm.MutableContext), stage, 16)
	require.NoError(t, err)
	h.commit(false)
	p, ctx = timezoneRoot(t, h, false)
	require.Zero(t, p.InstalledPages)
	_, exists = p.Pages.Get(ctx, timezonePageKey(p.SourceId, 0))
	require.False(t, exists)
	installTimezone(t, h, stage)
	p, ctx = timezoneRoot(t, h, false)
	zone, err := p.PreparedTimezone(ctx)
	require.NoError(t, err)
	for ordinal := uint32(0); ordinal < zone.PageCount(); ordinal++ {
		got, err := zone.Page(ordinal)
		require.NoError(t, err)
		require.Equal(t, stage.pages[ordinal], got)
		require.LessOrEqual(t, len(got), 230)
	}
	p, ctx = timezoneRoot(t, h, true)
	page, ok := p.Pages.Get(ctx, timezonePageKey(p.SourceId, 1))
	require.True(t, ok)
	page.Encoded[1]++
	p.Pages.Put(ctx.(chasm.MutableContext), timezonePageKey(p.SourceId, 1), page)
	h.commit(true)
	p, ctx = timezoneRoot(t, h, false)
	zone, err = p.PreparedTimezone(ctx)
	require.NoError(t, err)
	_, err = zone.Page(1)
	require.Error(t, err)
}
func TestTimezonePreparationReplacementCleanup(t *testing.T) {
	t.Parallel()
	calendar, err := StageCalendarSource(preparationCalendar(1), 1)
	require.NoError(t, err)
	stage, err := StageCalendarTimezone(calendar, "many", largeTimezoneFixture(), time.Unix(0, 0), 1)
	require.NoError(t, err)
	h := newPreparationHarness(t, calendar)
	bindTimezone(t, h, stage)
	p, ctx := timezoneRoot(t, h, true)
	_, err = p.InstallNextPages(ctx.(chasm.MutableContext), stage, 16)
	require.NoError(t, err)
	h.commit(true)
	second, err := StageCalendarTimezone(calendar, "many", largeTimezoneFixture(), time.Unix(0, 0), 2)
	require.NoError(t, err)
	require.Equal(t, stage.fingerprint, second.fingerprint)
	require.NotEqual(t, stage.ownerID, second.ownerID)
	bindTimezone(t, h, second)
	installTimezone(t, h, second)
	third, err := StageCalendarUTC(calendar, 3)
	require.NoError(t, err)
	bindTimezone(t, h, third)
	p, ctx = timezoneRoot(t, h, true)
	require.Equal(t, uint64(2), p.CleanupCount)
	// A bad second addressed page must not delete the first page in the batch.
	key := timezonePageKey(stage.ownerID[:], 1)
	saved, ok := p.Pages.Get(ctx, key)
	require.True(t, ok)
	bad := common.CloneProto(saved)
	bad.SourceId = bytes.Repeat([]byte{7}, 32)
	p.Pages.Put(ctx.(chasm.MutableContext), key, bad)
	before := common.CloneProto(p.TimezonePreparationState)
	_, err = p.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
	require.Error(t, err)
	require.True(t, proto.Equal(before, p.TimezonePreparationState))
	_, ok = p.Pages.Get(ctx, timezonePageKey(stage.ownerID[:], 0))
	require.True(t, ok)
	h.commit(false)
	calls := 0
	for {
		p, ctx = timezoneRoot(t, h, true)
		done, err := p.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
		require.NoError(t, err)
		calls++
		h.commit(true)
		if done {
			break
		}
	}
	require.Equal(t, 4, calls)
	p, ctx = timezoneRoot(t, h, false)
	zone, err := p.PreparedTimezone(ctx)
	require.NoError(t, err)
	require.Equal(t, occurrence.PreparedUTC().Fingerprint(), zone.Fingerprint())
	for _, old := range []*StagedTimezoneSource{stage, second} {
		for i := range old.pages {
			_, ok := p.Pages.Get(ctx, timezonePageKey(old.ownerID[:], uint32(i)))
			require.False(t, ok)
		}
	}
}
func TestTimezonePreparationDetachedAndEpoch(t *testing.T) {
	t.Parallel()
	calendar, err := StageCalendarSource(preparationCalendar(1), 1)
	require.NoError(t, err)
	stage, err := StageCalendarTimezone(calendar, "many", largeTimezoneFixture(), time.Unix(0, 0), 1)
	require.NoError(t, err)
	h := newPreparationHarness(t, calendar)
	bindTimezone(t, h, stage)
	installTimezone(t, h, stage)
	p, ctx := timezoneRoot(t, h, true)
	zone, err := p.PreparedTimezone(ctx)
	require.NoError(t, err)
	_, err = zone.Page(0)
	require.NoError(t, err)
	h.commit(true)
	require.Panics(t, func() { _, _ = zone.Page(0) })
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly))
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	durable := &persistencespb.WorkflowMutableState{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}, ExecutionState: &persistencespb.WorkflowExecutionState{}, ChasmNodes: cloneNodes(h.durable)}
	detached, ctx, err := chasm.NewDetachedExecution[*CalendarPreparation](t.Context(), durable, registry)
	require.NoError(t, err)
	zone, err = detached.PreparedTimezone(ctx)
	require.NoError(t, err)
	for ordinal := uint32(0); ordinal < zone.PageCount(); ordinal++ {
		page, err := zone.Page(ordinal)
		require.NoError(t, err)
		require.Equal(t, stage.pages[ordinal], page)
	}
}

func TestTimezonePreparationDefinitionBinding(t *testing.T) {
	t.Parallel()
	calendar, err := StageCalendarSource(preparationCalendar(1), 1)
	require.NoError(t, err)
	zone, err := StageCalendarUTC(calendar, 1)
	require.NoError(t, err)
	h := newPreparationHarness(t, calendar)
	bindTimezone(t, h, zone)
	next, err := StageCalendarSource(preparationCalendar(1), 2)
	require.NoError(t, err)
	p, ctx := preparationRoot(t, h, true)
	require.NoError(t, p.ReplaceSource(ctx.(chasm.MutableContext), next))
	h.commit(true)
	p, ctx = preparationRoot(t, h, true)
	_, err = p.PreparedTimezone(ctx)
	require.ErrorContains(t, err, "binding")
	require.ErrorContains(t, p.BindTimezone(ctx.(chasm.MutableContext), zone), "binding")
	replacement, err := StageCalendarUTC(next, 2)
	require.NoError(t, err)
	require.NoError(t, p.BindTimezone(ctx.(chasm.MutableContext), replacement))
	h.commit(true)
	p, ctx = preparationRoot(t, h, false)
	_, err = p.PreparedTimezone(ctx)
	require.NoError(t, err)
}

func TestTimezonePreparationEveryCleanupPageFault(t *testing.T) {
	t.Parallel()
	calendar, err := StageCalendarSource(preparationCalendar(1), 1)
	require.NoError(t, err)
	stage, err := StageCalendarTimezone(calendar, "many", largeTimezoneFixture(), time.Unix(0, 0), 1)
	require.NoError(t, err)
	h := newPreparationHarness(t, calendar)
	bindTimezone(t, h, stage)
	installTimezone(t, h, stage)
	next, err := StageCalendarUTC(calendar, 2)
	require.NoError(t, err)
	bindTimezone(t, h, next)
	for failed := uint32(0); failed < 16; failed++ {
		p, ctx := timezoneRoot(t, h, true)
		key := timezonePageKey(stage.ownerID[:], failed)
		page, ok := p.Pages.Get(ctx, key)
		require.True(t, ok)
		page.Encoded = page.Encoded[:len(page.Encoded)-1]
		p.Pages.Put(ctx.(chasm.MutableContext), key, page)
		before := common.CloneProto(p.TimezonePreparationState)
		_, err := p.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
		require.Error(t, err)
		require.True(t, proto.Equal(before, p.TimezonePreparationState))
		for ordinal := uint32(0); ordinal < 16; ordinal++ {
			_, ok := p.Pages.Get(ctx, timezonePageKey(stage.ownerID[:], ordinal))
			require.True(t, ok)
		}
		owner, ok := p.Superseded.Get(ctx, "0")
		require.True(t, ok)
		require.Zero(t, owner.Cursor)
		h.commit(false)
	}
}

func TestTimezonePreparationNativeReloadAndCleanup(t *testing.T) {
	t.Parallel()
	calendar, err := StageCalendarSource(preparationCalendar(1), 1)
	require.NoError(t, err)
	source := occurrence.WindowsTimezoneSource{WordBits: 64, InitializationYear: 2024, Bias: 480, DaylightBias: -60, StandardDate: occurrence.WindowsSystemTime{Month: 11, Day: 1, Hour: 2}, DaylightDate: occurrence.WindowsSystemTime{Month: 3, Day: 2, Hour: 2}}
	resolvedAt := time.Date(2024, 1, 1, 0, 0, 0, 123, time.UTC)
	stage, err := StageCalendarWindowsTimezone(calendar, "Local", source.Bytes(), resolvedAt, 1)
	require.NoError(t, err)
	require.Len(t, stage.pages, 25)
	h := newPreparationHarness(t, calendar)
	bindTimezone(t, h, stage)
	installTimezone(t, h, stage)
	p, ctx := timezoneRoot(t, h, false)
	snapshot, generation, binding := p.SourceSnapshot(ctx)
	require.Equal(t, calendar.sourceID[:], binding)
	require.Equal(t, source.Bytes(), snapshot.WindowsNativeData)
	require.Empty(t, snapshot.TzifData)
	require.False(t, snapshot.Utc)
	rebuilt, err := StageCalendarTimezoneSnapshot(calendar, snapshot, generation)
	require.NoError(t, err)
	require.Equal(t, stage.ownerID, rebuilt.ownerID)
	require.Equal(t, stage.header, rebuilt.header)
	require.Equal(t, stage.pages, rebuilt.pages)
	// Source input is not consulted while restoring the native capability.
	h.durable["Timezone$Source"].Data.Data = []byte{0xff}
	h.reload()
	p, ctx = timezoneRoot(t, h, false)
	zone, err := p.PreparedTimezone(ctx)
	require.NoError(t, err)
	require.Equal(t, stage.fingerprint, zone.Fingerprint())
	for i := uint32(0); i < zone.PageCount(); i++ {
		page, err := zone.Page(i)
		require.NoError(t, err)
		require.Len(t, page, 294)
		require.Equal(t, stage.pages[i], page)
	}
	next, err := StageCalendarUTC(calendar, 2)
	require.NoError(t, err)
	bindTimezone(t, h, next)
	p, ctx = timezoneRoot(t, h, true)
	before := common.CloneProto(p.TimezonePreparationState)
	_, err = p.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
	require.NoError(t, err)
	h.commit(false)
	p, ctx = timezoneRoot(t, h, false)
	require.True(t, proto.Equal(before, p.TimezonePreparationState))
	_, exists := p.Pages.Get(ctx, timezonePageKey(stage.ownerID[:], 0))
	require.True(t, exists)
	calls := 0
	for {
		p, ctx = timezoneRoot(t, h, true)
		done, err := p.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
		require.NoError(t, err)
		calls++
		h.commit(true)
		if done {
			break
		}
	}
	require.Equal(t, 2, calls)
	p, ctx = timezoneRoot(t, h, false)
	zone, err = p.PreparedTimezone(ctx)
	require.NoError(t, err)
	require.Equal(t, occurrence.PreparedUTC().Fingerprint(), zone.Fingerprint())
}

func TestTimezoneStagingResolvedVariants(t *testing.T) {
	t.Parallel()
	calendar, err := StageCalendarSource(preparationCalendar(1), 1)
	require.NoError(t, err)
	for _, kind := range []string{"UTC", "TZif", "native"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			now := clock.NewEventTimeSource().Update(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
			config := timezonesource.SourceConfig{Clock: now, LookupEnv: func(string) (string, bool) { return "", false }}
			name := "UTC"
			var custom []byte
			switch kind {
			case "UTC":
			case "TZif":
				name = "custom"
				custom = persistedNewYorkTZ
			case "native":
				name = "Local"
				config.WindowsLocal = func() (occurrence.WindowsTimezoneSource, error) {
					return occurrence.WindowsTimezoneSource{WordBits: 64, Bias: -60}, nil
				}
			default:
				t.Fatal("unexpected fixture")
			}
			snapshot, err := timezonesource.NewResolver(timezonesource.NewEnvironment(config)).Resolve(name, custom)
			require.NoError(t, err)
			stage, err := StageResolvedCalendarTimezone(calendar, snapshot, 1)
			require.NoError(t, err)
			require.Equal(t, snapshot.Prepared().Fingerprint(), stage.fingerprint)
			source := stage.sourceProto()
			rebuilt, err := StageCalendarTimezoneSnapshot(calendar, source, 1)
			require.NoError(t, err)
			require.Equal(t, stage.ownerID, rebuilt.ownerID)
			require.Equal(t, stage.header, rebuilt.header)
		})
	}
}

func TestTimezoneStagingRejectsConflictingSourceVariants(t *testing.T) {
	t.Parallel()
	calendar, err := StageCalendarSource(preparationCalendar(1), 1)
	require.NoError(t, err)
	_, err = StageResolvedCalendarTimezone(calendar, &timezonesource.Snapshot{}, 1)
	require.Error(t, err)
	native, err := StageCalendarWindowsTimezone(calendar, "Local", (occurrence.WindowsTimezoneSource{WordBits: 64, InitializationYear: 2024}).Bytes(), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), 1)
	require.NoError(t, err)
	for _, kind := range []string{"UTC with native", "TZif with native", "missing timestamp"} {
		source := native.sourceProto()
		switch kind {
		case "UTC with native":
			source.Utc = true
		case "TZif with native":
			source.TzifData = persistedNewYorkTZ
		case "missing timestamp":
			source.ResolvedAt = nil
		default:
			t.Fatal("unexpected fixture")
		}
		_, err = StageCalendarTimezoneSnapshot(calendar, source, 1)
		require.Error(t, err, kind)
	}
}

func TestTimezoneStagingRejectsInvalidPreparationInstants(t *testing.T) {
	t.Parallel()
	calendar, err := StageCalendarSource(preparationCalendar(1), 1)
	require.NoError(t, err)
	native := (occurrence.WindowsTimezoneSource{WordBits: 64, InitializationYear: 2024}).Bytes()
	for _, variant := range []string{"UTC", "TZif", "native"} {
		t.Run(variant, func(t *testing.T) {
			t.Parallel()
			for _, invalid := range []*timestamppb.Timestamp{{Nanos: -1}, {Nanos: 1000000000}, {Seconds: -62135596801}, {Seconds: 253402300800}} {
				source := &scheduler2pb.TimezonePreparationSource{Name: "test", ResolvedAt: invalid}
				switch variant {
				case "UTC":
					source.Utc = true
				case "TZif":
					source.TzifData = persistedNewYorkTZ
				case "native":
					source.WindowsNativeData = native
				default:
					t.Fatal("unexpected fixture")
				}
				_, err := StageCalendarTimezoneSnapshot(calendar, source, 1)
				require.ErrorContains(t, err, "preparation instant is invalid")
			}
		})
	}
	for _, sec := range []int64{-62135596801, 253402300800} {
		instant := time.Unix(sec, 0).UTC()
		_, err := StageCalendarTimezone(calendar, "custom", persistedNewYorkTZ, instant, 1)
		require.ErrorContains(t, err, "not representable")
		_, err = StageCalendarWindowsTimezone(calendar, "Local", native, instant, 1)
		require.ErrorContains(t, err, "not representable")
	}
	for _, instant := range []time.Time{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)} {
		stage, err := StageCalendarTimezone(calendar, "custom", persistedNewYorkTZ, instant, 1)
		require.NoError(t, err)
		require.NoError(t, stage.sourceProto().ResolvedAt.CheckValid())
		require.Equal(t, instant, stage.sourceProto().ResolvedAt.AsTime())
	}
}
