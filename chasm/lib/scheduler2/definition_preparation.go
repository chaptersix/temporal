package scheduler2

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"

	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
)

const maxPreparationInstallPages = 16

// CalendarPreparation is an unselected preparation component. It has no tasks
// and is not attached to the current Scheduler or Generator execution path.
// Superseded source pages remain fenced by source identity pending bounded cleanup.
type CalendarPreparation struct {
	chasm.UnimplementedComponent
	*scheduler2pb.CalendarPreparationState
	Source chasm.Field[*schedulepb.StructuredCalendarSpec]
	Pages  chasm.DataMap[*scheduler2pb.CalendarPreparationPage]
}

func (*CalendarPreparation) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
func (*CalendarPreparation) ContextMetadata(chasm.Context) map[string]string { return nil }

// Terminate is not an enabled lifecycle path for this unselected preparation slice.
func (*CalendarPreparation) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewUnimplemented("standalone calendar preparation termination is not enabled")
}

// StagedCalendarSource is immutable caller-side input preparation. Build it
// outside the mutation callback, including after a source snapshot reload.
type StagedCalendarSource struct {
	revision int64
	sourceID [32]byte
	source   *schedulepb.StructuredCalendarSpec
	counts   [7]uint32
	pages    []*scheduler2pb.CalendarPreparationPage
}

// StageCalendarSource validates and canonicalizes an input-proportional source.
// It does not access CHASM or make a bounded transaction-work claim.
func StageCalendarSource(calendar *schedulepb.StructuredCalendarSpec, revision int64) (*StagedCalendarSource, error) {
	if calendar == nil || revision <= 0 {
		return nil, serviceerror.NewInvalidArgument("calendar and positive revision are required")
	}
	builder := legacy.NewSpecBuilder(func() int { return 0 }, func() int { return 0 })
	compiled, err := builder.NewBoundedCompiledSpec(&schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{calendar}}, math.MaxInt, math.MaxInt)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}
	source := common.CloneProto(compiled.CanonicalForm().StructuredCalendar[0])
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(source)
	if err != nil {
		return nil, err
	}
	var version [8]byte
	binary.BigEndian.PutUint64(version[:], uint64(revision))
	stage := &StagedCalendarSource{revision: revision, source: source, sourceID: sha256.Sum256(append(wire, version[:]...))}
	fields := [][]*schedulepb.Range{source.Second, source.Minute, source.Hour, source.DayOfMonth, source.Month, source.DayOfWeek, source.Year}
	for field, ranges := range fields {
		if uint64(len(ranges)) > math.MaxUint32 {
			return nil, serviceerror.NewInvalidArgument("calendar range count exceeds representation")
		}
		stage.counts[field] = uint32(len(ranges))
		for begin := 0; begin < len(ranges); begin += occurrence.CalendarRangePageSize {
			page := &scheduler2pb.CalendarPreparationPage{SourceId: bytes.Clone(stage.sourceID[:]), Field: uint32(field), Page: uint32(begin / occurrence.CalendarRangePageSize)}
			for _, r := range ranges[begin:min(begin+occurrence.CalendarRangePageSize, len(ranges))] {
				page.Ranges = append(page.Ranges, &scheduler2pb.CalendarPreparationRange{Start: r.Start, End: r.End, Step: r.Step})
			}
			stage.pages = append(stage.pages, page)
		}
	}
	return stage, nil
}

func (stage *StagedCalendarSource) validate() error {
	if stage == nil || stage.revision <= 0 || stage.source == nil || stage.sourceID == [32]byte{} {
		return serviceerror.NewInvalidArgument("calendar source is uninitialized")
	}
	var pages uint64
	for _, count := range stage.counts {
		pages += (uint64(count) + occurrence.CalendarRangePageSize - 1) / occurrence.CalendarRangePageSize
	}
	if pages != uint64(len(stage.pages)) {
		return serviceerror.NewInvalidArgument("calendar source page count is invalid")
	}
	return nil
}

func (stage *StagedCalendarSource) state() *scheduler2pb.CalendarPreparationState {
	return &scheduler2pb.CalendarPreparationState{Revision: stage.revision, SourceId: bytes.Clone(stage.sourceID[:]), RangeCounts: append([]uint32(nil), stage.counts[:]...), TotalPages: uint64(len(stage.pages))}
}

// NewCalendarPreparation publishes source bytes and a fixed header only. Page
// installation is separately batched; no caller-side page slice enters root DATA.
func NewCalendarPreparation(ctx chasm.MutableContext, stage *StagedCalendarSource) (*CalendarPreparation, error) {
	if err := stage.validate(); err != nil {
		return nil, err
	}
	return &CalendarPreparation{CalendarPreparationState: stage.state(), Source: chasm.NewDataField(ctx, common.CloneProto(stage.source)), Pages: chasm.NewDataMap[*scheduler2pb.CalendarPreparationPage](ctx)}, nil
}

// ReplaceSource invalidates partial preparation atomically without scanning old pages.
func (p *CalendarPreparation) ReplaceSource(ctx chasm.MutableContext, stage *StagedCalendarSource) error {
	if err := stage.validate(); err != nil {
		return err
	}
	if stage.revision <= p.Revision {
		return serviceerror.NewFailedPrecondition("calendar replacement requires a newer revision")
	}
	p.CalendarPreparationState = stage.state()
	p.Source = chasm.NewDataField(ctx, common.CloneProto(stage.source))
	return nil
}

// SourceSnapshot returns detached input for caller-side staging after reload.
func (p *CalendarPreparation) SourceSnapshot(ctx chasm.Context) (*schedulepb.StructuredCalendarSpec, int64) {
	return common.CloneProto(p.Source.Get(ctx)), p.Revision
}

func calendarPageKey(source []byte, field, page uint32) string {
	return fmt.Sprintf("%s/%d/%d", hex.EncodeToString(source), field, page)
}

// InstallNextPages commits at most sixteen prepared pages. A source identity
// mismatch fails before writing; DataMap clones each supplied page.
func (p *CalendarPreparation) InstallNextPages(ctx chasm.MutableContext, stage *StagedCalendarSource, limit int) (bool, error) {
	if limit < 1 || limit > maxPreparationInstallPages {
		return false, serviceerror.NewInvalidArgument("calendar installation budget is outside supported bounds")
	}
	header, err := p.header()
	if err != nil {
		return false, err
	}
	if err := stage.validate(); err != nil {
		return false, err
	}
	if stage.revision != p.Revision || stage.sourceID != header.SourceID || stage.counts != header.Ranges || uint64(len(stage.pages)) != p.TotalPages {
		return false, serviceerror.NewFailedPrecondition("calendar installation source does not match")
	}
	end := p.InstalledPages + min(uint64(limit), p.TotalPages-p.InstalledPages)
	for i := p.InstalledPages; i < end; i++ {
		field, index := calendarPageOrdinal(header.Ranges, i)
		page := stage.pages[i]
		expected := min(uint32(occurrence.CalendarRangePageSize), header.Ranges[field]-index*occurrence.CalendarRangePageSize)
		if page == nil || !bytes.Equal(page.SourceId, p.SourceId) || page.Field != field || page.Page != index || len(page.Ranges) != int(expected) {
			return false, serviceerror.NewInvalidArgument("calendar staging page identity is invalid")
		}
	}
	for i := p.InstalledPages; i < end; i++ {
		page := stage.pages[i]
		p.Pages.Put(ctx, calendarPageKey(page.SourceId, page.Field, page.Page), page)
	}
	p.InstalledPages = end
	return end == p.TotalPages, nil
}

func calendarPageOrdinal(counts [7]uint32, ordinal uint64) (fieldIndex uint32, pageIndex uint32) {
	for field, count := range counts {
		pages := (uint64(count) + occurrence.CalendarRangePageSize - 1) / occurrence.CalendarRangePageSize
		if ordinal < pages {
			return uint32(field), uint32(ordinal)
		}
		ordinal -= pages
	}
	return 7, 0 // Header and install frontier validation make this unreachable.
}

func (p *CalendarPreparation) header() (occurrence.CalendarHeader, error) {
	var h occurrence.CalendarHeader
	if p.CalendarPreparationState == nil || p.Revision <= 0 || len(p.SourceId) != 32 || len(p.RangeCounts) != 7 {
		return h, serviceerror.NewInternal("invalid calendar preparation header")
	}
	copy(h.SourceID[:], p.SourceId)
	copy(h.Ranges[:], p.RangeCounts)
	var pages uint64
	for _, count := range h.Ranges {
		pages += (uint64(count) + occurrence.CalendarRangePageSize - 1) / occurrence.CalendarRangePageSize
	}
	if h.SourceID == [32]byte{} || pages != p.TotalPages || p.InstalledPages > pages {
		return h, serviceerror.NewInternal("invalid calendar page frontier")
	}
	return h, nil
}

// Advance performs bounded mask preparation only; it never loads source bytes,
// compiles a timezone, enumerates a candidate, or publishes an execution task.
func (p *CalendarPreparation) Advance(ctx chasm.MutableContext, budget int) (occurrence.PreparationStats, error) {
	h, err := p.header()
	if err != nil {
		return occurrence.PreparationStats{}, err
	}
	if p.InstalledPages != p.TotalPages {
		return occurrence.PreparationStats{}, serviceerror.NewFailedPrecondition("calendar source pages are not installed")
	}
	state, err := decodeCalendarCursor(p.Cursor)
	if err != nil {
		return occurrence.PreparationStats{}, err
	}
	next, stats, err := occurrence.PrepareCalendar(state, h, func(field uint8, index uint32) (occurrence.RangePage, error) { return p.readPage(ctx, h, field, index) }, budget)
	if err != nil {
		return stats, err
	}
	p.Cursor = encodeCalendarCursor(next)
	return stats, nil
}

func (p *CalendarPreparation) readPage(ctx chasm.Context, h occurrence.CalendarHeader, field uint8, index uint32) (occurrence.RangePage, error) {
	var result occurrence.RangePage
	page, ok := p.Pages.Get(ctx, calendarPageKey(h.SourceID[:], uint32(field), index))
	if !ok || !bytes.Equal(page.SourceId, h.SourceID[:]) || page.Field != uint32(field) || page.Page != index || len(page.Ranges) > occurrence.CalendarRangePageSize {
		return result, serviceerror.NewInternal("missing or invalid calendar source page")
	}
	result.Count = uint8(len(page.Ranges))
	for i, r := range page.Ranges {
		if r == nil {
			return result, serviceerror.NewInternal("calendar source range is missing")
		}
		result.Ranges[i] = occurrence.CalendarRange{Start: r.Start, End: r.End, Step: r.Step}
	}
	return result, nil
}

func encodeCalendarCursor(s occurrence.PreparationState) *scheduler2pb.CalendarPreparationCursor {
	return &scheduler2pb.CalendarPreparationCursor{SourceId: bytes.Clone(s.SourceID[:]), Field: uint32(s.Field), Range: s.Range, Next: s.Next, InRange: s.InRange, FieldMasks: append([]uint64(nil), s.Partial.Fields[:]...), YearMasks: append([]uint64(nil), s.Partial.Years[:]...), AllYears: s.Partial.AllYears, Done: s.Done}
}
func decodeCalendarCursor(c *scheduler2pb.CalendarPreparationCursor) (occurrence.PreparationState, error) {
	var s occurrence.PreparationState
	if c == nil {
		return s, nil
	}
	if len(c.SourceId) != 32 || c.Field > 7 || len(c.FieldMasks) != 6 || len(c.YearMasks) != 2 {
		return s, serviceerror.NewInternal("invalid persisted calendar preparation cursor")
	}
	copy(s.SourceID[:], c.SourceId)
	copy(s.Partial.Fields[:], c.FieldMasks)
	copy(s.Partial.Years[:], c.YearMasks)
	s.Field, s.Range, s.Next, s.InRange, s.Partial.AllYears, s.Done = uint8(c.Field), c.Range, c.Next, c.InRange, c.AllYears, c.Done
	return s, nil
}

// PreparedMasks exposes only a completed, structurally validated result.
func (p *CalendarPreparation) PreparedMasks() (occurrence.CalendarMasks, bool, error) {
	h, err := p.header()
	if err != nil {
		return occurrence.CalendarMasks{}, false, err
	}
	s, err := decodeCalendarCursor(p.Cursor)
	if err != nil {
		return occurrence.CalendarMasks{}, false, err
	}
	if !s.Done {
		return occurrence.CalendarMasks{}, false, nil
	}
	if p.InstalledPages != p.TotalPages {
		return occurrence.CalendarMasks{}, false, serviceerror.NewInternal("completed calendar has missing source pages")
	}
	checked, _, err := occurrence.PrepareCalendar(s, h, func(uint8, uint32) (occurrence.RangePage, error) {
		return occurrence.RangePage{}, serviceerror.NewInternal("completed calendar unexpectedly read a page")
	}, 1)
	return checked.Partial, err == nil, err
}
