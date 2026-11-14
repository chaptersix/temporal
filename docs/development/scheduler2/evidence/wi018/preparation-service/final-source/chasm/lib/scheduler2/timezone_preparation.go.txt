package scheduler2

import (
	"bytes"
	"encoding/hex"
	"math"
	"strconv"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TimezonePreparation owns an unselected immutable timezone snapshot and bounded
// installation and cleanup frontiers. It schedules no tasks.
type TimezonePreparation struct {
	chasm.UnimplementedComponent
	*scheduler2pb.TimezonePreparationState
	Source      chasm.Field[*scheduler2pb.TimezonePreparationSource]
	Pages       chasm.DataMap[*scheduler2pb.TimezonePreparationPage]
	Superseded  chasm.DataMap[*scheduler2pb.TimezoneSupersededSource]
	SourcePages chasm.DataMap[*scheduler2pb.TimezoneSourcePage]
}

func (*TimezonePreparation) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
func (*TimezonePreparation) ContextMetadata(chasm.Context) map[string]string { return nil }
func (*TimezonePreparation) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewUnimplemented("standalone timezone preparation termination is not enabled")
}
func (stage *StagedTimezoneSource) state() *scheduler2pb.TimezonePreparationState {
	return &scheduler2pb.TimezonePreparationState{FormatVersion: 1, DefinitionSourceId: bytes.Clone(stage.definitionID[:]), SourceId: bytes.Clone(stage.ownerID[:]), SemanticFingerprint: bytes.Clone(stage.fingerprint[:]), Generation: stage.generation, Header: bytes.Clone(stage.header), TotalPages: uint32(len(stage.pages))}
}
func (stage *StagedTimezoneSource) sourceProto() *scheduler2pb.TimezonePreparationSource {
	return &scheduler2pb.TimezonePreparationSource{Name: stage.name, TzifData: bytes.Clone(stage.source), ResolvedAt: timestamppb.New(stage.resolvedAt), Utc: stage.utc, WindowsNativeData: bytes.Clone(stage.native)}
}

// NewTimezonePreparation publishes control and detached input; pages are installed separately.
func NewTimezonePreparation(ctx chasm.MutableContext, stage *StagedTimezoneSource) (*TimezonePreparation, error) {
	if _, err := stage.validate(); err != nil {
		return nil, err
	}
	return &TimezonePreparation{TimezonePreparationState: stage.state(), Source: chasm.NewDataField(ctx, stage.sourceProto()), Pages: chasm.NewDataMap[*scheduler2pb.TimezonePreparationPage](ctx), Superseded: chasm.NewDataMap[*scheduler2pb.TimezoneSupersededSource](ctx)}, nil
}
func timezonePageKey(owner []byte, ordinal uint32) string {
	return hex.EncodeToString(owner) + "/" + strconv.FormatUint(uint64(ordinal), 10)
}
func validTimezoneID(id []byte) bool { return len(id) == 32 && !bytes.Equal(id, make([]byte, 32)) }
func (p *TimezonePreparation) header() (*occurrence.PreparedTimezone, error) {
	if p.TimezonePreparationState != nil && p.FormatVersion == 2 {
		return p.activePagedHeader()
	}
	if p.TimezonePreparationState == nil || p.FormatVersion != 1 || p.Generation == 0 || !validTimezoneID(p.SourceId) || !validTimezoneID(p.DefinitionSourceId) || !validTimezoneID(p.SemanticFingerprint) {
		return nil, serviceerror.NewInternal("invalid timezone control")
	}
	zone, err := occurrence.RestorePreparedTimezone(p.Header, func(uint32) ([]byte, error) { return nil, serviceerror.NewInternal("timezone metadata has no reader") })
	if err != nil {
		return nil, err
	}
	fingerprint := zone.Fingerprint()
	if !bytes.Equal(fingerprint[:], p.SemanticFingerprint) || zone.PageCount() != p.TotalPages || p.InstalledPages > p.TotalPages {
		return nil, serviceerror.NewInternal("invalid timezone frontier")
	}
	return zone, nil
}
func (p *TimezonePreparation) validateCleanupQueue() error {
	if p.CleanupHead > p.CleanupTail || p.CleanupCount != p.CleanupTail-p.CleanupHead {
		return serviceerror.NewInternal("invalid timezone cleanup queue")
	}
	return nil
}

// ReplaceSource retains the previous installed prefix under FIFO cleanup ownership.
func (p *TimezonePreparation) ReplaceSource(ctx chasm.MutableContext, stage *StagedTimezoneSource) error {
	if p.TimezonePreparationState != nil && p.FormatVersion == 2 {
		return serviceerror.NewFailedPrecondition("paged timezone requires candidate replacement")
	}
	if _, err := stage.validate(); err != nil {
		return err
	}
	if _, err := p.header(); err != nil {
		return err
	}
	if err := p.validateCleanupQueue(); err != nil {
		return err
	}
	if stage.generation <= p.Generation {
		return serviceerror.NewFailedPrecondition("timezone replacement requires a newer generation")
	}
	if p.CleanupTail == math.MaxUint64 || bytes.Equal(stage.ownerID[:], p.SourceId) {
		return serviceerror.NewFailedPrecondition("timezone owner sequence exhausted")
	}
	key := strconv.FormatUint(p.CleanupTail, 10)
	if _, exists := p.Superseded.Get(ctx, key); exists {
		return serviceerror.NewInternal("timezone cleanup tail already owned")
	}
	owner := &scheduler2pb.TimezoneSupersededSource{SourceId: bytes.Clone(p.SourceId), Header: bytes.Clone(p.Header), InstalledPages: p.InstalledPages}
	next := stage.state()
	next.CleanupHead, next.CleanupTail, next.CleanupCount = p.CleanupHead, p.CleanupTail+1, p.CleanupCount+1
	p.Superseded.Put(ctx, key, owner)
	p.TimezonePreparationState = next
	p.Source = chasm.NewDataField(ctx, stage.sourceProto())
	return nil
}

// SourceSnapshot returns legacy detached reconstruction input, generation and the exact
// bound calendar identity. Paged owners return no legacy source; use
// SourceSnapshotChecked for version-independent read/export reconstruction.
func (p *TimezonePreparation) SourceSnapshot(ctx chasm.Context) (*scheduler2pb.TimezonePreparationSource, uint64, []byte) {
	if p.TimezonePreparationState != nil && p.FormatVersion == 2 {
		return nil, p.Generation, bytes.Clone(p.DefinitionSourceId)
	}
	return common.CloneProto(p.Source.Get(ctx)), p.Generation, bytes.Clone(p.DefinitionSourceId)
}

// InstallNextPages preflights all addressed staging pages before publishing any.
func (p *TimezonePreparation) InstallNextPages(ctx chasm.MutableContext, stage *StagedTimezoneSource, limit int) (bool, error) {
	if limit < 1 || limit > maxPreparationInstallPages {
		return false, serviceerror.NewInvalidArgument("timezone installation budget is outside supported bounds")
	}
	if _, err := p.header(); err != nil {
		return false, err
	}
	zone, err := stage.validate()
	if err != nil {
		return false, err
	}
	if stage.generation != p.Generation || !bytes.Equal(stage.ownerID[:], p.SourceId) || !bytes.Equal(stage.definitionID[:], p.DefinitionSourceId) || !bytes.Equal(stage.header, p.Header) {
		return false, serviceerror.NewFailedPrecondition("timezone installation source does not match")
	}
	end := p.InstalledPages + min(uint32(limit), p.TotalPages-p.InstalledPages)
	pages := make([][]byte, 0, end-p.InstalledPages)
	for ordinal := p.InstalledPages; ordinal < end; ordinal++ {
		page, err := zone.Page(ordinal)
		if err != nil {
			return false, err
		}
		pages = append(pages, page)
	}
	for i, page := range pages {
		ordinal := p.InstalledPages + uint32(i)
		p.Pages.Put(ctx, timezonePageKey(p.SourceId, ordinal), &scheduler2pb.TimezonePreparationPage{SourceId: bytes.Clone(p.SourceId), Ordinal: ordinal, Encoded: page})
	}
	p.InstalledPages = end
	return end == p.TotalPages, nil
}
func (p *TimezonePreparation) restore(ctx chasm.Context, owner, header []byte) (*occurrence.PreparedTimezone, error) {
	return occurrence.RestorePreparedTimezone(header, func(ordinal uint32) ([]byte, error) {
		page, ok := p.Pages.Get(ctx, timezonePageKey(owner, ordinal))
		if !ok || page == nil || !bytes.Equal(page.SourceId, owner) || page.Ordinal != ordinal {
			return nil, serviceerror.NewInternal("missing or invalid timezone page owner")
		}
		return page.Encoded, nil
	})
}

// PreparedTimezone restores only fixed control. The capability is valid only for ctx's epoch.
func (p *TimezonePreparation) PreparedTimezone(ctx chasm.Context) (*occurrence.PreparedTimezone, error) {
	if _, err := p.header(); err != nil {
		return nil, err
	}
	if p.InstalledPages != p.TotalPages {
		return nil, serviceerror.NewFailedPrecondition("timezone pages are not installed")
	}
	return p.restore(ctx, bytes.Clone(p.SourceId), p.Header)
}

// CleanupSuperseded reclaims at most sixteen pages from one oldest owner.
func (p *TimezonePreparation) CleanupSuperseded(ctx chasm.MutableContext, limit int) (bool, error) {
	if p.TimezonePreparationState != nil && p.FormatVersion == 2 {
		return p.cleanupPagedSources(ctx, limit)
	}
	if limit < 1 || limit > maxPreparationInstallPages {
		return false, serviceerror.NewInvalidArgument("timezone cleanup budget is outside supported bounds")
	}
	if _, err := p.header(); err != nil {
		return false, err
	}
	if err := p.validateCleanupQueue(); err != nil {
		return false, err
	}
	if p.CleanupCount == 0 {
		return true, nil
	}
	key := strconv.FormatUint(p.CleanupHead, 10)
	owner, ok := p.Superseded.Get(ctx, key)
	if !ok || owner == nil || !validTimezoneID(owner.SourceId) || bytes.Equal(owner.SourceId, p.SourceId) {
		return false, serviceerror.NewInternal("missing or invalid timezone cleanup owner")
	}
	zone, err := p.restore(ctx, owner.SourceId, owner.Header)
	if err != nil {
		return false, err
	}
	if owner.Cursor > owner.InstalledPages || owner.InstalledPages > zone.PageCount() {
		return false, serviceerror.NewInternal("invalid timezone cleanup cursor")
	}
	end := owner.Cursor + min(uint32(limit), owner.InstalledPages-owner.Cursor)
	for ordinal := owner.Cursor; ordinal < end; ordinal++ {
		if _, err := zone.Page(ordinal); err != nil {
			return false, err
		}
	}
	for ordinal := owner.Cursor; ordinal < end; ordinal++ {
		p.Pages.Delete(ctx, timezonePageKey(owner.SourceId, ordinal))
	}
	owner.Cursor = end
	if end == owner.InstalledPages {
		p.Superseded.Delete(ctx, key)
		p.CleanupHead++
		p.CleanupCount--
	} else {
		p.Superseded.Put(ctx, key, owner)
	}
	return p.CleanupCount == 0, nil
}

// BindTimezone attaches a snapshot only to its currently installed definition.
func (p *CalendarPreparation) BindTimezone(ctx chasm.MutableContext, stage *StagedTimezoneSource) error {
	header, err := p.header()
	if err != nil {
		return err
	}
	if _, err := stage.validate(); err != nil {
		return err
	}
	if header.SourceID != stage.definitionID {
		return serviceerror.NewFailedPrecondition("timezone definition binding does not match")
	}
	if zone, ok := p.Timezone.TryGet(ctx); ok {
		return zone.ReplaceSource(ctx, stage)
	}
	zone, err := NewTimezonePreparation(ctx, stage)
	if err != nil {
		return err
	}
	p.Timezone = chasm.NewComponentField(ctx, zone)
	return nil
}

// PreparedTimezone rejects snapshots bound to an earlier calendar definition.
func (p *CalendarPreparation) PreparedTimezone(ctx chasm.Context) (*occurrence.PreparedTimezone, error) {
	header, err := p.header()
	if err != nil {
		return nil, err
	}
	zone, ok := p.Timezone.TryGet(ctx)
	if !ok {
		return nil, serviceerror.NewFailedPrecondition("calendar timezone is absent")
	}
	if _, err := zone.header(); err != nil {
		return nil, err
	}
	if !bytes.Equal(zone.DefinitionSourceId, header.SourceID[:]) {
		return nil, serviceerror.NewFailedPrecondition("timezone definition binding does not match")
	}
	return zone.PreparedTimezone(ctx)
}
