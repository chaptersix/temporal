package scheduler2

import (
	"bytes"
	"math"
	"strconv"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (d TimezoneCandidateDescriptor) pending() *scheduler2pb.TimezonePendingSource {
	return &scheduler2pb.TimezonePendingSource{DefinitionSourceId: bytes.Clone(d.definitionID[:]), SourceId: bytes.Clone(d.ownerID[:]), SemanticFingerprint: bytes.Clone(d.fingerprint[:]), Generation: d.generation, Header: bytes.Clone(d.header[:d.headerSize]), TotalPages: d.totalPages, SourceBytes: d.sourceBytes, SourceHash: bytes.Clone(d.sourceHash[:]), ResolvedAt: timestamppb.New(d.resolvedAt()), NativeInitializationYear: d.nativeYear, SourceKind: uint32(d.sourceKind), NativeWordBits: uint32(d.nativeWordBits)}
}
func pendingTimezoneDescriptor(p *scheduler2pb.TimezonePendingSource) (TimezoneCandidateDescriptor, error) {
	var d TimezoneCandidateDescriptor
	if p == nil || p.ResolvedAt == nil || p.ResolvedAt.CheckValid() != nil || p.SourceKind > uint32(timezonesource.WindowsNative) || p.NativeWordBits > 255 || !validTimezoneID(p.DefinitionSourceId) || !validTimezoneID(p.SourceId) || !validTimezoneID(p.SemanticFingerprint) || !validTimezoneID(p.SourceHash) || len(p.Header) > len(d.header) {
		return d, serviceerror.NewInternal("invalid pending timezone control")
	}
	d.resolvedAtSeconds = p.ResolvedAt.Seconds
	d.resolvedAtNanos = p.ResolvedAt.Nanos
	d.sourceKind = timezonesource.Kind(p.SourceKind)
	d.nativeYear = p.NativeInitializationYear
	d.nativeWordBits = uint8(p.NativeWordBits)
	d.definitionID = [32]byte(p.DefinitionSourceId)
	d.ownerID = [32]byte(p.SourceId)
	d.fingerprint = [32]byte(p.SemanticFingerprint)
	d.sourceHash = [32]byte(p.SourceHash)
	d.generation = p.Generation
	d.totalPages = p.TotalPages
	d.sourceBytes = p.SourceBytes
	d.headerSize = uint8(len(p.Header))
	copy(d.header[:], p.Header)
	if _, err := d.validate(); err != nil {
		return d, err
	}
	if p.InstalledPages > d.totalPages || p.InstalledSourcePages > d.SourcePageCount() || (p.InstalledSourcePages < d.SourcePageCount() && p.InstalledPages != 0) {
		return d, serviceerror.NewInternal("invalid pending timezone frontier")
	}
	return d, nil
}

// NewPendingTimezonePreparation publishes fixed metadata only. The initial
// selected-profile source uses the same addressed protocol as later refreshes.
func NewPendingTimezonePreparation(ctx chasm.MutableContext, d TimezoneCandidateDescriptor) (*TimezonePreparation, error) {
	if _, err := d.validate(); err != nil {
		return nil, err
	}
	return &TimezonePreparation{TimezonePreparationState: &scheduler2pb.TimezonePreparationState{FormatVersion: 2, LastGeneration: d.generation, Pending: d.pending()}, Pages: chasm.NewDataMap[*scheduler2pb.TimezonePreparationPage](ctx), SourcePages: chasm.NewDataMap[*scheduler2pb.TimezoneSourcePage](ctx), Superseded: chasm.NewDataMap[*scheduler2pb.TimezoneSupersededSource](ctx)}, nil
}
func (p *TimezonePreparation) validatePagedControl() error {
	if p.TimezonePreparationState == nil || p.FormatVersion != 2 || p.LastGeneration == 0 {
		return serviceerror.NewInternal("invalid paged timezone control")
	}
	if err := p.validateCleanupQueue(); err != nil {
		return err
	}
	if p.Generation == 0 {
		if len(p.DefinitionSourceId)+len(p.SourceId)+len(p.SemanticFingerprint)+len(p.Header)+len(p.SourceHash) != 0 || p.TotalPages != 0 || p.InstalledPages != 0 || p.SourceBytes != 0 {
			return serviceerror.NewInternal("invalid absent active timezone")
		}
	} else {
		if err := p.validateActivePagedMetadata(); err != nil {
			return err
		}

		if p.InstalledPages != p.TotalPages || p.Generation > p.LastGeneration {
			return serviceerror.NewInternal("active timezone is incomplete")
		}
	}
	if p.Pending != nil {
		d, err := pendingTimezoneDescriptor(p.Pending)
		if err != nil {
			return err
		}
		if d.generation <= p.Generation || d.generation > p.LastGeneration || bytes.Equal(d.ownerID[:], p.SourceId) {
			return serviceerror.NewInternal("invalid pending timezone generation")
		}
	}
	return nil
}
func (p *TimezonePreparation) activePagedHeader() (*occurrence.PreparedTimezone, error) {
	if err := p.validatePagedControl(); err != nil {
		return nil, err
	}
	if p.Generation == 0 {
		return nil, serviceerror.NewFailedPrecondition("timezone source is not active")
	}
	return occurrence.RestorePreparedTimezone(p.Header, func(uint32) ([]byte, error) { return nil, serviceerror.NewInternal("timezone metadata has no reader") })
}
func (p *TimezonePreparation) queuePendingOwner(ctx chasm.MutableContext, key string) {
	old := p.Pending
	p.Superseded.Put(ctx, key, &scheduler2pb.TimezoneSupersededSource{SourceId: bytes.Clone(old.SourceId), Header: bytes.Clone(old.Header), InstalledPages: old.InstalledPages, SourceBytes: old.SourceBytes, InstalledSourcePages: old.InstalledSourcePages})
	p.CleanupTail++
	p.CleanupCount++
}
func (p *TimezonePreparation) cleanupTailKey(ctx chasm.Context) (string, error) {
	if p.CleanupTail == math.MaxUint64 {
		return "", serviceerror.NewFailedPrecondition("timezone owner sequence exhausted")
	}
	key := strconv.FormatUint(p.CleanupTail, 10)
	if _, exists := p.Superseded.Get(ctx, key); exists {
		return "", serviceerror.NewInternal("timezone cleanup tail already owned")
	}
	return key, nil
}

// BeginCandidate retains the active source and transfers any superseded pending
// prefixes into cleanup. Repeating the identical candidate never resets a prefix.
func (p *TimezonePreparation) BeginCandidate(ctx chasm.MutableContext, d TimezoneCandidateDescriptor) error {
	if err := p.validatePagedControl(); err != nil {
		return err
	}
	if _, err := d.validate(); err != nil {
		return err
	}
	if p.Pending != nil {
		old, err := pendingTimezoneDescriptor(p.Pending)
		if err != nil {
			return err
		}
		if old == d {
			return nil
		}
	}
	if d.generation <= p.LastGeneration {
		return serviceerror.NewFailedPrecondition("timezone candidate requires a newer generation")
	}
	var key string
	var err error
	if p.Pending != nil {
		key, err = p.cleanupTailKey(ctx)
		if err != nil {
			return err
		}
	}
	if p.Pending != nil {
		p.queuePendingOwner(ctx, key)
	}
	p.Pending = d.pending()
	p.LastGeneration = d.generation
	return nil
}

// BeginTimezoneCandidate binds fixed pending metadata to the current definition.
// Legacy standalone version-one timezone owners are deliberately not migrated.
func (p *DefinitionPreparation) BeginTimezoneCandidate(ctx chasm.MutableContext, d TimezoneCandidateDescriptor) error {
	if _, err := p.header(); err != nil {
		return err
	}
	if !bytes.Equal(d.definitionID[:], p.OwnerId) {
		return serviceerror.NewFailedPrecondition("timezone definition binding does not match")
	}
	if zone, ok := p.Timezone.TryGet(ctx); ok {
		return zone.BeginCandidate(ctx, d)
	}
	zone, err := NewPendingTimezonePreparation(ctx, d)
	if err != nil {
		return err
	}
	p.Timezone = chasm.NewComponentField(ctx, zone)
	return nil
}

// PendingTimezone reports a detached fixed descriptor and its two installation
// frontiers. It does not read source or transition pages.
func (p *TimezonePreparation) PendingTimezone() (TimezoneCandidateDescriptor, uint64, uint32, bool, error) {
	if err := p.validatePagedControl(); err != nil {
		return TimezoneCandidateDescriptor{}, 0, 0, false, err
	}
	if p.Pending == nil {
		return TimezoneCandidateDescriptor{}, 0, 0, false, nil
	}
	d, err := pendingTimezoneDescriptor(p.Pending)
	return d, p.Pending.InstalledSourcePages, p.Pending.InstalledPages, true, err
}
func (p *TimezonePreparation) activatePending(ctx chasm.MutableContext, cleanupKey string) {
	pending := p.Pending
	if p.Generation != 0 {
		p.Superseded.Put(ctx, cleanupKey, &scheduler2pb.TimezoneSupersededSource{SourceId: bytes.Clone(p.SourceId), Header: bytes.Clone(p.Header), InstalledPages: p.InstalledPages, SourceBytes: p.SourceBytes, InstalledSourcePages: definitionPageCount(p.SourceBytes, timezoneSourcePageBytes)})
		p.CleanupTail++
		p.CleanupCount++
	}
	p.DefinitionSourceId = bytes.Clone(pending.DefinitionSourceId)
	p.SourceId = bytes.Clone(pending.SourceId)
	p.SemanticFingerprint = bytes.Clone(pending.SemanticFingerprint)
	p.Generation = pending.Generation
	p.Header = bytes.Clone(pending.Header)
	p.TotalPages = pending.TotalPages
	p.InstalledPages = pending.InstalledPages
	p.SourceBytes = pending.SourceBytes
	p.SourceHash = bytes.Clone(pending.SourceHash)
	p.Pending = nil
}

// InstallCandidateBatch preflights every raw/compiled page before writing. The
// caller fences its source-task token before invoking this bounded mutation.
func (p *TimezonePreparation) InstallCandidateBatch(ctx chasm.MutableContext, batch *TimezoneInstallBatch) (bool, error) {
	if err := p.validatePagedControl(); err != nil {
		return false, err
	}
	if batch == nil || batch.count > maxPreparationInstallPages {
		return false, serviceerror.NewInvalidArgument("invalid timezone batch")
	}
	d, err := pendingTimezoneDescriptor(p.Pending)
	if err != nil {
		return false, err
	}
	if d != batch.descriptor || p.Pending.InstalledSourcePages != batch.sourceFrontier || p.Pending.InstalledPages != batch.transitionFrontier {
		return false, serviceerror.NewFailedPrecondition("timezone batch owner or frontier does not match")
	}
	sourceEnd, transitionEnd, err := p.preflightTimezoneBatch(ctx, batch)
	if err != nil {
		return false, err
	}
	complete := sourceEnd == d.SourcePageCount() && transitionEnd == d.totalPages
	cleanupKey := ""
	if complete && p.Generation != 0 {
		cleanupKey, err = p.cleanupTailKey(ctx)
		if err != nil {
			return false, err
		}
	}
	for i := uint8(0); i < batch.count; i++ {
		page := batch.pages[i]
		if page.source {
			p.SourcePages.Put(ctx, timezoneSourcePageKey(d.ownerID[:], page.ordinal), &scheduler2pb.TimezoneSourcePage{SourceId: bytes.Clone(d.ownerID[:]), Ordinal: page.ordinal, Encoded: bytes.Clone(page.encoded)})
		} else {
			p.Pages.Put(ctx, timezonePageKey(d.ownerID[:], uint32(page.ordinal)), &scheduler2pb.TimezonePreparationPage{SourceId: bytes.Clone(d.ownerID[:]), Ordinal: uint32(page.ordinal), Encoded: bytes.Clone(page.encoded)})
		}
	}
	p.Pending = common.CloneProto(p.Pending)
	p.Pending.InstalledSourcePages = sourceEnd
	p.Pending.InstalledPages = transitionEnd
	if complete {
		p.activatePending(ctx, cleanupKey)
	}
	return complete, nil
}

func timezoneSourcePageKey(owner []byte, ordinal uint64) string {
	return definitionPageKey(owner, ordinal)
}
func validTimezoneSourcePage(encoded []byte, sourceBytes, ordinal uint64) bool {
	pages := definitionPageCount(sourceBytes, timezoneSourcePageBytes)
	return ordinal < pages && uint64(len(encoded)) == min(uint64(timezoneSourcePageBytes), sourceBytes-ordinal*timezoneSourcePageBytes)
}
func (p *TimezonePreparation) preflightTimezoneBatch(ctx chasm.Context, batch *TimezoneInstallBatch) (uint64, uint32, error) {
	d := batch.descriptor
	sourceEnd, transitionEnd := batch.sourceFrontier, batch.transitionFrontier
	if batch.count == 0 {
		return 0, 0, serviceerror.NewInvalidArgument("timezone installation batch is empty")
	}
	for i := uint8(0); i < batch.count; i++ {
		page := batch.pages[i]
		if page.source {
			if err := p.preflightSourceCandidatePage(ctx, d, page, sourceEnd, transitionEnd); err != nil {
				return 0, 0, err
			}
			sourceEnd++
		} else {
			if err := p.preflightCompiledCandidatePage(ctx, d, page, sourceEnd, transitionEnd); err != nil {
				return 0, 0, err
			}
			transitionEnd++
		}
	}
	return sourceEnd, transitionEnd, nil
}
func (p *TimezonePreparation) preflightSourceCandidatePage(ctx chasm.Context, d TimezoneCandidateDescriptor, page timezoneCandidatePage, sourceEnd uint64, transitionEnd uint32) error {
	if page.ordinal != sourceEnd || transitionEnd != 0 || !validTimezoneSourcePage(page.encoded, d.sourceBytes, page.ordinal) {
		return serviceerror.NewInternal("invalid timezone source batch page")
	}
	if _, exists := p.SourcePages.Get(ctx, timezoneSourcePageKey(d.ownerID[:], page.ordinal)); exists {
		return serviceerror.NewInternal("timezone source page already owned")
	}
	return nil
}
func (p *TimezonePreparation) preflightCompiledCandidatePage(ctx chasm.Context, d TimezoneCandidateDescriptor, page timezoneCandidatePage, sourceEnd uint64, transitionEnd uint32) error {
	if sourceEnd != d.SourcePageCount() || page.ordinal != uint64(transitionEnd) || transitionEnd >= d.totalPages {
		return serviceerror.NewInternal("invalid timezone transition batch page")
	}
	if _, exists := p.Pages.Get(ctx, timezonePageKey(d.ownerID[:], transitionEnd)); exists {
		return serviceerror.NewInternal("timezone transition page already owned")
	}
	zone, err := occurrence.RestorePreparedTimezone(d.header[:d.headerSize], func(ordinal uint32) ([]byte, error) {
		if uint64(ordinal) != page.ordinal {
			return nil, serviceerror.NewInternal("timezone batch page address mismatch")
		}
		return page.encoded, nil
	})
	if err != nil {
		return err
	}
	_, err = zone.Page(transitionEnd)
	return err
}

func (p *TimezonePreparation) validateActivePagedMetadata() error {
	if !validTimezoneID(p.DefinitionSourceId) || !validTimezoneID(p.SourceId) || !validTimezoneID(p.SemanticFingerprint) || !validTimezoneID(p.SourceHash) || p.SourceBytes == 0 || p.SourceBytes > uint64(math.MaxInt) {
		return serviceerror.NewInternal("invalid active timezone metadata")
	}
	zone, err := occurrence.RestorePreparedTimezone(p.Header, func(uint32) ([]byte, error) { return nil, serviceerror.NewInternal("timezone metadata has no reader") })
	if err != nil {
		return err
	}
	fingerprint := zone.Fingerprint()
	if !bytes.Equal(fingerprint[:], p.SemanticFingerprint) || zone.PageCount() != p.TotalPages {
		return serviceerror.NewInternal("active timezone header does not match")
	}
	return nil
}
