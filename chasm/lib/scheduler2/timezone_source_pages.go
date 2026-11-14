package scheduler2

import (
	"bytes"
	"crypto/sha256"
	"math"
	"strconv"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

// SourceSnapshotChecked reconstructs the complete original input as a read/export
// operation. Version two reads addressed bytes; it never consults Source.
func (p *TimezonePreparation) SourceSnapshotChecked(ctx chasm.Context) (*scheduler2pb.TimezonePreparationSource, uint64, []byte, error) {
	if p.TimezonePreparationState != nil && p.FormatVersion == 2 {
		if err := p.validatePagedControl(); err != nil {
			return nil, 0, nil, err
		}
		if p.Generation == 0 {
			return nil, 0, nil, serviceerror.NewFailedPrecondition("timezone source is not active")
		}
		source, err := p.readSourcePages(ctx, [32]byte(p.SourceId), p.SourceBytes, [32]byte(p.SourceHash))
		return source, p.Generation, bytes.Clone(p.DefinitionSourceId), err
	}
	if _, err := p.header(); err != nil {
		return nil, 0, nil, err
	}
	source, generation, owner := p.SourceSnapshot(ctx)
	return source, generation, owner, nil
}

// PendingSourceSnapshot becomes available once all raw input is durable, even
// while compiled pages are incomplete. An incomplete prefix cannot be guessed.
func (p *TimezonePreparation) PendingSourceSnapshot(ctx chasm.Context) (*scheduler2pb.TimezonePreparationSource, uint64, []byte, error) {
	if err := p.validatePagedControl(); err != nil {
		return nil, 0, nil, err
	}
	d, err := pendingTimezoneDescriptor(p.Pending)
	if err != nil {
		return nil, 0, nil, err
	}
	if p.Pending.InstalledSourcePages != d.SourcePageCount() {
		return nil, 0, nil, serviceerror.NewFailedPrecondition("pending timezone source bytes are incomplete")
	}
	source, err := p.readSourcePages(ctx, d.ownerID, d.sourceBytes, d.sourceHash)
	return source, d.generation, bytes.Clone(d.definitionID[:]), err
}
func (p *TimezonePreparation) readSourcePages(ctx chasm.Context, owner [32]byte, sourceBytes uint64, hash [32]byte) (*scheduler2pb.TimezonePreparationSource, error) {
	// Do not allocate from an advertised length before verifying available pages.
	var encoded []byte
	for ordinal := uint64(0); ordinal < definitionPageCount(sourceBytes, timezoneSourcePageBytes); ordinal++ {
		page, ok := p.SourcePages.Get(ctx, timezoneSourcePageKey(owner[:], ordinal))
		if !ok || page == nil || !bytes.Equal(page.SourceId, owner[:]) || page.Ordinal != ordinal || !validTimezoneSourcePage(page.Encoded, sourceBytes, ordinal) {
			return nil, serviceerror.NewInternal("missing or invalid timezone source page")
		}
		encoded = append(encoded, page.Encoded...)
	}
	if sha256.Sum256(encoded) != hash {
		return nil, serviceerror.NewInternal("timezone source hash does not match")
	}
	var source scheduler2pb.TimezonePreparationSource
	if err := proto.Unmarshal(encoded, &source); err != nil {
		return nil, err
	}
	return &source, nil
}

func (p *TimezonePreparation) cleanupPagedSources(ctx chasm.MutableContext, limit int) (bool, error) {
	if limit < 1 || limit > maxPreparationInstallPages {
		return false, serviceerror.NewInvalidArgument("timezone cleanup budget is outside supported bounds")
	}
	if err := p.validatePagedControl(); err != nil {
		return false, err
	}
	if p.CleanupCount == 0 {
		return true, nil
	}
	key := strconv.FormatUint(p.CleanupHead, 10)
	old, ok := p.Superseded.Get(ctx, key)
	if !ok || old == nil || !validTimezoneID(old.SourceId) || bytes.Equal(old.SourceId, p.SourceId) || (p.Pending != nil && bytes.Equal(old.SourceId, p.Pending.SourceId)) {
		return false, serviceerror.NewInternal("invalid timezone cleanup owner")
	}
	end, sourceEnd, err := p.preflightPagedCleanup(ctx, old, limit)
	if err != nil {
		return false, err
	}

	for ordinal := old.Cursor; ordinal < end; ordinal++ {
		p.Pages.Delete(ctx, timezonePageKey(old.SourceId, ordinal))
	}
	for ordinal := old.SourceCursor; ordinal < sourceEnd; ordinal++ {
		p.SourcePages.Delete(ctx, timezoneSourcePageKey(old.SourceId, ordinal))
	}
	next := common.CloneProto(old)
	next.Cursor = end
	next.SourceCursor = sourceEnd
	if end == old.InstalledPages && sourceEnd == old.InstalledSourcePages {
		p.Superseded.Delete(ctx, key)
		p.CleanupHead++
		p.CleanupCount--
	} else {
		p.Superseded.Put(ctx, key, next)
	}
	return p.CleanupCount == 0, nil
}

func (p *TimezonePreparation) preflightPagedCleanup(ctx chasm.Context, old *scheduler2pb.TimezoneSupersededSource, limit int) (uint32, uint64, error) {
	zone, err := p.restore(ctx, old.SourceId, old.Header)
	if err != nil {
		return 0, 0, err
	}
	if old.Cursor > old.InstalledPages || old.InstalledPages > zone.PageCount() || old.SourceBytes == 0 || old.SourceBytes > uint64(math.MaxInt) || old.SourceCursor > old.InstalledSourcePages || old.InstalledSourcePages > definitionPageCount(old.SourceBytes, timezoneSourcePageBytes) {
		return 0, 0, serviceerror.NewInternal("invalid timezone cleanup frontier")
	}
	end := old.Cursor + min(uint32(limit), old.InstalledPages-old.Cursor)
	sourceEnd := old.SourceCursor + min(uint64(limit)-uint64(end-old.Cursor), old.InstalledSourcePages-old.SourceCursor)
	for ordinal := old.Cursor; ordinal < end; ordinal++ {
		if _, err = zone.Page(ordinal); err != nil {
			return 0, 0, err
		}
	}
	for ordinal := old.SourceCursor; ordinal < sourceEnd; ordinal++ {
		page, exists := p.SourcePages.Get(ctx, timezoneSourcePageKey(old.SourceId, ordinal))
		if !exists || page == nil || !bytes.Equal(page.SourceId, old.SourceId) || page.Ordinal != ordinal || !validTimezoneSourcePage(page.Encoded, old.SourceBytes, ordinal) {
			return 0, 0, serviceerror.NewInternal("invalid timezone cleanup source page")
		}
	}
	return end, sourceEnd, nil
}
