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
)

func definitionReader(h definitionHeader, owner [32]byte, read func(uint64) ([]byte, error)) occurrence.PreparedSpecReader {
	return occurrence.PreparedSpecReader{
		Term: func(exclusion bool, index uint64) (occurrence.PreparedSpecTerm, error) {
			if exclusion {
				if index >= h.exclusions {
					return occurrence.PreparedSpecTerm{}, serviceerror.NewInternal("definition exclusion is out of bounds")
				}
				index += h.terms
			} else if index >= h.terms {
				return occurrence.PreparedSpecTerm{}, serviceerror.NewInternal("definition term is out of bounds")
			}
			ordinal := index / definitionTermsPerPage
			encoded, err := read(ordinal)
			if err != nil {
				return occurrence.PreparedSpecTerm{}, err
			}
			terms, err := decodeDefinitionTerms(encoded, h, owner, ordinal)
			if err != nil {
				return occurrence.PreparedSpecTerm{}, err
			}
			return terms[index%definitionTermsPerPage], nil
		},
		Seed: func(offset uint64) (occurrence.SeedWindow, error) {
			var window occurrence.SeedWindow
			if offset > h.seedBytes {
				return window, serviceerror.NewInternal("definition seed is out of bounds")
			}
			window.Count = uint8(min(uint64(definitionSeedPageBytes), h.seedBytes-offset))
			termPages, _, err := h.pages()
			if err != nil {
				return occurrence.SeedWindow{}, err
			}
			copied := 0
			for copied < int(window.Count) {
				ordinal := termPages + offset/definitionSeedPageBytes
				encoded, err := read(ordinal)
				if err != nil {
					return occurrence.SeedWindow{}, err
				}
				data, err := decodeDefinitionSeed(encoded, h, ordinal)
				if err != nil {
					return occurrence.SeedWindow{}, err
				}
				n := copy(window.Bytes[copied:window.Count], data[offset%definitionSeedPageBytes:])
				copied += n
				offset += uint64(n)
			}
			return window, nil
		},
	}
}

// DefinitionPreparation owns fixed compiled pages and FIFO superseded prefixes.
// Source is exclusively an ingestion/reconstruction/read-export field.
type DefinitionPreparation struct {
	chasm.UnimplementedComponent
	*scheduler2pb.DefinitionPreparationState
	Source     chasm.Field[*scheduler2pb.DefinitionPreparationSource]
	Timezone   chasm.Field[*TimezonePreparation]
	Pages      chasm.DataMap[*scheduler2pb.DefinitionPreparationPage]
	Superseded chasm.DataMap[*scheduler2pb.DefinitionSupersededSource]
}

func (*DefinitionPreparation) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
func (*DefinitionPreparation) ContextMetadata(chasm.Context) map[string]string { return nil }
func (*DefinitionPreparation) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewUnimplemented("standalone definition preparation termination is not enabled")
}

// NewDefinitionPreparation admits the detached source and fixed control. Page
// installation occurs separately; source publication is input-proportional.
func NewDefinitionPreparation(ctx chasm.MutableContext, stage *StagedDefinitionSource) (*DefinitionPreparation, error) {
	if err := stage.validate(); err != nil {
		return nil, err
	}
	return publishNewDefinitionPreparation(ctx, stage), nil
}

// publishNewDefinitionPreparation follows stage.validate in the same mutation
// plan. Source admission is input-proportional, but publication cannot reject.
func publishNewDefinitionPreparation(ctx chasm.MutableContext, stage *StagedDefinitionSource) *DefinitionPreparation {
	return &DefinitionPreparation{DefinitionPreparationState: stage.state(), Source: chasm.NewDataField(ctx, common.CloneProto(stage.source)), Pages: chasm.NewDataMap[*scheduler2pb.DefinitionPreparationPage](ctx), Superseded: chasm.NewDataMap[*scheduler2pb.DefinitionSupersededSource](ctx)}
}

func (p *DefinitionPreparation) header() (definitionHeader, error) {
	if p.DefinitionPreparationState == nil || p.FormatVersion != 1 || p.Revision <= 0 || !validTimezoneID(p.SourceId) || !validTimezoneID(p.OwnerId) {
		return definitionHeader{}, serviceerror.NewInternal("invalid definition control")
	}
	h, err := decodeDefinitionHeader(p.Header)
	if err != nil {
		return h, err
	}
	owner := definitionOwner(h.identity, p.Revision)
	_, total, err := h.pages()
	if err != nil {
		return h, err
	}
	if !bytes.Equal(h.identity[:], p.SourceId) || !bytes.Equal(owner[:], p.OwnerId) || total != p.TotalPages || p.InstalledPages > total || p.CleanupHead > p.CleanupTail || p.CleanupCount != p.CleanupTail-p.CleanupHead {
		return h, serviceerror.NewInternal("invalid definition ownership or frontier")
	}
	return h, nil
}
func definitionPageKey(owner []byte, ordinal uint64) string {
	return hex.EncodeToString(owner) + "/" + strconv.FormatUint(ordinal, 10)
}

type definitionReplacementPlan struct {
	cleanupKey string
	retired    *scheduler2pb.DefinitionSupersededSource
	next       *scheduler2pb.DefinitionPreparationState
	source     *scheduler2pb.DefinitionPreparationSource
}

// prepareDefinitionReplacement uses only reads and constructs detached values.
// Publish in the same transaction lease without an intervening definition write.
func (p *DefinitionPreparation) prepareDefinitionReplacement(ctx chasm.Context, stage *StagedDefinitionSource) (*definitionReplacementPlan, error) {
	if err := stage.validate(); err != nil {
		return nil, err
	}
	if _, err := p.header(); err != nil {
		return nil, err
	}
	if stage.revision <= p.Revision || p.CleanupTail == math.MaxUint64 {
		return nil, serviceerror.NewFailedPrecondition("definition replacement requires a newer available revision")
	}
	key := strconv.FormatUint(p.CleanupTail, 10)
	if _, exists := p.Superseded.Get(ctx, key); exists {
		return nil, serviceerror.NewInternal("definition cleanup tail already owned")
	}
	next := stage.state()
	next.CleanupHead, next.CleanupTail, next.CleanupCount = p.CleanupHead, p.CleanupTail+1, p.CleanupCount+1
	return &definitionReplacementPlan{cleanupKey: key, retired: &scheduler2pb.DefinitionSupersededSource{OwnerId: bytes.Clone(p.OwnerId), Header: bytes.Clone(p.Header), InstalledPages: p.InstalledPages}, next: next, source: common.CloneProto(stage.source)}, nil
}

// publishDefinitionReplacement has no rejection path after surrounding root/blob
// publication. The private plan was validated against this unchanged lease.
func (p *DefinitionPreparation) publishDefinitionReplacement(ctx chasm.MutableContext, plan *definitionReplacementPlan) {
	p.Superseded.Put(ctx, plan.cleanupKey, plan.retired)
	p.DefinitionPreparationState = plan.next
	p.Source = chasm.NewDataField(ctx, plan.source)
}

// ReplaceSource transfers the old installed prefix to constant-size FIFO ownership.
func (p *DefinitionPreparation) ReplaceSource(ctx chasm.MutableContext, stage *StagedDefinitionSource) error {
	plan, err := p.prepareDefinitionReplacement(ctx, stage)
	if err != nil {
		return err
	}
	p.publishDefinitionReplacement(ctx, plan)
	return nil
}

// SourceSnapshot is a full read-export operation usable before installation. It
// returns the revision and identity needed to fence a later reconstructed stage.
func (p *DefinitionPreparation) SourceSnapshot(ctx chasm.Context) (*scheduler2pb.DefinitionPreparationSource, int64, []byte, error) {
	if _, err := p.header(); err != nil {
		return nil, 0, nil, err
	}
	return common.CloneProto(p.Source.Get(ctx)), p.Revision, bytes.Clone(p.SourceId), nil
}
func validateDefinitionPage(encoded []byte, h definitionHeader, owner [32]byte, ordinal uint64) error {
	terms, total, err := h.pages()
	if err != nil {
		return err
	}
	if ordinal >= total {
		return serviceerror.NewInternal("definition page is out of bounds")
	}
	if ordinal < terms {
		_, err = decodeDefinitionTerms(encoded, h, owner, ordinal)
	} else {
		_, err = decodeDefinitionSeed(encoded, h, ordinal)
	}
	return err
}

// InstallNextPages validates every addressed record before publishing the batch.
func (p *DefinitionPreparation) InstallNextPages(ctx chasm.MutableContext, stage *StagedDefinitionSource, limit int) (bool, error) {
	if limit < 1 || limit > maxPreparationInstallPages {
		return false, serviceerror.NewInvalidArgument("definition installation budget is outside supported bounds")
	}
	h, err := p.header()
	if err != nil {
		return false, err
	}
	if err = stage.validate(); err != nil {
		return false, err
	}
	if stage.revision != p.Revision || !bytes.Equal(stage.owner[:], p.OwnerId) || !bytes.Equal(stage.header.encode(), p.Header) {
		return false, serviceerror.NewFailedPrecondition("definition installation source does not match")
	}
	end := p.InstalledPages + min(uint64(limit), p.TotalPages-p.InstalledPages)
	for ordinal := p.InstalledPages; ordinal < end; ordinal++ {
		if err = validateDefinitionPage(stage.pages[ordinal], h, stage.owner, ordinal); err != nil {
			return false, err
		}
		if _, exists := p.Pages.Get(ctx, definitionPageKey(p.OwnerId, ordinal)); exists {
			return false, serviceerror.NewInternal("definition installation page already owned")
		}
	}
	for ordinal := p.InstalledPages; ordinal < end; ordinal++ {
		p.Pages.Put(ctx, definitionPageKey(p.OwnerId, ordinal), &scheduler2pb.DefinitionPreparationPage{OwnerId: bytes.Clone(p.OwnerId), Ordinal: ordinal, Encoded: bytes.Clone(stage.pages[ordinal])})
	}
	p.InstalledPages = end
	return end == p.TotalPages, nil
}

// Prepared restores only fixed control; captured readers are valid for ctx's epoch.
func (p *DefinitionPreparation) Prepared(ctx chasm.Context, policy DefinitionEvaluationPolicy) (occurrence.PreparedSpecHeader, occurrence.PreparedSpecReader, error) {
	h, err := p.header()
	if err != nil {
		return occurrence.PreparedSpecHeader{}, occurrence.PreparedSpecReader{}, err
	}
	if p.InstalledPages != p.TotalPages {
		return occurrence.PreparedSpecHeader{}, occurrence.PreparedSpecReader{}, serviceerror.NewFailedPrecondition("definition pages are not installed")
	}
	owner := [32]byte(p.OwnerId)
	pages := p.Pages
	read := func(ordinal uint64) ([]byte, error) {
		page, ok := pages.Get(ctx, definitionPageKey(owner[:], ordinal))
		if !ok || page == nil || !bytes.Equal(page.OwnerId, owner[:]) || page.Ordinal != ordinal {
			return nil, serviceerror.NewInternal("missing or invalid definition page owner")
		}
		return page.Encoded, nil
	}
	return h.prepared(owner, policy), definitionReader(h, owner, read), nil
}

// CleanupSuperseded reclaims at most sixteen addressed pages from one oldest owner.
func (p *DefinitionPreparation) CleanupSuperseded(ctx chasm.MutableContext, limit int) (bool, error) {
	if limit < 1 || limit > maxPreparationInstallPages {
		return false, serviceerror.NewInvalidArgument("definition cleanup budget is outside supported bounds")
	}
	if _, err := p.header(); err != nil {
		return false, err
	}
	if p.CleanupCount == 0 {
		return true, nil
	}
	key := strconv.FormatUint(p.CleanupHead, 10)
	old, ok := p.Superseded.Get(ctx, key)
	if !ok || old == nil || !validTimezoneID(old.OwnerId) || bytes.Equal(old.OwnerId, p.OwnerId) {
		return false, serviceerror.NewInternal("invalid definition cleanup owner")
	}
	h, err := decodeDefinitionHeader(old.Header)
	if err != nil {
		return false, err
	}
	_, total, err := h.pages()
	if err != nil {
		return false, err
	}
	if old.Cursor > old.InstalledPages || old.InstalledPages > total {
		return false, serviceerror.NewInternal("invalid definition cleanup prefix")
	}
	end := old.Cursor + min(uint64(limit), old.InstalledPages-old.Cursor)
	for ordinal := old.Cursor; ordinal < end; ordinal++ {
		page, exists := p.Pages.Get(ctx, definitionPageKey(old.OwnerId, ordinal))
		if !exists || page == nil || !bytes.Equal(page.OwnerId, old.OwnerId) || page.Ordinal != ordinal {
			return false, serviceerror.NewInternal("missing definition cleanup page")
		}
		if err = validateDefinitionPage(page.Encoded, h, [32]byte(old.OwnerId), ordinal); err != nil {
			return false, err
		}
	}
	for ordinal := old.Cursor; ordinal < end; ordinal++ {
		p.Pages.Delete(ctx, definitionPageKey(old.OwnerId, ordinal))
	}
	next := common.CloneProto(old)
	next.Cursor = end
	if end == old.InstalledPages {
		p.Superseded.Delete(ctx, key)
		p.CleanupHead++
		p.CleanupCount--
	} else {
		p.Superseded.Put(ctx, key, next)
	}
	return p.CleanupCount == 0, nil
}

// BindTimezone preserves independent timezone generation ownership. A source
// replacement leaves the old child unavailable until the new binding is installed.
func (p *DefinitionPreparation) BindTimezone(ctx chasm.MutableContext, stage *StagedTimezoneSource) error {
	if _, err := p.header(); err != nil {
		return err
	}
	if _, err := stage.validate(); err != nil {
		return err
	}
	if !bytes.Equal(stage.definitionID[:], p.OwnerId) {
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
func (p *DefinitionPreparation) PreparedTimezone(ctx chasm.Context) (*occurrence.PreparedTimezone, error) {
	if _, err := p.header(); err != nil {
		return nil, err
	}
	zone, ok := p.Timezone.TryGet(ctx)
	if !ok || !bytes.Equal(zone.DefinitionSourceId, p.OwnerId) {
		return nil, serviceerror.NewFailedPrecondition("timezone definition binding is unavailable")
	}
	return zone.PreparedTimezone(ctx)
}
