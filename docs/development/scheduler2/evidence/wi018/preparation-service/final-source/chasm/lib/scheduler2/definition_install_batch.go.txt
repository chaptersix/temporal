package scheduler2

import (
	"bytes"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
)

// DefinitionInstallBatch detaches at most sixteen bounded pages outside mutation.
// It contains no complete source, variable term list or transaction context.
type DefinitionInstallBatch struct {
	header   definitionHeader
	owner    [32]byte
	revision int64
	frontier uint64
	count    uint8
	pages    [maxPreparationInstallPages][]byte
}

func (b *DefinitionInstallBatch) OwnerID() [32]byte { return b.owner }
func (b *DefinitionInstallBatch) Frontier() uint64  { return b.frontier }

func (s *StagedDefinitionSource) InstallBatch(frontier uint64, limit int) (*DefinitionInstallBatch, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > maxPreparationInstallPages || frontier > uint64(len(s.pages)) {
		return nil, serviceerror.NewInvalidArgument("invalid definition batch frontier or budget")
	}
	batch := &DefinitionInstallBatch{header: s.header, owner: s.owner, revision: s.revision, frontier: frontier, count: uint8(min(uint64(limit), uint64(len(s.pages))-frontier))}
	for i := uint8(0); i < batch.count; i++ {
		page := s.pages[frontier+uint64(i)]
		if err := validateDefinitionPage(page, s.header, s.owner, frontier+uint64(i)); err != nil {
			return nil, err
		}
		batch.pages[i] = bytes.Clone(page)
	}
	return batch, nil
}

// InstallBatch accepts only the exact current frontier. The enclosing source-task
// token is independently fenced before calling this method.
func (p *DefinitionPreparation) InstallBatch(ctx chasm.MutableContext, batch *DefinitionInstallBatch) (bool, error) {
	h, err := p.header()
	if err != nil {
		return false, err
	}
	if batch == nil || batch.count > maxPreparationInstallPages {
		return false, serviceerror.NewInvalidArgument("invalid definition installation batch")
	}
	if batch.revision != p.Revision || !bytes.Equal(batch.owner[:], p.OwnerId) || batch.frontier != p.InstalledPages || !bytes.Equal(batch.header.encode(), p.Header) {
		return false, serviceerror.NewFailedPrecondition("definition batch owner or frontier does not match")
	}
	if uint64(batch.count) > p.TotalPages-p.InstalledPages || (batch.count == 0 && p.InstalledPages != p.TotalPages) {
		return false, serviceerror.NewInternal("invalid definition batch page count")
	}
	for i := uint8(0); i < batch.count; i++ {
		ordinal := batch.frontier + uint64(i)
		if err := validateDefinitionPage(batch.pages[i], h, batch.owner, ordinal); err != nil {
			return false, err
		}
		if _, exists := p.Pages.Get(ctx, definitionPageKey(p.OwnerId, ordinal)); exists {
			return false, serviceerror.NewInternal("definition installation page already owned")
		}
	}
	for i := uint8(0); i < batch.count; i++ {
		ordinal := batch.frontier + uint64(i)
		p.Pages.Put(ctx, definitionPageKey(p.OwnerId, ordinal), &scheduler2pb.DefinitionPreparationPage{OwnerId: bytes.Clone(p.OwnerId), Ordinal: ordinal, Encoded: bytes.Clone(batch.pages[i])})
	}
	p.InstalledPages += uint64(batch.count)
	return p.InstalledPages == p.TotalPages, nil
}
