package scheduler2

import (
	"bytes"
	"errors"
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type listPreviewStep struct {
	job          *scheduler2pb.PublicListPreview
	stats        occurrence.PreparedSpecStats
	stale, reset bool
}

func newListPreview(owner [32]byte, stamp uint64, after time.Time, limited bool, remaining int64) (*scheduler2pb.PublicListPreview, error) {
	target := int64(5)
	if limited {
		target = max(0, min(target, remaining))
	}
	job := &scheduler2pb.PublicListPreview{OwnerId: bytes.Clone(owner[:]), Stamp: stamp, OriginalAfter: timestamppb.New(after), After: timestamppb.New(after), Limited: limited, Remaining: remaining, TargetCount: uint32(target), Done: target == 0}
	if err := validateListPreview(job); err != nil {
		return nil, err
	}
	return job, nil
}

func validateListPreview(job *scheduler2pb.PublicListPreview) error {
	if job == nil || !validListOwner(job.OwnerId) || job.Stamp == 0 || job.TargetCount > 5 || len(job.FutureActionTimes) > int(job.TargetCount) {
		return errors.New("invalid list preview identity or count")
	}
	target := int64(5)
	if job.Limited {
		target = max(0, min(target, job.Remaining))
	}
	if job.TargetCount != uint32(target) || job.OriginalAfter == nil || job.OriginalAfter.CheckValid() != nil || job.After == nil || job.After.CheckValid() != nil {
		return errors.New("invalid list preview snapshot")
	}
	if err := validateListPreviewWork(job); err != nil {
		return err
	}
	after := job.OriginalAfter.AsTime()
	for _, at := range job.FutureActionTimes {
		if at == nil || at.CheckValid() != nil || !at.AsTime().After(after) {
			return errors.New("invalid list preview output order")
		}
		after = at.AsTime()
	}
	if !job.After.AsTime().Equal(after) {
		return errors.New("invalid list preview boundary")
	}
	return nil
}

func validateListPreviewWork(job *scheduler2pb.PublicListPreview) error {
	if len(job.ZoneFingerprint) != 0 && !validListOwner(job.ZoneFingerprint) {
		return errors.New("invalid list preview timezone")
	}
	if (len(job.Work) != 0 && len(job.Work) != occurrence.PreparedSpecStateBytes) || (len(job.Work) != 0 && !job.PolicyCaptured) || (job.Done && len(job.Work) != 0) {
		return errors.New("invalid list preview continuation")
	}
	if int64(int(job.WarnIterations)) != job.WarnIterations || int64(int(job.MaxIterations)) != job.MaxIterations {
		return errors.New("list preview policy exceeds machine width")
	}

	if !job.Done && len(job.FutureActionTimes) == int(job.TargetCount) {
		return errors.New("unfinished list preview reached target")
	}
	if job.Done && job.PolicyCaptured {
		return errors.New("completed list preview retained policy")
	}
	return nil
}

// advanceListPreview is read-only: the caller owns durable job/window publication.
// owner/stamp must come from the current validated ListControl, not a task payload.
func advanceListPreview(ctx chasm.Context, p *DefinitionPreparation, job *scheduler2pb.PublicListPreview, owner [32]byte, stamp uint64, limits func() (int, int), budget int) (listPreviewStep, error) {
	if budget < 1 || budget > occurrence.MaxPreparationSteps {
		return listPreviewStep{}, errors.New("invalid list preview work budget")
	}
	if err := validateListPreview(job); err != nil {
		return listPreviewStep{}, err
	}
	if job.Stamp != stamp || !bytes.Equal(job.OwnerId, owner[:]) {
		return listPreviewStep{stale: true}, nil
	}
	if p == nil {
		return listPreviewStep{}, errors.New("missing list preview definition")
	}
	if _, err := p.header(); err != nil {
		return listPreviewStep{}, err
	}
	if !bytes.Equal(p.OwnerId, owner[:]) {
		return listPreviewStep{stale: true}, nil
	}
	next := common.CloneProto(job)
	result := listPreviewStep{job: next}
	if next.TargetCount == 0 {
		next.Done = true
		return result, nil
	}
	zone, err := p.PreparedTimezone(ctx)
	if err != nil {
		return listPreviewStep{}, err
	}
	location, err := occurrence.NewResolvedLocation(zone)
	if err != nil {
		return listPreviewStep{}, err
	}
	fingerprint := location.Fingerprint()
	if len(next.ZoneFingerprint) != 0 && !bytes.Equal(next.ZoneFingerprint, fingerprint[:]) {
		next.After = common.CloneProto(next.OriginalAfter)
		next.FutureActionTimes = nil
		next.Work = nil
		next.PolicyCaptured = false
		next.WarnIterations = 0
		next.MaxIterations = 0
		next.Done = false
		result.reset = true
	}
	next.ZoneFingerprint = bytes.Clone(fingerprint[:])
	if next.Done {
		return result, nil
	}
	if !next.PolicyCaptured {
		if limits == nil {
			return listPreviewStep{}, errors.New("missing list preview policy sampler")
		}
		warn, maximum := limits()
		next.WarnIterations = int64(warn)
		next.MaxIterations = int64(maximum)
		next.PolicyCaptured = true
	}
	header, reader, err := p.Prepared(ctx, DefinitionEvaluationPolicy{WarnIterations: int(next.WarnIterations), MaxIterations: int(next.MaxIterations)})
	if err != nil {
		return listPreviewStep{}, err
	}
	var work occurrence.PreparedSpecState
	if len(next.Work) != 0 {
		work, err = occurrence.DecodePreparedSpecState(next.Work, header, location, next.After.AsTime())
		if err != nil {
			return listPreviewStep{}, err
		}
	}
	advanced, stats, err := occurrence.AdvancePreparedSpec(work, header, location, next.After.AsTime(), reader, budget)
	result.stats = stats
	if errors.Is(err, legacy.ErrComputeLimitExceeded) {
		next.Done = true
		next.Work = nil
		next.PolicyCaptured = false
		return result, nil
	}
	if err != nil {
		return listPreviewStep{}, err
	}
	return finishListPreview(result, advanced)
}

func finishListPreview(result listPreviewStep, advanced occurrence.PreparedSpecState) (listPreviewStep, error) {
	next := result.job
	var err error
	candidate := advanced.Result()
	if !candidate.Done {
		next.Work, err = occurrence.EncodePreparedSpecState(advanced)
		if err != nil {
			return listPreviewStep{}, err
		}
		return result, nil
	}
	next.Work = nil
	next.PolicyCaptured = false
	if !candidate.Found || candidate.Actual.IsZero() {
		next.Done = true
		return result, nil
	}
	if !candidate.Actual.After(next.After.AsTime()) {
		return listPreviewStep{}, errors.New("list preview candidate did not advance")
	}
	next.After = timestamppb.New(candidate.Actual)
	next.FutureActionTimes = append(next.FutureActionTimes, common.CloneProto(next.After))
	next.Done = len(next.FutureActionTimes) == int(next.TargetCount)
	return result, nil
}
