package scheduler2

import (
	"bytes"
	"errors"
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common"
)

// preparedMaintenance inspects fixed control only. Source preparation blocks
// preview evaluation, but does not block reclamation of superseded owners.
func (s *Scheduler) preparedMaintenance(ctx chasm.Context, preparing bool) (*DefinitionPreparation, [3]bool, error) {
	var ready [3]bool
	if !s.HasGenerator {
		return nil, ready, nil
	}
	g := s.Generator.Get(ctx)
	if !g.PreparedDefinition.IsSet() {
		return nil, ready, nil
	}
	p := g.PreparedDefinition.Get(ctx)
	if _, err := p.header(); err != nil {
		return nil, ready, err
	}
	ready[0] = p.CleanupCount != 0
	if p.Timezone.IsSet() {
		ready[1] = p.Timezone.Get(ctx).CleanupCount != 0
	}
	if !preparing {
		previewReady, _, err := s.previewMaintenanceWake(ctx)
		ready[2] = previewReady
		if err != nil {
			return nil, ready, err
		}
	}
	return p, ready, nil
}

func (s *Scheduler) preparedMaintenanceReady(ctx chasm.Context, preparing bool) (bool, error) {
	_, ready, err := s.preparedMaintenance(ctx, preparing)
	return ready[0] || ready[1] || ready[2], err
}

func (s *Scheduler) preparedMaintenanceWake(ctx chasm.Context, preparing bool) (bool, time.Time, error) {
	ready, err := s.preparedMaintenanceReady(ctx, preparing)
	if err != nil || ready || preparing {
		return ready, time.Time{}, err
	}
	if !s.HasGenerator || !s.Generator.Get(ctx).PreparedDefinition.IsSet() {
		return false, time.Time{}, nil
	}
	_, wake, err := s.previewMaintenanceWake(ctx)
	return false, wake, err
}

func (s *Scheduler) previewTarget() uint32 {
	if s.Limited {
		return uint32(max(int64(0), min(int64(5), s.Remaining)))
	}
	return 5
}

func (s *Scheduler) previewMaintenanceWake(ctx chasm.Context) (bool, time.Time, error) {
	if s.ListPreview.IsSet() {
		return true, time.Time{}, nil
	}
	if !s.ListControl.IsSet() {
		return false, time.Time{}, nil
	}
	control, err := s.listControl(ctx)
	if err != nil {
		return false, time.Time{}, err
	}
	if control.PublishedTargetCount > 5 {
		return false, time.Time{}, errors.New("invalid published preview target")
	}
	if control.PublishedSlot == 0 {
		return false, time.Time{}, nil
	}
	if control.PublishedTargetCount != s.previewTarget() {
		return true, time.Time{}, nil
	}
	if len(control.FutureActionTimes) == 0 {
		return false, time.Time{}, nil
	}
	wake := control.FutureActionTimes[0].AsTime()
	if !wake.After(ctx.Now(s)) {
		return true, time.Time{}, nil
	}
	return false, wake, nil
}

func selectPreparedMaintenance(start uint32, ready [3]bool) (uint32, error) {
	if start > 3 {
		return 0, errors.New("invalid prepared maintenance cursor")
	}
	for offset := uint32(0); offset < 3; offset++ {
		candidate := (max(start, 1)-1+offset)%3 + 1
		if ready[candidate-1] {
			return candidate, nil
		}
	}
	return 0, nil
}

// maintainPrepared consumes one coordinator quantum. The caller publishes
// planned after success, preserving its updated maintenance rotation cursor.
func (s *Scheduler) maintainPrepared(ctx chasm.MutableContext, planned *scheduler2pb.CoordinationState, limits func() (int, int), ensureSource func(chasm.MutableContext, *scheduler2pb.PublicListPreview) (bool, error)) error {
	if planned == nil {
		return errors.New("missing prepared maintenance control")
	}
	p, ready, err := s.preparedMaintenance(ctx, s.sourcePreparationPending(ctx))
	if err != nil {
		return err
	}
	selected, err := selectPreparedMaintenance(planned.NextPreparedMaintenance, ready)
	if err != nil {
		return err
	}
	switch selected {
	case 0:
		return nil
	case 1:
		_, err = p.CleanupSuperseded(ctx, maxPreparationInstallPages)
	case 2:
		_, err = p.Timezone.Get(ctx).CleanupSuperseded(ctx, maxPreparationInstallPages)
	case 3:
		err = s.maintainListPreview(ctx, p, limits, ensureSource)
	default:
		return errors.New("invalid prepared maintenance selection")
	}
	if err != nil {
		return err
	}
	planned.NextPreparedMaintenance = selected%3 + 1
	return nil
}

func (s *Scheduler) maintainListPreview(ctx chasm.MutableContext, p *DefinitionPreparation, limits func() (int, int), ensureSource func(chasm.MutableContext, *scheduler2pb.PublicListPreview) (bool, error)) error {
	if !s.ListPreview.IsSet() {
		return s.startListPreviewRefresh(ctx, p)
	}
	job := s.ListPreview.Get(ctx)
	if err := validateListPreview(job); err != nil {
		return err
	}
	control, err := s.listControl(ctx)
	if err != nil {
		return err
	}
	owner := [32]byte(p.OwnerId)
	if control.PendingStamp != job.Stamp || !bytes.Equal(listSlotOwner(control, control.PendingSlot), job.OwnerId) || !bytes.Equal(job.OwnerId, owner[:]) {
		s.ListPreview = chasm.Field[*scheduler2pb.PublicListPreview]{}
		return nil
	}
	// Finish the captured snapshot even if physical task latency outlives its
	// first output; resetting here can starve a fast recurring schedule forever.
	if job.TargetCount != s.previewTarget() {
		replacement, err := s.refreshedListPreview(ctx, owner, job.Stamp)
		if err != nil {
			return err
		}
		s.ListPreview = chasm.NewDataField(ctx, replacement)
		return nil
	}
	if !job.Done && !job.PreparedSourceReady {
		if ensureSource == nil {
			return errors.New("missing preview source readiness provider")
		}
		ready, err := ensureSource(ctx, job)
		if err != nil || !ready {
			return err
		}
		job = common.CloneProto(job)
		job.PreparedSourceReady = true
	}
	step, err := advanceListPreviewQuantum(ctx, p, job, owner, control.PendingStamp, limits)
	if err != nil {
		return err
	}
	if step.stale {
		return errors.New("preview identity changed during maintenance")
	}
	// The reference compiles once for the whole window, while sampling limits
	// separately for each output. Keep its source permit across those outputs.
	if step.job.Done {
		step.job.PreparedSourceReady = false
	}
	if !step.job.Done {
		s.ListPreview = chasm.NewDataField(ctx, step.job)
		return nil
	}
	plan, matches, err := s.prepareListWindow(ctx, owner, job.Stamp, step.job.FutureActionTimes)
	if err != nil {
		return err
	}
	if !matches {
		return errors.New("preview publication identity changed")
	}
	ctx, err = s.reserveMemoVersion(ctx)
	if err != nil {
		return err
	}
	plan.control.PublishedTargetCount = step.job.TargetCount
	s.publishListWindow(ctx, plan)
	s.ListPreview = chasm.Field[*scheduler2pb.PublicListPreview]{}
	return nil
}

// refreshedListPreview captures the same boundary used by the public preview.
func (s *Scheduler) refreshedListPreview(ctx chasm.Context, owner [32]byte, stamp uint64) (*scheduler2pb.PublicListPreview, error) {
	after := ctx.Now(s)
	if cursor := time.Unix(0, s.Cursor); cursor.After(after) {
		after = cursor
	}
	if info, ok := s.PublicInfo.TryGet(ctx); ok && info.UpdateTime.AsTime().After(after) {
		after = info.UpdateTime.AsTime()
	}
	return newListPreview(owner, stamp, after, s.Limited, s.Remaining)
}

func (s *Scheduler) startListPreviewRefresh(ctx chasm.MutableContext, p *DefinitionPreparation) error {
	control, err := s.listControl(ctx)
	if err != nil {
		return err
	}
	owner := [32]byte(p.OwnerId)
	plan, err := s.prepareListRefresh(ctx, owner, nil, control.PublishedListLimit, control.RecentActions, nil)
	if err != nil {
		return err
	}
	job, err := s.refreshedListPreview(ctx, owner, plan.control.PendingStamp)
	if err != nil {
		return err
	}
	// Reusing static and recent values does not change the visible memo.
	if plan.memoChanged {
		return errors.New("preview refresh unexpectedly changes memo")
	}
	s.publishListRefresh(ctx, plan)
	s.ListPreview = chasm.NewDataField(ctx, job)
	return nil
}

// advanceListPreviewQuantum spends unused steps on subsequent outputs of the
// same window. It never exceeds the original 64-step quantum or five outputs.
func advanceListPreviewQuantum(ctx chasm.Context, p *DefinitionPreparation, job *scheduler2pb.PublicListPreview, owner [32]byte, stamp uint64, limits func() (int, int)) (listPreviewStep, error) {
	remaining := occurrence.MaxPreparationSteps
	var step listPreviewStep
	var total occurrence.PreparedSpecStats
	for remaining > 0 {
		var err error
		step, err = advanceListPreview(ctx, p, job, owner, stamp, limits, remaining)
		if err != nil || step.stale {
			return step, err
		}
		spent := step.stats.Steps
		total.Steps += spent
		total.TermReads += step.stats.TermReads
		total.SeedReads += step.stats.SeedReads
		total.Calendar.Steps += step.stats.Calendar.Steps
		total.Calendar.ZoneConversions += step.stats.Calendar.ZoneConversions
		total.Calendar.TimezoneLookups += step.stats.Calendar.TimezoneLookups
		total.Calendar.TimezonePageReads += step.stats.Calendar.TimezonePageReads
		step.stats = total
		if step.job.Done {
			return step, nil
		}
		if spent <= 0 || spent > remaining {
			return listPreviewStep{}, errors.New("preview quantum did not make bounded progress")
		}
		remaining -= spent
		job = step.job
	}
	return step, nil
}
