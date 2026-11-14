package scheduler2

import (
	"bytes"
	"context"
	"math"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
)

const (
	sourceDefinition uint32 = iota + 1
	sourceResolve
	sourceTimezone
)

type sourceRead struct {
	task                 *pb.SourcePreparationTask
	definition, timezone bool
}
type sourceSnapshot struct {
	request    *pb.SourcePreparationRequest
	definition *pb.DefinitionPreparationState
	zone       *pb.TimezonePreparationState
	source     *pb.DefinitionPreparationSource
	zoneSource *pb.TimezonePreparationSource
}
type sourceResponse struct {
	task       *pb.SourcePreparationTask
	definition *DefinitionInstallBatch
	candidate  *TimezoneCandidateDescriptor
	timezone   *TimezoneInstallBatch
	unchanged  bool
}

func (s *Scheduler) validSourceTask(ctx chasm.Context, task *pb.SourcePreparationTask) bool {
	if s.Closed || task == nil || task.Token == 0 || !s.SourceRequest.IsSet() {
		return false
	}
	r := s.SourceRequest.Get(ctx)
	return r.Pending && r.Token == task.Token && len(task.DefinitionOwnerId) == 32 && bytes.Equal(r.DefinitionOwnerId, task.DefinitionOwnerId)
}

// readSourcePreparation copies fixed control first; complete sources are exported
// only on an explicitly requested cache miss under the same task fence.
func (s *Scheduler) readSourcePreparation(ctx chasm.Context, input sourceRead) (sourceSnapshot, error) {
	if !s.validSourceTask(ctx, input.task) {
		return sourceSnapshot{}, nil
	}
	r := s.SourceRequest.Get(ctx)
	p := s.Generator.Get(ctx).PreparedDefinition.Get(ctx)
	if _, err := p.header(); err != nil {
		return sourceSnapshot{}, err
	}
	if !bytes.Equal(r.DefinitionOwnerId, p.OwnerId) {
		return sourceSnapshot{}, serviceerror.NewInternal("source task definition binding does not match")
	}
	out := sourceSnapshot{request: common.CloneProto(r), definition: common.CloneProto(p.DefinitionPreparationState)}
	if input.definition {
		var err error
		out.source, _, _, err = p.SourceSnapshot(ctx)
		if err != nil {
			return sourceSnapshot{}, err
		}
	}
	if zone, ok := p.Timezone.TryGet(ctx); ok {
		if err := zone.validatePagedControl(); err != nil {
			return sourceSnapshot{}, err
		}
		out.zone = common.CloneProto(zone.TimezonePreparationState)
		if input.timezone {
			var err error
			out.zoneSource, _, _, err = zone.PendingSourceSnapshot(ctx)
			if err != nil {
				return sourceSnapshot{}, err
			}
		}
	}
	return out, nil
}

// beginSourcePreparation creates one fenced obligation. Repeated checks coalesce
// while the same definition already has an outstanding source task.
func (s *Scheduler) beginSourcePreparation(ctx chasm.MutableContext) error {
	if s.Closed {
		return serviceerror.NewNotFound("schedule is closed")
	}
	p := s.Generator.Get(ctx).PreparedDefinition.Get(ctx)
	if _, err := p.header(); err != nil {
		return err
	}
	token := uint64(1)
	if old, ok := s.SourceRequest.TryGet(ctx); ok {
		if old.Pending && bytes.Equal(old.DefinitionOwnerId, p.OwnerId) {
			return nil
		}
		if old.Token == math.MaxUint64 {
			return serviceerror.NewFailedPrecondition("source task token exhausted")
		}
		token = old.Token + 1
	}
	r := &pb.SourcePreparationRequest{DefinitionOwnerId: bytes.Clone(p.OwnerId), Token: token, Pending: true, Phase: sourceDefinition}
	if p.InstalledPages == p.TotalPages {
		r.Phase = sourceResolve
	}
	s.publishSourceRequest(ctx, r)
	return nil
}
func (s *Scheduler) publishSourceRequest(ctx chasm.MutableContext, r *pb.SourcePreparationRequest) {
	s.SourceRequest = chasm.NewDataField(ctx, r)
	if r.Pending {
		ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: ctx.Now(s)}, &pb.SourcePreparationTask{DefinitionOwnerId: bytes.Clone(r.DefinitionOwnerId), Token: r.Token})
	}
}

func (s *Scheduler) mergeSourcePreparation(ctx chasm.MutableContext, response sourceResponse) (chasm.NoValue, error) {
	if !s.validSourceTask(ctx, response.task) {
		return nil, nil
	}
	r := common.CloneProto(s.SourceRequest.Get(ctx))
	if r.Token == math.MaxUint64 {
		return nil, serviceerror.NewFailedPrecondition("source task token exhausted")
	}
	p := s.Generator.Get(ctx).PreparedDefinition.Get(ctx)
	if !bytes.Equal(p.OwnerId, r.DefinitionOwnerId) {
		return nil, serviceerror.NewInternal("source response definition binding does not match")
	}
	var grant sourceGrant
	if sourceResponseCompletes(r, response) {
		if err := s.checkSourceReady(ctx); err != nil {
			return nil, err
		}
		var err error
		grant, err = s.prepareSourceGrant(ctx, r, p)
		if err != nil {
			return nil, err
		}
	}
	if err := mergeSourcePages(ctx, p, r, response); err != nil {
		return nil, err
	}
	r.Token++
	if !r.Pending {
		s.publishSourceGrant(ctx, grant)
	}
	s.publishSourceRequest(ctx, r)
	if !r.Pending {
		return nil, s.reconcileWork(ctx)
	}
	return nil, nil
}
func mergeSourcePages(ctx chasm.MutableContext, p *DefinitionPreparation, r *pb.SourcePreparationRequest, response sourceResponse) error {
	switch r.Phase {
	case sourceDefinition:
		done, err := p.InstallBatch(ctx, response.definition)
		if err != nil {
			return err
		}
		if done {
			r.Phase = sourceResolve
		}
	case sourceResolve:
		if response.unchanged {
			r.Pending = false
			return nil
		}
		return mergeSourceCandidate(ctx, p, r, response.candidate)
	case sourceTimezone:
		zone := p.Timezone.Get(ctx)
		if zone.Pending == nil || zone.Pending.Generation != r.TimezoneGeneration || !bytes.Equal(zone.Pending.SourceId, r.CandidateSourceId) {
			return serviceerror.NewInternal("source response candidate binding does not match")
		}
		if response.candidate != nil {
			return mergeSourceCandidate(ctx, p, r, response.candidate)
		}
		done, err := zone.InstallCandidateBatch(ctx, response.timezone)
		if err != nil {
			return err
		}
		if done {
			r.Pending = false
		}
	default:
		return serviceerror.NewInternal("invalid source preparation phase")
	}
	return nil
}
func mergeSourceCandidate(ctx chasm.MutableContext, p *DefinitionPreparation, r *pb.SourcePreparationRequest, d *TimezoneCandidateDescriptor) error {
	if d == nil {
		return serviceerror.NewInternal("source response candidate is missing")
	}
	if err := p.BeginTimezoneCandidate(ctx, *d); err != nil {
		return err
	}
	r.Phase = sourceTimezone
	r.TimezoneGeneration = d.generation
	r.CandidateSourceId = bytes.Clone(d.ownerID[:])
	return nil
}

// SourcePreparationHandler performs source I/O and compilation outside mutations.
// Only a bounded detached response enters the committing callback.
type SourcePreparationHandler struct {
	chasm.SideEffectTaskHandlerBase[*pb.SourcePreparationTask]
	service *PreparationService
}

func (*SourcePreparationHandler) Validate(ctx chasm.Context, s *Scheduler, _ chasm.TaskInvocation, task *pb.SourcePreparationTask) (bool, error) {
	return s.validSourceTask(ctx, task), nil
}
func (h *SourcePreparationHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, task *pb.SourcePreparationTask) error {
	if h == nil || h.service == nil {
		return serviceerror.NewUnavailable("source preparation service is not configured")
	}
	snap, err := chasm.ReadComponent(ctx, ref, (*Scheduler).readSourcePreparation, sourceRead{task: task})
	if err != nil || snap.request == nil {
		return err
	}
	response, err := h.prepare(ctx, ref, task, snap)
	if err != nil || response.task == nil {
		return err
	}
	_, _, err = chasm.UpdateComponent(ctx, chasm.NewComponentRef[*Scheduler](ref.ExecutionKey), (*Scheduler).mergeSourcePreparation, response)
	return err
}
func (h *SourcePreparationHandler) prepare(ctx context.Context, ref chasm.ComponentRef, task *pb.SourcePreparationTask, snap sourceSnapshot) (sourceResponse, error) {
	owner := [32]byte(snap.request.DefinitionOwnerId)
	stage, hit := h.service.Definition(owner)
	if !hit {
		restored, err := chasm.ReadComponent(ctx, ref, (*Scheduler).readSourcePreparation, sourceRead{task: task, definition: true})
		if err != nil || restored.request == nil {
			return sourceResponse{}, err
		}
		stage, err = h.service.RestoreDefinition(restored.source, restored.definition.Revision, owner)
		if err != nil {
			return sourceResponse{}, err
		}
	}
	out := sourceResponse{task: common.CloneProto(task)}
	switch snap.request.Phase {
	case sourceDefinition:
		var err error
		out.definition, err = stage.InstallBatch(snap.definition.InstalledPages, maxPreparationInstallPages)
		return out, err
	case sourceResolve:
		return h.resolve(stage, snap, out)
	case sourceTimezone:
		return h.prepareTimezone(ctx, ref, stage, snap, out)
	default:
		return sourceResponse{}, serviceerror.NewInternal("invalid source preparation phase")
	}
}
func (h *SourcePreparationHandler) resolve(stage *StagedDefinitionSource, snap sourceSnapshot, out sourceResponse) (sourceResponse, error) {
	generation := snap.zone.GetLastGeneration()
	if generation == math.MaxUint64 {
		return sourceResponse{}, serviceerror.NewFailedPrecondition("timezone generation exhausted")
	}
	source, err := h.service.ResolveDefinition(stage)
	if err != nil {
		return sourceResponse{}, err
	}
	candidate, err := h.service.PrepareTimezone(stage, source, generation+1)
	if err != nil {
		return sourceResponse{}, err
	}
	d := candidate.Descriptor()
	// A no-change acknowledgment retains the exact active source and its original
	// resolution instant, rather than substituting cache metadata into its owner.
	if snap.zone.GetPending() == nil && bytes.Equal(snap.zone.GetDefinitionSourceId(), d.definitionID[:]) && bytes.Equal(snap.zone.GetSemanticFingerprint(), d.fingerprint[:]) {
		out.unchanged = true
	} else {
		out.candidate = &d
	}
	return out, nil
}
func (h *SourcePreparationHandler) prepareTimezone(ctx context.Context, ref chasm.ComponentRef, stage *StagedDefinitionSource, snap sourceSnapshot, out sourceResponse) (sourceResponse, error) {
	d, err := pendingTimezoneDescriptor(snap.zone.GetPending())
	if err != nil {
		return sourceResponse{}, err
	}
	candidate, hit := h.service.Timezone(d.ownerID)
	if !hit {
		if snap.zone.Pending.InstalledSourcePages == d.SourcePageCount() {
			restored, readErr := chasm.ReadComponent(ctx, ref, (*Scheduler).readSourcePreparation, sourceRead{task: out.task, timezone: true})
			if readErr != nil || restored.request == nil {
				return sourceResponse{}, readErr
			}
			candidate, err = h.service.RestoreTimezone(stage, restored.zoneSource, d.generation, d.ownerID)
		} else {
			candidate, hit, err = h.service.RecoverTimezone(stage, d)
			if err == nil && !hit {
				return h.resolve(stage, snap, out)
			}
		}
		if err != nil {
			return sourceResponse{}, err
		}
	}
	out.timezone, err = candidate.InstallBatch(snap.zone.Pending.InstalledSourcePages, snap.zone.Pending.InstalledPages, maxPreparationInstallPages)
	return out, err
}

// sourcePreparationPending prevents scans and time advancement from racing a source check.
func (s *Scheduler) sourcePreparationPending(ctx chasm.Context) bool {
	r, ok := s.SourceRequest.TryGet(ctx)
	return ok && r.Pending
}

func sourceResponseCompletes(r *pb.SourcePreparationRequest, response sourceResponse) bool {
	if r.Phase == sourceResolve {
		return response.unchanged
	}
	b := response.timezone
	if r.Phase != sourceTimezone || response.candidate != nil || b == nil {
		return false
	}
	return b.sourceFrontier+uint64(b.transitionFrontier)+uint64(b.count) == b.descriptor.SourcePageCount()+uint64(b.descriptor.totalPages)
}

// checkSourceReady exposes every scheduling rejection before installing the last
// source page or advancing its durable task token.
func (s *Scheduler) checkSourceReady(ctx chasm.Context) error {
	state, err := s.stateFor(ctx, "")
	if err != nil {
		return err
	}
	c, _, wake, err := s.planWorkForSource(ctx, state, false)
	if err != nil {
		return err
	}
	if err := checkWorkWakeToken(c, wake); err != nil {
		return err
	}
	if ready, err := s.preparedMaintenanceReady(ctx, false); err != nil || ready {
		return err
	}
	zone, ok := s.Generator.Get(ctx).PreparedDefinition.Get(ctx).Timezone.TryGet(ctx)
	if ok && zone.Generation != 0 && zone.Pending != nil {
		return nil
	}
	_, _, err = s.checkIdlePlan(state, s.Generator.Get(ctx).Continuation, s.HasGenerator, len(s.Backfills) > 0)
	return err
}
