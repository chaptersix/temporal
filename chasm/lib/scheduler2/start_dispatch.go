package scheduler2

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
)

type startClaimInput struct {
	task     *pb.StartDispatchTask
	deadline time.Time
}
type claimedStart struct {
	snapshot startSnapshot
	claim    *pb.StartClaim
	token    uint64
}
type startClaimResult struct {
	issuance claimedStart
	event    decision.Event
}

func (s *Scheduler) validStartDispatch(token uint64, wake int64, recovery bool) bool {
	if s.Closed || s.OperationStore == nil || s.OperationStore.StartDispatch == nil {
		return false
	}
	c := s.OperationStore.StartDispatch
	if recovery && (c.Claim == nil || c.Claim.Expiry != wake) {
		return false
	}
	return token != 0 && c.Armed && c.Token == token && c.At == wake && (c.Claim != nil) == recovery
}

func (s *Scheduler) publishStartDispatch(ctx chasm.MutableContext, changed bool) {
	if !changed || s.Closed {
		return
	}
	c := s.OperationStore.StartDispatch
	if !c.Armed {
		return
	}
	attrs := chasm.TaskAttributes{ScheduledTime: time.Unix(0, c.At)}
	if c.Claim != nil {
		ctx.AddTask(s, attrs, &pb.StartRecoveryTask{Token: c.Token, WakeTime: c.At})
	} else {
		ctx.AddTask(s, attrs, &pb.StartDispatchTask{Token: c.Token, WakeTime: c.At})
	}
}

// claimStart exposes request bytes only as the output of the committing update.
func (s *Scheduler) claimStart(ctx chasm.MutableContext, input startClaimInput) (claimedStart, error) {
	if err := s.validateOperationStore(); err != nil {
		return claimedStart{}, err
	}
	if !s.validStartDispatch(input.task.Token, input.task.WakeTime, false) {
		return claimedStart{}, nil
	}
	now, err := activityNano(ctx.Now(s))
	if err != nil {
		return claimedStart{}, err
	}
	if now < input.task.WakeTime {
		return claimedStart{}, serviceerror.NewUnavailable("start dispatcher is not due")
	}
	control := common.CloneProto(s.OperationStore)
	editor, err := s.newOperationReadinessEditor(ctx, control)
	if err != nil {
		return claimedStart{}, err
	}
	defer editor.abort()
	next, err := editor.minimum()
	if err != nil {
		return claimedStart{}, err
	}
	if next == nil {
		return claimedStart{}, serviceerror.NewInternal("armed start dispatcher has no ready operation")
	}
	if now >= next.Operation.Deadline {
		editor.abort()
		return claimedStart{}, s.Apply(ctx, decision.Event{Kind: decision.Expire, Incarnation: s.Incarnation, OperationID: next.Operation.Id})
	}
	snapshot, err := s.startSnapshot(ctx, next.Operation.Id)
	if err != nil {
		return claimedStart{}, err
	}
	if snapshot.request == nil {
		return claimedStart{}, serviceerror.NewInternal("ready operation has no issuance snapshot")
	}
	// Convert the actual captured physical deadline using the same snapshot as
	// issuance. Waiting for the execution lock must not extend the claim lifetime.
	expiry, err := activityNano(input.deadline.Add(snapshot.skipped))
	if err != nil {
		return claimedStart{}, err
	}
	if expiry <= now {
		return claimedStart{}, context.DeadlineExceeded
	}
	if err := editor.claim(next, now, expiry); err != nil {
		return claimedStart{}, err
	}
	changed, err := prepareStartDispatch(editor, true)
	if err != nil {
		return claimedStart{}, err
	}
	plan, err := editor.finish()
	if err != nil {
		return claimedStart{}, err
	}
	result := claimedStart{snapshot: snapshot, claim: common.CloneProto(control.StartDispatch.Claim), token: control.StartDispatch.Token}
	packedPlan, err := s.preparePackedReadiness(ctx, plan, control)
	if err != nil {
		return claimedStart{}, err
	}
	s.publishPackedMutation(ctx, packedPlan)
	s.publishOperationReadiness(ctx, plan)
	s.OperationStore = control
	s.publishStartDispatch(ctx, changed)
	return result, nil
}

func (s *Scheduler) mergeStartClaim(ctx chasm.MutableContext, result startClaimResult) (chasm.NoValue, error) {
	if result.event.Kind != decision.Acknowledged {
		c := s.OperationStore.GetStartDispatch()
		claim := c.GetClaim()
		expected := result.issuance.claim
		if claim != nil && !ctx.Now(s).Before(time.Unix(0, claim.Expiry)) {
			return nil, nil
		}
		if expected == nil || claim == nil || c.Token != result.issuance.token || claim.OperationId != expected.OperationId || claim.ReadySequence != expected.ReadySequence || claim.LogicalWake != expected.LogicalWake {
			return nil, nil
		}
	}
	return nil, s.Apply(ctx, result.event)
}

// StartDispatchHandler shares the finite handler's permit and RPC configuration.
type StartDispatchHandler struct {
	chasm.SideEffectTaskHandlerBase[*pb.StartDispatchTask]
	start *StartHandler
}

func (*StartDispatchHandler) Validate(_ chasm.Context, s *Scheduler, _ chasm.TaskInvocation, task *pb.StartDispatchTask) (bool, error) {
	if err := s.validateOperationStore(); err != nil {
		return false, err
	}
	return s.validStartDispatch(task.Token, task.WakeTime, false), nil
}
func (h *StartDispatchHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, task *pb.StartDispatchTask) error {
	if h == nil || h.start == nil || h.start.start == nil && h.start.startWorkflow == nil {
		return serviceerror.NewUnavailable("local workflow starter is not configured")
	}
	select {
	case h.start.permit <- struct{}{}:
		defer func() { <-h.start.permit }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return serviceerror.NewInternal("start dispatcher requires physical task deadline")
	}
	resultRef := chasm.NewComponentRef[*Scheduler](ref.ExecutionKey)
	issuance, _, err := chasm.UpdateComponent(ctx, resultRef, (*Scheduler).claimStart, startClaimInput{task: task, deadline: deadline})
	if err != nil {
		return err
	}
	if issuance.claim == nil {
		return nil
	}
	event, err := h.start.issueStart(ctx, issuance.snapshot, decision.Event{Kind: decision.Expire, Incarnation: ref.RunID, OperationID: issuance.claim.OperationId})
	if err != nil {
		return err
	}
	_, _, err = chasm.UpdateComponent(ctx, resultRef, (*Scheduler).mergeStartClaim, startClaimResult{issuance: issuance, event: event})
	return err
}

// StartRecoveryHandler returns an expired claim to the chronological ready index.
type StartRecoveryHandler struct{ chasm.PureTaskHandlerBase }

func (*StartRecoveryHandler) Validate(_ chasm.Context, s *Scheduler, _ chasm.TaskInvocation, task *pb.StartRecoveryTask) (bool, error) {
	if err := s.validateOperationStore(); err != nil {
		return false, err
	}
	return s.validStartDispatch(task.Token, task.WakeTime, true), nil
}
func (*StartRecoveryHandler) Execute(ctx chasm.MutableContext, s *Scheduler, _ chasm.TaskAttributes, task *pb.StartRecoveryTask) error {
	if !s.validStartDispatch(task.Token, task.WakeTime, true) {
		return nil
	}
	now, err := activityNano(ctx.Now(s))
	if err != nil {
		return err
	}
	if now < task.WakeTime {
		return serviceerror.NewUnavailable("start claim recovery is not due")
	}
	control := common.CloneProto(s.OperationStore)
	editor, err := s.newOperationReadinessEditor(ctx, control)
	if err != nil {
		return err
	}
	defer editor.abort()
	entry, err := editor.claimEntry()
	if err != nil {
		return err
	}
	if now >= entry.Operation.Deadline {
		editor.abort()
		return s.Apply(ctx, decision.Event{Kind: decision.Expire, Incarnation: s.Incarnation, OperationID: entry.Operation.Id})
	}
	if _, err := editor.recover(now); err != nil {
		return err
	}
	changed, err := prepareStartDispatch(editor, false)
	if err != nil {
		return err
	}
	plan, err := editor.finish()
	if err != nil {
		return err
	}
	packedPlan, err := s.preparePackedReadiness(ctx, plan, control)
	if err != nil {
		return err
	}
	s.publishPackedMutation(ctx, packedPlan)
	s.publishOperationReadiness(ctx, plan)
	s.OperationStore = control
	s.publishStartDispatch(ctx, changed)
	return nil
}
