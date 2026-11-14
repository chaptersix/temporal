package scheduler2

import (
	"fmt"
	"math"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

type workDemand struct {
	class     scheduler2pb.WorkClass
	key       string
	scan      bool
	vacancy   bool
	operation bool
	deferred  bool
}

func resolvedPolicy(override, policy enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	if override != enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		return override
	}
	if policy != enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		return policy
	}
	return enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
}
func (s *Scheduler) directAdmission(policy enumspb.ScheduleOverlapPolicy) bool {
	return policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL || (s.BlockingOperationId == "" && s.queueLength() == 0)
}
func directAdmissionFor(state decision.State, policy enumspb.ScheduleOverlapPolicy) bool {
	return policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL || (state.Root.BlockingOperationID == "" && decision.QueueLength(state) == 0)
}

// demands describes metadata readiness without materializing workflow payloads.
func (s *Scheduler) demands(ctx chasm.Context, state decision.State) ([]workDemand, time.Time, error) {
	return s.demandsForSource(ctx, state, s.sourcePreparationPending(ctx))
}

func (s *Scheduler) demandsForSource(ctx chasm.Context, state decision.State, preparing bool) ([]workDemand, time.Time, error) {
	var demands []workDemand
	var future time.Time
	now := ctx.Now(s)
	if !s.HasGenerator {
		return nil, future, nil
	}
	g := s.Generator.Get(ctx)
	if decision.NeedsDrain(state) {
		operation, deferred := false, false
		if state.IndexedQueue.Enabled {
			plan, err := s.planQueueDrain(ctx, state, now.UnixNano(), g.CatchupWindow)
			if err != nil {
				return nil, time.Time{}, err
			}
			deferred = state.IndexedQueue.Maintenance
			operation = plan != nil && plan.Selected
		} else {
			operation = decision.DrainIndex(state, now.UnixNano(), g.CatchupWindow) >= 0
		}
		demands = append(demands, workDemand{class: scheduler2pb.WORK_CLASS_DRAIN, key: "drain", operation: operation, deferred: deferred})
	}
	if len(state.Manuals) > 0 {
		m := state.Manuals[0]
		demands = append(demands, workDemand{class: scheduler2pb.WORK_CLASS_MANUAL, key: fmt.Sprintf("manual/%d", m.ID), vacancy: true, operation: directAdmissionFor(state, resolvedPolicy(m.Policy, state.Root.Policy))})
	}
	if b := s.selectedBackfill(); b != nil {
		scan := backfillNeedsScan(g, b)
		key := fmt.Sprintf("backfill/%d", b.Id)
		if !scan {
			key = fmt.Sprintf("backfill/%d/%s", b.Id, decodeDecisionTime(b.Continuation.Pending.NominalTime).Key(b.Continuation.Pending.Nominal))
		}
		deferred := !scan && s.publicSkipBatch(b) && !s.suppressBackfill(b) && s.backfillBatchCapacity(ctx) == 0
		if deferred {
			// Keep an existing lease identity while delaying service. A dynamic
			// budget change is not a handoff or a candidate invalidation.
			future = now.Add(time.Second)
		}
		demands = append(demands, workDemand{class: scheduler2pb.WORK_CLASS_BACKFILL, key: key, scan: scan, vacancy: !scan, deferred: deferred, operation: !scan && !s.suppressBackfill(b) && directAdmissionFor(state, resolvedPolicy(b.Policy, state.Root.Policy))})
	}
	if automatic, wake := automaticDemand(g, state, now); automatic != nil {
		demands = append(demands, *automatic)
	} else {
		if future.IsZero() || (!wake.IsZero() && wake.Before(future)) {
			future = wake
		}
	}
	if state.IndexedQueue.Maintenance {
		future = now
	}
	if preparing {
		demands = withoutSourceScans(demands)
	}
	return s.withPreparedMaintenance(ctx, demands, future, preparing)
}
func automaticDemand(g *Generator, state decision.State, now time.Time) (*workDemand, time.Time) {
	if !g.Armed {
		return nil, time.Time{}
	}
	at := time.Unix(0, g.WakeAt)
	if at.After(now) {
		return nil, at
	}
	pending := g.Continuation.Pending
	d := workDemand{class: scheduler2pb.WORK_CLASS_AUTOMATIC, key: "scan", scan: pending == nil || pending.Excluded}
	if d.scan {
		return &d, time.Time{}
	}
	d.key = fmt.Sprintf("automatic/%d/%d", pending.Revision, pending.Nominal)
	if pending.Actual > now.UnixNano() {
		return nil, time.Unix(0, pending.Actual)
	}
	eligible := !state.Root.Paused && (!state.Root.Limited || state.Root.Remaining > 0) && !time.Unix(0, pending.Actual).Before(now.Add(-time.Duration(g.CatchupWindow)))
	if eligible {
		policy := resolvedPolicy(state.Root.Policy, state.Root.Policy)
		if directAdmissionFor(state, policy) {
			d.operation = decision.OperationAvailable(state)
		} else {
			d.vacancy = decision.QueueAvailable(state) && policy != enumspb.SCHEDULE_OVERLAP_POLICY_SKIP && (policy != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || decision.QueueLength(state) == 0)
		}
	}
	return &d, time.Time{}
}

func cyclicClass(start scheduler2pb.WorkClass, eligible func(scheduler2pb.WorkClass) bool) (scheduler2pb.WorkClass, bool) {
	for offset := 0; offset < 5; offset++ {
		class := scheduler2pb.WorkClass((max(int(start), 1)-1+offset)%5 + 1)
		if eligible(class) {
			return class, true
		}
	}
	return 0, false
}
func nextClass(class scheduler2pb.WorkClass) scheduler2pb.WorkClass { return class%4 + 1 }

// planWork keeps resource contention distinct from runnable scan selection.
func (s *Scheduler) planWork(ctx chasm.Context) (*scheduler2pb.CoordinationState, *workDemand, time.Time, error) {
	state, err := s.stateFor(ctx, "")
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	return s.planWorkFor(ctx, state)
}

// planWorkFor previews next control metadata without publishing it. Context.Now
// is a transaction snapshot (chasm.immutableCtx.Now), so preview and commit agree.
func (s *Scheduler) planWorkFor(ctx chasm.Context, state decision.State) (*scheduler2pb.CoordinationState, *workDemand, time.Time, error) {
	return s.planWorkForSource(ctx, state, s.sourcePreparationPending(ctx))
}

func (s *Scheduler) planWorkForSource(ctx chasm.Context, state decision.State, preparing bool) (*scheduler2pb.CoordinationState, *workDemand, time.Time, error) {
	c := &scheduler2pb.CoordinationState{}
	if s.Coordination != nil {
		c = common.CloneProto(s.Coordination)
	}
	demands, future, err := s.demandsForSource(ctx, state, preparing)
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	byClass := make(map[scheduler2pb.WorkClass]workDemand, len(demands))
	for _, d := range demands {
		byClass[d.class] = d
		if state.Capacity.Public && d.vacancy && !decision.QueueAvailable(state) {
			// Live configuration can restore capacity without an operation result.
			// Keep accepted intent and its lease, with a physical retry rather than an immediate loop.
			retry := ctx.Now(s).Add(time.Second)
			if future.IsZero() || retry.Before(future) {
				future = retry
			}
		}
	}
	planQueueOwner(c, byClass, state)
	owns := func(d workDemand) bool {
		return !d.vacancy || (c.OwnerClass == d.class && c.OwnerKey == d.key && decision.QueueAvailable(state))
	}
	op, hasOp := cyclicClass(c.NextOperation, func(class scheduler2pb.WorkClass) bool {
		d, ok := byClass[class]
		return ok && !d.deferred && d.operation && owns(d)
	})
	selected, ready := cyclicClass(c.NextWork, func(class scheduler2pb.WorkClass) bool {
		d, ok := byClass[class]
		return ok && !d.deferred && owns(d) && (!d.operation || (hasOp && op == class && decision.OperationAvailable(state)))
	})
	if ready {
		d := byClass[selected]
		return c, &d, ctx.Now(s), nil
	}
	return c, nil, future, nil
}

func planQueueOwner(c *scheduler2pb.CoordinationState, byClass map[scheduler2pb.WorkClass]workDemand, state decision.State) {
	owner, exists := byClass[c.OwnerClass]
	if c.OwnerKey != "" && (!exists || !owner.vacancy || owner.key != c.OwnerKey) {
		c.OwnerKey = ""
	}
	if c.OwnerKey == "" && decision.QueueAvailable(state) {
		if class, ok := cyclicClass(c.NextQueue, func(class scheduler2pb.WorkClass) bool { return byClass[class].vacancy && !byClass[class].deferred }); ok {
			c.OwnerClass = class
			c.OwnerKey = byClass[class].key
		}
	}
}

func checkWorkWakeToken(c *scheduler2pb.CoordinationState, wake time.Time) error {
	if c == nil || wake.IsZero() || (c.Armed && c.At <= wake.UnixNano()) || c.Token < math.MaxUint64 {
		return nil
	}
	return serviceerror.NewFailedPrecondition("coordinator token exhausted")
}

func (s *Scheduler) checkNextWorkToken(ctx chasm.Context, next decision.State) error {
	if !s.HasGenerator || s.Coordination == nil || s.Coordination.Token < math.MaxUint64 {
		return nil
	}
	c, _, wake, err := s.planWorkFor(ctx, next)
	if err != nil {
		return err
	}
	return checkWorkWakeToken(c, wake)
}

func (s *Scheduler) reconcileWork(ctx chasm.MutableContext) error {
	state, stateErr := s.stateFor(ctx, "")
	if stateErr != nil {
		return stateErr
	}
	if err := s.checkNextIdle(ctx, state); err != nil {
		return err
	}
	if err := s.reconcileCoordinator(ctx); err != nil {
		return err
	}
	return s.reconcileIdle(ctx)
}

func (s *Scheduler) reconcileCoordinator(ctx chasm.MutableContext) error {
	if s.Closed {
		return nil
	}
	if !s.HasGenerator {
		return nil
	}
	c, _, wake, err := s.planWork(ctx)
	if err != nil {
		return err
	}
	if wake.IsZero() {
		c.Armed = false
		s.Coordination = c
		s.ManualArmed = false
		s.DrainArmed = false
		return nil
	}
	atNano := wake.UnixNano()
	if c.Armed && c.At <= atNano {
		s.Coordination = c
		s.ManualArmed = false
		s.DrainArmed = false
		// A callback can make an already-due physical obligation ready. Promote
		// it to the bounded immediate chain without changing its logical fence.
		if s.remainingWork(ctx) > 0 && c.At <= ctx.Now(s).UnixNano() {
			s.addWorkTask(ctx, time.Unix(0, c.At), c.Token)
		}
		return nil
	}
	if err := checkWorkWakeToken(c, wake); err != nil {
		return err
	}
	c.Token++
	c.Armed = true
	c.At = atNano
	s.Coordination = c
	s.ManualArmed = false
	s.DrainArmed = false
	s.addWorkTask(ctx, wake, c.Token)
	return nil
}

// WorkHandler is the sole owner of scan and admission transactions.
type WorkHandler struct{ chasm.PureTaskHandlerBase }

func (*WorkHandler) Validate(ctx chasm.Context, s *Scheduler, attrs chasm.TaskInvocation, task *scheduler2pb.WorkTask) (bool, error) {
	return validWorkTask(ctx, s, attrs.TaskAttributes, task), nil
}
func (*WorkHandler) Execute(ctx chasm.MutableContext, s *Scheduler, attrs chasm.TaskAttributes, task *scheduler2pb.WorkTask) error {
	c := s.Coordination
	if !validWorkTask(ctx, s, attrs, task) {
		return nil
	}
	if ctx.Now(s).Before(time.Unix(0, c.At)) {
		return serviceerror.NewUnavailable("coordinator work is not due")
	}
	remaining := s.workQuanta(ctx)
	if attrs.IsImmediate() {
		remaining = int(task.RemainingQuanta)
	}
	ctx = chasm.ContextWithValue(ctx, workRemainingKey{}, max(0, remaining-1))
	if s.queueMaintenancePending() {
		if c.Token == math.MaxUint64 {
			return serviceerror.NewFailedPrecondition("coordinator token exhausted")
		}
		if _, err := s.maintainQueue(ctx); err != nil {
			return err
		}
	}
	planned, d, _, err := s.planWork(ctx)
	if err != nil {
		return err
	}
	if planned.Token == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("coordinator token exhausted")
	}
	planned.Armed = false
	if d == nil {
		state, stateErr := s.stateFor(ctx, "")
		if stateErr != nil {
			return stateErr
		}
		if err := s.checkNextIdle(ctx, state); err != nil {
			return err
		}
		s.Coordination = planned
		return s.reconcileWork(ctx)
	}
	beforeUnfinished := s.Unfinished
	switch d.class {
	case scheduler2pb.WORK_CLASS_DRAIN:
		err = s.applyTransition(ctx, decision.Event{Kind: decision.Drain, Incarnation: s.Incarnation})
	case scheduler2pb.WORK_CLASS_MANUAL:
		err = s.applyTransition(ctx, decision.Event{Kind: decision.DispatchManual, Incarnation: s.Incarnation})
	case scheduler2pb.WORK_CLASS_BACKFILL:
		if d.scan {
			err = s.scanBackfill(ctx)
		} else {
			err = s.transferBackfill(ctx)
		}
	case scheduler2pb.WORK_CLASS_AUTOMATIC:
		if d.scan {
			err = s.scanAutomatic(ctx)
		} else {
			err = s.transferAutomatic(ctx)
		}
	case scheduler2pb.WORK_CLASS_PREPARED_MAINTENANCE:
		err = s.maintainPrepared(ctx, planned, previewLimits(ctx), s.ensurePreviewSourceReady)
	default:
		return serviceerror.NewInternal("unknown coordinator work class")
	}
	if err != nil {
		return err
	}
	s.Coordination = planned
	c = planned
	c.NextWork = s.nextWorkClass(ctx, d.class)
	if d.vacancy {
		c.OwnerKey = ""
		c.NextQueue = nextClass(d.class)
	}
	if s.Unfinished > beforeUnfinished {
		c.NextOperation = nextClass(d.class)
	}
	return s.reconcileWork(ctx)
}
func (s *Scheduler) currentEnumerator(ctx chasm.Context) (*occurrence.Enumerator, error) {
	g := s.Generator.Get(ctx)
	definition := g.Definition.Get(ctx)
	spec := &schedulepb.ScheduleSpec{}
	if err := proto.Unmarshal(definition.Spec, spec); err != nil {
		return nil, err
	}
	return s.newEnumerator(spec, definition.Seed, g.Continuation.Revision)
}
func (s *Scheduler) scanAutomatic(ctx chasm.MutableContext) error {
	g := s.Generator.Get(ctx)
	if g.Token == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("generation token exhausted")
	}
	if g.PreparedDefinition.IsSet() {
		return s.scanPreparedAutomatic(ctx, g)
	}
	e, err := s.currentEnumerator(ctx)
	if err != nil {
		return err
	}
	result, err := e.Peek(decodeContinuation(g.Continuation), ctx.Now(s), 8)
	if err != nil {
		return err
	}
	return s.publishAutomaticScan(ctx, g, result, nil)
}

func (s *Scheduler) publishAutomaticScan(ctx chasm.MutableContext, g *Generator, result occurrence.Result, work *scheduler2pb.Payload) error {
	continuation := encodeContinuation(result.State)
	state, err := s.stateFor(ctx, "")
	if err != nil {
		return err
	}
	if _, _, err := s.checkIdlePlan(state, continuation, true, len(s.Backfills) > 0); err != nil {
		return err
	}
	if g.PreparedDefinition.IsSet() {
		if work == nil {
			g.PreparedWork = chasm.NewEmptyField[*scheduler2pb.Payload]()
		} else {
			g.PreparedWork = chasm.NewDataField(ctx, work)
		}
	}
	g.Continuation = continuation
	s.Cursor = max(s.Cursor, result.State.DisposedThrough.UnixNano())
	g.Token++
	g.LastStatus = scheduler2pb.EnumerationStatus(result.Status + 1)
	g.Armed = true
	g.WakeAt = ctx.Now(s).UnixNano()
	switch result.Status {
	case occurrence.Active:
	case occurrence.Yield:
		g.WakeAt = min(ctx.Now(s).Add(time.Millisecond).UnixNano(), time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond).UnixNano())
	case occurrence.Waiting:
		if result.State.Pending != nil {
			g.LastStatus = scheduler2pb.ENUMERATION_STATUS_WAITING
			g.WakeAt = result.State.Pending.Actual.UnixNano()
		} else {
			g.Armed = false
		}
	default:
		g.Armed = false
	}
	return nil
}
func (s *Scheduler) transferAutomatic(ctx chasm.MutableContext) error {
	g := s.Generator.Get(ctx)
	pending := decodeContinuation(g.Continuation).Pending
	if pending == nil {
		return serviceerror.NewFailedPrecondition("automatic candidate is missing")
	}
	e, err := s.automaticDisposer(ctx, g)
	if err != nil {
		return err
	}
	state, err := e.DisposeRange(decodeContinuation(g.Continuation))
	if err != nil {
		return err
	}
	template, err := s.generationRequest(ctx)
	if err != nil {
		return err
	}
	if err = s.admitGenerated(ctx, template, *pending, time.Duration(g.CatchupWindow)); err != nil {
		return err
	}
	g.Continuation = encodeContinuation(state)
	g.Armed = true
	g.WakeAt = ctx.Now(s).UnixNano()
	return nil
}
func (s *Scheduler) transferBackfill(ctx chasm.MutableContext) error {
	b := s.selectedBackfill()
	if b == nil || b.Continuation == nil || b.Continuation.Pending == nil {
		return serviceerror.NewFailedPrecondition("backfill candidate is missing")
	}
	event := decision.Event{Kind: decision.DispatchBackfill, Incarnation: s.Incarnation, BackfillID: b.Id, Nominal: b.Continuation.Pending.Nominal, Actual: b.Continuation.Pending.Actual, NominalTime: decodeDecisionTime(b.Continuation.Pending.NominalTime), ActualTime: decodeDecisionTime(b.Continuation.Pending.ActualTime), Policy: b.Policy}
	policy := resolvedPolicy(b.Policy, s.Policy)
	event.BackfillBatchSkip = s.suppressBackfill(b)
	capacity := int64(0)
	if s.publicSkipBatch(b) && !event.BackfillBatchSkip {
		capacity = s.backfillBatchCapacity(ctx)
		if capacity == 0 {
			return serviceerror.NewUnavailable("backfill batch capacity is unavailable")
		}
	}
	needsPayload := !event.BackfillBatchSkip && (s.directAdmission(policy) || (policy != enumspb.SCHEDULE_OVERLAP_POLICY_SKIP && (policy != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || s.queueLength() == 0)))
	if needsPayload {
		var err error
		event.Request, err = s.generationRequest(ctx)
		if err != nil {
			return err
		}
		request := &workflowservice.StartWorkflowExecutionRequest{}
		if err := proto.Unmarshal(event.Request, request); err != nil {
			return err
		}
		event.WorkflowID = request.WorkflowId + "-" + event.NominalTime.Time(event.Nominal).Format(time.RFC3339)
	}
	if err := s.applyTransition(ctx, event); err != nil {
		return err
	}
	// publishTransition replaces root metadata; update the current range record,
	// not the stale pointer captured before the handoff.
	current := s.selectedBackfill()
	if event.BackfillBatchSkip {
		current.SkipBatchRemaining--
	} else if s.publicSkipBatch(current) {
		current.SkipBatchRemaining = capacity - 1
		current.SkipBatchRevision = s.Revision
	} else {
		current.SkipBatchRemaining = 0
	}
	return s.finishBackfill(ctx)
}

func withoutSourceScans(demands []workDemand) []workDemand {
	ready := demands[:0]
	for _, demand := range demands {
		if !demand.scan {
			ready = append(ready, demand)
		}
	}
	return ready
}

func (s *Scheduler) withPreparedMaintenance(ctx chasm.Context, demands []workDemand, future time.Time, preparing bool) ([]workDemand, time.Time, error) {
	maintenance, maintenanceWake, err := s.preparedMaintenanceWake(ctx, preparing)
	if err != nil {
		return nil, time.Time{}, err
	}
	if !maintenanceWake.IsZero() && (future.IsZero() || maintenanceWake.Before(future)) {
		future = maintenanceWake
	}
	if maintenance {
		demands = append(demands, workDemand{class: scheduler2pb.WORK_CLASS_PREPARED_MAINTENANCE, key: "prepared-maintenance"})
	}
	return demands, future, nil
}

// nextWorkClass adds maintenance only for prepared executions. Existing profiles
// retain their persisted four-class rotation and saved replay behavior.
func (s *Scheduler) nextWorkClass(ctx chasm.Context, class scheduler2pb.WorkClass) scheduler2pb.WorkClass {
	if class == scheduler2pb.WORK_CLASS_PREPARED_MAINTENANCE {
		return scheduler2pb.WORK_CLASS_DRAIN
	}
	if class == scheduler2pb.WORK_CLASS_AUTOMATIC && s.HasGenerator && s.Generator.Get(ctx).PreparedDefinition.IsSet() {
		return scheduler2pb.WORK_CLASS_PREPARED_MAINTENANCE
	}
	return nextClass(class)
}
