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
	return policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL || (s.BlockingOperationId == "" && len(s.Queue) == 0)
}
func directAdmissionFor(state decision.State, policy enumspb.ScheduleOverlapPolicy) bool {
	return policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL || (state.Root.BlockingOperationID == "" && len(state.Queue) == 0)
}

// demands describes metadata readiness without materializing workflow payloads.
func (s *Scheduler) demands(ctx chasm.Context, state decision.State) ([]workDemand, time.Time) {
	var demands []workDemand
	var future time.Time
	now := ctx.Now(s)
	if !s.HasGenerator {
		return nil, future
	}
	g := s.Generator.Get(ctx)
	if decision.NeedsDrain(state) {
		demands = append(demands, workDemand{class: scheduler2pb.WORK_CLASS_DRAIN, key: "drain", operation: decision.DrainIndex(state, now.UnixNano(), g.CatchupWindow) >= 0})
	}
	if len(state.Manuals) > 0 {
		m := state.Manuals[0]
		demands = append(demands, workDemand{class: scheduler2pb.WORK_CLASS_MANUAL, key: fmt.Sprintf("manual/%d", m.ID), vacancy: true, operation: directAdmissionFor(state, resolvedPolicy(m.Policy, state.Root.Policy))})
	}
	if b := s.selectedBackfill(); b != nil {
		scan := backfillNeedsScan(g, b)
		key := fmt.Sprintf("backfill/%d", b.Id)
		if !scan {
			key = fmt.Sprintf("backfill/%d/%d", b.Id, b.Continuation.Pending.Nominal)
		}
		demands = append(demands, workDemand{class: scheduler2pb.WORK_CLASS_BACKFILL, key: key, scan: scan, vacancy: !scan, operation: !scan && directAdmissionFor(state, resolvedPolicy(b.Policy, state.Root.Policy))})
	}
	if automatic, wake := automaticDemand(g, state, now); automatic != nil {
		demands = append(demands, *automatic)
	} else {
		future = wake
	}
	return demands, future
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
	eligible := !state.Root.Paused && (!state.Root.Limited || state.Root.Remaining > 0) && (now.UnixNano() <= pending.Actual || now.UnixNano()-pending.Actual <= g.CatchupWindow)
	if eligible {
		policy := resolvedPolicy(state.Root.Policy, state.Root.Policy)
		if directAdmissionFor(state, policy) {
			d.operation = state.Root.Unfinished < 4
		} else {
			d.vacancy = len(state.Queue) < 4 && policy != enumspb.SCHEDULE_OVERLAP_POLICY_SKIP && (policy != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || len(state.Queue) == 0)
		}
	}
	return &d, time.Time{}
}

func cyclicClass(start scheduler2pb.WorkClass, eligible func(scheduler2pb.WorkClass) bool) (scheduler2pb.WorkClass, bool) {
	for offset := 0; offset < 4; offset++ {
		class := scheduler2pb.WorkClass((max(int(start), 1)-1+offset)%4 + 1)
		if eligible(class) {
			return class, true
		}
	}
	return 0, false
}
func nextClass(class scheduler2pb.WorkClass) scheduler2pb.WorkClass { return class%4 + 1 }

// planWork keeps resource contention distinct from runnable scan selection.
func (s *Scheduler) planWork(ctx chasm.Context) (*scheduler2pb.CoordinationState, *workDemand, time.Time) {
	return s.planWorkFor(ctx, s.stateFor(ctx, ""))
}

// planWorkFor previews next control metadata without publishing it. Context.Now
// is a transaction snapshot (chasm.immutableCtx.Now), so preview and commit agree.
func (s *Scheduler) planWorkFor(ctx chasm.Context, state decision.State) (*scheduler2pb.CoordinationState, *workDemand, time.Time) {
	c := &scheduler2pb.CoordinationState{}
	if s.Coordination != nil {
		c = common.CloneProto(s.Coordination)
	}
	demands, future := s.demands(ctx, state)
	byClass := make(map[scheduler2pb.WorkClass]workDemand, len(demands))
	for _, d := range demands {
		byClass[d.class] = d
	}
	owner, exists := byClass[c.OwnerClass]
	if c.OwnerKey != "" && (!exists || !owner.vacancy || owner.key != c.OwnerKey) {
		c.OwnerKey = ""
	}
	if c.OwnerKey == "" && len(state.Queue) < 4 {
		if class, ok := cyclicClass(c.NextQueue, func(class scheduler2pb.WorkClass) bool { return byClass[class].vacancy }); ok {
			c.OwnerClass = class
			c.OwnerKey = byClass[class].key
		}
	}
	owns := func(d workDemand) bool {
		return !d.vacancy || (c.OwnerClass == d.class && c.OwnerKey == d.key && len(state.Queue) < 4)
	}
	op, hasOp := cyclicClass(c.NextOperation, func(class scheduler2pb.WorkClass) bool { d, ok := byClass[class]; return ok && d.operation && owns(d) })
	selected, ready := cyclicClass(c.NextWork, func(class scheduler2pb.WorkClass) bool {
		d, ok := byClass[class]
		return ok && owns(d) && (!d.operation || (hasOp && op == class && state.Root.Unfinished < 4))
	})
	if ready {
		d := byClass[selected]
		return c, &d, ctx.Now(s)
	}
	return c, nil, future
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
	c, _, wake := s.planWorkFor(ctx, next)
	return checkWorkWakeToken(c, wake)
}

func (s *Scheduler) reconcileWork(ctx chasm.MutableContext) error {
	if s.Closed {
		return nil
	}
	if !s.HasGenerator {
		return nil
	}
	c, _, wake := s.planWork(ctx)
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
	ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: wake}, &scheduler2pb.WorkTask{Token: c.Token})
	return nil
}

// WorkHandler is the sole owner of scan and admission transactions.
type WorkHandler struct{ chasm.PureTaskHandlerBase }

func (*WorkHandler) Validate(_ chasm.Context, s *Scheduler, attrs chasm.TaskInvocation, task *scheduler2pb.WorkTask) (bool, error) {
	c := s.Coordination
	return !s.Closed && c != nil && c.Armed && c.Token == task.Token && c.At == attrs.ScheduledTime.UnixNano(), nil
}
func (*WorkHandler) Execute(ctx chasm.MutableContext, s *Scheduler, attrs chasm.TaskAttributes, task *scheduler2pb.WorkTask) error {
	c := s.Coordination
	if s.Closed || c == nil || !c.Armed || c.Token != task.Token || c.At != attrs.ScheduledTime.UnixNano() {
		return nil
	}
	if ctx.Now(s).Before(time.Unix(0, c.At)) {
		return serviceerror.NewUnavailable("coordinator work is not due")
	}
	planned, d, _ := s.planWork(ctx)
	if planned.Token == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("coordinator token exhausted")
	}
	planned.Armed = false
	s.Coordination = planned
	if d == nil {
		return s.reconcileWork(ctx)
	}
	beforeUnfinished := s.Unfinished
	var err error
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
	default:
		return serviceerror.NewInternal("unknown coordinator work class")
	}
	if err != nil {
		return err
	}
	c = s.Coordination
	c.NextWork = nextClass(d.class)
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
	return occurrence.New(spec, definition.Seed, g.Continuation.Revision)
}
func (s *Scheduler) scanAutomatic(ctx chasm.MutableContext) error {
	g := s.Generator.Get(ctx)
	if g.Token == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("generation token exhausted")
	}
	e, err := s.currentEnumerator(ctx)
	if err != nil {
		return err
	}
	result, err := e.Peek(decodeContinuation(g.Continuation), ctx.Now(s), 8)
	if err != nil {
		return err
	}
	g.Continuation = encodeContinuation(result.State)
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
	e, err := s.currentEnumerator(ctx)
	if err != nil {
		return err
	}
	if err = s.admitGenerated(ctx, g.Definition.Get(ctx).Request, *pending, time.Duration(g.CatchupWindow)); err != nil {
		return err
	}
	state, err := e.DisposeRange(decodeContinuation(g.Continuation))
	if err != nil {
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
	event := decision.Event{Kind: decision.DispatchBackfill, Incarnation: s.Incarnation, BackfillID: b.Id, Nominal: b.Continuation.Pending.Nominal, Actual: b.Continuation.Pending.Actual, Policy: b.Policy}
	policy := resolvedPolicy(b.Policy, s.Policy)
	needsPayload := s.directAdmission(policy) || (policy != enumspb.SCHEDULE_OVERLAP_POLICY_SKIP && (policy != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || len(s.Queue) == 0))
	if needsPayload {
		event.Request = s.Generator.Get(ctx).Definition.Get(ctx).Request
		request := &workflowservice.StartWorkflowExecutionRequest{}
		if err := proto.Unmarshal(event.Request, request); err != nil {
			return err
		}
		event.WorkflowID = request.WorkflowId + "-" + time.Unix(0, event.Nominal).UTC().Format(time.RFC3339)
	}
	if err := s.applyTransition(ctx, event); err != nil {
		return err
	}
	return s.finishBackfill(ctx)
}
