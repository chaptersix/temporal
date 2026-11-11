package scheduler2

import (
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
	"google.golang.org/protobuf/proto"
)

// Generator persists occurrence progress independently of physical timer delivery.
type Generator struct {
	chasm.UnimplementedComponent
	*scheduler2pb.GeneratorState
	Scheduler  chasm.ParentPtr[*Scheduler]
	Definition chasm.Field[*scheduler2pb.GenerationDefinition]
}

func (*Generator) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// GenerationConfig supplies an immutable local definition and workflow template.
type GenerationConfig struct {
	Policy        enumspb.ScheduleOverlapPolicy
	Spec          *schedulepb.ScheduleSpec
	Seed          string
	Revision      int64
	After         time.Time
	CatchupWindow time.Duration
	Request       *workflowservice.StartWorkflowExecutionRequest
}

// ConfigureGeneration replaces future enumeration while preserving admitted requests.
func (s *Scheduler) ConfigureGeneration(ctx chasm.MutableContext, config GenerationConfig) error {
	if !decision.ValidPolicy(config.Policy) || config.Request == nil || config.Request.WorkflowId == "" || proto.Size(config.Request) > 64*1024 || len(config.Seed) > 64*1024 || config.CatchupWindow < 0 || config.CatchupWindow > 365*24*time.Hour || config.Revision < s.Revision {
		return serviceerror.NewInvalidArgument("invalid local generation configuration")
	}
	e, err := occurrence.New(config.Spec, config.Seed, config.Revision)
	if err != nil {
		return err
	}
	now := ctx.Now(s)
	var g *Generator
	var state occurrence.State
	var superseded *occurrence.Candidate
	if s.HasGenerator {
		g = s.Generator.Get(ctx)
		state, superseded, err = e.Update(decodeContinuation(g.Continuation), now)
	} else {
		after := config.After
		if boundary := time.Unix(0, s.Cursor).UTC(); boundary.After(after) {
			after = boundary
		}
		state, err = e.Begin(after)
	}
	if err != nil {
		return err
	}
	// Validate the execution clock without consuming the new continuation.
	if _, err = e.Run(state, now, occurrence.Limits{Probes: 1, Outputs: 1}); err != nil {
		return err
	}
	spec, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.Spec)
	if err != nil {
		return err
	}
	request, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.Request)
	if err != nil {
		return err
	}
	if g != nil && g.Token == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("generation token exhausted")
	}
	if g == nil {
		g = &Generator{GeneratorState: &scheduler2pb.GeneratorState{}}
		s.Generator = chasm.NewComponentField(ctx, g)
	}
	g.Definition = chasm.NewDataField(ctx, &scheduler2pb.GenerationDefinition{Spec: spec, Seed: config.Seed, Request: request})
	g.Continuation = encodeContinuation(state)
	g.CatchupWindow = int64(config.CatchupWindow)
	g.Superseded = encodeCandidate(superseded)
	g.LastStatus = scheduler2pb.ENUMERATION_STATUS_ACTIVE
	s.HasGenerator = true
	s.Revision = config.Revision
	s.Policy = config.Policy
	s.Cursor = max(s.Cursor, state.DisposedThrough.UnixNano())
	g.arm(ctx, now)
	return nil
}

func (g *Generator) arm(ctx chasm.MutableContext, at time.Time) {
	g.Token++
	g.Armed = true
	g.WakeAt = at.UnixNano()
	ctx.AddTask(g, chasm.TaskAttributes{ScheduledTime: at}, &scheduler2pb.GenerateTask{Token: g.Token})
}

// GenerateHandler runs bounded local enumeration under the execution write lock.
type GenerateHandler struct{ chasm.PureTaskHandlerBase }

func (*GenerateHandler) Validate(_ chasm.Context, g *Generator, attrs chasm.TaskInvocation, task *scheduler2pb.GenerateTask) (bool, error) {
	return g.Armed && g.Token == task.Token && g.WakeAt == attrs.ScheduledTime.UnixNano(), nil
}

func (*GenerateHandler) Execute(ctx chasm.MutableContext, g *Generator, attrs chasm.TaskAttributes, task *scheduler2pb.GenerateTask) error {
	if !g.Armed || g.Token != task.Token || g.WakeAt != attrs.ScheduledTime.UnixNano() {
		return nil
	}
	now := ctx.Now(g)
	if now.Before(time.Unix(0, g.WakeAt)) {
		return serviceerror.NewUnavailable("generation is not due")
	}
	if g.Token == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("generation token exhausted")
	}
	definition := g.Definition.Get(ctx)
	spec := &schedulepb.ScheduleSpec{}
	if err := proto.Unmarshal(definition.Spec, spec); err != nil {
		return err
	}
	e, err := occurrence.New(spec, definition.Seed, g.Continuation.Revision)
	if err != nil {
		return err
	}
	result, err := e.Run(decodeContinuation(g.Continuation), now, occurrence.Limits{Probes: 8, Outputs: 4})
	if err != nil {
		return err
	}
	s := g.Scheduler.Get(ctx)
	for _, candidate := range result.Occurrences {
		if err := s.admitGenerated(ctx, definition.Request, candidate, time.Duration(g.CatchupWindow)); err != nil {
			return err
		}
	}
	s.Cursor = max(s.Cursor, result.State.DisposedThrough.UnixNano())
	g.Continuation = encodeContinuation(result.State)
	g.LastStatus = scheduler2pb.EnumerationStatus(result.Status + 1)
	g.Armed = false
	switch result.Status {
	case occurrence.Waiting:
		g.arm(ctx, result.State.Pending.Actual)
	case occurrence.Yield, occurrence.Limited:
		wake := now.Add(time.Millisecond)
		end := time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond)
		if wake.After(end) {
			wake = end
		}
		g.arm(ctx, wake)
	case occurrence.Empty, occurrence.Impossible, occurrence.Exhausted:
		g.Token++
	default:
		return serviceerror.NewInternal("unexpected enumeration status")
	}
	return nil
}

func (s *Scheduler) admitGenerated(ctx chasm.MutableContext, template []byte, candidate occurrence.Candidate, catchup time.Duration) error {
	request := &workflowservice.StartWorkflowExecutionRequest{}
	if err := proto.Unmarshal(template, request); err != nil {
		return err
	}
	request.WorkflowId += "-" + candidate.Nominal.UTC().Format(time.RFC3339)
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return err
	}
	return s.Apply(ctx, decision.Event{Kind: decision.Due, Incarnation: s.Incarnation, Nominal: candidate.Nominal.UnixNano(), Actual: candidate.Actual.UnixNano(), WorkflowID: request.WorkflowId, CatchupDeadline: candidate.Actual.Add(catchup).UnixNano(), Request: encoded})
}

func encodeCandidate(c *occurrence.Candidate) *scheduler2pb.OccurrenceCandidate {
	if c == nil {
		return nil
	}
	return &scheduler2pb.OccurrenceCandidate{Nominal: c.Nominal.UnixNano(), Actual: c.Actual.UnixNano(), Revision: c.Revision, Excluded: c.Excluded}
}
func encodeContinuation(s occurrence.State) *scheduler2pb.OccurrenceContinuation {
	return &scheduler2pb.OccurrenceContinuation{Origin: s.Origin.UnixNano(), DefinitionId: s.DefinitionID, Revision: s.Revision, SearchAfter: s.SearchAfter.UnixNano(), DisposedThrough: s.DisposedThrough.UnixNano(), Pending: encodeCandidate(s.Pending), FullRange: s.FullRange, EligibleSeen: s.EligibleSeen, Terminal: scheduler2pb.EnumerationStatus(s.Terminal + 1)}
}
func decodeContinuation(s *scheduler2pb.OccurrenceContinuation) occurrence.State {
	state := occurrence.State{Origin: time.Unix(0, s.Origin).UTC(), DefinitionID: s.DefinitionId, Revision: s.Revision, SearchAfter: time.Unix(0, s.SearchAfter).UTC(), DisposedThrough: time.Unix(0, s.DisposedThrough).UTC(), FullRange: s.FullRange, EligibleSeen: s.EligibleSeen, Terminal: occurrence.Status(s.Terminal) - 1}
	if c := s.Pending; c != nil {
		state.Pending = &occurrence.Candidate{Nominal: time.Unix(0, c.Nominal).UTC(), Actual: time.Unix(0, c.Actual).UTC(), Revision: c.Revision, Excluded: c.Excluded}
	}
	return state
}
