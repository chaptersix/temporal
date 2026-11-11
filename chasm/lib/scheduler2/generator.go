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
	if s.Closed {
		return serviceerror.NewNotFound("schedule is closed")
	}
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
	// Configuration arms an immediate scan even for an empty source. Preflight
	// its coordinator wake before replacing any root, generator or DATA fields.
	if err := checkWorkWakeToken(s.Coordination, now); err != nil {
		return err
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
	return s.reconcileWork(ctx)
}

func (g *Generator) arm(ctx chasm.MutableContext, at time.Time) {
	g.Token++
	g.Armed = true
	g.WakeAt = at.UnixNano()

}

// GenerateHandler runs bounded local enumeration under the execution write lock.
type GenerateHandler struct{ chasm.PureTaskHandlerBase }

// Validate rejects legacy generation tasks after coordinator adoption.
func (*GenerateHandler) Validate(chasm.Context, *Generator, chasm.TaskInvocation, *scheduler2pb.GenerateTask) (bool, error) {
	return false, nil
}
func (*GenerateHandler) Execute(chasm.MutableContext, *Generator, chasm.TaskAttributes, *scheduler2pb.GenerateTask) error {
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
	return s.applyTransition(ctx, decision.Event{Kind: decision.Due, Incarnation: s.Incarnation, Nominal: candidate.Nominal.UnixNano(), Actual: candidate.Actual.UnixNano(), WorkflowID: request.WorkflowId, CatchupDeadline: candidate.Actual.Add(catchup).UnixNano(), Request: encoded})
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
