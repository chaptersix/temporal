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
	Scheduler          chasm.ParentPtr[*Scheduler]
	Definition         chasm.Field[*scheduler2pb.GenerationDefinition]
	PreparedDefinition chasm.Field[*DefinitionPreparation]
	PreparedWork       chasm.Field[*scheduler2pb.Payload]
	Request            chasm.Field[*scheduler2pb.Payload]
}

func (*Generator) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// GenerationConfig supplies an immutable local definition and workflow template.
type GenerationConfig struct {
	prepared      *StagedDefinitionSource
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
	return s.configureGeneration(ctx, config, nil)
}

// configureGeneration prepares the full successor before publishing any fields.
// Both control entry points check terminal authority before calling it.
type generationPreparation struct {
	prepared   *preparedGenerationPlan
	generator  *Generator
	definition *scheduler2pb.GenerationDefinition
	state      occurrence.State
	superseded *occurrence.Candidate
	config     GenerationConfig
	preview    decision.State
	now        time.Time
	blobs      payloadPreparation
	owner      blobOwnerChange
}

func (s *Scheduler) configureGeneration(ctx chasm.MutableContext, config GenerationConfig, controls *UpdateConfig) error {
	plan, err := s.prepareGeneration(ctx, config, controls)
	if err != nil {
		return err
	}
	return s.commitGeneration(ctx, plan)
}
func (s *Scheduler) commitGeneration(ctx chasm.MutableContext, plan *generationPreparation) error {
	if (s.PublicList.IsSet() || s.ListControl.IsSet()) && s.Paused != plan.preview.Root.Paused {
		var err error
		ctx, err = s.reserveMemoVersion(ctx)
		if err != nil {
			return err
		}
	}
	blobs, err := s.prepareBlobDelta(ctx, []blobOwnerChange{plan.owner}, plan.blobs)
	if err != nil {
		return err
	}
	s.publishBlobDelta(ctx, blobs)
	s.publishGeneration(ctx, plan)
	if err := s.reconcileWork(ctx); err != nil {
		return err
	}
	s.collectPayloadBlobs(ctx)
	return nil
}

func (s *Scheduler) prepareGeneration(ctx chasm.Context, config GenerationConfig, controls *UpdateConfig) (*generationPreparation, error) {
	if err := config.validate(s.Revision); err != nil {
		return nil, err
	}
	g, e, prepared, err := s.prepareGenerationCursor(ctx, config)
	if err != nil {
		return nil, err
	}
	now := ctx.Now(s)
	var state occurrence.State
	var superseded *occurrence.Candidate
	if g != nil {
		state, superseded, err = e.Update(decodeContinuation(g.Continuation), now)
	} else {
		after := config.After
		if boundary := time.Unix(0, s.Cursor).UTC(); boundary.After(after) {
			after = boundary
		}
		state, err = e.Begin(after)
	}
	if err != nil {
		return nil, err
	}
	var spec []byte
	if legacy, ok := e.(*occurrence.Enumerator); ok {
		if _, err := legacy.Run(state, now, occurrence.Limits{Probes: 1, Outputs: 1}); err != nil {
			return nil, err
		}
		spec, err = proto.MarshalOptions{Deterministic: true}.Marshal(config.Spec)
		if err != nil {
			return nil, err
		}
	}
	request, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.Request)
	if err != nil {
		return nil, err
	}
	requestRecord, blobs, err := s.splitRequest(ctx, request)
	if err != nil {
		return nil, err
	}
	if requestRecord.ResultRef != "" || requestRecord.FailureRef != "" {
		return nil, serviceerror.NewInvalidArgument("generation template cannot contain completion carryover")
	}
	if g != nil && g.Token == math.MaxUint64 {
		return nil, serviceerror.NewFailedPrecondition("generation token exhausted")
	}
	// Configuration arms an immediate scan even for an empty source. Preflight
	// its coordinator wake before replacing any root, generator or DATA fields.
	if err := checkWorkWakeToken(s.Coordination, now); err != nil {
		return nil, err
	}
	atNano, err := activityNano(now)
	if err != nil {
		return nil, err
	}
	preview, err := s.stateFor(ctx, "")
	if err != nil {
		return nil, err
	}
	if controls != nil {
		preview.Root.Paused = controls.Paused
		preview.Root.Limited = controls.Limited
		preview.Root.Remaining = controls.Remaining
		preview.Root.PauseOnFailure = controls.PauseOnFailure
	}
	if !preview.Root.ActivitySet || atNano > preview.Root.ActivityAt {
		preview.Root.ActivityAt = atNano
	}
	preview.Root.ActivitySet = true
	if _, _, err := s.checkIdlePlan(preview, encodeContinuation(state), true, len(s.Backfills) > 0); err != nil {
		return nil, err
	}

	oldRef := s.generatorOwnerRef(ctx, g)
	return &generationPreparation{prepared: prepared, generator: g, definition: &scheduler2pb.GenerationDefinition{Spec: spec, Seed: config.Seed, Request: requestRecord.Data, ActionRef: requestRecord.ActionRef}, state: state, superseded: superseded, config: config, preview: preview, now: now, blobs: blobs, owner: blobOwnerChange{owner: "generator-definition", before: [3]string{oldRef}, after: [3]string{requestRecord.ActionRef}}}, nil
}

func (config GenerationConfig) validate(currentRevision int64) error {
	if !decision.ValidPolicy(config.Policy) || config.Request == nil || config.Request.WorkflowId == "" || config.CatchupWindow < 0 || config.Revision < currentRevision {
		return serviceerror.NewInvalidArgument("invalid local generation configuration")
	}
	return nil
}

func (s *Scheduler) publishGeneration(ctx chasm.MutableContext, plan *generationPreparation) {
	g, state, superseded := plan.generator, plan.state, plan.superseded
	config, preview, now := plan.config, plan.preview, plan.now
	if g == nil {
		g = &Generator{GeneratorState: &scheduler2pb.GeneratorState{}}
		s.Generator = chasm.NewComponentField(ctx, g)
	}
	if plan.prepared != nil {
		s.publishPreparedGeneration(ctx, g, plan.prepared)
	} else {
		g.Definition = chasm.NewDataField(ctx, plan.definition)
	}
	if s.OperationStore != nil {
		g.Request = chasm.NewDataField(ctx, &scheduler2pb.Payload{Data: plan.definition.Request, ActionRef: plan.definition.ActionRef})
	}
	g.Continuation = encodeContinuation(state)
	g.CatchupWindow = int64(config.CatchupWindow)
	g.Superseded = encodeCandidate(superseded)
	g.LastStatus = scheduler2pb.ENUMERATION_STATUS_ACTIVE
	s.ActivitySet, s.ActivityAt = preview.Root.ActivitySet, preview.Root.ActivityAt
	if (s.PublicList.IsSet() || s.ListControl.IsSet()) && s.Paused != preview.Root.Paused {
		s.publishMemoVersion(ctx)
	}
	s.Paused = preview.Root.Paused
	s.Limited = preview.Root.Limited
	s.Remaining = preview.Root.Remaining
	s.PauseOnFailure = preview.Root.PauseOnFailure
	s.HasGenerator = true
	s.Revision = config.Revision
	s.Policy = config.Policy
	s.Cursor = max(s.Cursor, state.DisposedThrough.UnixNano())
	g.arm(ctx, now)

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
	return s.applyTransition(ctx, decision.Event{Kind: decision.Due, Incarnation: s.Incarnation, Nominal: candidate.Nominal.UnixNano(), Actual: candidate.Actual.UnixNano(), WorkflowID: request.WorkflowId, CatchupDeadline: catchupDeadline(candidate.Actual.UnixNano(), catchup), Request: encoded})
}

func encodeCandidate(c *occurrence.Candidate) *scheduler2pb.OccurrenceCandidate {
	if c == nil {
		return nil
	}
	nominal, nominalTime := encodeTime(c.Nominal)
	actual, actualTime := encodeTime(c.Actual)
	return &scheduler2pb.OccurrenceCandidate{Nominal: nominal, NominalTime: nominalTime, Actual: actual, ActualTime: actualTime, Revision: c.Revision, Excluded: c.Excluded}
}
func encodeContinuation(s occurrence.State) *scheduler2pb.OccurrenceContinuation {
	origin, originTime := encodeTime(s.Origin)
	search, searchTime := encodeTime(s.SearchAfter)
	disposed, disposedTime := encodeTime(s.DisposedThrough)
	return &scheduler2pb.OccurrenceContinuation{Origin: origin, OriginTime: originTime, DefinitionId: s.DefinitionID, Revision: s.Revision, SearchAfter: search, SearchAfterTime: searchTime, DisposedThrough: disposed, DisposedThroughTime: disposedTime, Pending: encodeCandidate(s.Pending), FullRange: s.FullRange, EligibleSeen: s.EligibleSeen, Terminal: scheduler2pb.EnumerationStatus(s.Terminal + 1)}
}
func decodeContinuation(s *scheduler2pb.OccurrenceContinuation) occurrence.State {
	state := occurrence.State{Origin: decodeTime(s.Origin, s.OriginTime), DefinitionID: s.DefinitionId, Revision: s.Revision, SearchAfter: decodeTime(s.SearchAfter, s.SearchAfterTime), DisposedThrough: decodeTime(s.DisposedThrough, s.DisposedThroughTime), FullRange: s.FullRange, EligibleSeen: s.EligibleSeen, Terminal: occurrence.Status(s.Terminal) - 1}
	if c := s.Pending; c != nil {
		state.Pending = &occurrence.Candidate{Nominal: decodeTime(c.Nominal, c.NominalTime), Actual: decodeTime(c.Actual, c.ActualTime), Revision: c.Revision, Excluded: c.Excluded}
	}
	return state
}

// catchupDeadline saturates only the stored timestamp representation. Every
// representable execution time remains eligible when the mathematical deadline
// lies beyond it.
func catchupDeadline(actual int64, window time.Duration) int64 {
	if actual > math.MaxInt64-int64(window) {
		return math.MaxInt64
	}
	return actual + int64(window)
}

// newEnumerator preserves the internal verification profile while public
// definitions retain the frontend's accepted size limits across reconstruction.
func (s *Scheduler) newEnumerator(spec *schedulepb.ScheduleSpec, seed string, revision int64) (*occurrence.Enumerator, error) {
	if s.PublicRevision != 0 {
		return occurrence.NewPublic(spec, seed, revision)
	}
	return occurrence.New(spec, seed, revision)
}

func (s *Scheduler) generatorOwnerRef(ctx chasm.Context, g *Generator) string {
	if s.PayloadStore != nil && g != nil {
		if s.OperationStore != nil {
			return g.Request.Get(ctx).ActionRef
		}
		return g.Definition.Get(ctx).ActionRef
	}
	return ""
}
