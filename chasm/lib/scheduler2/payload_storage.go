package scheduler2

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"maps"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

type payloadPreparation map[string][]byte

func blobIdentity(kind string, data []byte) string {
	return fmt.Sprintf("%s-%x", kind, sha256.Sum256(data))
}

func payloadWire(message proto.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

func (s *Scheduler) prepareBlob(ctx chasm.Context, kind string, data []byte, prepared payloadPreparation) (string, error) {
	if err := s.validatePayloadStore(); err != nil {
		return "", err
	}
	id := blobIdentity(kind, data)
	if record, ok := s.blobRecord(ctx, id); ok {
		if !bytes.Equal(record.Data, data) {
			return "", serviceerror.NewInternal("payload digest collision")
		}
	} else {
		prepared[id] = bytes.Clone(data)
	}
	return id, nil
}
func (s *Scheduler) publishBlobs(ctx chasm.MutableContext, prepared payloadPreparation) {
	if s.PayloadBlobs == nil {
		s.PayloadBlobs = make(chasm.Map[string, *scheduler2pb.Payload])
	}
	for id, data := range prepared {
		s.PayloadBlobs[id] = chasm.NewDataField(ctx, &scheduler2pb.Payload{Data: data})
	}
}
func (s *Scheduler) payloadBlob(ctx chasm.Context, id string, kind string) ([]byte, error) {
	if err := s.validatePayloadStore(); err != nil {
		return nil, err
	}
	record, ok := s.blobRecord(ctx, id)
	if !ok {
		return nil, serviceerror.NewInternal("referenced schedule payload is missing")
	}
	data := record.Data
	if id != fmt.Sprintf("%s-%x", kind, sha256.Sum256(data)) {
		return nil, serviceerror.NewInternal("referenced schedule payload identity is invalid")
	}
	return data, nil
}
func (s *Scheduler) actionBundle(ctx chasm.Context, id string) (*scheduler2pb.ActionPayloadBundle, error) {
	data, err := s.payloadBlob(ctx, id, "action")
	if err != nil {
		return nil, err
	}
	bundle := &scheduler2pb.ActionPayloadBundle{}
	if err := proto.Unmarshal(data, bundle); err != nil {
		return nil, err
	}
	return bundle, nil
}

type publicDefinitionPlan struct {
	schedule  *schedulepb.Schedule
	actionRef string
	blobs     payloadPreparation
	owner     blobOwnerChange
}

func (s *Scheduler) preparePublicDefinition(ctx chasm.Context, definition *schedulepb.Schedule) (publicDefinitionPlan, error) {
	schedule := common.CloneProto(definition)
	action := schedule.GetAction().GetStartWorkflow()
	if action == nil {
		return publicDefinitionPlan{}, serviceerror.NewInvalidArgument("schedule action is required")
	}
	bundle := &scheduler2pb.ActionPayloadBundle{Input: action.Input, Header: action.Header, Memo: action.Memo, UserSearchAttributes: action.SearchAttributes, UserMetadata: action.UserMetadata}
	wire, err := payloadWire(bundle)
	if err != nil {
		return publicDefinitionPlan{}, err
	}
	prepared := payloadPreparation{}
	id, err := s.prepareBlob(ctx, "action", wire, prepared)
	if err != nil {
		return publicDefinitionPlan{}, err
	}
	action.Input, action.Header, action.Memo, action.SearchAttributes, action.UserMetadata = nil, nil, nil, nil, nil
	return publicDefinitionPlan{schedule: schedule, actionRef: id, blobs: prepared, owner: blobOwnerChange{owner: "public-definition", before: [3]string{s.PublicActionRef}, after: [3]string{id}}}, nil
}
func (s *Scheduler) publishPublicDefinition(ctx chasm.MutableContext, plan publicDefinitionPlan) {
	s.PublicActionRef = plan.actionRef
	s.PublicSchedule = chasm.NewDataField(ctx, plan.schedule)
	if s.OperationStore != nil {
		action := plan.schedule.GetAction().GetStartWorkflow()
		s.PublicMetadata = chasm.NewDataField(ctx, &scheduler2pb.PublicActionMetadata{
			WorkflowType: action.GetWorkflowType().GetName(), TaskQueue: action.GetTaskQueue().GetName(),
		})
	}
}

func (s *Scheduler) storePublicDefinition(ctx chasm.MutableContext, definition *schedulepb.Schedule) error {
	plan, err := s.preparePublicDefinition(ctx, definition)
	if err != nil {
		return err
	}
	blobs, err := s.prepareBlobDelta(ctx, []blobOwnerChange{plan.owner}, plan.blobs)
	if err != nil {
		return err
	}
	s.publishBlobDelta(ctx, blobs)
	s.publishPublicDefinition(ctx, plan)
	return nil
}

func (s *Scheduler) publicDefinition(ctx chasm.Context) (*schedulepb.Schedule, error) {
	schedule := common.CloneProto(s.PublicSchedule.Get(ctx))
	if s.PublicActionRef == "" {
		if s.PublicRevision != 0 {
			return nil, serviceerror.NewInternal("public action payload reference is missing")
		}
		return schedule, nil
	}
	bundle, err := s.actionBundle(ctx, s.PublicActionRef)
	if err != nil {
		return nil, err
	}
	action := schedule.GetAction().GetStartWorkflow()
	if action == nil {
		return nil, serviceerror.NewInternal("public action skeleton is missing")
	}
	action.Input, action.Header, action.Memo, action.SearchAttributes, action.UserMetadata = bundle.Input, bundle.Header, bundle.Memo, bundle.UserSearchAttributes, bundle.UserMetadata
	return schedule, nil
}

// splitRequest keeps immutable operation overrides in the request skeleton.
func (s *Scheduler) splitRequest(ctx chasm.Context, wire []byte) (*scheduler2pb.Payload, payloadPreparation, error) {
	record := &scheduler2pb.Payload{Data: bytes.Clone(wire)}
	prepared := payloadPreparation{}
	if s.PublicRevision == 0 {
		return record, prepared, nil
	}
	request := &workflowservice.StartWorkflowExecutionRequest{}
	if err := proto.Unmarshal(wire, request); err != nil {
		return nil, nil, err
	}
	bundle, generated := splitActionFields(request)
	data, err := payloadWire(bundle)
	if err != nil {
		return nil, nil, err
	}
	record.ActionRef, err = s.prepareBlob(ctx, "action", data, prepared)
	if err != nil {
		return nil, nil, err
	}
	if generated != nil {
		record.ActionRef, err = s.reuseActionBundle(ctx, bundle, record.ActionRef, prepared)
		if err != nil {
			return nil, nil, err
		}
	}
	request.Input, request.Header, request.Memo, request.UserMetadata = nil, nil, nil, nil
	request.SearchAttributes = generated
	if request.LastCompletionResult != nil {
		data, err = payloadWire(request.LastCompletionResult)
		if err != nil {
			return nil, nil, err
		}
		record.ResultRef, err = s.prepareBlob(ctx, "result", data, prepared)
		if err != nil {
			return nil, nil, err
		}
		request.LastCompletionResult = nil
	}
	if request.ContinuedFailure != nil {
		data, err = payloadWire(request.ContinuedFailure)
		if err != nil {
			return nil, nil, err
		}
		record.FailureRef, err = s.prepareBlob(ctx, "failure", data, prepared)
		if err != nil {
			return nil, nil, err
		}
		request.ContinuedFailure = nil
	}
	record.Data, err = payloadWire(request)
	return record, prepared, err
}

// splitActionFields separates generated search attributes from shared user fields.
func splitActionFields(request *workflowservice.StartWorkflowExecutionRequest) (*scheduler2pb.ActionPayloadBundle, *commonpb.SearchAttributes) {
	userSA, generated := splitGeneratedAttributes(request.SearchAttributes)
	return &scheduler2pb.ActionPayloadBundle{Input: request.Input, Header: request.Header, Memo: request.Memo, UserSearchAttributes: userSA, UserMetadata: request.UserMetadata}, generated
}

func splitGeneratedAttributes(attributes *commonpb.SearchAttributes) (userSA, generated *commonpb.SearchAttributes) {
	userSA = common.CloneProto(attributes)
	if userSA == nil {
		return nil, nil
	}
	for _, name := range []string{"TemporalScheduledById", "TemporalScheduledStartTime"} {
		if value, ok := userSA.IndexedFields[name]; ok {
			if generated == nil {
				generated = &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{}}
			}
			generated.IndexedFields[name] = value
			delete(userSA.IndexedFields, name)
		}
	}
	if len(userSA.IndexedFields) == 0 && generated != nil {
		userSA = nil
	}
	return userSA, generated
}

// Generated attributes override user fields, so an existing bundle can remain
// shared even when its map contains overridden keys or an allocated-empty map.
func (s *Scheduler) reuseActionBundle(ctx chasm.Context, bundle *scheduler2pb.ActionPayloadBundle, preparedID string, prepared payloadPreparation) (string, error) {
	candidates := []string{s.PublicActionRef}
	if s.HasGenerator {
		candidates = append(candidates, s.Generator.Get(ctx).Definition.Get(ctx).ActionRef)
	}
	for _, id := range candidates {
		if id == "" {
			continue
		}
		existing, err := s.actionBundle(ctx, id)
		if err != nil {
			return "", err
		}
		existing.UserSearchAttributes, _ = splitGeneratedAttributes(existing.UserSearchAttributes)
		if existing.UserSearchAttributes != nil && len(existing.UserSearchAttributes.IndexedFields) == 0 {
			existing.UserSearchAttributes = nil
		}
		if proto.Equal(existing, bundle) {
			delete(prepared, preparedID)
			return id, nil
		}
	}
	return preparedID, nil
}

func (s *Scheduler) assembleRequest(ctx chasm.Context, record *scheduler2pb.Payload) (*workflowservice.StartWorkflowExecutionRequest, error) {
	if s.PublicRevision != 0 && record.ActionRef == "" {
		return nil, serviceerror.NewInternal("action payload reference is missing")
	}
	request := &workflowservice.StartWorkflowExecutionRequest{}
	if err := proto.Unmarshal(record.Data, request); err != nil {
		return nil, err
	}
	if record.ActionRef != "" {
		bundle, err := s.actionBundle(ctx, record.ActionRef)
		if err != nil {
			return nil, err
		}
		request.Input, request.Header, request.Memo, request.UserMetadata = bundle.Input, bundle.Header, bundle.Memo, bundle.UserMetadata
		generated := request.SearchAttributes
		request.SearchAttributes = bundle.UserSearchAttributes
		if generated != nil {
			if request.SearchAttributes == nil {
				request.SearchAttributes = &commonpb.SearchAttributes{}
			}
			if request.SearchAttributes.IndexedFields == nil {
				request.SearchAttributes.IndexedFields = map[string]*commonpb.Payload{}
			}
			maps.Copy(request.SearchAttributes.IndexedFields, generated.IndexedFields)
		}
	}
	if record.ResultRef != "" {
		data, err := s.payloadBlob(ctx, record.ResultRef, "result")
		if err != nil {
			return nil, err
		}
		request.LastCompletionResult = &commonpb.Payloads{}
		if err := proto.Unmarshal(data, request.LastCompletionResult); err != nil {
			return nil, err
		}
	}
	if record.FailureRef != "" {
		data, err := s.payloadBlob(ctx, record.FailureRef, "failure")
		if err != nil {
			return nil, err
		}
		request.ContinuedFailure = &failurepb.Failure{}
		if err := proto.Unmarshal(data, request.ContinuedFailure); err != nil {
			return nil, err
		}
	}
	return request, nil
}
func (s *Scheduler) generationRequest(ctx chasm.Context) ([]byte, error) {
	definition := s.Generator.Get(ctx).Definition.Get(ctx)
	request, err := s.assembleRequest(ctx, &scheduler2pb.Payload{Data: definition.Request, ActionRef: definition.ActionRef})
	if err != nil {
		return nil, err
	}
	return payloadWire(request)
}

func (s *Scheduler) prepareCarryover(ctx chasm.Context, value *scheduler2pb.CarryoverState) (*scheduler2pb.CarryoverState, payloadPreparation, error) {
	stored := common.CloneProto(value)
	prepared := payloadPreparation{}
	if s.PublicRevision == 0 {
		return stored, prepared, nil
	}
	if len(value.Result) > 0 {
		payload := &commonpb.Payload{}
		if err := proto.Unmarshal(value.Result, payload); err != nil {
			return nil, nil, err
		}
		data, err := payloadWire(&commonpb.Payloads{Payloads: []*commonpb.Payload{payload}})
		if err != nil {
			return nil, nil, err
		}
		stored.ResultRef, err = s.prepareBlob(ctx, "result", data, prepared)
		if err != nil {
			return nil, nil, err
		}
		stored.Result = nil
	}
	if value.FailurePresent {
		var err error
		stored.FailureRef, err = s.prepareBlob(ctx, "failure", value.Failure, prepared)
		if err != nil {
			return nil, nil, err
		}
		stored.Failure = nil
	}
	return stored, prepared, nil
}
func (s *Scheduler) carryoverValue(ctx chasm.Context) (*scheduler2pb.CarryoverState, error) {
	value := common.CloneProto(s.Carryover.Get(ctx))
	if s.PublicRevision != 0 && value.FailurePresent && value.FailureRef == "" {
		return nil, serviceerror.NewInternal("failure payload reference is missing")
	}
	if value.ResultRef != "" {
		data, err := s.payloadBlob(ctx, value.ResultRef, "result")
		if err != nil {
			return nil, err
		}
		payloads := &commonpb.Payloads{}
		if err := proto.Unmarshal(data, payloads); err != nil {
			return nil, err
		}
		if len(payloads.Payloads) != 1 {
			return nil, serviceerror.NewInternal("invalid result payload reference")
		}
		value.Result, err = payloadWire(payloads.Payloads[0])
		if err != nil {
			return nil, err
		}
	}
	if value.FailureRef != "" {
		data, err := s.payloadBlob(ctx, value.FailureRef, "failure")
		if err != nil {
			return nil, err
		}
		value.Failure = bytes.Clone(data)
	}
	return value, nil
}

// collectPayloadBlobs retains the legacy owner scan. Keyed payload storage uses
// transactional owner deltas and never enters this collector.
func (s *Scheduler) collectPayloadBlobs(ctx chasm.Context) {
	if s.PublicRevision == 0 || s.PayloadStore != nil {
		return
	}
	live := map[string]bool{s.PublicActionRef: true}
	if s.HasGenerator {
		live[s.Generator.Get(ctx).Definition.Get(ctx).ActionRef] = true
	}
	for _, field := range s.Requests {
		record := field.Get(ctx)
		live[record.ActionRef] = true
		live[record.ResultRef] = true
		live[record.FailureRef] = true
	}
	if s.CarryoverSet {
		record := s.Carryover.Get(ctx)
		live[record.ResultRef] = true
		live[record.FailureRef] = true
	}
	for id := range s.PayloadBlobs {
		if !live[id] {
			delete(s.PayloadBlobs, id)
		}
	}
}
