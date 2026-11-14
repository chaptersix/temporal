package scheduler2

import (
	"bytes"
	"encoding/binary"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

func (s *Scheduler) fixedEffectStorage() bool {
	return s.OperationStore != nil && s.OperationStore.StartDispatch != nil
}
func (s *Scheduler) effectSlot(kind decision.EffectKind) *chasm.Field[*pb.Payload] {
	switch kind {
	case decision.Cancel:
		return &s.CancelRequest
	case decision.Terminate:
		return &s.TerminateRequest
	default:
		return nil
	}
}
func (s *Scheduler) effectRequestCount() int {
	if !s.fixedEffectStorage() {
		return len(s.EffectRequests)
	}
	count := 0
	if s.CancelRequest.IsSet() {
		count++
	}
	if s.TerminateRequest.IsSet() {
		count++
	}
	return count
}
func (s *Scheduler) effectRequestPresent(id string, kind decision.EffectKind) bool {
	if !s.fixedEffectStorage() {
		_, present := s.EffectRequests[id]
		return present
	}
	slot := s.effectSlot(kind)
	if slot == nil || !slot.IsSet() {
		return false
	}
	// Slot ownership is established by the old bounded metadata; no RPC bytes
	// are decoded merely to retain or replace a request during another transition.
	for _, effect := range s.Effects {
		if effect != nil && effect.Id == id && decision.EffectKind(effect.Kind) == kind {
			return true
		}
	}
	return false
}
func encodeEffectRequest(owner string, wire []byte) *pb.Payload {
	envelope := []byte{1}
	envelope = binary.AppendUvarint(envelope, uint64(len(owner)))
	envelope = append(envelope, owner...)
	envelope = append(envelope, wire...)
	return &pb.Payload{Data: envelope}
}
func decodeEffectRequest(record *pb.Payload, owner string) ([]byte, error) {
	if record == nil || len(record.Data) < 2 || record.Data[0] != 1 || record.ActionRef != "" || record.ResultRef != "" || record.FailureRef != "" {
		return nil, serviceerror.NewInternal("invalid effect request envelope")
	}
	size, n := binary.Uvarint(record.Data[1:])
	if n <= 0 || size == 0 || size > uint64(len(record.Data)-1-n) {
		return nil, serviceerror.NewInternal("invalid effect request owner length")
	}
	canonical := binary.AppendUvarint(nil, size)
	if !bytes.Equal(canonical, record.Data[1:1+n]) {
		return nil, serviceerror.NewInternal("noncanonical effect request owner length")
	}
	end := 1 + n + int(size)
	if string(record.Data[1+n:end]) != owner {
		return nil, serviceerror.NewInternal("effect request owner mismatch")
	}
	return bytes.Clone(record.Data[end:]), nil
}
func (s *Scheduler) effectRequestBytes(ctx chasm.Context, effect decision.Effect) ([]byte, error) {
	if !s.effectRequestPresent(effect.ID, effect.Kind) {
		return nil, serviceerror.NewInternal("overlap request is missing")
	}
	if !s.fixedEffectStorage() {
		return bytes.Clone(s.EffectRequests[effect.ID].Get(ctx).Data), nil
	}
	record, present := s.effectSlot(effect.Kind).TryGet(ctx)
	if !present {
		return nil, serviceerror.NewInternal("overlap request is missing")
	}
	return decodeEffectRequest(record, effect.ID)
}
func (s *Scheduler) storeEffectRequest(ctx chasm.MutableContext, effect decision.Effect, wire []byte) {
	if !s.fixedEffectStorage() {
		s.EffectRequests[effect.ID] = chasm.NewDataField(ctx, &pb.Payload{Data: bytes.Clone(wire)})
		return
	}
	*s.effectSlot(effect.Kind) = chasm.NewDataField(ctx, encodeEffectRequest(effect.ID, wire))
}
func (s *Scheduler) deleteEffectRequest(effect decision.Effect) {
	if !s.fixedEffectStorage() {
		delete(s.EffectRequests, effect.ID)
		return
	}
	*s.effectSlot(effect.Kind) = chasm.NewEmptyField[*pb.Payload]()
}
