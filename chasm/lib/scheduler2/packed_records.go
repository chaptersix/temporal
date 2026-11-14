package scheduler2

import (
	"encoding/binary"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	packedOperationOwner byte = 1
	packedActionOwner    byte = 2
)

type packedIdentity struct {
	locator uint64
	owners  byte
}

func (v *packedView) identity(id string) (packedIdentity, bool, error) {
	wire, ok, err := v.batch.Get([16]byte{packedIdentityKind}, []byte(id))
	if err != nil || !ok {
		return packedIdentity{}, ok, err
	}
	if len(wire) != 9 {
		return packedIdentity{}, false, serviceerror.NewInternal("invalid packed identity")
	}
	value := packedIdentity{locator: binary.BigEndian.Uint64(wire), owners: wire[8]}
	if value.locator == 0 || value.locator >= v.control.NextLocator || value.owners == 0 || value.owners&^byte(3) != 0 {
		return packedIdentity{}, false, serviceerror.NewInternal("invalid packed identity authority")
	}
	return value, true, nil
}
func packedWrap(wire []byte) []byte { return protowire.AppendBytes([]byte{10}, wire) }
func packedUnwrap(wire []byte) ([]byte, error) {
	if len(wire) == 0 || wire[0] != 10 {
		return nil, serviceerror.NewInternal("invalid packed record envelope")
	}
	value, n := protowire.ConsumeBytes(wire[1:])
	if n < 0 || n != len(wire)-1 {
		return nil, serviceerror.NewInternal("invalid packed record bytes")
	}
	return value, nil
}
func packedMarshal(value proto.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(value)
}
func (v *packedView) operationAt(locator uint64) (*pb.OperationState, error) {
	wire, ok, err := v.get(packedOperationKind, packedLocatorKey(locator))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, serviceerror.NewInternal("packed operation owner is missing")
	}
	inner, err := packedUnwrap(wire)
	if err != nil {
		return nil, err
	}
	chasm.ObservePackedRecordDecode(v.ctx, len(inner))
	return expandPackedOperation(v.scheduler.Incarnation, inner)
}
func (v *packedView) operation(id string) (*pb.OperationState, bool, error) {
	identity, ok, err := v.identity(id)
	if err != nil || !ok {
		return nil, ok, err
	}
	if identity.owners&packedOperationOwner == 0 {
		return nil, false, nil
	}
	op, err := v.operationAt(identity.locator)
	if err != nil {
		return nil, false, err
	}
	if op.Id != id {
		return nil, false, serviceerror.NewInternal("packed operation identity mismatch")
	}
	return op, true, nil
}
func (v *packedView) requestEnvelope(id string) (*pb.Payload, uint64, bool, error) {
	identity, ok, err := v.identity(id)
	if err != nil || !ok {
		return nil, 0, ok, err
	}
	if identity.owners&packedOperationOwner == 0 {
		return nil, 0, false, nil
	}
	wire, present, err := v.get(packedEnvelopeKind, packedLocatorKey(identity.locator))
	if err != nil || !present {
		return nil, identity.locator, present, err
	}
	chasm.ObservePackedRecordDecode(v.ctx, len(wire))
	envelope := &pb.Payload{}
	if err := proto.Unmarshal(wire, envelope); err != nil {
		return nil, 0, false, err
	}
	if len(envelope.Data) != 0 {
		return nil, 0, false, serviceerror.NewInternal("packed request envelope contains issuance bytes")
	}
	return envelope, identity.locator, true, nil
}

// request is used only when materializing the one admitted issuance snapshot.
func (v *packedView) request(id string) (*pb.Payload, bool, error) {
	envelope, locator, ok, err := v.requestEnvelope(id)
	if err != nil || !ok {
		return nil, ok, err
	}
	stored, present := v.scheduler.PackedRequests.Get(v.ctx, packedRequestAddress(locator))
	if !present {
		return nil, false, serviceerror.NewInternal("packed immutable request is missing")
	}
	if stored.ActionRef != "" || stored.ResultRef != "" || stored.FailureRef != "" {
		return nil, false, serviceerror.NewInternal("mixed packed request envelope")
	}
	template, present, err := v.template(packedRequestKind)
	if err != nil {
		return nil, false, err
	}
	if !present {
		return nil, false, serviceerror.NewInternal("packed request template is missing")
	}
	chasm.ObservePackedRecordDecode(v.ctx, len(template)+len(stored.Data))
	envelope.Data, err = expandPackedRequest(template, stored.Data)
	return envelope, err == nil, err
}
func (v *packedView) action(id string) (*pb.PublicActionRecord, bool, error) {
	identity, ok, err := v.identity(id)
	if err != nil || !ok {
		return nil, ok, err
	}
	if identity.owners&packedActionOwner == 0 {
		return nil, false, nil
	}
	wire, ok, err := v.get(packedActionKind, packedLocatorKey(identity.locator))
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, serviceerror.NewInternal("packed action owner is missing")
	}
	chasm.ObservePackedRecordDecode(v.ctx, len(wire))
	record := &pb.PublicActionRecord{}
	if err := proto.Unmarshal(wire, record); err != nil {
		return nil, false, err
	}
	if record.Action == nil {
		return nil, false, serviceerror.NewInternal("packed action is empty")
	}
	if record.OperationOwned {
		if identity.owners&packedOperationOwner == 0 || record.Action.OperationId != "" {
			return nil, false, serviceerror.NewInternal("packed action operation ownership mismatch")
		}
		op, err := v.operationAt(identity.locator)
		if err != nil {
			return nil, false, err
		}
		record.Action.OperationId = op.Id
	}
	if record.Action.OperationId != id {
		return nil, false, serviceerror.NewInternal("packed action identity mismatch")
	}
	return record, true, nil
}
func (v *packedView) operationIdentityAt(locator uint64) (string, error) {
	op, err := v.operationAt(locator)
	if err != nil {
		return "", err
	}
	identity, ok, err := v.identity(op.Id)
	if err != nil {
		return "", err
	}
	if !ok || identity.locator != locator || identity.owners&packedOperationOwner == 0 {
		return "", serviceerror.NewInternal("packed locator identity mismatch")
	}
	return op.Id, nil
}
func (v *packedView) ready(sequence uint64) (*pb.ReadyRecord, bool, error) {
	wire, ok, err := v.get(packedReadyKind, packedLocatorKey(sequence))
	if err != nil || !ok {
		return nil, ok, err
	}
	inner, err := packedUnwrap(wire)
	if err != nil {
		return nil, false, err
	}
	if len(inner) < 8 {
		return nil, false, serviceerror.NewInternal("packed ready locator is truncated")
	}
	chasm.ObservePackedRecordDecode(v.ctx, len(inner))
	record := &pb.ReadyRecord{}
	if err := proto.Unmarshal(inner[8:], record); err != nil {
		return nil, false, err
	}
	if record.OperationId != "" {
		return nil, false, serviceerror.NewInternal("mixed packed ready identity")
	}
	record.OperationId, err = v.operationIdentityAt(binary.BigEndian.Uint64(inner))
	return record, err == nil, err
}
func (v *packedView) running(sequence uint64) (*pb.RunningPublicAction, bool, error) {
	wire, ok, err := v.get(packedRunningKind, packedLocatorKey(sequence))
	if err != nil || !ok {
		return nil, ok, err
	}
	inner, err := packedUnwrap(wire)
	if err != nil {
		return nil, false, err
	}
	if len(inner) < 8 {
		return nil, false, serviceerror.NewInternal("packed running locator is truncated")
	}
	chasm.ObservePackedRecordDecode(v.ctx, len(inner))
	record := &pb.RunningPublicAction{}
	if err := proto.Unmarshal(inner[8:], record); err != nil {
		return nil, false, err
	}
	if record.OperationId != "" || record.Sequence != sequence {
		return nil, false, serviceerror.NewInternal("mixed packed running identity")
	}
	record.OperationId, err = v.operationIdentityAt(binary.BigEndian.Uint64(inner))
	return record, err == nil, err
}
func (v *packedView) queued(sequence uint64) (*pb.QueuedOccurrence, bool, error) {
	wire, ok, err := v.get(packedQueueKind, packedLocatorKey(sequence))
	if err != nil || !ok {
		return nil, ok, err
	}
	chasm.ObservePackedRecordDecode(v.ctx, len(wire))
	record := &pb.QueuedOccurrence{}
	err = proto.Unmarshal(wire, record)
	return record, err == nil, err
}
