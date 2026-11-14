package scheduler2

import (
	"bytes"
	"math"
	"slices"
	"strings"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/packedstore"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
	"go.temporal.io/server/common"
)

type packedEditor struct {
	*packedView
	overlay  *packedOverlay
	requests map[string]*pb.Payload
	touched  map[byte]bool
	err      error
	closed   bool
}

func (s *Scheduler) newPackedEditor(ctx chasm.Context) (*packedEditor, error) {
	v, err := s.newPackedView(ctx)
	if err != nil {
		return nil, err
	}
	v.close()
	overlay := &packedOverlay{reader: v.reader, changes: map[string]queueindex.IndexChange{}}
	v.reader = overlay
	v.batch = packedstore.NewBatch(overlay)
	v.control = common.CloneProto(s.PackedStorage)
	return &packedEditor{packedView: v, overlay: overlay, requests: map[string]*pb.Payload{}, touched: map[byte]bool{}}, nil
}
func (e *packedEditor) fail(err error) {
	if err != nil && e.err == nil {
		e.err = err
	}
}
func (e *packedEditor) available() error {
	if e.err != nil {
		return e.err
	}
	if e.closed {
		return serviceerror.NewInternal("packed editor closed")
	}
	return nil
}
func (e *packedEditor) abort() { e.closed = true; e.batch.Abort() }
func (e *packedEditor) baseline(kind byte, wire []byte) ([]byte, error) {
	template, present, err := e.template(kind)
	if err != nil {
		return nil, err
	}
	if !present {
		ownerKind := kind
		if kind == packedRequestKind {
			ownerKind = packedEnvelopeKind
		}
		count, err := e.batch.Count(packedstore.ID{ownerKind})
		if err != nil {
			return nil, err
		}
		if count != 0 {
			return nil, serviceerror.NewInternal("live packed records lost template")
		}
		template = bytes.Clone(wire)
		e.overlay.changes[packedTemplateKey(kind)] = queueindex.IndexChange{Key: packedTemplateKey(kind), Value: template}
	}
	e.touched[kind] = true
	return template, nil
}
func (e *packedEditor) put(kind byte, key, wire []byte) error {
	template, err := e.baseline(kind, wire)
	if err != nil {
		return err
	}
	chasm.ObservePackedRecordDecode(e.ctx, len(template)+len(wire))
	delta, err := packedRequest(template, wire)
	if err != nil {
		return err
	}
	chasm.ObservePackedRecordEncode(e.ctx, len(wire)+len(delta))
	return e.batch.Put(packedstore.ID{kind}, key, delta)
}
func (e *packedEditor) remove(kind byte, key []byte) error {
	e.touched[kind] = true
	return e.batch.Delete(packedstore.ID{kind}, key)
}
func (e *packedEditor) owner(id string, bit byte, present bool) (packedIdentity, error) {
	if id == "" {
		return packedIdentity{}, serviceerror.NewInternal("empty packed identity")
	}
	identity, exists, err := e.identity(id)
	if err != nil {
		return packedIdentity{}, err
	}
	if !exists {
		if !present {
			return packedIdentity{}, nil
		}
		if e.control.NextLocator == math.MaxUint64 {
			return packedIdentity{}, serviceerror.NewFailedPrecondition("packed locator exhausted")
		}
		identity.locator = e.control.NextLocator
		e.control.NextLocator++
	}
	if present {
		identity.owners |= bit
	} else {
		identity.owners &^= bit
	}
	if identity.owners == 0 {
		return identity, e.batch.Delete(packedstore.ID{packedIdentityKind}, []byte(id))
	}
	wire := append(packedLocatorKey(identity.locator), identity.owners)
	return identity, e.batch.Put(packedstore.ID{packedIdentityKind}, []byte(id), wire)
}
func (e *packedEditor) setOperation(id string, op *pb.OperationState) (err error) {
	defer func() { e.fail(err) }()
	if err = e.available(); err != nil {
		return err
	}
	if op != nil && op.Id != id {
		return serviceerror.NewInternal("packed operation identity mismatch")
	}
	identity, err := e.owner(id, packedOperationOwner, op != nil)
	if err != nil {
		return err
	}
	if identity.locator == 0 {
		return nil
	}
	if op == nil {
		return e.remove(packedOperationKind, packedLocatorKey(identity.locator))
	}
	wire, err := packedOperation(e.scheduler.Incarnation, op)
	if err != nil {
		return err
	}
	return e.put(packedOperationKind, packedLocatorKey(identity.locator), packedWrap(wire))
}
func (e *packedEditor) setRequest(id string, record *pb.Payload) (err error) {
	defer func() { e.fail(err) }()
	if err = e.available(); err != nil {
		return err
	}
	identity, present, err := e.identity(id)
	if err != nil {
		return err
	}
	if !present {
		if record == nil {
			return nil
		}
		return serviceerror.NewInternal("packed request requires operation owner")
	}
	if record != nil && identity.owners&packedOperationOwner == 0 {
		return serviceerror.NewInternal("packed request requires operation owner")
	}
	locatorKey := packedLocatorKey(identity.locator)
	requestKey := packedRequestAddress(identity.locator)
	_, had, err := e.batch.Get(packedstore.ID{packedEnvelopeKind}, locatorKey)
	if err != nil {
		return err
	}
	if record == nil {
		e.requests[requestKey] = nil
		e.touched[packedRequestKind] = true
		return e.remove(packedEnvelopeKind, locatorKey)
	}
	if had {
		return serviceerror.NewInternal("immutable packed request cannot be replaced")
	}
	// Check only a new locator. Deletion never reads immutable request bytes.
	if _, exists := e.scheduler.PackedRequests.Get(e.ctx, requestKey); exists {
		return serviceerror.NewInternal("packed request locator collision")
	}
	template, err := e.baseline(packedRequestKind, record.Data)
	if err != nil {
		return err
	}
	chasm.ObservePackedRecordDecode(e.ctx, len(template)+len(record.Data))
	delta, err := packedRequest(template, record.Data)
	if err != nil {
		return err
	}
	envelope := common.CloneProto(record)
	envelope.Data = nil
	wire, err := packedMarshal(envelope)
	if err != nil {
		return err
	}
	if err = e.put(packedEnvelopeKind, locatorKey, wire); err != nil {
		return err
	}
	chasm.ObservePackedRecordEncode(e.ctx, len(delta))
	e.requests[requestKey] = &pb.Payload{Data: delta}
	return nil
}
func (e *packedEditor) setAction(id string, record *pb.PublicActionRecord) (err error) {
	defer func() { e.fail(err) }()
	if err = e.available(); err != nil {
		return err
	}
	if record != nil && (record.Action == nil || record.Action.OperationId != id) {
		return serviceerror.NewInternal("packed action identity mismatch")
	}
	identity, err := e.owner(id, packedActionOwner, record != nil)
	if err != nil {
		return err
	}
	if identity.locator == 0 {
		return nil
	}
	if record == nil {
		return e.remove(packedActionKind, packedLocatorKey(identity.locator))
	}
	copyRecord := common.CloneProto(record)
	if copyRecord.OperationOwned {
		if identity.owners&packedOperationOwner == 0 {
			return serviceerror.NewInternal("packed action requires operation owner")
		}
		copyRecord.Action.OperationId = ""
	}
	wire, err := packedMarshal(copyRecord)
	if err != nil {
		return err
	}
	return e.put(packedActionKind, packedLocatorKey(identity.locator), wire)
}
func (e *packedEditor) setReady(sequence uint64, record *pb.ReadyRecord) (err error) {
	defer func() { e.fail(err) }()
	if err = e.available(); err != nil {
		return err
	}
	key := packedLocatorKey(sequence)
	if record == nil {
		return e.remove(packedReadyKind, key)
	}
	identity, present, err := e.identity(record.OperationId)
	if err != nil {
		return err
	}
	if !present || identity.owners&packedOperationOwner == 0 {
		return serviceerror.NewInternal("packed ready record requires operation")
	}
	copyRecord := common.CloneProto(record)
	copyRecord.OperationId = ""
	wire, err := packedMarshal(copyRecord)
	if err != nil {
		return err
	}
	return e.put(packedReadyKind, key, packedWrap(append(packedLocatorKey(identity.locator), wire...)))
}
func (e *packedEditor) setRunning(sequence uint64, record *pb.RunningPublicAction) (err error) {
	defer func() { e.fail(err) }()
	if err = e.available(); err != nil {
		return err
	}
	key := packedLocatorKey(sequence)
	if record == nil {
		return e.remove(packedRunningKind, key)
	}
	if record.Sequence != sequence {
		return serviceerror.NewInternal("packed running sequence mismatch")
	}
	identity, present, err := e.identity(record.OperationId)
	if err != nil {
		return err
	}
	if !present || identity.owners&packedOperationOwner == 0 {
		return serviceerror.NewInternal("packed running record requires operation")
	}
	copyRecord := common.CloneProto(record)
	copyRecord.OperationId = ""
	wire, err := packedMarshal(copyRecord)
	if err != nil {
		return err
	}
	return e.put(packedRunningKind, key, packedWrap(append(packedLocatorKey(identity.locator), wire...)))
}
func (e *packedEditor) setQueued(sequence uint64, record *pb.QueuedOccurrence) (err error) {
	defer func() { e.fail(err) }()
	if err = e.available(); err != nil {
		return err
	}
	key := packedLocatorKey(sequence)
	if record == nil {
		return e.remove(packedQueueKind, key)
	}
	wire, err := packedMarshal(record)
	if err != nil {
		return err
	}
	return e.put(packedQueueKind, key, wire)
}
func (e *packedEditor) applyIndex(kind byte, changes []queueindex.IndexChange) (err error) {
	defer func() { e.fail(err) }()
	if err = e.available(); err != nil {
		return err
	}
	if kind != packedReadyIndexKind && kind != packedRetirementIndexKind && kind != packedRunningIndexKind && kind != packedQueueIndexKind {
		return serviceerror.NewInternal("invalid packed index namespace")
	}
	for _, c := range changes {
		if c.Delete {
			err = e.batch.Delete(packedstore.ID{kind}, []byte(c.Key))
		} else {
			err = e.batch.Put(packedstore.ID{kind}, []byte(c.Key), c.Value)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func (e *packedEditor) finish(operations *pb.OperationStoreControl, projection *pb.ProjectionStoreControl) (plan packedMutation, err error) {
	defer func() { e.fail(err) }()
	if err = e.available(); err != nil {
		return plan, err
	}
	if err = e.checkCounts(operations, projection); err != nil {
		return plan, err
	}
	for kind := range e.touched {
		ownerKind := kind
		if kind == packedRequestKind {
			ownerKind = packedEnvelopeKind
		}
		count, err := e.batch.Count(packedstore.ID{ownerKind})
		if err != nil {
			return plan, err
		}
		if count == 0 {
			key := packedTemplateKey(kind)
			e.overlay.changes[key] = queueindex.IndexChange{Key: key, Delete: true}
		}
	}
	if err = e.batch.Commit(); err != nil {
		return plan, err
	}
	e.closed = true
	plan.pages = sortedPackedChanges(e.overlay.changes)
	plan.control = common.CloneProto(e.control)
	for key, value := range e.requests {
		plan.requests = append(plan.requests, packedRequestChange{key: key, value: common.CloneProto(value)})
	}
	slices.SortFunc(plan.requests, func(a, b packedRequestChange) int { return strings.Compare(a.key, b.key) })
	return plan, nil
}
