package scheduler2

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/packedstore"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
)

const packedStorageVersion = 1
const (
	packedOperationKind       byte = 'O'
	packedActionKind          byte = 'A'
	packedReadyKind           byte = 'R'
	packedEnvelopeKind        byte = 'E'
	packedRequestKind         byte = 'Q'
	packedRunningKind         byte = 'U'
	packedQueueKind           byte = 'B'
	packedIdentityKind        byte = 'I'
	packedReadyIndexKind      byte = 'X'
	packedRetirementIndexKind byte = 'T'
	packedRunningIndexKind    byte = 'P'
	packedQueueIndexKind      byte = 'K'
)

type packedPageBackend struct {
	ctx   chasm.Context
	pages chasm.DataMap[*commonpb.Payload]
}

func (b *packedPageBackend) Read(key string) ([]byte, bool, error) {
	value, ok := b.pages.Get(b.ctx, key)
	size := 0
	if ok {
		size = len(value.Data)
	}
	chasm.ObservePackedBackendRead(b.ctx, size)
	if !ok {
		return nil, false, nil
	}
	if len(value.Metadata) != 0 {
		return nil, false, serviceerror.NewInternal("invalid packed page envelope")
	}
	return bytes.Clone(value.Data), true, nil
}

// This backend is a read view. Publication is performed only from a detached plan.
func (*packedPageBackend) Apply([]queueindex.IndexChange) error {
	return serviceerror.NewInternal("packed read view cannot publish")
}

type packedOverlay struct {
	reader  queueindex.IndexBackend
	changes map[string]queueindex.IndexChange
}

func (b *packedOverlay) Read(key string) ([]byte, bool, error) {
	if c, ok := b.changes[key]; ok {
		return bytes.Clone(c.Value), !c.Delete, nil
	}
	return b.reader.Read(key)
}
func (b *packedOverlay) Apply(changes []queueindex.IndexChange) error {
	for _, c := range changes {
		b.changes[c.Key] = queueindex.IndexChange{Key: c.Key, Value: bytes.Clone(c.Value), Delete: c.Delete}
	}
	return nil
}
func packedLocatorKey(locator uint64) []byte { return binary.BigEndian.AppendUint64(nil, locator) }
func packedRequestAddress(locator uint64) string {
	return "q/" + base64.RawURLEncoding.EncodeToString(packedLocatorKey(locator))
}
func packedTemplateKey(kind byte) string { return fmt.Sprintf("t/%c", kind) }

type packedRequestChange struct {
	key   string
	value *pb.Payload
}
type packedMutation struct {
	pages    []queueindex.IndexChange
	requests []packedRequestChange
	control  *pb.PackedStorageControl
}

func (s *Scheduler) publishPackedMutation(ctx chasm.MutableContext, p packedMutation) {
	if p.control == nil {
		return
	}
	for _, change := range p.pages {
		if change.Delete {
			s.PackedPages.Delete(ctx, change.Key)
		} else {
			s.PackedPages.Put(ctx, change.Key, &commonpb.Payload{Data: change.Value})
		}
	}
	for _, change := range p.requests {
		if change.value == nil {
			s.PackedRequests.Delete(ctx, change.key)
		} else {
			s.PackedRequests.Put(ctx, change.key, change.value)
		}
	}
	s.PackedStorage = p.control
}
func (s *Scheduler) validatePackedStorage() error {
	if s.PackedStorage == nil {
		return nil
	}
	if s.PackedStorage.FormatVersion != packedStorageVersion || s.PackedStorage.NextLocator == 0 {
		return serviceerror.NewInternal("invalid packed storage control")
	}
	if !s.completeOperationProfile() {
		return serviceerror.NewInternal("packed storage requires complete addressed profile")
	}
	return nil
}

type packedView struct {
	scheduler *Scheduler
	control   *pb.PackedStorageControl
	ctx       chasm.Context
	reader    queueindex.IndexBackend
	batch     *packedstore.Batch
}

func (s *Scheduler) newPackedView(ctx chasm.Context) (*packedView, error) {
	if err := s.validatePackedStorage(); err != nil {
		return nil, err
	}
	if s.PackedStorage == nil {
		return nil, serviceerror.NewInternal("missing packed storage selector")
	}
	reader := &packedPageBackend{ctx: ctx, pages: s.PackedPages}
	view := &packedView{scheduler: s, control: s.PackedStorage, ctx: ctx, reader: reader, batch: packedstore.NewBatch(reader)}
	if err := view.checkCounts(s.OperationStore, s.ProjectionStore); err != nil {
		view.close()
		return nil, err
	}
	return view, nil
}
func (v *packedView) close() { v.batch.Abort() }
func (v *packedView) template(kind byte) ([]byte, bool, error) {
	return v.reader.Read(packedTemplateKey(kind))
}
func (v *packedView) get(kind byte, key []byte) ([]byte, bool, error) {
	wire, ok, err := v.batch.Get(packedstore.ID{kind}, key)
	if err != nil || !ok {
		return nil, ok, err
	}
	template, present, err := v.template(kind)
	if err != nil {
		return nil, false, err
	}
	if !present {
		return nil, false, serviceerror.NewInternal("packed record template is missing")
	}
	chasm.ObservePackedRecordDecode(v.ctx, len(template)+len(wire))
	value, err := expandPackedRequest(template, wire)
	return value, err == nil, err
}
func sortedPackedChanges(changes map[string]queueindex.IndexChange) []queueindex.IndexChange {
	result := make([]queueindex.IndexChange, 0, len(changes))
	for _, change := range changes {
		result = append(result, change)
	}
	slices.SortFunc(result, func(a, b queueindex.IndexChange) int { return strings.Compare(a.Key, b.Key) })
	return result
}

func (v *packedView) checkCounts(operations *pb.OperationStoreControl, projection *pb.ProjectionStoreControl) error {
	if operations == nil || projection == nil {
		return serviceerror.NewInternal("packed owner controls are missing")
	}
	expected := []struct {
		kind  byte
		count int64
	}{{packedOperationKind, operations.OperationCount}, {packedEnvelopeKind, operations.RequestCount}, {packedReadyKind, operations.PendingCount}, {packedRunningKind, projection.RunningCount}}
	for _, item := range expected {
		count, err := v.batch.Count(packedstore.ID{item.kind})
		if err != nil {
			return err
		}
		if item.count < 0 || count != uint64(item.count) {
			return serviceerror.NewInternal("packed record count disagrees with owner")
		}
	}
	actions, err := v.batch.Count(packedstore.ID{packedActionKind})
	if err != nil {
		return err
	}
	identities, err := v.batch.Count(packedstore.ID{packedIdentityKind})
	if err != nil {
		return err
	}
	if actions < uint64(operations.OperationCount) || actions-uint64(operations.OperationCount) > 10 || identities != actions {
		return serviceerror.NewInternal("packed public identity ownership count mismatch")
	}
	return nil
}

func (b *packedPageBackend) ObservePackedRead(entries, encodedBytes int) {
	chasm.ObservePackedRead(b.ctx, entries, encodedBytes)
}
func (b *packedPageBackend) ObservePackedWrite(entries, encodedBytes int) {
	chasm.ObservePackedWrite(b.ctx, entries, encodedBytes)
}
func (b *packedOverlay) ObservePackedRead(entries, encodedBytes int) {
	if observer, ok := b.reader.(interface{ ObservePackedRead(int, int) }); ok {
		observer.ObservePackedRead(entries, encodedBytes)
	}
}
func (b *packedOverlay) ObservePackedWrite(entries, encodedBytes int) {
	if observer, ok := b.reader.(interface{ ObservePackedWrite(int, int) }); ok {
		observer.ObservePackedWrite(entries, encodedBytes)
	}
}
