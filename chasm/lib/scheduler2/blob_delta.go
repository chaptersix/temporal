package scheduler2

import (
	"bytes"
	"math"
	"slices"
	"strings"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
)

const payloadStoreVersion = 1

func (s *Scheduler) validatePayloadStore() error {
	if s.PayloadStore == nil {
		return nil
	}
	if s.PayloadStore.FormatVersion != payloadStoreVersion || len(s.PayloadBlobs) != 0 {
		return serviceerror.NewInternal("invalid payload storage representation")
	}
	return nil
}

func (s *Scheduler) blobRecord(ctx chasm.Context, id string) (*pb.Payload, bool) {
	if s.PayloadStore != nil {
		return s.BlobRecords.Get(ctx, id)
	}
	field, ok := s.PayloadBlobs[id]
	if !ok {
		return nil, false
	}
	return common.CloneProto(field.Get(ctx)), true
}

func payloadRefs(p *pb.Payload) [3]string {
	if p == nil {
		return [3]string{}
	}
	return [3]string{p.ActionRef, p.ResultRef, p.FailureRef}
}
func carryoverRefs(p *pb.CarryoverState) [3]string {
	if p == nil {
		return [3]string{}
	}
	return [3]string{p.ResultRef, p.FailureRef}
}

// blobOwnerChange describes the exact immutable references held by one owner.
// Repeated nonempty references within an owner count once.
type blobOwnerChange struct {
	owner         string
	before, after [3]string
}

type blobCountChange struct {
	id    string
	count uint64
}

// blobDeltaPlan contains preflighted, detached values for nonrejecting publication.
type blobDeltaPlan struct {
	prepared payloadPreparation
	counts   []blobCountChange
}

// prepareBlobDelta visits at most six owners and 36 reference slots. It never
// decodes old blob bytes for releases; immutable content is checked on issuance
// and when newly supplied bytes are compared by prepareBlob.
func (s *Scheduler) prepareBlobDelta(ctx chasm.Context, owners []blobOwnerChange, prepared payloadPreparation) (blobDeltaPlan, error) {
	if err := s.validatePayloadStore(); err != nil {
		return blobDeltaPlan{}, err
	}
	if len(owners) > 6 || len(prepared) > 36 {
		return blobDeltaPlan{}, serviceerror.NewInternal("blob owner mutation budget exceeded")
	}
	plan := blobDeltaPlan{prepared: payloadPreparation{}}
	if s.PayloadStore == nil {
		for id, data := range prepared {
			plan.prepared[id] = bytes.Clone(data)
		}
		return plan, nil
	}
	deltas, err := blobOwnerDeltas(owners)
	if err != nil {
		return blobDeltaPlan{}, err
	}
	keys := make([]string, 0, len(deltas))
	for id := range deltas {
		keys = append(keys, id)
	}
	slices.Sort(keys)
	for _, id := range keys {
		if err := s.prepareBlobCount(ctx, id, deltas[id], prepared, &plan); err != nil {
			return blobDeltaPlan{}, err
		}
	}
	return plan, nil
}

type blobOwnerDelta struct{ added, removed uint64 }

func blobOwnerDeltas(owners []blobOwnerChange) (map[string]blobOwnerDelta, error) {
	result := make(map[string]blobOwnerDelta)
	seen := make(map[string]bool, len(owners))
	for _, owner := range owners {
		if owner.owner == "" || seen[owner.owner] {
			return nil, serviceerror.NewInternal("duplicate or empty blob owner identity")
		}
		seen[owner.owner] = true
		for direction, refs := range [2][3]string{owner.before, owner.after} {
			for i, id := range refs {
				if id == "" || slices.Contains(refs[:i], id) {
					continue
				}
				if !validBlobID(id) {
					return nil, serviceerror.NewInternal("invalid blob reference identity")
				}
				delta := result[id]
				if direction == 0 {
					delta.removed++
				} else {
					delta.added++
				}
				result[id] = delta
			}
		}
	}
	return result, nil
}
func validBlobID(id string) bool {
	kind, digest, ok := strings.Cut(id, "-")
	if !ok || len(digest) != 64 || kind != "action" && kind != "result" && kind != "failure" {
		return false
	}
	for _, r := range digest {
		if r < '0' || r > '9' && r < 'a' || r > 'f' {
			return false
		}
	}
	return true
}
func validPreparedBlob(id string, data []byte) bool {
	kind, _, _ := strings.Cut(id, "-")
	return id == blobIdentity(kind, data)
}

func (s *Scheduler) publishBlobDelta(ctx chasm.MutableContext, plan blobDeltaPlan) {
	if s.PayloadStore == nil {
		s.publishBlobs(ctx, plan.prepared)
		return
	}
	for id, data := range plan.prepared {
		s.BlobRecords.Put(ctx, id, &pb.Payload{Data: data})
	}
	for _, change := range plan.counts {
		if change.count == 0 {
			s.BlobReferences.Delete(ctx, change.id)
			s.BlobRecords.Delete(ctx, change.id)
		} else {
			s.BlobReferences.Put(ctx, change.id, &pb.BlobReference{OwnerCount: change.count})
		}
	}
}

func (s *Scheduler) prepareBlobCount(ctx chasm.Context, id string, delta blobOwnerDelta, prepared payloadPreparation, plan *blobDeltaPlan) error {
	old, exists := s.BlobReferences.Get(ctx, id)
	if data, supplied := prepared[id]; supplied {
		if !validPreparedBlob(id, data) {
			return serviceerror.NewInternal("prepared blob identity mismatch")
		}
		if exists {
			return serviceerror.NewInternal("counted blob content is missing")
		}
	}
	var count uint64
	if exists {
		if old.OwnerCount == 0 {
			return serviceerror.NewInternal("invalid blob owner count")
		}
		count = old.OwnerCount
	}
	if delta.removed > count {
		return serviceerror.NewInternal("blob owner count underflow")
	}
	after := count - delta.removed
	if delta.added > math.MaxUint64-after {
		return serviceerror.NewInternal("blob owner count overflow")
	}
	after += delta.added
	if after > 0 && !exists {
		data, ok := prepared[id]
		if !ok {
			return serviceerror.NewInternal("new blob owner has no content")
		}
		if !validPreparedBlob(id, data) {
			return serviceerror.NewInternal("prepared blob identity mismatch")
		}
		plan.prepared[id] = bytes.Clone(data)
	}
	if after != count {
		plan.counts = append(plan.counts, blobCountChange{id: id, count: after})
	}
	return nil
}
