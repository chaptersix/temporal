package scheduler2

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
)

// indexBackend keeps index pages and owning roots in the enclosing CHASM
// transaction. A backend is scoped to one context/epoch and must not be cached.
// Framework panics abort the enclosing lease; callers must not recover and
// continue using a partially staged transaction.
type indexBackend struct {
	ctx   chasm.Context
	pages chasm.DataMap[*commonpb.Payload]
}

var _ queueindex.IndexBackend = (*indexBackend)(nil)

func (b *indexBackend) Read(key string) ([]byte, bool, error) {
	page, ok := b.pages.Get(b.ctx, key)
	if !ok {
		return nil, false, nil
	}
	return page.Data, true, nil
}

func (b *indexBackend) Apply(changes []queueindex.IndexChange) error {
	ctx, ok := b.ctx.(chasm.MutableContext)
	if !ok {
		return serviceerror.NewInternal("index writes require a mutable context")
	}
	// DataMap copies each value. There is no independently published index commit:
	// all staged pages become durable only with the enclosing CHASM mutation.
	for _, change := range changes {
		if change.Delete {
			b.pages.Delete(ctx, change.Key)
		} else {
			b.pages.Put(ctx, change.Key, &commonpb.Payload{Data: change.Value})
		}
	}
	return nil
}
