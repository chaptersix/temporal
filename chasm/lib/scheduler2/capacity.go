package scheduler2

import (
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

func (s *Scheduler) capacity(ctx chasm.Context) decision.Capacity {
	if s.PublicRevision == 0 {
		return decision.Capacity{}
	}
	tweaks := schedulerconfig.DefaultTweakables
	if getter, ok := ctx.Value(backfillBatchConfigKey{}).(func(string) schedulerconfig.Tweakables); ok && getter != nil {
		tweaks = getter(ctx.NamespaceEntry().Name().String())
	}
	result := decision.Capacity{Public: true, MaxBuffer: int64(tweaks.MaxBufferSize)}
	result.BufferedOperations = s.Unfinished
	if runtime, ok := s.PublicRuntime.TryGet(ctx); ok {
		result.BufferedOperations += int64(len(runtime.CompletedOperations))
	}
	return result
}
