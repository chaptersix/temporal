package propertytest

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func BenchmarkOperationalMaximumResultSlice(b *testing.B) {
	candidate := redTeamSeedCandidates()[3]
	options := ComputeOptions{MaxResults: 1_000, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000}
	spec := renderRedTeamCandidate(candidate)
	b.ReportAllocs()
	for range b.N {
		result, err := ComputeMatchingTimes(backgroundContext, spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Times) != 1_000 {
			b.Fatalf("returned %d results", len(result.Times))
		}
	}
}

func BenchmarkOperationalValidationDayBuffers(b *testing.B) {
	candidate := redTeamSeedCandidates()[6]
	spec := renderRedTeamCandidate(candidate)
	b.ReportAllocs()
	for range b.N {
		result, err := ValidateSchedule(backgroundContext, spec, ValidationOptions{MaxIterations: 2_000_000})
		if err == nil || result.Work.Total == 0 {
			b.Fatalf("validation-abuse case lost classification: %+v %v", result, err)
		}
	}
}

func BenchmarkOperationalManyCalendarRanges(b *testing.B) {
	candidate := redTeamSeedCandidates()[5]
	spec := renderRedTeamCandidate(candidate)
	b.ReportAllocs()
	for range b.N {
		if _, err := ValidateSchedule(backgroundContext, spec, ValidationOptions{MaxIterations: 2_000_000}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOperationalLargeTimezoneData(b *testing.B) {
	candidate := redTeamSeedCandidates()[5]
	spec := renderRedTeamCandidate(candidate)
	options := ComputeOptions{MaxResults: 1, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000}
	b.ReportAllocs()
	for range b.N {
		if _, err := ComputeMatchingTimes(backgroundContext, spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOperationalRapidCancellation(b *testing.B) {
	candidate := redTeamSeedCandidates()[0]
	spec := renderRedTeamCandidate(candidate)
	options := ComputeOptions{MaxResults: 1_000, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000}
	b.ReportAllocs()
	for range b.N {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := ComputeMatchingTimes(ctx, spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
		if err == nil || result.Complete {
			b.Fatalf("cancelled request completed: %+v %v", result, err)
		}
	}
}

func BenchmarkOperationalRepeatedValidationCache(b *testing.B) {
	candidate := redTeamSeedCandidates()[6]
	spec := renderRedTeamCandidate(candidate)
	options := ComputeOptions{MaxResults: 1, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000}
	for _, mode := range []OperationalCacheMode{OperationalCacheUncached, OperationalCacheCached} {
		b.Run(string(mode), func(b *testing.B) {
			calculator := NewOperationalCalculator(mode)
			b.ReportAllocs()
			for range b.N {
				result, report, err := calculator.Compute(backgroundContext, spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
				if err == nil || result.Complete {
					b.Fatalf("invalid validation completed: %+v %v", result, err)
				}
				b.ReportMetric(float64(report.ExecutedValidationWork), "executed-validation-work/op")
			}
		})
	}
}

func TestOperationalPartialErrorsReleaseResultBackingArray(t *testing.T) {
	candidate := redTeamSeedCandidates()[3]
	candidate.ResultLimit = 1_000
	baseline, err := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
		MaxResults: 1, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000,
	})
	if err != nil || len(baseline.Times) != 1 {
		t.Fatalf("baseline failed: %+v %v", baseline, err)
	}
	result, cancelErr := cancelCandidateAt(t, candidate, func(tick WorkTick) bool {
		return tick.Phase == WorkPhaseMatching && tick.Kind == "result-loop" && tick.Total == baseline.Work.Total
	}, 10_000_000)
	assertCancelledIncomplete(t, result, cancelErr)
	if cap(result.Times) != len(result.Times) {
		t.Fatalf("partial result retained backing capacity %d for %d results", cap(result.Times), len(result.Times))
	}
}

func TestOperationalMemoryPlateau(t *testing.T) {
	if testing.Short() {
		t.Skip("memory plateau check is excluded from short mode")
	}
	candidate := redTeamSeedCandidates()[5]
	spec := renderRedTeamCandidate(candidate)
	options := ComputeOptions{MaxResults: 1, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000}
	calculator := NewOperationalCalculator(OperationalCacheCached)
	measure := func() uint64 {
		for range 20 {
			if _, _, err := calculator.Compute(backgroundContext, spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options); err != nil {
				t.Fatal(err)
			}
		}
		runtime.GC()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		return memory.HeapAlloc
	}
	first := measure()
	second := measure()
	if second > first+(8<<20) {
		t.Fatalf("repeated workload did not plateau: first=%d second=%d", first, second)
	}
}

func TestOperationalCancellationReleasesBuffers(t *testing.T) {
	candidate := redTeamSeedCandidates()[5]
	spec := renderRedTeamCandidate(candidate)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		_, _ = ComputeMatchingTimes(ctx, spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
			MaxResults: 1_000, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000,
			WorkTickHook: func(tick WorkTick) {
				if tick.Phase == WorkPhaseMatching {
					cancel()
				}
			},
		})
		cancel()
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	if after.HeapAlloc > before.HeapAlloc+(16<<20) {
		t.Fatalf("cancelled buffers retained excessive heap: before=%d after=%d", before.HeapAlloc, after.HeapAlloc)
	}
}

func BenchmarkOperationalAlternatingCheapExpensive(b *testing.B) {
	candidates := []RedTeamCandidate{redTeamSeedCandidates()[3], redTeamSeedCandidates()[0]}
	b.ReportAllocs()
	started := time.Now()
	for index := range b.N {
		candidate := candidates[index%len(candidates)]
		_, _ = ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
			MaxResults: min(candidate.ResultLimit, 10), MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000,
		})
	}
	b.ReportMetric(float64(time.Since(started).Nanoseconds())/float64(max(1, b.N)), "mixed-ns/op")
}
