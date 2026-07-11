package propertytest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestOperationalRaceSharedSpecBuilderTimezoneCache(t *testing.T) {
	builder := NewSpecBuilder()
	candidate := redTeamSeedCandidates()[4]
	spec := renderRedTeamCandidate(candidate)
	original := proto.Clone(spec)
	var wg sync.WaitGroup
	errorsSeen := make(chan error, 20)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := builder.NewCompiledSpec(spec)
			errorsSeen <- err
		}()
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	if !proto.Equal(spec, original) {
		t.Fatal("shared builder mutated its input")
	}
}

func TestOperationalRaceCachedCompiledRequests(t *testing.T) {
	cache := NewOperationalCompiledSpecCache()
	builder := NewSpecBuilder()
	candidate := redTeamSeedCandidates()[0]
	spec := renderRedTeamCandidate(candidate)
	var wg sync.WaitGroup
	results := make([]*CompiledSpec, 20)
	errorsSeen := make([]error, 20)
	for index := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[index], _, errorsSeen[index] = cache.Get(backgroundContext, builder, spec)
		}()
	}
	wg.Wait()
	for index := range results {
		if errorsSeen[index] != nil || results[index] == nil {
			t.Fatalf("worker %d failed: %+v %v", index, results[index], errorsSeen[index])
		}
		if results[index] != results[0] {
			t.Fatalf("worker %d received a different compiled cache entry", index)
		}
	}
}

func TestOperationalRaceConcurrentValidationOneSpec(t *testing.T) {
	candidate := redTeamSeedCandidates()[6]
	spec := renderRedTeamCandidate(candidate)
	original := proto.Clone(spec)
	var wg sync.WaitGroup
	results := make([]ValidationResult, 20)
	errorsSeen := make([]error, 20)
	for index := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[index], errorsSeen[index] = ValidateSchedule(backgroundContext, spec, ValidationOptions{MaxIterations: 2_000_000})
		}()
	}
	wg.Wait()
	for index := range results {
		if !errors.Is(errorsSeen[index], ErrUnsatisfiableSpec) || results[index].Work != results[0].Work || results[index].Status != results[0].Status {
			t.Fatalf("validation worker %d changed outcome: %+v %v", index, results[index], errorsSeen[index])
		}
	}
	if !proto.Equal(spec, original) {
		t.Fatal("concurrent validation mutated its input")
	}
}

func TestOperationalRaceConcurrentJitterSeeds(t *testing.T) {
	candidate := redTeamSeedCandidates()[0]
	spec := renderRedTeamCandidate(candidate)
	options := ComputeOptions{MaxResults: 10, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000}
	seeds := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa"}
	results := make([]ComputeResult, len(seeds))
	errorsSeen := make([]error, len(seeds))
	var wg sync.WaitGroup
	for index := range seeds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[index], errorsSeen[index] = ComputeMatchingTimes(backgroundContext, spec, candidate.QueryStart, candidate.QueryEnd, seeds[index], options)
		}()
	}
	wg.Wait()
	for index := range results {
		if errorsSeen[index] != nil {
			t.Fatalf("seed %s failed: %v", seeds[index], errorsSeen[index])
		}
		if results[index].Work != results[0].Work || results[index].Validation.Work != results[0].Validation.Work {
			t.Fatalf("seed %s changed work", seeds[index])
		}
		for resultIndex, value := range results[index].Times {
			if value.Before(results[0].Times[resultIndex].Add(-time.Second)) || value.After(results[0].Times[resultIndex].Add(time.Second)) {
				t.Fatalf("seed %s changed more than jitter at result %d", seeds[index], resultIndex)
			}
		}
	}
}

func TestOperationalRaceCancellationDuringCacheLookup(t *testing.T) {
	cache := NewOperationalCompiledSpecCache()
	builder := NewSpecBuilder()
	candidate := redTeamSeedCandidates()[0]
	spec := renderRedTeamCandidate(candidate)
	buildStarted := make(chan struct{})
	releaseBuild := make(chan struct{})
	cache.beforeBuild = func() {
		close(buildStarted)
		<-releaseBuild
	}
	leaderDone := make(chan error, 1)
	go func() {
		_, _, err := cache.Get(backgroundContext, builder, spec)
		leaderDone <- err
	}()
	<-buildStarted
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	compiled, hit, err := cache.Get(ctx, builder, spec)
	if !errors.Is(err, context.Canceled) || compiled != nil || hit {
		t.Fatalf("cache lookup cancellation completed: compiled=%v hit=%t error=%v", compiled, hit, err)
	}
	close(releaseBuild)
	if err := <-leaderDone; err != nil {
		t.Fatal(err)
	}
}
