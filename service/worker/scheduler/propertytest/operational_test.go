package propertytest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const operationalProfileVersion = "plan3-operational-v1"

type operationalCase struct {
	class     string
	candidate RedTeamCandidate
	options   ComputeOptions
}

func plan3OperationalCorpus() []operationalCase {
	seeds := redTeamSeedCandidates()
	base := seeds[3].QueryStart
	low := cloneRedTeamCandidate(seeds[3])
	low.CandidateID = "plan3-low-cost-valid"
	low.ResultLimit = 1
	typical := cloneRedTeamCandidate(seeds[1])
	typical.CandidateID = "plan3-typical-uniform-generated"
	typical.ResultLimit = 10
	transition := cloneRedTeamCandidate(seeds[0])
	transition.CandidateID = "plan3-exact-budget-transition"
	transition.ResultLimit = 10
	maximum := cloneRedTeamCandidate(seeds[5])
	maximum.CandidateID = "plan3-maximum-matching-work"
	maximum.ResultLimit = 1_000
	maximumValidation := cloneRedTeamCandidate(seeds[6])
	maximumValidation.CandidateID = "plan3-maximum-validation-work"
	queryEmpty := cloneRedTeamCandidate(seeds[3])
	queryEmpty.CandidateID = "plan3-query-local-empty"
	queryEmpty.QueryStart = base.Add(100 * time.Millisecond)
	queryEmpty.QueryEnd = base.Add(900 * time.Millisecond)
	queryEmpty.ResultLimit = 1
	structural := cloneRedTeamCandidate(seeds[3])
	structural.CandidateID = "plan3-structural-invalid"
	structural.Model.timezone = "Invalid/Plan3"
	unsatisfiable := cloneRedTeamCandidate(seeds[3])
	unsatisfiable.CandidateID = "plan3-unsatisfiable"
	unsatisfiable.Model.calendars = nil
	unsatisfiable.Model.intervals = nil
	lordHoweWitness := time.Date(2020, 4, 4, 14, 30, 0, 0, time.UTC)
	lordHowe, err := time.LoadLocation("Australia/Lord_Howe")
	if err != nil {
		panic(err)
	}
	timezone := RedTeamCandidate{
		SchemaVersion: redTeamCandidateSchemaVersion, CandidateID: "plan3-timezone-dst",
		AttackTrack: "sparse-first-result", Representation: "structured-calendar",
		Model:      scheduleModel{calendars: []calendarModel{exactCalendarModel(lordHoweWitness.In(lordHowe))}, timezone: "Australia/Lord_Howe"},
		QueryStart: lordHoweWitness.Add(-time.Second), QueryEnd: lordHoweWitness.Add(2 * time.Hour), ResultLimit: 10,
	}
	largeTimezone := cloneRedTeamCandidate(seeds[5])
	largeTimezone.CandidateID = "plan3-large-timezone-data"
	largeTimezone.ResultLimit = 1
	makeCase := func(class string, candidate RedTeamCandidate) operationalCase {
		return operationalCase{class: class, candidate: candidate, options: ComputeOptions{
			MaxResults: candidate.ResultLimit, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000,
		}}
	}
	return []operationalCase{
		makeCase("low-cost-valid", low),
		makeCase("typical-uniform-generated", typical),
		makeCase("exact-budget-transition", transition),
		makeCase("maximum-matching-work", maximum),
		makeCase("maximum-validation-work", maximumValidation),
		makeCase("query-local-empty", queryEmpty),
		makeCase("structural-invalid", structural),
		makeCase("unsatisfiable", unsatisfiable),
		makeCase("timezone-dst", timezone),
		makeCase("large-timezone-data", largeTimezone),
	}
}

func TestOperationalContextCheckOrder(t *testing.T) {
	candidate := redTeamSeedCandidates()[0]
	spec := renderRedTeamCandidate(candidate)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := ComputeMatchingTimes(ctx, spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
		MaxResults: 1, MaxIterations: 1, MaxValidationIterations: 1,
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrValidationLimit) || errors.Is(err, ErrIterationLimit) {
		t.Fatalf("context did not win before work budgets: result=%+v error=%v", result, err)
	}
	if result.Validation.Work.Total != 0 || result.Validation.Work.CancellationChecks != 1 || result.Complete || result.Validation.Complete {
		t.Fatalf("cancelled request reported work or completion: %+v", result)
	}

	deadline, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	result, err = ComputeMatchingTimes(deadline, spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
		MaxResults: 1, MaxIterations: 1, MaxValidationIterations: 1,
	})
	if !errors.Is(err, context.DeadlineExceeded) || result.Validation.Status != ValidationDeadlineExceeded || result.Complete {
		t.Fatalf("expired deadline was not preserved: result=%+v error=%v", result, err)
	}
}

func TestOperationalDeterministicCancellationCampaign(t *testing.T) {
	t.Run("before-validation", func(t *testing.T) {
		candidate := redTeamSeedCandidates()[0]
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := ComputeMatchingTimes(ctx, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
			MaxResults: candidate.ResultLimit, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000,
		})
		assertCancelledIncomplete(t, result, err)
	})

	t.Run("component-satisfiability", func(t *testing.T) {
		candidate := redTeamSeedCandidates()[2]
		result, err := cancelCandidateAt(t, candidate, func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseValidation && tick.Kind == "civil-day" && tick.Total >= 10
		}, 10_000_000)
		assertCancelledIncomplete(t, result, err)
		if result.Validation.Status != ValidationCancelled || result.Validation.Work.Total < 10 {
			t.Fatalf("component cancellation point was not reached: %+v", result.Validation)
		}
	})

	t.Run("effective-set-proof", func(t *testing.T) {
		candidate := redTeamSeedCandidates()[6]
		result, err := cancelCandidateAt(t, candidate, func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseValidation && tick.Kind == "effective-day" && tick.Total >= 1_000
		}, 10_000_000)
		assertCancelledIncomplete(t, result, err)
		if result.Validation.Status != ValidationCancelled {
			t.Fatalf("effective proof cancellation classified %s", result.Validation.Status)
		}
	})

	t.Run("before-first-result", func(t *testing.T) {
		candidate := redTeamSeedCandidates()[3]
		result, err := cancelCandidateAt(t, candidate, func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseMatching && tick.Total == 0
		}, 10_000_000)
		assertCancelledIncomplete(t, result, err)
		if len(result.Times) != 0 || result.Work.Total != 0 {
			t.Fatalf("work continued before first result: %+v", result)
		}
	})

	t.Run("after-one-result-prefix-stable", func(t *testing.T) {
		candidate := redTeamSeedCandidates()[3]
		candidate.ResultLimit = 2
		options := ComputeOptions{MaxResults: 1, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000}
		baseline, err := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
		if err != nil || len(baseline.Times) != 1 {
			t.Fatalf("baseline failed: %+v %v", baseline, err)
		}
		result, cancelErr := cancelCandidateAt(t, candidate, func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseMatching && tick.Kind == "result-loop" && tick.Total == baseline.Work.Total
		}, 10_000_000)
		assertCancelledIncomplete(t, result, cancelErr)
		if !slices.Equal(result.Times, baseline.Times) {
			t.Fatalf("prefix changed: %v versus %v", result.Times, baseline.Times)
		}
	})

	t.Run("exclusion-retry", func(t *testing.T) {
		candidate := redTeamSeedCandidates()[1]
		result, err := cancelCandidateAt(t, candidate, func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseMatching && tick.Kind == "excluded-retry" && tick.Total >= 50
		}, 10_000_000)
		assertCancelledIncomplete(t, result, err)
		if result.Work.ExcludedCandidateRetries == 0 {
			t.Fatalf("no exclusion retry was observed: %+v", result.Work)
		}
	})

	t.Run("one-before-budget-limit", func(t *testing.T) {
		candidate := redTeamSeedCandidates()[0]
		const limit = 1_000
		result, err := cancelCandidateAt(t, candidate, func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseMatching && tick.Total == limit-1
		}, limit)
		assertCancelledIncomplete(t, result, err)
		if result.Work.Total != limit-1 || errors.Is(err, ErrIterationLimit) {
			t.Fatalf("budget won before cancellation: work=%d error=%v", result.Work.Total, err)
		}
	})
}

func TestOperationalCancellationMonotonicAndBudgetIndependent(t *testing.T) {
	candidate := redTeamSeedCandidates()[0]
	workAt := func(cancelAt int, budget int) ComputeResult {
		result, err := cancelCandidateAt(t, candidate, func(tick WorkTick) bool {
			return tick.Phase == WorkPhaseMatching && tick.Total == cancelAt
		}, budget)
		assertCancelledIncomplete(t, result, err)
		return result
	}
	early := workAt(100, 10_000)
	late := workAt(200, 10_000)
	higherBudget := workAt(100, 10_000_000)
	if early.Work.Total > late.Work.Total || early.Work.Total != higherBudget.Work.Total {
		t.Fatalf("cancellation monotonicity or budget independence failed: early=%d late=%d higher=%d", early.Work.Total, late.Work.Total, higherBudget.Work.Total)
	}
}

func TestOperationalSimultaneousConcurrentCancellation(t *testing.T) {
	const workers = 10
	candidate := redTeamSeedCandidates()[3]
	ready := make(chan struct{})
	release := make(chan struct{})
	var reached atomic.Int32
	contexts := make([]context.Context, workers)
	cancels := make([]context.CancelFunc, workers)
	results := make([]ComputeResult, workers)
	errorsSeen := make([]error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		contexts[worker], cancels[worker] = context.WithCancel(context.Background())
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			options := ComputeOptions{
				MaxResults: 10, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000,
				WorkTickHook: func(tick WorkTick) {
					if tick.Phase == WorkPhaseMatching && tick.Total == 0 {
						if reached.Add(1) == workers {
							close(ready)
						}
						<-release
					}
				},
			}
			results[index], errorsSeen[index] = ComputeMatchingTimes(contexts[index], renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
		}(worker)
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-ready:
	case <-timer.C:
		close(release)
		t.Fatal("workers did not reach the deterministic cancellation barrier")
	}
	for _, cancel := range cancels {
		cancel()
	}
	close(release)
	wg.Wait()
	for worker := range workers {
		assertCancelledIncomplete(t, results[worker], errorsSeen[worker])
		if results[worker].Work.Total != 0 {
			t.Fatalf("worker %d continued for %d ticks", worker, results[worker].Work.Total)
		}
	}
}

func TestOperationalDeadlineCampaign(t *testing.T) {
	corpus := plan3OperationalCorpus()
	classes := []string{"low-cost-valid", "exact-budget-transition", "maximum-matching-work"}
	durations := []time.Duration{-time.Second, time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, time.Second, 10 * time.Second}
	for _, class := range classes {
		index := slices.IndexFunc(corpus, func(value operationalCase) bool { return value.class == class })
		if index < 0 {
			t.Fatal("missing operational class " + class)
		}
		workload := corpus[index]
		for _, duration := range durations {
			t.Run(fmt.Sprintf("%s/%s", class, duration), func(t *testing.T) {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(duration))
				result, err := ComputeMatchingTimes(ctx, renderRedTeamCandidate(workload.candidate), workload.candidate.QueryStart, workload.candidate.QueryEnd, workload.candidate.JitterSeed, workload.options)
				cancel()
				if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrIterationLimit) && !errors.Is(err, ErrValidationLimit) && !errors.Is(err, ErrInvalidSpec) {
					t.Fatalf("unexpected deadline outcome: %v", err)
				}
				if err != nil && result.Complete {
					t.Fatalf("deadline/budget error reported completion: %+v %v", result, err)
				}
				if duration < 0 && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expired deadline lost to another outcome: %v", err)
				}
			})
		}
	}
}

func TestOperationalCachedAndUncachedSemantics(t *testing.T) {
	candidate := redTeamSeedCandidates()[1]
	spec := renderRedTeamCandidate(candidate)
	original := proto.Clone(spec).(*schedulepb.ScheduleSpec)
	options := ComputeOptions{MaxResults: 10, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000}
	uncached := NewOperationalCalculator(OperationalCacheUncached)
	cached := NewOperationalCalculator(OperationalCacheCached)
	cold, coldReport, coldErr := uncached.Compute(backgroundContext, spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
	_, warmupReport, warmupErr := cached.Compute(backgroundContext, spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
	warm, warmReport, warmErr := cached.Compute(backgroundContext, spec, candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
	if coldErr != nil || warmupErr != nil || warmErr != nil {
		t.Fatalf("cache comparison failed: %v %v %v", coldErr, warmupErr, warmErr)
	}
	if !slices.Equal(cold.Times, warm.Times) || cold.Work != warm.Work || cold.Validation.Work != warm.Validation.Work || cold.Complete != warm.Complete {
		t.Fatalf("cache changed semantics: cold=%+v warm=%+v", cold, warm)
	}
	if coldReport.Hit || warmupReport.Hit || !warmReport.Hit || warmReport.ExecutedMatchingWork != 0 || warmReport.LogicalMatchingWork != cold.Work.Total {
		t.Fatalf("cold/warm accounting is incomplete: cold=%+v warmup=%+v warm=%+v", coldReport, warmupReport, warmReport)
	}
	if !proto.Equal(spec, original) {
		t.Fatal("calculator mutated the input protobuf")
	}
}

func TestOperationalConcurrentCacheLookupCancellation(t *testing.T) {
	candidate := redTeamSeedCandidates()[0]
	calculator := NewOperationalCalculator(OperationalCacheCached)
	options := ComputeOptions{MaxResults: 10, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000}
	// Populate first because request-level cache hits are deterministic and do not need a background worker.
	_, _, err := calculator.Compute(backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, report, err := calculator.Compute(ctx, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
	if !errors.Is(err, context.Canceled) || result.Complete || report.Hit {
		t.Fatalf("cancellation lost during cache lookup: result=%+v report=%+v error=%v", result, report, err)
	}
}

type operationalRetryStrategy string

const (
	retryIdentical     operationalRetryStrategy = "identical"
	retrySmallerWindow operationalRetryStrategy = "smaller-window"
	retryFewerResults  operationalRetryStrategy = "fewer-results"
	retryLargerBudget  operationalRetryStrategy = "larger-budget"
	retryNone          operationalRetryStrategy = "none"
)

func TestOperationalRetryPolicySimulator(t *testing.T) {
	candidate := redTeamSeedCandidates()[0]
	options := ComputeOptions{MaxResults: 10, MaxIterations: 1_000, MaxValidationIterations: 2_000_000}
	first, firstErr := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
	second, secondErr := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, options)
	if !errors.Is(firstErr, ErrIterationLimit) || !errors.Is(secondErr, ErrIterationLimit) || first.Work != second.Work || !slices.Equal(first.Times, second.Times) {
		t.Fatalf("identical deterministic retry unexpectedly progressed: first=%+v/%v second=%+v/%v", first, firstErr, second, secondErr)
	}
	larger := options
	larger.MaxIterations = 10_000
	if result, err := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, larger); err != nil || !result.Complete {
		t.Fatalf("larger budget was not actionable: %+v %v", result, err)
	}
	fewer := options
	fewer.MaxResults = 1
	if result, err := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, fewer); err != nil || !result.Complete {
		t.Fatalf("fewer results was not actionable: %+v %v", result, err)
	}
	invalid := cloneRedTeamCandidate(redTeamSeedCandidates()[3])
	invalid.Model.timezone = "Invalid/Retry"
	if _, err := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(invalid), invalid.QueryStart, invalid.QueryEnd, invalid.JitterSeed, ComputeOptions{
		MaxResults: 1, MaxIterations: 10_000, MaxValidationIterations: 10_000,
	}); !errors.Is(err, ErrInvalidSpec) || operationalRetryAdvice("invalid")[retryIdentical] {
		t.Fatalf("structural invalidity invited retry: %v", err)
	}
	unsatisfiable := &schedulepb.ScheduleSpec{}
	if _, err := ComputeMatchingTimes(backgroundContext, unsatisfiable, candidate.QueryStart, candidate.QueryEnd, "", ComputeOptions{
		MaxResults: 1, MaxIterations: 10_000, MaxValidationIterations: 10_000,
	}); !errors.Is(err, ErrUnsatisfiableSpec) || operationalRetryAdvice("unsatisfiable")[retryIdentical] {
		t.Fatalf("unsatisfiability invited retry: %v", err)
	}
	validationAbuse := redTeamSeedCandidates()[6]
	validationOptions := ComputeOptions{MaxResults: 1, MaxIterations: 10_000, MaxValidationIterations: 1_000}
	if _, err := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(validationAbuse), validationAbuse.QueryStart, validationAbuse.QueryEnd, validationAbuse.JitterSeed, validationOptions); !errors.Is(err, ErrValidationLimit) {
		t.Fatalf("validation indeterminacy was not preserved: %v", err)
	}
	validationOptions.MaxValidationIterations = 2_000_000
	if _, err := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(validationAbuse), validationAbuse.QueryStart, validationAbuse.QueryEnd, validationAbuse.JitterSeed, validationOptions); !errors.Is(err, ErrUnsatisfiableSpec) {
		t.Fatalf("larger validation budget did not reach classification: %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ComputeMatchingTimes(cancelled, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, larger); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation was not retry-distinct: %v", err)
	}
	expired, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	if _, err := ComputeMatchingTimes(expired, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, larger); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline was not retry-distinct: %v", err)
	}
	_ = []operationalRetryStrategy{retryIdentical, retrySmallerWindow, retryFewerResults, retryLargerBudget, retryNone}
}

func TestOperationalRepeatedAbuseModes(t *testing.T) {
	cheap := redTeamSeedCandidates()[3]
	expensive := redTeamSeedCandidates()[0]
	invalid := redTeamSeedCandidates()[6]
	for _, mode := range []OperationalCacheMode{OperationalCacheUncached, OperationalCacheCached} {
		t.Run(string(mode), func(t *testing.T) {
			calculator := NewOperationalCalculator(mode)
			sequence := []RedTeamCandidate{expensive, expensive, cheap, expensive, invalid, invalid}
			for _, candidate := range sequence {
				result, _, err := calculator.Compute(backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
					MaxResults: min(candidate.ResultLimit, 10), MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000,
				})
				if err == nil && !result.Complete {
					t.Fatalf("successful request incomplete for %s", candidate.CandidateID)
				}
				if err != nil && result.Complete {
					t.Fatalf("failed request complete for %s: %v", candidate.CandidateID, err)
				}
			}
		})
	}
}

func TestOperationalRepeatedAbuseConcurrent(t *testing.T) {
	expensive := []RedTeamCandidate{redTeamSeedCandidates()[0], redTeamSeedCandidates()[1], redTeamSeedCandidates()[5]}
	for _, mode := range []OperationalCacheMode{OperationalCacheUncached, OperationalCacheCached} {
		for _, distributed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/distributed=%t", mode, distributed), func(t *testing.T) {
				calculator := NewOperationalCalculator(mode)
				results := make([]ComputeResult, 10)
				reports := make([]OperationalCacheReport, 10)
				errorsSeen := make([]error, 10)
				var wg sync.WaitGroup
				for caller := range 10 {
					candidate := expensive[0]
					if distributed {
						candidate = expensive[caller%len(expensive)]
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						results[caller], reports[caller], errorsSeen[caller] = calculator.Compute(
							backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed,
							ComputeOptions{MaxResults: min(candidate.ResultLimit, 10), MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000},
						)
					}()
				}
				wg.Wait()
				for caller := range 10 {
					if errorsSeen[caller] != nil || !results[caller].Complete {
						t.Fatalf("caller %d failed: %+v %+v %v", caller, results[caller], reports[caller], errorsSeen[caller])
					}
				}
			})
		}
	}
}

func operationalRetryAdvice(outcome string) map[operationalRetryStrategy]bool {
	advice := map[operationalRetryStrategy]bool{
		retryIdentical: false, retrySmallerWindow: false, retryFewerResults: false,
		retryLargerBudget: false, retryNone: true,
	}
	switch outcome {
	case "validation-budget":
		advice[retryLargerBudget] = true
	case "matching-budget":
		advice[retrySmallerWindow] = true
		advice[retryFewerResults] = true
		advice[retryLargerBudget] = true
	case "cancelled", "deadline":
		advice[retryIdentical] = true
	}
	return advice
}

func TestOperationalRetryAdviceCoversTypedOutcomes(t *testing.T) {
	for _, outcome := range []string{"invalid", "unsatisfiable", "validation-budget", "matching-budget", "cancelled", "deadline"} {
		advice := operationalRetryAdvice(outcome)
		if len(advice) != 5 {
			t.Fatalf("%s has incomplete retry matrix: %v", outcome, advice)
		}
		if (outcome == "invalid" || outcome == "unsatisfiable") && (advice[retryIdentical] || advice[retryLargerBudget]) {
			t.Fatalf("deterministic invalidity invited retry: %s %v", outcome, advice)
		}
		if outcome == "matching-budget" && advice[retryIdentical] {
			t.Fatalf("deterministic matching exhaustion invited identical retry: %v", advice)
		}
		if outcome == "validation-budget" && (!advice[retryLargerBudget] || advice[retryIdentical]) {
			t.Fatalf("validation indeterminacy advice is not actionable: %v", advice)
		}
	}
}

func TestOperationalNoGoroutineLeakOnRapidCancellation(t *testing.T) {
	before := runtime.NumGoroutine()
	candidate := redTeamSeedCandidates()[0]
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := ComputeMatchingTimes(ctx, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
			MaxResults: 1_000, MaxIterations: 10_000_000, MaxValidationIterations: 2_000_000,
		})
		assertCancelledIncomplete(t, result, err)
	}
	runtime.GC()
	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Fatalf("goroutine count grew from %d to %d", before, after)
	}
}

func TestOperationalResultAndDiagnosticBounds(t *testing.T) {
	candidate := redTeamSeedCandidates()[3]
	result, err := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
		MaxResults: maxAnalysisResults + 1, MaxIterations: 10_000, MaxValidationIterations: 10_000,
	})
	if !errors.Is(err, ErrInvalidOptions) || result.Complete || len(result.Times) != 0 {
		t.Fatalf("oversized result request allocated or completed: %+v %v", result, err)
	}
	cron := strings.Repeat("☃", 1_024)
	_, err = ValidateSchedule(backgroundContext, &schedulepb.ScheduleSpec{CronString: []string{cron}}, ValidationOptions{MaxIterations: 10_000})
	if err == nil || len(err.Error()) > 1_024 || strings.Contains(err.Error(), cron) {
		t.Fatalf("diagnostic retained unbounded raw input: length=%d error=%v", len(err.Error()), err)
	}
}

type operationalWorkerObservation struct {
	Class          string
	Latency        time.Duration
	Outcome        string
	ValidationWork int
	MatchingWork   int
	ResultCount    int
}

type operationalConcurrencySummary struct {
	Workers          int
	Requests         int
	Throughput       float64
	P50              time.Duration
	P90              time.Duration
	P99              time.Duration
	Maximum          time.Duration
	AllocatedBytes   uint64
	PeakHeapBytes    uint64
	GoroutinesBefore int
	GoroutinesAfter  int
	Outcomes         map[string]int
	Observations     []operationalWorkerObservation
}

func runOperationalConcurrency(workloads []operationalCase, workers int, mixed bool) operationalConcurrencySummary {
	requests := workers
	if !mixed {
		requests = workers * len(workloads)
	}
	jobs := make(chan operationalCase)
	observations := make(chan operationalWorkerObservation, requests)
	start := make(chan struct{})
	var wg sync.WaitGroup
	beforeGoroutines := runtime.NumGoroutine()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for workload := range jobs {
				requestStarted := time.Now()
				result, err := ComputeMatchingTimes(backgroundContext, renderRedTeamCandidate(workload.candidate), workload.candidate.QueryStart, workload.candidate.QueryEnd, workload.candidate.JitterSeed, workload.options)
				observations <- operationalWorkerObservation{
					Class: workload.class, Latency: time.Since(requestStarted), Outcome: operationalOutcome(result, err),
					ValidationWork: result.Validation.Work.Total, MatchingWork: result.Work.Total, ResultCount: len(result.Times),
				}
			}
		}()
	}
	close(start)
	go func() {
		if mixed {
			for index := range requests {
				jobs <- workloads[index%len(workloads)]
			}
		} else {
			for _, workload := range workloads {
				for range workers {
					jobs <- workload
				}
			}
		}
		close(jobs)
	}()
	wg.Wait()
	close(observations)
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	summary := operationalConcurrencySummary{
		Workers: workers, Requests: requests, Throughput: float64(requests) / elapsed.Seconds(),
		AllocatedBytes: after.TotalAlloc - before.TotalAlloc, PeakHeapBytes: after.HeapSys,
		GoroutinesBefore: beforeGoroutines, GoroutinesAfter: runtime.NumGoroutine(), Outcomes: make(map[string]int),
	}
	var latencies []time.Duration
	for observation := range observations {
		summary.Observations = append(summary.Observations, observation)
		latencies = append(latencies, observation.Latency)
		summary.Outcomes[observation.Outcome]++
	}
	sort.Slice(latencies, func(i int, j int) bool { return latencies[i] < latencies[j] })
	if len(latencies) > 0 {
		summary.P50 = operationalPercentile(latencies, 50)
		summary.P90 = operationalPercentile(latencies, 90)
		summary.P99 = operationalPercentile(latencies, 99)
		summary.Maximum = latencies[len(latencies)-1]
	}
	return summary
}

func operationalPercentile(sorted []time.Duration, percentile int) time.Duration {
	index := (len(sorted)*percentile + 99) / 100
	return sorted[max(0, index-1)]
}

func operationalOutcome(result ComputeResult, err error) string {
	switch {
	case err == nil && result.Complete:
		return "complete"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, ErrValidationLimit):
		return "validation-budget"
	case errors.Is(err, ErrIterationLimit):
		return "matching-budget"
	case errors.Is(err, ErrInvalidSpec):
		return "invalid"
	default:
		return "other-error"
	}
}

func TestOperationalConcurrencyMatrix(t *testing.T) {
	if os.Getenv("SCHEDULE_PROPERTY_OPERATIONAL_MATRIX") == "" {
		t.Skip("set SCHEDULE_PROPERTY_OPERATIONAL_MATRIX=1 to run the complete Plan 3 matrix")
	}
	corpus := plan3OperationalCorpus()
	baseline := runOperationalConcurrency(corpus, 1, false)
	baselineWork := make(map[string][2]int)
	for _, observation := range baseline.Observations {
		baselineWork[observation.Class] = [2]int{observation.ValidationWork, observation.MatchingWork}
	}
	for _, workers := range []int{2, 10, 20} {
		for _, mixed := range []bool{false, true} {
			summary := runOperationalConcurrency(corpus, workers, mixed)
			if summary.GoroutinesAfter > summary.GoroutinesBefore+2 {
				t.Fatalf("%d workers leaked goroutines: before=%d after=%d", workers, summary.GoroutinesBefore, summary.GoroutinesAfter)
			}
			for _, observation := range summary.Observations {
				want := baselineWork[observation.Class]
				if observation.ValidationWork != want[0] || observation.MatchingWork != want[1] {
					t.Fatalf("work changed under %d workers for %s: got=(%d,%d) want=%v", workers, observation.Class, observation.ValidationWork, observation.MatchingWork, want)
				}
			}
		}
	}
}

func TestOperationalConcurrencyStress100(t *testing.T) {
	if os.Getenv("SCHEDULE_PROPERTY_OPERATIONAL_STRESS_100") == "" {
		t.Skip("set SCHEDULE_PROPERTY_OPERATIONAL_STRESS_100=1 for the opt-in 100-worker tier")
	}
	summary := runOperationalConcurrency(plan3OperationalCorpus(), 100, true)
	if summary.Requests != 100 || summary.GoroutinesAfter > summary.GoroutinesBefore+2 {
		t.Fatalf("invalid stress summary: %+v", summary)
	}
}

func cancelCandidateAt(t *testing.T, candidate RedTeamCandidate, predicate func(WorkTick) bool, budget int) (ComputeResult, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once sync.Once
	result, err := ComputeMatchingTimes(ctx, renderRedTeamCandidate(candidate), candidate.QueryStart, candidate.QueryEnd, candidate.JitterSeed, ComputeOptions{
		MaxResults: candidate.ResultLimit, MaxIterations: budget, MaxValidationIterations: 2_000_000,
		WorkTickHook: func(tick WorkTick) {
			if predicate(tick) {
				once.Do(cancel)
			}
		},
	})
	return result, err
}

func assertCancelledIncomplete(t *testing.T, result ComputeResult, err error) {
	t.Helper()
	if !errors.Is(err, context.Canceled) || result.Complete {
		t.Fatalf("cancellation was not distinct and incomplete: result=%+v error=%v", result, err)
	}
}

func plan3SimpleIntervalSpec() *schedulepb.ScheduleSpec {
	return &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}}
}
