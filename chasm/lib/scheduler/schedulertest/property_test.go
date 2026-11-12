package schedulertest_test

import (
	"errors"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

// TestSchedulerProperties drives a randomly-generated schedule through a random
// sequence of operations (advance time, pause, unpause, trigger, backfill,
// update, start RPC failures, workflow completion) as a rapid state machine and
// asserts the scheduler invariants hold after every settled transition. rapid
// shrinks any failing operation sequence to a minimal reproducer, which is
// directly useful for the "stuck open" bug class.
//
// The driver is built on the *rapid.T, so harness failures (not just invariant
// violations) are shrunk by rapid and recorded in its failfile, and each
// iteration's mocks and logger are scoped to that iteration.
//
// Invariants run in the driver's AfterStep hook rather than in Repeat's ""
// action: the hook also observes the intermediate settled states inside a
// multi-transition step (several Steps, or a patch applied between
// transitions), which a per-action check would not see.
//
// An action that does not apply to the current state calls t.Skip before it
// draws or mutates anything, so rapid picks another action instead.
func TestSchedulerProperties(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		// A small IdleTime lets fire-time gaps cross the idle window, which the
		// 7d default never does with these intervals.
		idle := rapid.SampledFrom([]time.Duration{10 * time.Minute, time.Hour, 2 * time.Hour}).Draw(rt, "idleTime")
		// Small buffer and action limits so the buffer-bound and action-budget
		// invariants are reachable; the defaults (1000 and 5) are not. A reserve
		// below MaxBufferSize/2 leaves the backfiller room to make progress.
		//
		// MaxBufferSize stays at or above 20: the backfiller overfills a smaller
		// buffer (see TestBackfillRespectsSmallMaxBufferSize).
		maxBuffer := rapid.SampledFrom([]int{20, 24, 40}).Draw(rt, "maxBufferSize")
		reserve := rapid.IntRange(0, 1).Draw(rt, "generatorBufferReserve")
		// The budget stays well below the 10 recent actions the driver reads
		// recorded starts from, so one execution cannot trim them out of view.
		policies := []enumspb.ScheduleOverlapPolicy{
			enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
			enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
			enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		}
		maxActions := rapid.SampledFrom([]int{1, 2, 3}).Draw(rt, "maxActionsPerExecution")
		sched := drawSchedule(rt, start, idle, policies)

		d := schedulertest.NewDriver(rt,
			schedulertest.WithStartTime(start),
			schedulertest.WithSchedule(sched),
			schedulertest.WithConfig(configWith(func(tw *scheduler.Tweakables) {
				tw.IdleTime = idle
				tw.MaxBufferSize = maxBuffer
				tw.GeneratorBufferReserveSize = reserve
				tw.MaxActionsPerExecution = maxActions
			})),
			// InvokerExecuteTask may lag the timers, and workflows may run, but
			// neither for a whole idle window. Past that, V1 and V2 both close
			// with the start still buffered.
			schedulertest.WithMaxSideEffectLag(idle/2),
			schedulertest.WithMaxRunDuration(idle/2),
		)
		hook := schedulertest.CheckInvariantsHook(rt)
		d.AfterStep = hook

		// Check the initial settled state too.
		hook(d)

		faultsLeft := scheduler.InvokerMaxStartAttempts - 1
		rt.Repeat(map[string]func(*rapid.T){
			// A uniform choice over the actions would rarely advance the clock, so
			// stepping is a compound action: a drawn run of one to six steps.
			// "pure" fires timers without dispatching side-effect tasks, so
			// InvokerExecuteTask can lag behind the generator and idle tasks.
			"step": func(t *rapid.T) {
				if !d.HasPendingTask() {
					t.Skip("quiescent")
				}
				for range rapid.IntRange(1, 6).Draw(t, "steps") {
					if !d.Step() {
						return
					}
				}
			},
			"pure": func(t *rapid.T) {
				if !d.StepPure() {
					t.Skip("no pure task can fire")
				}
			},
			"sideEffect": func(t *rapid.T) {
				if !d.StepSideEffect() {
					t.Skip("no side-effect task due")
				}
			},
			"complete": func(t *rapid.T) {
				if !d.CompleteWorkflow() {
					t.Skip("no workflow running")
				}
			},
			"pause":   func(t *rapid.T) { checkPatchError(t, d.Pause()) },
			"unpause": func(t *rapid.T) { checkPatchError(t, d.Unpause()) },
			"trigger": func(t *rapid.T) { checkPatchError(t, d.TriggerImmediately()) },
			"backfill": func(t *rapid.T) {
				// Ranges end at or before now: a backfill covers the past.
				now := d.Now()
				from := now.Add(-rapid.SampledFrom([]time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour}).Draw(t, "backfillFrom"))
				to := from.Add(rapid.SampledFrom([]time.Duration{0, time.Minute, 10 * time.Minute, time.Hour}).Draw(t, "backfillSpan"))
				if to.After(now) {
					to = now
				}
				checkPatchError(t, d.Backfill(&schedulepb.BackfillRequest{
					StartTime:     timestamppb.New(from),
					EndTime:       timestamppb.New(to),
					OverlapPolicy: drawOverlapPolicy(t, "backfillOverlap", policies),
				}))
			},
			"update": func(t *rapid.T) {
				interval := time.Duration(drawIntervalSec(t)) * time.Second
				overlap := drawOverlapPolicy(t, "updateOverlap", policies)
				paused := rapid.Bool().Draw(t, "updatePaused")
				checkPatchError(t, d.Update(func(s *schedulepb.Schedule) {
					// The frontend rejects a phase beyond the interval.
					phase := s.Spec.Interval[0].Phase.AsDuration() % interval
					s.Spec.Interval[0].Interval = durationpb.New(interval)
					s.Spec.Interval[0].Phase = durationpb.New(phase)
					s.Policies.OverlapPolicy = overlap
					s.State.Paused = paused
				}))
			},
			// The scheduler must retry a rejected start, and must deduplicate the
			// retry of a start whose response was lost. The scheduler gives up on a
			// start after InvokerMaxStartAttempts failed attempts, orphaning one
			// that was accepted but whose response was lost, so a run injects fewer
			// failures than that in total and every start eventually gets through.
			"failStarts": func(t *rapid.T) {
				if faultsLeft == 0 {
					t.Skip("failure budget spent")
				}
				n := rapid.IntRange(1, min(3, faultsLeft)).Draw(t, "failedStarts")
				faultsLeft -= n
				d.FailNextStarts(n)
			},
			"loseStartResponses": func(t *rapid.T) {
				if faultsLeft == 0 {
					t.Skip("failure budget spent")
				}
				n := rapid.IntRange(1, min(3, faultsLeft)).Draw(t, "lostResponses")
				faultsLeft -= n
				d.LoseNextStartResponses(n)
			},
		})
	})
}

func drawOverlapPolicy(t *rapid.T, label string, policies []enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	return rapid.SampledFrom(policies).Draw(t, label)
}

func drawIntervalSec(t *rapid.T) int64 {
	return rapid.SampledFrom([]int64{10, 30, 60, 300, 3600, 3 * 3600}).Draw(t, "intervalSec")
}

// drawSchedule generates a random but valid schedule definition. Some
// schedules end, with their last fire time placed around the idle window.
func drawSchedule(rt *rapid.T, start time.Time, idle time.Duration, policies []enumspb.ScheduleOverlapPolicy) *schedulepb.Schedule {
	s := schedulertest.DefaultSchedule()

	interval := time.Duration(drawIntervalSec(rt)) * time.Second
	s.Spec.Interval[0].Interval = durationpb.New(interval)

	if jitterSec := rapid.SampledFrom([]int64{0, 1, 5, 30}).Draw(rt, "jitterSec"); jitterSec > 0 {
		s.Spec.Jitter = durationpb.New(time.Duration(jitterSec) * time.Second)
	}

	if catchupSec := rapid.SampledFrom([]int64{60, 300, 3600}).Draw(rt, "catchupSec"); catchupSec > 0 {
		s.Policies.CatchupWindow = durationpb.New(time.Duration(catchupSec) * time.Second)
	}

	s.Policies.OverlapPolicy = drawOverlapPolicy(rt, "overlapPolicy", policies)

	switch rapid.SampledFrom([]string{"forever", "endTime", "oneShot"}).Draw(rt, "shape") {
	case "endTime":
		// Last fire lands a few intervals in, so the final gap is the interval.
		n := rapid.IntRange(1, 4).Draw(rt, "firesBeforeEnd")
		s.Spec.EndTime = timestamppb.New(start.Add(time.Duration(n)*interval + time.Second))
	case "oneShot":
		// A single fire, before, at, or after the idle window since creation.
		offset := rapid.SampledFrom([]time.Duration{
			idle / 2, idle - time.Minute, idle, idle + time.Minute, 3 * idle,
		}).Draw(rt, "oneShotOffset")
		s.Spec.Interval[0].Interval = durationpb.New(time.Hour)
		s.Spec.Interval[0].Phase = durationpb.New(offset % time.Hour)
		s.Spec.StartTime = timestamppb.New(start.Add(offset))
		s.Spec.EndTime = timestamppb.New(start.Add(offset + time.Minute))
	default:
	}

	if rapid.Bool().Draw(rt, "paused") {
		s.State.Paused = true
	}
	if rapid.Bool().Draw(rt, "limited") {
		s.State.LimitedActions = true
		s.State.RemainingActions = int64(rapid.IntRange(0, 5).Draw(rt, "remainingActions"))
	}

	return s
}

func checkPatchError(t *rapid.T, err error) {
	t.Helper()
	if err != nil && !errors.Is(err, scheduler.ErrClosed) {
		t.Fatalf("unexpected patch failure: %v", err)
	}
}
