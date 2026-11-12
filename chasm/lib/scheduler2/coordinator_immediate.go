package scheduler2

import (
	"time"

	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
)

// A scan and a handoff may share a transaction; further work yields physically.
const coordinatorTransactionQuanta = 2

type workQuantaKey struct{}
type workRemainingKey struct{}

func (s *Scheduler) workQuanta(ctx chasm.Context) int {
	if s.PublicRevision == 0 {
		return 0
	}
	quanta, ok := ctx.Value(workQuantaKey{}).(int)
	if !ok {
		return 0
	}
	return min(coordinatorTransactionQuanta, max(0, quanta))
}

func (s *Scheduler) remainingWork(ctx chasm.Context) int {
	if remaining, ok := ctx.Value(workRemainingKey{}).(int); ok {
		return remaining
	}
	return s.workQuanta(ctx)
}

func (s *Scheduler) addWorkTask(ctx chasm.MutableContext, wake time.Time, token uint64) {
	task := &scheduler2pb.WorkTask{Token: token}
	if remaining := s.remainingWork(ctx); remaining > 0 && !wake.After(ctx.Now(s)) {
		task.RemainingQuanta = uint32(remaining)
		wake = chasm.TaskScheduledTimeImmediate
	}
	ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: wake}, task)
}

func validWorkTask(ctx chasm.Context, s *Scheduler, attrs chasm.TaskAttributes, task *scheduler2pb.WorkTask) bool {
	c := s.Coordination
	if s.Closed || c == nil || !c.Armed || c.Token != task.Token {
		return false
	}
	if attrs.IsImmediate() {
		return task.RemainingQuanta > 0 && task.RemainingQuanta <= uint32(s.workQuanta(ctx)) && c.At <= ctx.Now(s).UnixNano()
	}
	return task.RemainingQuanta == 0 && c.At == attrs.ScheduledTime.UnixNano()
}
