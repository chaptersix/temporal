package scheduler2

import (
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type publicTransitionPlan struct {
	runtime  *scheduler2pb.PublicRuntime
	info     *schedulepb.ScheduleInfo
	eviction string
}

// preparePublicTransition exposes completion retention before any owner writes.
func (s *Scheduler) preparePublicTransition(ctx chasm.Context, before, next decision.State, event decision.Event, admitted bool) publicTransitionPlan {
	if s.PublicRevision == 0 {
		return publicTransitionPlan{}
	}
	runtime := &scheduler2pb.PublicRuntime{}
	if stored, ok := s.PublicRuntime.TryGet(ctx); ok {
		runtime = common.CloneProto(stored)
	}
	plan := publicTransitionPlan{runtime: runtime}
	skipped, missed, dropped := publicDispositionCounts(before, next, event)
	runtime.OverlapSkipped += skipped
	runtime.MissedCatchupWindow += missed
	if dropped != 0 {
		info := common.CloneProto(s.PublicInfo.Get(ctx))
		info.BufferDropped += dropped
		plan.info = info
	}
	if admitted {
		at := event.ActualTime.Time(event.Actual)
		switch event.Kind {
		case decision.DispatchManual:
			at = next.Operation.NominalTime.Time(next.Operation.Nominal)
		case decision.Drain:
			if before.IndexedQueue.Enabled {
				candidate := next.QueueChange.Drain.Candidate
				at = candidate.ActualTime.Time(candidate.Actual)
			} else {
				index := decision.DrainIndex(before, event.Now, event.CatchupWindow)
				if index >= 0 {
					at = before.Queue[index].ActualTime.Time(before.Queue[index].Actual)
				}
			}
		default:
			// Automatic and backfill events carry their jittered time.
		}
		runtime.Actions = append(runtime.Actions, &scheduler2pb.PublicAction{OperationId: next.Operation.ID, Result: &schedulepb.ScheduleActionResult{ScheduleTime: timestamppb.New(at), StartWorkflowResult: &commonpb.WorkflowExecution{WorkflowId: next.Operation.WorkflowID}}})
	}
	plan.eviction = retainPublicCompletions(runtime, before, next, event)
	updatePublicResult(runtime, before, next, event)
	return plan
}

func (s *Scheduler) publishPublicTransition(ctx chasm.MutableContext, plan publicTransitionPlan) {
	if plan.runtime == nil {
		return
	}
	runtime := plan.runtime
	if plan.info != nil {
		s.PublicInfo = chasm.NewDataField(ctx, plan.info)
	}
	s.retainPublicActions(runtime)
	s.PublicRuntime = chasm.NewDataField(ctx, runtime)
	if stored, ok := s.PublicSchedule.TryGet(ctx); ok {
		s.publishPublicList(ctx, stored)
	}
}

func (s *Scheduler) publicRecent(ctx chasm.Context, count int) []*schedulepb.ScheduleActionResult {
	runtime, ok := s.PublicRuntime.TryGet(ctx)
	if !ok {
		return nil
	}
	var actions []*schedulepb.ScheduleActionResult
	for _, a := range runtime.Actions {
		if a.Result.GetStartWorkflowResult().GetRunId() != "" {
			actions = append(actions, common.CloneProto(a.Result))
		}
	}
	slices.SortStableFunc(actions, func(a, b *schedulepb.ScheduleActionResult) int {
		return a.ActualTime.AsTime().Compare(b.ActualTime.AsTime())
	})
	return actions[max(0, len(actions)-count):]
}

type publicSpecBuilderKey struct{}
type publicListSpecLimitKey struct{}

// publicFuture computes at most ten times using the same bounded compiled-spec
// adapter as matching-times queries. A compute limit returns a partial preview.
func (s *Scheduler) publicFuture(ctx chasm.Context, spec *schedulepb.ScheduleSpec, count int) []*timestamppb.Timestamp {
	if s.Limited {
		count = int(max(0, min(int64(count), s.Remaining)))
	}
	if count == 0 {
		return nil
	}
	builder, ok := ctx.Value(publicSpecBuilderKey{}).(*legacy.SpecBuilder)
	if !ok || builder == nil {
		return nil
	}
	compiled, err := builder.NewCompiledSpec(spec)
	if err != nil {
		return nil
	}
	after := ctx.Now(s)
	if cursor := time.Unix(0, s.Cursor); cursor.After(after) {
		after = cursor
	}
	if info, ok := s.PublicInfo.TryGet(ctx); ok && info.UpdateTime.AsTime().After(after) {
		after = info.UpdateTime.AsTime()
	}
	seed := ctx.ExecutionKey().NamespaceID + "-" + ctx.ExecutionKey().BusinessID
	var result []*timestamppb.Timestamp
	for range count {
		next, err := compiled.GetNextTime(seed, after)
		if err != nil || next.Next.IsZero() {
			break
		}
		after = next.Next
		result = append(result, timestamppb.New(after))
	}
	return result
}

func (s *Scheduler) publicInfoProjection(ctx chasm.Context, info *schedulepb.ScheduleInfo, spec *schedulepb.ScheduleSpec) {
	info.ActionCount = s.Started
	info.BufferSize = s.queueLength() + s.UnconfirmedStarts
	info.RecentActions = s.publicRecent(ctx, 10)
	info.FutureActionTimes = s.publicFuture(ctx, spec, 10)
	info.RunningWorkflows = nil
	if runtime, ok := s.PublicRuntime.TryGet(ctx); ok {
		info.MissedCatchupWindow = runtime.MissedCatchupWindow
		info.OverlapSkipped = runtime.OverlapSkipped
		for _, a := range runtime.Actions {
			op, exists := s.operationRecord(ctx, a.OperationId)
			if !exists {
				continue
			}
			if op.Phase == scheduler2pb.PHASE_STARTED && a.Result.GetStartWorkflowResult().GetRunId() != "" {
				info.RunningWorkflows = append(info.RunningWorkflows, common.CloneProto(a.Result.StartWorkflowResult))
			}
		}
	}
}

func (s *Scheduler) publicListProjection(ctx chasm.Context, info *schedulepb.ScheduleListInfo, spec *schedulepb.ScheduleSpec) {
	if limitFn, ok := ctx.Value(publicListSpecLimitKey{}).(func() int); ok && info.Spec != nil {
		limit := max(0, limitFn())
		info.Spec.Interval = info.Spec.Interval[:min(limit, len(info.Spec.Interval))]
		info.Spec.StructuredCalendar = info.Spec.StructuredCalendar[:min(limit, len(info.Spec.StructuredCalendar))]
		info.Spec.ExcludeStructuredCalendar = info.Spec.ExcludeStructuredCalendar[:min(limit, len(info.Spec.ExcludeStructuredCalendar))]
	}

	info.RecentActions = s.publicRecent(ctx, 5)
	info.FutureActionTimes = s.publicFuture(ctx, spec, 5)
}

func updatePublicResult(runtime *scheduler2pb.PublicRuntime, before, next decision.State, event decision.Event) {
	op := next.Operation
	if op != nil && event.OperationID == op.ID && event.ChainID != "" && event.ChainID == op.ChainID {
		for _, action := range runtime.Actions {
			if action.OperationId != op.ID {
				continue
			}
			result := action.Result
			if event.Kind == decision.Acknowledged && event.PublicRunID != "" && result.StartWorkflowResult.RunId == "" {
				result.StartWorkflowResult.RunId = event.PublicRunID
				result.ActualTime = timestamppb.New(time.Unix(0, event.StartObservedAt).UTC())
				if result.StartWorkflowStatus == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
					result.StartWorkflowStatus = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
				}
			}
			if event.Kind == decision.Callback && before.Operation != nil && !decision.Terminal(before.Operation.Phase) && op.Phase == decision.Completed {
				result.StartWorkflowStatus = event.Status
			}
		}
	}
}

func (s *Scheduler) retainPublicActions(runtime *scheduler2pb.PublicRuntime) {
	// Keep ten acknowledged starts, plus current operation metadata needed to
	// merge callbacks or late acknowledgments. Operation retention is separately bounded.
	acknowledged := make([]*scheduler2pb.PublicAction, 0, len(runtime.Actions))
	for _, a := range runtime.Actions {
		if a.Result.GetStartWorkflowResult().GetRunId() != "" {
			acknowledged = append(acknowledged, a)
		}
	}
	slices.SortStableFunc(acknowledged, func(a, b *scheduler2pb.PublicAction) int {
		return a.Result.ActualTime.AsTime().Compare(b.Result.ActualTime.AsTime())
	})
	keep := make(map[string]bool)
	for _, a := range acknowledged[max(0, len(acknowledged)-10):] {
		keep[a.OperationId] = true
	}
	retained := runtime.Actions[:0]
	for _, a := range runtime.Actions {
		if _, active := s.Operations[a.OperationId]; active || keep[a.OperationId] {
			retained = append(retained, a)
		}
	}
	runtime.Actions = retained
}

// publicDispositionCounts distinguishes policy skips from buffer capacity loss,
// including manual/range dispositions and automatic waiters that expire later.
func publicDispositionCounts(before, next decision.State, event decision.Event) (skipped, missed, dropped int64) {
	policy := before.Root.Policy
	eligible := !before.Root.Paused && (!before.Root.Limited || before.Root.Remaining > 0)
	switch event.Kind {
	case decision.Due:
		if next.Root.Cursor <= before.Root.Cursor || !eligible {
			return
		}
		if event.Now > event.CatchupDeadline {
			return 0, 1, 0
		}
	case decision.DispatchManual:
		if len(next.Manuals) >= len(before.Manuals) {
			return
		}
		if override := before.Manuals[0].Policy; override != enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
			policy = override
		}
	case decision.DispatchBackfill:
		if event.BackfillBatchSkip {
			return 1, 0, 0
		}
		if !decision.QueueAvailable(before) {
			return
		}
		if event.Policy != enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
			policy = event.Policy
		}
	case decision.Drain:
		if eligible {
			if before.IndexedQueue.Enabled {
				if next.QueueChange.Drained {
					for _, count := range next.QueueChange.Drain.Ineligible[:3] {
						missed += count
					}
				}
			} else {
				missed = publicExpiredWaiters(before.Queue, next.Queue, event.Now, event.CatchupWindow)
			}
		}
		return
	default:
		return
	}
	skipped, dropped = publicOverlapCounts(before, next, policy)
	return
}

func publicOverlapCounts(before, next decision.State, policy enumspb.ScheduleOverlapPolicy) (skipped, dropped int64) {
	occupied := before.Root.BlockingOperationID != "" || decision.QueueLength(before) > 0
	policySkip := policy == enumspb.SCHEDULE_OVERLAP_POLICY_SKIP || policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED || (policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE && decision.QueueLength(before) > 0)
	if before.Capacity.Public && !decision.QueueAvailable(before) && next.Root.BufferDropped > before.Root.BufferDropped {
		return 0, next.Root.BufferDropped - before.Root.BufferDropped
	}
	if occupied && policySkip {
		skipped = 1
	}
	if next.Root.BufferDropped > before.Root.BufferDropped && skipped == 0 {
		dropped = next.Root.BufferDropped - before.Root.BufferDropped
	}
	return
}

func publicExpiredWaiters(before, after []decision.Queued, now, catchup int64) int64 {
	var count int64
	for _, q := range before {
		if q.ManualID == 0 && q.BackfillID == 0 && time.Unix(0, q.Actual).Before(time.Unix(0, now).Add(-time.Duration(catchup))) && !slices.Contains(after, q) {
			count++
		}
	}
	return count
}
