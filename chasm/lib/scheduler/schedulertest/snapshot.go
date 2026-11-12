package schedulertest

import (
	"cmp"
	"maps"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/metrics"
)

// Snapshot captures the observable scheduler state at a settled point (after a
// step's CloseTransaction). It is the input to invariant checks.
type Snapshot struct {
	UntransferableActions int
	AcceptedStarts        map[string]ExternalStart
	RecordedStarts        map[string]ExternalStart
	ActionCount           int64
	InitialActionCount    int64

	// Now is the engine clock at capture time.
	Now time.Time
	// HasPendingTask is true if any task is scheduled to fire.
	HasPendingTask bool

	Closed     bool
	IsSentinel bool
	Paused     bool
	// BackfillerCount is the number of live backfiller components.
	BackfillerCount int
	// IsHeldOpen mirrors Scheduler.isHeldOpen: the schedule must stay open
	// regardless of having no work (paused or pending backfill, non-sentinel).
	IsHeldOpen bool
	// IdleCloseTime is the armed idle-close deadline, or zero if no idle task is armed.
	IdleCloseTime time.Time

	// GeneratorLPT / InvokerLPT are the generator and invoker high-water marks.
	GeneratorLPT time.Time
	InvokerLPT   time.Time

	// BufferedStartsCount is len(invoker.GetBufferedStarts()) — the total buffered
	// starts (pending + retained). MaxBufferSize is the configured cap; the
	// buffer-bound invariant requires BufferedStartsCount <= MaxBufferSize.
	BufferedStartsCount int
	MaxBufferSize       int

	// MaxActionsObserved is the high-water mark of action RPCs (start + terminate +
	// cancel) taken by any single InvokerExecuteTask so far. MaxActionsPerExecution
	// is the configured per-execution cap; the action-budget invariant requires
	// MaxActionsObserved <= MaxActionsPerExecution.
	MaxActionsObserved     int
	MaxActionsPerExecution int

	// Start ledger inputs. PendingStarts are buffered starts with no RunId yet.
	// StartRequested holds every RequestId sent to StartWorkflowExecution.
	// OverlapSkipped and DroppedStarts count the starts the scheduler discarded
	// on purpose (Info.OverlapSkipped, and the ScheduleBufferedStartDropped
	// metric for missed-catchup and paused/limited drops).
	PendingStarts  map[string]struct{}
	StartRequested map[string]struct{}
	// ReplacingStarts counts the pending starts that no start RPC has been attempted for (including deferred ones) whose overlap policy
	// is CANCEL_OTHER or TERMINATE_OTHER. With nothing running, such a start
	// replaces the one the buffer would otherwise have started
	// (legacyscheduler.ProcessBuffer), discarding it without counting an overlap
	// skip, so each can account for one silent disappearance. ReplacingSeen is
	// the number of distinct such starts ever seen in the buffer in any state,
	// which covers a replacer that was buffered and started between two snapshots.
	ReplacingStarts int
	ReplacingSeen   int
	OverlapSkipped  int64
	DroppedStarts   int64
	// Migrating is true once the schedule hands off to a V1 workflow, which
	// takes its buffered starts with it.
	Migrating bool
	// DeferredStarts are pending starts waiting on a workflow completion
	// (Attempt == -1); RunningWorkflows counts started, uncompleted workflows.
	DeferredStarts   int
	RunningWorkflows int
}

// Snapshot reads current scheduler state into a Snapshot.
func (d *Driver) Snapshot() Snapshot {
	d.T.Helper()
	d.observeRecordedStarts()
	tweakables := d.Config.Tweakables(Namespace)
	snap := Snapshot{
		AcceptedStarts: d.external.snapshot(), RecordedStarts: maps.Clone(d.recorded),
		InitialActionCount:     d.initialActionCount,
		Now:                    d.Now(),
		HasPendingTask:         d.HasPendingTask(),
		MaxBufferSize:          tweakables.MaxBufferSize,
		MaxActionsObserved:     d.maxActionsObserved,
		MaxActionsPerExecution: tweakables.MaxActionsPerExecution,
		PendingStarts:          make(map[string]struct{}),
		StartRequested:         d.startRequestsCopy(),
		DroppedStarts:          d.droppedStarts(),
	}
	d.ReadScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) {
		snap.ActionCount = s.Info.GetActionCount()
		snap.Closed = s.Closed
		snap.IsSentinel = s.IsSentinel()
		snap.Paused = s.Schedule.GetState().GetPaused()
		snap.BackfillerCount = len(s.Backfillers)
		snap.OverlapSkipped = s.Info.GetOverlapSkipped()
		snap.Migrating = s.GetWorkflowMigration() != nil
		// Mirror Scheduler.isHeldOpen() from public state.
		snap.IsHeldOpen = !snap.IsSentinel && (snap.Paused || snap.BackfillerCount > 0)
		if s.IdleCloseTime != nil {
			snap.IdleCloseTime = s.IdleCloseTime.AsTime()
		}
		// Sentinels have no generator/invoker sub-components.
		if !snap.IsSentinel {
			if g := s.Generator.Get(ctx); g != nil && g.LastProcessedTime != nil {
				snap.GeneratorLPT = g.LastProcessedTime.AsTime()
			}
			if inv := s.Invoker.Get(ctx); inv != nil {
				snap.UntransferableActions = len(inv.GetCancelWorkflows()) + len(inv.GetTerminateWorkflows())
				snap.BufferedStartsCount = len(inv.GetBufferedStarts())
				snap.addBufferedStarts(inv.GetBufferedStarts(), s.Schedule.GetPolicies().GetOverlapPolicy(), d.replacingSeen)
				snap.ReplacingSeen = len(d.replacingSeen)
				if inv.LastProcessedTime != nil {
					snap.InvokerLPT = inv.LastProcessedTime.AsTime()
				}
			}
		}
	})
	return snap
}

// addBufferedStarts classifies buffered starts into pending (deferred or not)
// and running. scheduleOverlap resolves starts with an unspecified policy.
// replacingSeen accumulates the request IDs of replacing starts across snapshots.
func (snap *Snapshot) addBufferedStarts(starts []*schedulespb.BufferedStart, scheduleOverlap enumspb.ScheduleOverlapPolicy, replacingSeen map[string]struct{}) {
	for _, start := range starts {
		policy := cmp.Or(start.GetOverlapPolicy(), scheduleOverlap)
		replacing := policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER ||
			policy == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
		if replacing {
			replacingSeen[start.GetRequestId()] = struct{}{}
		}
		switch {
		case start.GetRunId() == "":
			if replacing && start.GetAttempt() <= 0 {
				snap.ReplacingStarts++
			}
			if start.GetAttempt() > 0 {
				snap.UntransferableActions++
			}
			snap.PendingStarts[start.GetRequestId()] = struct{}{}
			if start.GetAttempt() == -1 {
				snap.DeferredStarts++
			}
		case start.GetCompleted() == nil:
			snap.RunningWorkflows++
		default:
		}
	}
}

// droppedStarts sums the ScheduleBufferedStartDropped counter.
func (d *Driver) droppedStarts() int64 {
	var total int64
	for _, rec := range d.metricsCapture.SnapshotMetric(metrics.ScheduleBufferedStartDropped.Name()) {
		if v, ok := rec.Value.(int64); ok {
			total += v
		}
	}
	return total
}
