// Package schedutil provides operations for remediating degraded schedule workflows.
// Functions in this package are called by the tdbg schedule dedup and force-can commands.
package schedutil

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/worker/scheduler"
	"golang.org/x/term"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// RunDedup describes the schedule and, if duplicates are found, writes
// before/after JSON files to outDir and (if execute) sends an UpdateSchedule
// with duplicates removed. Files are written in both dry-run and execute mode.
func RunDedup(ctx context.Context, cl workflowservice.WorkflowServiceClient, namespace, scheduleID, outDir string, execute bool) error {
	desc, err := cl.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  namespace,
		ScheduleId: scheduleID,
	})
	if err != nil {
		return fmt.Errorf("describe schedule: %w", err)
	}

	beforeJSON, err := protojson.Marshal(desc.Schedule)
	if err != nil {
		return fmt.Errorf("marshal schedule: %w", err)
	}

	nCalBefore := len(desc.Schedule.Spec.StructuredCalendar)
	desc.Schedule.Spec.StructuredCalendar = deduplicateStructuredCalendarsProto(desc.Schedule.Spec.StructuredCalendar)
	nCalAfter := len(desc.Schedule.Spec.StructuredCalendar)

	afterJSON, err := protojson.Marshal(desc.Schedule)
	if err != nil {
		return fmt.Errorf("marshal deduped schedule: %w", err)
	}

	if nCalBefore == nCalAfter {
		fmt.Printf("  %s: no duplicates found\n", scheduleID)
		return nil
	}

	key := fileKey(namespace, scheduleID)
	beforePath := outDir + "/" + key + "-before.json"
	if err := os.WriteFile(beforePath, beforeJSON, 0o644); err != nil {
		return fmt.Errorf("write before: %w", err)
	}
	afterPath := outDir + "/" + key + "-after.json"
	if err := os.WriteFile(afterPath, afterJSON, 0o644); err != nil {
		return fmt.Errorf("write after: %w", err)
	}

	if !execute {
		fmt.Printf("  %s: %d→%d calendars (dry run)\n", scheduleID, nCalBefore, nCalAfter)
		fmt.Printf("    before: %s\n", beforePath)
		fmt.Printf("    after:  %s\n", afterPath)
		return nil
	}

	if _, err := cl.UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
		Namespace:     namespace,
		ScheduleId:    scheduleID,
		Schedule:      desc.Schedule,
		ConflictToken: desc.ConflictToken,
		RequestId:     uuid.NewString(),
	}); err != nil {
		return fmt.Errorf("update schedule: %w", err)
	}
	fmt.Printf("  %s: %d→%d calendars\n", scheduleID, nCalBefore, nCalAfter)
	fmt.Printf("    before: %s\n", beforePath)
	fmt.Printf("    after:  %s\n", afterPath)
	return nil
}

// RunForceCAN sends (or in dry-run mode, prints) a force-continue-as-new
// signal for the scheduler workflow of the given schedule ID.
func RunForceCAN(ctx context.Context, cl workflowservice.WorkflowServiceClient, namespace, scheduleID string, execute bool) error {
	workflowID := scheduler.WorkflowIDPrefix + scheduleID
	if !execute {
		fmt.Printf("  %s: would signal %q (dry run)\n", scheduleID, scheduler.SignalNameForceCAN)
		return nil
	}
	if _, err := cl.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		SignalName:        scheduler.SignalNameForceCAN,
	}); err != nil {
		return fmt.Errorf("signal workflow: %w", err)
	}
	fmt.Printf("  %s: signalled %q\n", scheduleID, scheduler.SignalNameForceCAN)
	return nil
}

// RunDedupRecreate reads the schedule state from workflow history (bypassing the
// query size limit), deduplicates the spec, and (if execute) deletes the broken
// schedule and recreates it with the clean spec. Use when the workflow is too
// degraded to process an update signal.
//
// State reconstruction scans the full current run's history:
//   - The WorkflowExecutionStarted input provides the baseline (state at last CAN).
//   - The last "update" signal, if present, contains a more recent FullUpdateRequest
//     whose Schedule supersedes the baseline.
//   - "patch" signals after the last update are replayed to carry forward
//     pause/unpause state. TriggerImmediately and BackfillRequest are skipped —
//     those are one-time side effects that should not be re-fired.
func RunDedupRecreate(ctx context.Context, cl workflowservice.WorkflowServiceClient, namespace, scheduleID, outDir string, execute bool) error {
	workflowID := scheduler.WorkflowIDPrefix + scheduleID

	// Page through the entire current run's history. Omitting RunId causes the
	// server to resolve the latest run.
	schedule, err := reconstructScheduleFromHistory(ctx, cl, namespace, workflowID)
	if err != nil {
		return err
	}

	beforeJSON, err := protojson.Marshal(schedule)
	if err != nil {
		return fmt.Errorf("marshal before schedule: %w", err)
	}

	nCalBefore := len(schedule.Spec.StructuredCalendar)
	schedule.Spec.StructuredCalendar = deduplicateStructuredCalendarsProto(schedule.Spec.StructuredCalendar)
	nCalAfter := len(schedule.Spec.StructuredCalendar)

	if nCalBefore == nCalAfter {
		fmt.Printf("  %s: no duplicates found\n", scheduleID)
		return nil
	}

	afterJSON, err := protojson.Marshal(schedule)
	if err != nil {
		return fmt.Errorf("marshal after schedule: %w", err)
	}

	key := fileKey(namespace, scheduleID)
	beforePath := outDir + "/" + key + "-before.json"
	if err := os.WriteFile(beforePath, beforeJSON, 0o644); err != nil {
		return fmt.Errorf("write before: %w", err)
	}
	afterPath := outDir + "/" + key + "-after.json"
	if err := os.WriteFile(afterPath, afterJSON, 0o644); err != nil {
		return fmt.Errorf("write after: %w", err)
	}

	if !execute {
		fmt.Printf("  %s: %d→%d calendars (dry run, would recreate)\n", scheduleID, nCalBefore, nCalAfter)
		fmt.Printf("    before: %s\n", beforePath)
		fmt.Printf("    after:  %s\n", afterPath)
		return nil
	}

	// The server blocks external API calls from starting workflows on internal
	// task queues, so we cannot replicate continue-as-new exactly. CreateSchedule
	// is the only client-accessible path. It preserves the spec (after dedup),
	// paused state, notes, and policies, but resets the high watermark
	// (LastProcessedTime) to now. Actions that would have fired during the
	// degraded period will not fire on the recreated schedule.
	if _, err := cl.DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{
		Namespace:  namespace,
		ScheduleId: scheduleID,
		Identity:   "schedutil",
	}); err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}
	if _, err := cl.CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  namespace,
		ScheduleId: scheduleID,
		Schedule:   schedule,
		Identity:   "schedutil",
		RequestId:  uuid.NewString(),
	}); err != nil {
		return fmt.Errorf("recreate schedule: %w", err)
	}
	fmt.Printf("  %s: %d→%d calendars (recreated)\n", scheduleID, nCalBefore, nCalAfter)
	fmt.Printf("    before: %s\n", beforePath)
	fmt.Printf("    after:  %s\n", afterPath)
	return nil
}

// reconstructScheduleFromHistory fetches the current run's history via the
// gRPC client and delegates to replayScheduleHistory to reconstruct the spec.
func reconstructScheduleFromHistory(ctx context.Context, cl workflowservice.WorkflowServiceClient, namespace, workflowID string) (*schedulepb.Schedule, error) {
	var pageToken []byte
	var pending []*historypb.HistoryEvent
	done := false

	next := func() (*historypb.HistoryEvent, error) {
		for len(pending) == 0 && !done {
			resp, err := cl.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:     namespace,
				Execution:     &commonpb.WorkflowExecution{WorkflowId: workflowID},
				NextPageToken: pageToken,
			})
			if err != nil {
				return nil, fmt.Errorf("get workflow history: %w", err)
			}
			pending = resp.History.Events
			pageToken = resp.NextPageToken
			if len(pageToken) == 0 {
				done = true
			}
		}
		if len(pending) == 0 {
			return nil, nil
		}
		event := pending[0]
		pending = pending[1:]
		return event, nil
	}

	return replayScheduleHistory(workflowID, next)
}

// replayScheduleHistory consumes events from next() one at a time and returns
// the reconstructed Schedule. The iterator signals exhaustion by returning a
// nil event with a nil error.
//
// State reconstruction:
//   - WorkflowExecutionStarted provides the baseline (state at last CAN).
//   - The last "update" signal supersedes the baseline with a more recent spec.
//   - "patch" signals after the last update are replayed for pause/unpause only.
func replayScheduleHistory(workflowID string, next func() (*historypb.HistoryEvent, error)) (*schedulepb.Schedule, error) {
	var baseArgs schedulespb.StartScheduleArgs
	var lastUpdate *schedulespb.FullUpdateRequest
	var patchesAfterUpdate []*schedulepb.SchedulePatch

	for {
		event, err := next()
		if err != nil {
			return nil, err
		}
		if event == nil {
			break
		}
		if started := event.GetWorkflowExecutionStartedEventAttributes(); started != nil {
			if err := applyStartedEvent(workflowID, started.WorkflowType.GetName(), started.Input, &baseArgs); err != nil {
				return nil, err
			}
		} else if signaled := event.GetWorkflowExecutionSignaledEventAttributes(); signaled != nil {
			applySignaledEvent(signaled.GetSignalName(), signaled.Input, &lastUpdate, &patchesAfterUpdate)
		}
	}

	if baseArgs.Schedule == nil {
		return nil, fmt.Errorf("no WorkflowExecutionStarted event found for %s", workflowID)
	}

	schedule := baseArgs.Schedule
	if lastUpdate != nil && lastUpdate.Schedule != nil {
		schedule = lastUpdate.Schedule
	}
	applySchedulePatches(schedule, patchesAfterUpdate)
	return schedule, nil
}

func applyStartedEvent(workflowID, workflowType string, input *commonpb.Payloads, baseArgs *schedulespb.StartScheduleArgs) error {
	if workflowType != scheduler.WorkflowType {
		return fmt.Errorf("workflow %s has unexpected type %q (expected %q); not a schedule workflow",
			workflowID, workflowType, scheduler.WorkflowType)
	}
	if err := payloads.Decode(input, baseArgs); err != nil {
		return fmt.Errorf("decode StartScheduleArgs: %w", err)
	}
	return nil
}

func applySignaledEvent(
	signalName string,
	input *commonpb.Payloads,
	lastUpdate **schedulespb.FullUpdateRequest,
	patchesAfterUpdate *[]*schedulepb.SchedulePatch,
) {
	switch signalName {
	case scheduler.SignalNameUpdate:
		var req schedulespb.FullUpdateRequest
		if err := payloads.Decode(input, &req); err == nil {
			*lastUpdate = &req
			*patchesAfterUpdate = nil
		}
	case scheduler.SignalNamePatch:
		if *lastUpdate == nil {
			return
		}
		var patch schedulepb.SchedulePatch
		if err := payloads.Decode(input, &patch); err == nil {
			*patchesAfterUpdate = append(*patchesAfterUpdate, &patch)
		}
	default:
	}
}

func applySchedulePatches(schedule *schedulepb.Schedule, patches []*schedulepb.SchedulePatch) {
	if schedule.State == nil {
		schedule.State = &schedulepb.ScheduleState{}
	}
	for _, patch := range patches {
		if patch.Pause != "" {
			schedule.State.Paused = true
			schedule.State.Notes = patch.Pause
			continue
		}
		if patch.Unpause != "" {
			schedule.State.Paused = false
			schedule.State.Notes = patch.Unpause
		}
	}
}

// ForEachSchedule resolves the schedule ID set and calls fn for each,
// continuing on per-schedule errors and reporting a combined failure at the end.
// If stdin is a pipe, IDs are read from it one per line; otherwise all
// schedules in the namespace are listed.
func ForEachSchedule(ctx context.Context, cl workflowservice.WorkflowServiceClient, namespace string, fn func(string) error) error {
	var scheduleIDs []string
	if stdinIsPipe() {
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			scheduleIDs = append(scheduleIDs, line)
		}
		if err := sc.Err(); err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
	} else {
		var pageToken []byte
		for {
			resp, err := cl.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
				Namespace:       namespace,
				MaximumPageSize: 1000,
				NextPageToken:   pageToken,
			})
			if err != nil {
				return fmt.Errorf("list schedules: %w", err)
			}
			for _, entry := range resp.Schedules {
				scheduleIDs = append(scheduleIDs, entry.ScheduleId)
			}
			pageToken = resp.NextPageToken
			if len(pageToken) == 0 {
				break
			}
		}
	}

	if len(scheduleIDs) == 0 {
		fmt.Printf("No schedules found in namespace %q.\n", namespace)
		return nil
	}

	fmt.Printf("Processing %d schedule(s) in namespace %q:\n", len(scheduleIDs), namespace)
	var failed []string
	for _, sid := range scheduleIDs {
		if err := fn(sid); err != nil {
			fmt.Printf("  ERROR %s: %v\n", sid, err)
			failed = append(failed, sid)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d schedule(s) failed: %v", len(failed), failed)
	}
	return nil
}

// fileKey returns a filesystem-safe key for a namespace+scheduleID pair.
func fileKey(namespace, scheduleID string) string {
	sanitize := func(s string) string {
		var b strings.Builder
		for _, r := range s {
			if r == '-' || r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
				b.WriteRune(r)
			} else {
				b.WriteRune('_')
			}
		}
		return b.String()
	}
	return sanitize(namespace) + "_" + sanitize(scheduleID)
}

// deduplicateStructuredCalendarsProto removes duplicate StructuredCalendarSpec
// entries using semantic comparison that normalizes proto default fields before
// comparing, so entries that differ only in representation (e.g. Step 0 vs 1,
// End unset vs End==Start) are treated as equal.
func deduplicateStructuredCalendarsProto(entries []*schedulepb.StructuredCalendarSpec) []*schedulepb.StructuredCalendarSpec {
	out := make([]*schedulepb.StructuredCalendarSpec, 0)
	seenNormalized := make([]*schedulepb.StructuredCalendarSpec, 0)
	for _, e := range entries {
		normalized := normalizeStructuredCalendarProtoForCompare(e)
		duplicate := false
		for _, seen := range seenNormalized {
			if proto.Equal(normalized, seen) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			out = append(out, e)
			seenNormalized = append(seenNormalized, normalized)
		}
	}
	return out
}

// normalizeStructuredCalendarProtoForCompare applies the same Range defaulting
// as cleanSpec in service/worker/scheduler/spec.go: End defaults to Start when
// unset, and Step defaults to 1 when unset. This ensures entries that differ
// only in proto default representation compare as equal.
func normalizeStructuredCalendarProtoForCompare(in *schedulepb.StructuredCalendarSpec) *schedulepb.StructuredCalendarSpec {
	if in == nil {
		return nil
	}
	out := common.CloneProto(in)
	normalizeRanges := func(ranges []*schedulepb.Range) {
		for _, r := range ranges {
			if r == nil {
				continue
			}
			if r.End < r.Start {
				r.End = r.Start
			}
			if r.Step == 0 {
				r.Step = 1
			}
		}
	}
	normalizeRanges(out.Second)
	normalizeRanges(out.Minute)
	normalizeRanges(out.Hour)
	normalizeRanges(out.DayOfMonth)
	normalizeRanges(out.Month)
	normalizeRanges(out.Year)
	normalizeRanges(out.DayOfWeek)
	return out
}

// stdinIsPipe reports whether stdin is connected to a pipe or redirect rather
// than an interactive terminal.
func stdinIsPipe() bool {
	return !term.IsTerminal(int(os.Stdin.Fd()))
}
