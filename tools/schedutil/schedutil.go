// Package schedutil provides operations for remediating degraded schedule workflows.
// Functions in this package are called by the tdbg schedule dedup and force-can commands.
package schedutil

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/worker/scheduler"
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

	beforeJSON, err := json.MarshalIndent(desc.Schedule, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal schedule: %w", err)
	}

	nCalBefore := len(desc.Schedule.Spec.StructuredCalendar)
	desc.Schedule.Spec.StructuredCalendar = deduplicateStructuredCalendarsProto(desc.Schedule.Spec.StructuredCalendar)
	nCalAfter := len(desc.Schedule.Spec.StructuredCalendar)

	afterJSON, err := json.MarshalIndent(desc.Schedule, "", "  ")
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
func RunDedupRecreate(ctx context.Context, cl workflowservice.WorkflowServiceClient, namespace, scheduleID, outDir string, execute bool) error {
	workflowID := scheduler.WorkflowIDPrefix + scheduleID
	// Omitting RunId causes the server to resolve the latest run. The first
	// event of that run is WorkflowExecutionStarted, whose Input carries the
	// full StartScheduleArgs — including the accumulated spec — as of the most
	// recent continue-as-new. This is the most current durable state available
	// for a schedule whose workflow is too degraded to answer queries.
	resp, err := cl.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       namespace,
		Execution:       &commonpb.WorkflowExecution{WorkflowId: workflowID},
		MaximumPageSize: 1,
	})
	if err != nil {
		return fmt.Errorf("get workflow history: %w", err)
	}
	if len(resp.History.Events) == 0 {
		return fmt.Errorf("no history events found for %s", workflowID)
	}
	attrs := resp.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	if attrs == nil {
		return errors.New("first event is not WorkflowExecutionStarted")
	}
	// Verify this is a scheduler workflow before attempting to decode its input.
	if attrs.WorkflowType.GetName() != scheduler.WorkflowType {
		return fmt.Errorf("workflow %s has unexpected type %q (expected %q); not a schedule workflow",
			workflowID, attrs.WorkflowType.GetName(), scheduler.WorkflowType)
	}

	var args schedulespb.StartScheduleArgs
	if err := payloads.Decode(attrs.Input, &args); err != nil {
		return fmt.Errorf("decode StartScheduleArgs: %w", err)
	}

	beforeJSON, err := protojson.MarshalOptions{Multiline: true}.Marshal(args.Schedule)
	if err != nil {
		return fmt.Errorf("marshal before schedule: %w", err)
	}

	spec := args.Schedule.Spec
	nCalBefore := len(spec.StructuredCalendar)
	spec.StructuredCalendar = deduplicateStructuredCalendarsProto(spec.StructuredCalendar)
	nCalAfter := len(spec.StructuredCalendar)

	if nCalBefore == nCalAfter {
		fmt.Printf("  %s: no duplicates found\n", scheduleID)
		return nil
	}

	afterJSON, err := protojson.MarshalOptions{Multiline: true}.Marshal(args.Schedule)
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
		Schedule:   args.Schedule,
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
	out := &schedulepb.StructuredCalendarSpec{}
	proto.Merge(out, in)
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
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice == 0
}
