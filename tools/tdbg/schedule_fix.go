package tdbg

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	fixPauseNote          = "tdbg-fix: temporarily paused to regenerate tasks"
	fqnGeneratorComponent = "scheduler.generator"
	defaultFixParallelism = 10
)

type scheduleFixResult struct {
	Namespace      string `json:"namespace"`
	ScheduleID     string `json:"scheduleId"`
	Action         string `json:"action"` // "fixed", "skipped", "error"
	Reason         string `json:"reason,omitempty"`
	Error          string `json:"error,omitempty"`
	HighWatermark  string `json:"highWatermark,omitempty"`
	BackfillStart  string `json:"backfillStart,omitempty"`
	BackfillEnd    string `json:"backfillEnd,omitempty"`
	Backfilled     bool   `json:"backfilled"`
	CatchupWindow  string `json:"catchupWindow,omitempty"`
	OverlapPolicy  string `json:"overlapPolicy,omitempty"`
}

type fixInput struct {
	Namespace  string `json:"namespace"`
	ScheduleID string `json:"scheduleId"`
}

type fixSummary struct {
	mu      sync.Mutex
	fixed   int
	skipped int
	errors  int
}

func (s *fixSummary) record(action string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch action {
	case "fixed":
		s.fixed++
	case "skipped":
		s.skipped++
	default:
		s.errors++
	}
}

func AdminFixSchedule(c *cli.Context, clientFactory ClientFactory) error {
	adminClient := clientFactory.AdminClient(c)
	wfClient := clientFactory.WorkflowClient(c)

	logger := log.NewNoopLogger()
	registry, err := newChasmRegistry(logger)
	if err != nil {
		return fmt.Errorf("failed to create CHASM registry: %w", err)
	}

	parallelism := c.Int("parallelism")
	if parallelism <= 0 {
		parallelism = intFromEnv("TDBG_FIX_PARALLELISM", defaultFixParallelism)
	}
	nsParallelism := c.Int("ns-parallelism")
	if nsParallelism <= 0 {
		nsParallelism = intFromEnv("TDBG_FIX_NS_PARALLELISM", defaultScheduleCheckNsParallel)
	}

	enc := &threadSafeEncoder{enc: json.NewEncoder(c.App.Writer)}

	// Single schedule via flags.
	if stat, _ := os.Stdin.Stat(); (stat.Mode() & os.ModeCharDevice) != 0 {
		ns := c.String(FlagNamespace)
		sid := c.String(FlagScheduleID)
		if ns == "" || sid == "" {
			return fmt.Errorf("provide -n and --schedule-id, or pipe JSONL with namespace and scheduleId fields")
		}
		result := fixSchedule(c, wfClient, adminClient, registry, ns, sid)
		return enc.Encode(result)
	}

	// Streaming piped input — assumes sorted by namespace.
	// Group consecutive lines by namespace, dispatch each group with ns-parallelism.
	summary := &fixSummary{}
	nsSem := make(chan struct{}, nsParallelism)
	var nsWg sync.WaitGroup

	scanner := bufio.NewScanner(os.Stdin)
	var currentNS string
	var batch []string

	flushBatch := func(ns string, ids []string) {
		nsWg.Add(1)
		nsSem <- struct{}{}
		go func() {
			defer nsWg.Done()
			defer func() { <-nsSem }()

			fmt.Fprintf(c.App.ErrWriter, "[%s] Fixing %d schedules (parallelism=%d)...\n", ns, len(ids), parallelism)

			sem := make(chan struct{}, parallelism)
			var wg sync.WaitGroup
			for _, sid := range ids {
				wg.Add(1)
				go func(id string) {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()

					result := fixSchedule(c, wfClient, adminClient, registry, ns, id)
					_ = enc.Encode(result)
					summary.record(result.Action)
				}(sid)
			}
			wg.Wait()
			fmt.Fprintf(c.App.ErrWriter, "[%s] Done.\n", ns)
		}()
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var input fixInput
		if json.Unmarshal([]byte(line), &input) != nil || input.Namespace == "" || input.ScheduleID == "" {
			continue
		}

		if input.Namespace != currentNS {
			if len(batch) > 0 {
				flushBatch(currentNS, batch)
				batch = nil
			}
			currentNS = input.Namespace
		}
		batch = append(batch, input.ScheduleID)
		if len(batch) >= 100 {
			flushBatch(currentNS, batch)
			batch = nil
		}
	}
	if len(batch) > 0 {
		flushBatch(currentNS, batch)
	}

	nsWg.Wait()

	fmt.Fprintf(c.App.ErrWriter, "Done. %d fixed, %d skipped, %d errors\n",
		summary.fixed, summary.skipped, summary.errors)
	return nil
}

func fixSchedule(
	c *cli.Context,
	wfClient workflowservice.WorkflowServiceClient,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	namespace string,
	scheduleID string,
) scheduleFixResult {
	result := scheduleFixResult{
		Namespace:  namespace,
		ScheduleID: scheduleID,
	}

	// Step 1: Describe schedule to check pause status.
	ctx, cancel := newContext(c)
	descResp, err := wfClient.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  namespace,
		ScheduleId: scheduleID,
	})
	cancel()
	if err != nil {
		result.Action = "error"
		result.Error = fmt.Sprintf("describe schedule: %v", err)
		return result
	}

	state := descResp.GetSchedule().GetState()
	isPaused := state.GetPaused()
	notes := state.GetNotes()

	// If paused by someone else (note doesn't contain our marker), skip.
	if isPaused && !strings.Contains(notes, fixPauseNote) {
		result.Action = "skipped"
		result.Reason = fmt.Sprintf("already paused (notes: %q)", notes)
		return result
	}

	// Step 2: Check CHASM tasks and extract generator high watermark.
	var highWatermark *timestamppb.Timestamp
	skipTaskCheck := c.Bool("force")
	if !isPaused {
		checkResult, hwm := checkScheduleTasksWithWatermark(c, adminClient, registry, namespace, scheduleID)
		if checkResult.Error != "" {
			result.Action = "error"
			result.Error = fmt.Sprintf("check tasks: %s", checkResult.Error)
			return result
		}
		if !skipTaskCheck && !isMissingTasks(checkResult) {
			result.Action = "skipped"
			result.Reason = "not missing tasks"
			return result
		}
		highWatermark = hwm
	} else {
		// Already paused by us from a prior run — still need the watermark.
		_, hwm := checkScheduleTasksWithWatermark(c, adminClient, registry, namespace, scheduleID)
		highWatermark = hwm
	}

	now := time.Now().UTC()
	policies := descResp.GetSchedule().GetPolicies()
	catchupWindow := policies.GetCatchupWindow().AsDuration()
	overlapPolicy := policies.GetOverlapPolicy()

	if highWatermark != nil {
		result.HighWatermark = highWatermark.AsTime().UTC().Format(time.RFC3339)
	}
	result.BackfillEnd = now.Format(time.RFC3339)

	// Step 3: Pause with marker note (skip if we already paused it).
	if !isPaused {
		ctx, cancel = newContext(c)
		_, err = wfClient.PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{
				Pause: fixPauseNote,
			},
		})
		cancel()
		if err != nil {
			result.Action = "error"
			result.Error = fmt.Sprintf("pause: %v", err)
			return result
		}
	}

	// Unpause with backfill. By default, backfill from max(HWM, now-catchupWindow)
	// to now — only covering the range the generator would naturally process.
	// With --skip-catchup-window, backfill from HWM to now regardless.
	unpausePatch := &schedulepb.SchedulePatch{
		Unpause: " ",
	}
	result.CatchupWindow = catchupWindow.String()
	result.OverlapPolicy = overlapPolicy.String()

	if highWatermark != nil {
		hwmTime := highWatermark.AsTime()
		backfillStart := hwmTime

		if !c.Bool("skip-catchup-window") && catchupWindow > 0 {
			catchupBoundary := now.Add(-catchupWindow)
			if catchupBoundary.After(backfillStart) {
				backfillStart = catchupBoundary
			}
		}

		result.BackfillStart = backfillStart.UTC().Format(time.RFC3339)
		result.Backfilled = true
		unpausePatch.BackfillRequest = []*schedulepb.BackfillRequest{
			{
				StartTime: timestamppb.New(backfillStart),
				EndTime:   timestamppb.New(now),
			},
		}
	}

	ctx, cancel = newContext(c)
	_, err = wfClient.PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
		Namespace:  namespace,
		ScheduleId: scheduleID,
		Patch:      unpausePatch,
	})
	cancel()
	if err != nil {
		result.Action = "error"
		result.Error = fmt.Sprintf("unpause: %v", err)
		return result
	}

	// Step 5: Verify tasks were regenerated.
	verifyResult, _ := checkScheduleTasksWithWatermark(c, adminClient, registry, namespace, scheduleID)
	if verifyResult.Error != "" {
		result.Action = "error"
		result.Error = fmt.Sprintf("verify after fix: %s", verifyResult.Error)
		return result
	}
	if isMissingTasks(verifyResult) {
		result.Action = "error"
		result.Error = "still missing tasks after pause/unpause cycle"
		return result
	}

	result.Action = "fixed"
	return result
}

// checkScheduleTasksWithWatermark extends checkScheduleTasks to also extract
// the generator's LastProcessedTime (high watermark) from the CHASM tree.
func checkScheduleTasksWithWatermark(
	c *cli.Context,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	namespace string,
	scheduleID string,
) (scheduleCheckResult, *timestamppb.Timestamp) {
	result := scheduleCheckResult{
		Namespace:  namespace,
		ScheduleID: scheduleID,
	}

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: scheduleID,
		},
		Archetype: chasm.SchedulerArchetype,
	})
	if err != nil {
		result.Error = err.Error()
		return result, nil
	}

	chasmNodes := resp.GetDatabaseMutableState().GetChasmNodes()
	if len(chasmNodes) == 0 {
		result.Error = "no CHASM nodes found"
		return result, nil
	}

	var highWatermark *timestamppb.Timestamp
	var allTaskFQNs []string

	for _, node := range chasmNodes {
		componentAttr := node.GetMetadata().GetComponentAttributes()
		if componentAttr == nil {
			continue
		}

		// Check if this is the generator component and extract its state.
		componentFQN, _ := registry.ComponentFqnByID(componentAttr.GetTypeId())
		if componentFQN == fqnGeneratorComponent {
			dataBlob := node.GetData()
			if dataBlob != nil && len(dataBlob.GetData()) > 0 {
				var genState schedulerpb.GeneratorState
				if err := serialization.Decode(dataBlob, &genState); err == nil {
					highWatermark = genState.LastProcessedTime
				}
			}
		}

		for _, task := range componentAttr.GetSideEffectTasks() {
			taskFQN, _ := registry.TaskFqnByID(task.GetTypeId())
			if taskFQN != "" {
				allTaskFQNs = append(allTaskFQNs, taskFQN)
			}
		}
		for _, task := range componentAttr.GetPureTasks() {
			taskFQN, _ := registry.TaskFqnByID(task.GetTypeId())
			if taskFQN != "" {
				allTaskFQNs = append(allTaskFQNs, taskFQN)
			}
		}
	}

	result.TaskFQNs = allTaskFQNs
	for _, taskFQN := range allTaskFQNs {
		switch taskFQN {
		case fqnGeneratorTask:
			result.HasGenerator = true
		case fqnSchedulerIdleTask:
			result.HasIdle = true
		}
	}

	return result, highWatermark
}
