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
	enumspb "go.temporal.io/api/enums/v1"
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
	fixPauseNote           = "tdbg-fix: temporarily paused to regenerate tasks"
	fqnGeneratorComponent  = "scheduler.generator"
	fqnSchedulerComponent  = "scheduler.scheduler"
	defaultFixParallelism  = 10
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
	// Each namespace gets one worker goroutine with its own schedule-level
	// semaphore, so --parallelism is the true cap per namespace.
	// nsSem gates how many namespace workers run concurrently.
	summary := &fixSummary{}
	nsSem := make(chan struct{}, nsParallelism)
	var nsWg sync.WaitGroup

	// startNSWorker spins up a single goroutine for a namespace that drains
	// schedule IDs from the returned channel.
	startNSWorker := func(ns string) chan<- string {
		ch := make(chan string, 100)
		nsWg.Add(1)
		go func() {
			defer nsWg.Done()
			nsSem <- struct{}{}
			defer func() { <-nsSem }()

			sem := make(chan struct{}, parallelism)
			var wg sync.WaitGroup
			total := 0
			for sid := range ch {
				total++
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
			fmt.Fprintf(c.App.ErrWriter, "[%s] Done. %d processed.\n", ns, total)
		}()
		return ch
	}

	scanner := bufio.NewScanner(os.Stdin)
	var currentNS string
	var nsCh chan<- string

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
			if nsCh != nil {
				close(nsCh)
			}
			currentNS = input.Namespace
			nsCh = startNSWorker(currentNS)
			fmt.Fprintf(c.App.ErrWriter, "[%s] Starting fix (parallelism=%d)...\n", currentNS, parallelism)
		}
		nsCh <- input.ScheduleID
	}
	if nsCh != nil {
		close(nsCh)
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

	// Single DescribeMutableState call to get everything: pause status, notes,
	// policies, task presence, and generator high watermark.
	inspection, err := inspectSchedule(c, adminClient, registry, namespace, scheduleID)
	if err != nil {
		result.Action = "error"
		result.Error = err.Error()
		return result
	}

	hwmStr := "<nil>"
	if inspection.highWatermark != nil {
		hwmStr = inspection.highWatermark.AsTime().UTC().Format(time.RFC3339)
	}
	fmt.Fprintf(c.App.ErrWriter, "[%s/%s] paused=%v notes=%q catchupWindow=%s overlapPolicy=%s hwm=%s hasGenerator=%v hasIdle=%v taskFQNs=%v\n",
		namespace, scheduleID,
		inspection.isPaused,
		inspection.notes,
		inspection.catchupWindow,
		inspection.overlapPolicy,
		hwmStr,
		inspection.checkResult.HasGenerator,
		inspection.checkResult.HasIdle,
		inspection.checkResult.TaskFQNs,
	)

	// If paused by someone else (note doesn't contain our marker), skip.
	if inspection.isPaused && !strings.Contains(inspection.notes, fixPauseNote) {
		result.Action = "skipped"
		result.Reason = fmt.Sprintf("already paused (notes: %q)", inspection.notes)
		return result
	}

	// Check task presence (unless --force or resuming from our own pause).
	if !inspection.isPaused && !c.Bool("force") {
		if inspection.checkResult.HasGenerator || inspection.checkResult.HasIdle {
			result.Action = "skipped"
			result.Reason = "not missing tasks"
			return result
		}
	}

	now := time.Now().UTC()
	catchupWindow := inspection.catchupWindow
	overlapPolicy := inspection.overlapPolicy
	highWatermark := inspection.highWatermark

	if highWatermark != nil {
		result.HighWatermark = highWatermark.AsTime().UTC().Format(time.RFC3339)
	}
	result.BackfillEnd = now.Format(time.RFC3339)
	result.CatchupWindow = catchupWindow.String()
	result.OverlapPolicy = overlapPolicy.String()

	// Pause with marker note (skip if we already paused it).
	if !inspection.isPaused {
		ctx, cancel := newContext(c)
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
	// Note: this will overwrite the schedule's existing pause note.
	unpausePatch := &schedulepb.SchedulePatch{
		Unpause: " ",
	}

	if !c.Bool("no-backfill") && highWatermark != nil {
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

	ctx, cancel := newContext(c)
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

	result.Action = "fixed"
	return result
}

type scheduleInspection struct {
	isPaused      bool
	notes         string
	catchupWindow time.Duration
	overlapPolicy enumspb.ScheduleOverlapPolicy
	highWatermark *timestamppb.Timestamp
	checkResult   scheduleCheckResult
}

// inspectSchedule uses a single DescribeMutableState call to extract pause
// status, notes, policies, task presence, and generator high watermark.
func inspectSchedule(
	c *cli.Context,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	namespace string,
	scheduleID string,
) (*scheduleInspection, error) {
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
		return nil, fmt.Errorf("describe mutable state: %w", err)
	}

	chasmNodes := resp.GetDatabaseMutableState().GetChasmNodes()
	if len(chasmNodes) == 0 {
		return nil, fmt.Errorf("no CHASM nodes found")
	}

	inspection := &scheduleInspection{}
	inspection.checkResult = scheduleCheckResult{
		Namespace:  namespace,
		ScheduleID: scheduleID,
	}

	var allTaskFQNs []string

	for _, node := range chasmNodes {
		componentAttr := node.GetMetadata().GetComponentAttributes()
		if componentAttr == nil {
			continue
		}

		componentFQN, _ := registry.ComponentFqnByID(componentAttr.GetTypeId())

		switch componentFQN {
		case fqnSchedulerComponent:
			// Decode root scheduler state for pause status, notes, policies.
			dataBlob := node.GetData()
			if dataBlob != nil && len(dataBlob.GetData()) > 0 {
				var schedState schedulerpb.SchedulerState
				if err := serialization.Decode(dataBlob, &schedState); err == nil {
					scheduleState := schedState.GetSchedule().GetState()
					inspection.isPaused = scheduleState.GetPaused()
					inspection.notes = scheduleState.GetNotes()

					policies := schedState.GetSchedule().GetPolicies()
					inspection.catchupWindow = policies.GetCatchupWindow().AsDuration()
					inspection.overlapPolicy = policies.GetOverlapPolicy()
				}
			}

		case fqnGeneratorComponent:
			// Decode generator state for high watermark.
			dataBlob := node.GetData()
			if dataBlob != nil && len(dataBlob.GetData()) > 0 {
				var genState schedulerpb.GeneratorState
				if err := serialization.Decode(dataBlob, &genState); err == nil {
					inspection.highWatermark = genState.LastProcessedTime
				}
			}
		}

		// Collect task FQNs from all component nodes.
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

	inspection.checkResult.TaskFQNs = allTaskFQNs
	for _, taskFQN := range allTaskFQNs {
		switch taskFQN {
		case fqnGeneratorTask:
			inspection.checkResult.HasGenerator = true
		case fqnSchedulerIdleTask:
			inspection.checkResult.HasIdle = true
		}
	}

	return inspection, nil
}

