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
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	fqnGeneratorTask     = "scheduler.generate"
	fqnSchedulerIdleTask = "scheduler.idle"

	scheduleCheckPageSize    = 100
	scheduleCheckParallelism = 3
	scheduleCheckPageDelay   = time.Second
)

var chasmScheduleQuery = fmt.Sprintf(
	"TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Running' AND TemporalSchedulePaused = false",
	chasm.SchedulerArchetypeID,
)

type scheduleCheckResult struct {
	Namespace    string   `json:"namespace"`
	ScheduleID   string   `json:"scheduleId"`
	HasGenerator bool     `json:"hasGenerator"`
	HasIdle      bool     `json:"hasIdle"`
	Error        string   `json:"error,omitempty"`
	TaskFQNs     []string `json:"taskFQNs,omitempty"`
}

func isMissingTasks(r scheduleCheckResult) bool {
	return (!r.HasGenerator && !r.HasIdle) || r.Error != ""
}

func AdminCheckSchedules(c *cli.Context, clientFactory ClientFactory) error {
	adminClient := clientFactory.AdminClient(c)

	ns, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	parallelism := c.Int("parallelism")
	if parallelism <= 0 {
		parallelism = scheduleCheckParallelism
	}
	onlyMissing := c.Bool("only-missing")

	logger := log.NewNoopLogger()
	registry, err := newChasmRegistry(logger)
	if err != nil {
		return fmt.Errorf("failed to create CHASM registry: %w", err)
	}

	enc := json.NewEncoder(c.App.Writer)
	emit := func(r scheduleCheckResult) error {
		if onlyMissing && !isMissingTasks(r) {
			return nil
		}
		return enc.Encode(r)
	}

	// If IDs come from flag or stdin, process them as a single batch.
	if ids, ok, err := getExplicitScheduleIDs(c); err != nil {
		return err
	} else if ok {
		fmt.Fprintf(c.App.ErrWriter, "Checking %d schedules...\n", len(ids))
		return processPage(c, adminClient, registry, ns, ids, parallelism, emit)
	}

	// Otherwise, list from server page by page, processing each page before fetching the next.
	wfClient := clientFactory.WorkflowClient(c)
	fmt.Fprintf(c.App.ErrWriter, "Listing unpaused CHASM schedules in %s...\n", ns)

	var nextPageToken []byte
	total := 0
	pageNum := 0

	for {
		ctx, cancel := newContext(c)
		resp, err := wfClient.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     ns,
			PageSize:      scheduleCheckPageSize,
			Query:         chasmScheduleQuery,
			NextPageToken: nextPageToken,
		})
		cancel()
		if err != nil {
			if retryAfter, ok := retryableRateLimit(err); ok {
				fmt.Fprintf(c.App.ErrWriter, "Rate limited, retrying in %v...\n", retryAfter)
				time.Sleep(retryAfter)
				continue
			}
			return fmt.Errorf("listing workflows: %w", err)
		}

		var ids []string
		for _, exec := range resp.Executions {
			ids = append(ids, exec.Execution.WorkflowId)
		}

		if len(ids) > 0 {
			pageNum++
			total += len(ids)
			fmt.Fprintf(c.App.ErrWriter, "Page %d: checking %d schedules (%d total)...\n", pageNum, len(ids), total)

			if err := processPage(c, adminClient, registry, ns, ids, parallelism, emit); err != nil {
				return err
			}
		}

		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			break
		}

		time.Sleep(scheduleCheckPageDelay)
	}

	if total == 0 {
		fmt.Fprintln(c.App.ErrWriter, "No unpaused CHASM schedules found.")
	}

	return nil
}

func getExplicitScheduleIDs(c *cli.Context) ([]string, bool, error) {
	// If --schedule-id is provided, check just that one.
	if sid := c.String(FlagScheduleID); sid != "" {
		return []string{sid}, true, nil
	}

	// If stdin is piped, read schedule IDs from it.
	if stat, _ := os.Stdin.Stat(); (stat.Mode() & os.ModeCharDevice) == 0 {
		var ids []string
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				ids = append(ids, line)
			}
		}
		if len(ids) > 0 {
			return ids, true, nil
		}
	}

	return nil, false, nil
}

func processPage(
	c *cli.Context,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	namespace string,
	ids []string,
	parallelism int,
	emit func(scheduleCheckResult) error,
) error {
	results := make(chan scheduleCheckResult, len(ids))
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			results <- checkScheduleTasks(c, adminClient, registry, namespace, id)
		}(id)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		if err := emit(r); err != nil {
			return err
		}
	}

	return nil
}

func checkScheduleTasks(
	c *cli.Context,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	namespace string,
	scheduleID string,
) scheduleCheckResult {
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
		return result
	}

	chasmNodes := resp.GetDatabaseMutableState().GetChasmNodes()
	if len(chasmNodes) == 0 {
		result.Error = "no CHASM nodes found"
		return result
	}

	var allTaskFQNs []string
	for _, node := range chasmNodes {
		componentAttr := node.GetMetadata().GetComponentAttributes()
		if componentAttr == nil {
			continue
		}
		for _, task := range componentAttr.GetSideEffectTasks() {
			fqn, _ := registry.TaskFqnByID(task.GetTypeId())
			if fqn != "" {
				allTaskFQNs = append(allTaskFQNs, fqn)
			}
		}
		for _, task := range componentAttr.GetPureTasks() {
			fqn, _ := registry.TaskFqnByID(task.GetTypeId())
			if fqn != "" {
				allTaskFQNs = append(allTaskFQNs, fqn)
			}
		}
	}

	result.TaskFQNs = allTaskFQNs

	for _, fqn := range allTaskFQNs {
		switch fqn {
		case fqnGeneratorTask:
			result.HasGenerator = true
		case fqnSchedulerIdleTask:
			result.HasIdle = true
		}
	}

	return result
}

func retryableRateLimit(err error) (time.Duration, bool) {
	if s, ok := status.FromError(err); ok && s.Code() == codes.ResourceExhausted {
		return time.Second, true
	}
	return 0, false
}
