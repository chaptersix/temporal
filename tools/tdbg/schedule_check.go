package tdbg

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
)

const (
	chasmDivision = "403648407"

	fqnGeneratorTask     = "scheduler.generate"
	fqnSchedulerIdleTask = "scheduler.idle"
)

var chasmScheduleQuery = fmt.Sprintf(
	"TemporalNamespaceDivision = '%s' AND ExecutionStatus = 'Running' AND TemporalSchedulePaused = false",
	chasmDivision,
)

type scheduleCheckResult struct {
	Namespace      string   `json:"namespace"`
	ScheduleID     string   `json:"scheduleId"`
	HasGenerator   bool     `json:"hasGenerator"`
	HasIdle        bool     `json:"hasIdle"`
	MissingTasks   []string `json:"missingTasks,omitempty"`
	Error          string   `json:"error,omitempty"`
	TaskFQNs       []string `json:"taskFQNs,omitempty"`
}

func AdminCheckSchedules(c *cli.Context, clientFactory ClientFactory) error {
	adminClient := clientFactory.AdminClient(c)

	ns, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	parallelism := c.Int("parallelism")
	if parallelism <= 0 {
		parallelism = 10
	}
	limit := c.Int("limit")

	logger := log.NewNoopLogger()
	registry, err := newChasmRegistry(logger)
	if err != nil {
		return fmt.Errorf("failed to create CHASM registry: %w", err)
	}

	// Determine schedule IDs: explicit flag, piped stdin, or list from server.
	ids, err := getScheduleIDs(c, clientFactory, ns, limit)
	if err != nil {
		return err
	}

	if len(ids) == 0 {
		fmt.Fprintln(c.App.ErrWriter, "No schedules to check.")
		return nil
	}

	fmt.Fprintf(c.App.ErrWriter, "Checking %d schedules...\n", len(ids))

	results := make(chan scheduleCheckResult, len(ids))
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			r := checkScheduleTasks(c, adminClient, registry, ns, id)
			results <- r
		}(id)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	exclude := c.String("exclude")

	enc := json.NewEncoder(c.App.Writer)
	for r := range results {
		if shouldExclude(r, exclude) {
			continue
		}
		if err := enc.Encode(r); err != nil {
			return err
		}
	}

	return nil
}

func getScheduleIDs(c *cli.Context, clientFactory ClientFactory, namespace string, limit int) ([]string, error) {
	// If --schedule-id is provided, check just that one.
	if sid := c.String(FlagScheduleID); sid != "" {
		return []string{sid}, nil
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
			if limit > 0 && len(ids) > limit {
				ids = ids[:limit]
			}
			return ids, nil
		}
	}

	// Otherwise, list all unpaused CHASM schedules from the server.
	wfClient := clientFactory.WorkflowClient(c)
	fmt.Fprintf(c.App.ErrWriter, "Listing unpaused CHASM schedules in %s...\n", namespace)
	return listChasmScheduleIDs(c, wfClient, namespace, limit)
}

func listChasmScheduleIDs(c *cli.Context, wfClient workflowservice.WorkflowServiceClient, namespace string, limit int) ([]string, error) {
	var ids []string
	var nextPageToken []byte

	ctx, cancel := newContext(c)
	defer cancel()

	for {
		resp, err := wfClient.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     namespace,
			Query:         chasmScheduleQuery,
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("listing workflows: %w", err)
		}
		for _, exec := range resp.Executions {
			ids = append(ids, exec.Execution.WorkflowId)
		}
		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			break
		}
		if limit > 0 && len(ids) >= limit {
			ids = ids[:limit]
			break
		}
	}

	return ids, nil
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

	if !result.HasGenerator {
		result.MissingTasks = append(result.MissingTasks, "GeneratorTask")
	}
	if !result.HasIdle {
		result.MissingTasks = append(result.MissingTasks, "SchedulerIdleTask")
	}

	return result
}

// shouldExclude returns true if the result should be excluded from output.
//   - "healthy": exclude schedules that have both generator and idle tasks
//   - "unhealthy": exclude schedules that are missing any task
func shouldExclude(r scheduleCheckResult, exclude string) bool {
	switch strings.ToLower(exclude) {
	case "healthy":
		return r.HasGenerator && r.HasIdle && r.Error == ""
	case "unhealthy":
		return len(r.MissingTasks) > 0 || r.Error != ""
	default:
		return false
	}
}
