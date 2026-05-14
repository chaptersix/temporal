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

	scheduleCheckPageSize       = 100
	scheduleCheckParallelism    = 3
	scheduleCheckNsParallelism  = 3
)

var chasmScheduleBaseQuery = fmt.Sprintf(
	"TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Running' AND TemporalSchedulePaused = false",
	chasm.SchedulerArchetypeID,
)

func buildScheduleQuery(after, before string) string {
	q := chasmScheduleBaseQuery
	if after != "" {
		q += fmt.Sprintf(" AND StartTime >= '%s'", after)
	}
	if before != "" {
		q += fmt.Sprintf(" AND StartTime <= '%s'", before)
	}
	return q
}

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

type threadSafeEncoder struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func (e *threadSafeEncoder) Encode(v any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.enc.Encode(v)
}

func AdminCheckSchedules(c *cli.Context, clientFactory ClientFactory) error {
	adminClient := clientFactory.AdminClient(c)
	wfClient := clientFactory.WorkflowClient(c)

	namespaces := getNamespaces(c)
	if len(namespaces) == 0 {
		return fmt.Errorf("no namespaces provided; pass via -n flag, positional args, or piped stdin")
	}

	parallelism := c.Int("parallelism")
	if parallelism <= 0 {
		parallelism = scheduleCheckParallelism
	}
	nsParallelism := c.Int("ns-parallelism")
	if nsParallelism <= 0 {
		nsParallelism = scheduleCheckNsParallelism
	}
	onlyMissing := c.Bool("only-missing")
	query := buildScheduleQuery(c.String("after"), c.String("before"))

	logger := log.NewNoopLogger()
	registry, err := newChasmRegistry(logger)
	if err != nil {
		return fmt.Errorf("failed to create CHASM registry: %w", err)
	}

	enc := &threadSafeEncoder{enc: json.NewEncoder(c.App.Writer)}
	emit := func(r scheduleCheckResult) error {
		if onlyMissing && !isMissingTasks(r) {
			return nil
		}
		return enc.Encode(r)
	}

	// If --schedule-id is provided, check just that one across the first namespace.
	if sid := c.String(FlagScheduleID); sid != "" {
		fmt.Fprintf(c.App.ErrWriter, "Checking schedule %s in %s...\n", sid, namespaces[0])
		return processPage(c, adminClient, registry, namespaces[0], []string{sid}, parallelism, emit)
	}

	// Fan out across namespaces.
	nsSem := make(chan struct{}, nsParallelism)
	var wg sync.WaitGroup
	errCh := make(chan error, len(namespaces))

	for _, ns := range namespaces {
		wg.Add(1)
		go func(ns string) {
			defer wg.Done()
			nsSem <- struct{}{}
			defer func() { <-nsSem }()

			if err := checkNamespace(c, wfClient, adminClient, registry, ns, query, parallelism, emit); err != nil {
				errCh <- fmt.Errorf("%s: %w", ns, err)
			}
		}(ns)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		fmt.Fprintf(c.App.ErrWriter, "ERROR: %v\n", err)
	}

	return nil
}

func getNamespaces(c *cli.Context) []string {
	// Positional args first.
	if c.Args().Len() > 0 {
		return c.Args().Slice()
	}

	// Piped stdin.
	if stat, _ := os.Stdin.Stat(); (stat.Mode() & os.ModeCharDevice) == 0 {
		var namespaces []string
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				namespaces = append(namespaces, line)
			}
		}
		if len(namespaces) > 0 {
			return namespaces
		}
	}

	// Fall back to -n flag.
	if ns := c.String(FlagNamespace); ns != "" && ns != "default" {
		return []string{ns}
	}

	return nil
}

func checkNamespace(
	c *cli.Context,
	wfClient workflowservice.WorkflowServiceClient,
	adminClient adminservice.AdminServiceClient,
	registry *chasm.Registry,
	namespace string,
	query string,
	parallelism int,
	emit func(scheduleCheckResult) error,
) error {
	fmt.Fprintf(c.App.ErrWriter, "Listing unpaused CHASM schedules in %s...\n", namespace)

	var nextPageToken []byte
	total := 0
	pageNum := 0

	for {
		ctx, cancel := newContext(c)
		resp, err := wfClient.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     namespace,
			PageSize:      scheduleCheckPageSize,
			Query:         query,
			NextPageToken: nextPageToken,
		})
		cancel()
		if err != nil {
			if retryAfter, ok := retryableRateLimit(err); ok {
				fmt.Fprintf(c.App.ErrWriter, "[%s] Rate limited, retrying in %v...\n", namespace, retryAfter)
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
			fmt.Fprintf(c.App.ErrWriter, "[%s] Page %d: checking %d schedules (%d total)...\n", namespace, pageNum, len(ids), total)

			if err := processPage(c, adminClient, registry, namespace, ids, parallelism, emit); err != nil {
				return err
			}
		}

		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			break
		}
	}

	if total == 0 {
		fmt.Fprintf(c.App.ErrWriter, "[%s] No unpaused CHASM schedules found.\n", namespace)
	}

	return nil
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
