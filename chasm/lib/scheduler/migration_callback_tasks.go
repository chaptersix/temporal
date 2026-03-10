package scheduler

import (
	"context"
	"fmt"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/fx"
)

type (
	MigrationCallbackTaskExecutorOptions struct {
		fx.In

		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		FrontendClient workflowservice.WorkflowServiceClient
	}

	MigrationCallbackTaskExecutor struct {
		metricsHandler metrics.Handler
		baseLogger     log.Logger
		frontendClient workflowservice.WorkflowServiceClient
	}
)

func NewMigrationCallbackTaskExecutor(opts MigrationCallbackTaskExecutorOptions) *MigrationCallbackTaskExecutor {
	return &MigrationCallbackTaskExecutor{
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
		frontendClient: opts.FrontendClient,
	}
}

func (e *MigrationCallbackTaskExecutor) Validate(
	ctx chasm.Context,
	scheduler *Scheduler,
	_ chasm.TaskAttributes,
	_ *schedulerpb.MigrationCallbackTask,
) (bool, error) {
	// Valid as long as there are running workflows that need callbacks attached.
	invoker := scheduler.Invoker.Get(ctx)
	return len(invoker.runningWorkflowExecutions()) > 0, nil
}

func (e *MigrationCallbackTaskExecutor) Execute(
	ctx context.Context,
	schedulerRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *schedulerpb.MigrationCallbackTask,
) error {
	var scheduler *Scheduler
	var callback *commonpb.Callback
	var runningWorkflows []*commonpb.WorkflowExecution

	// Read the scheduler state and generate a callback.
	_, err := chasm.ReadComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, ctx chasm.Context, _ any) (struct{}, error) {
			scheduler = &Scheduler{
				SchedulerState:     common.CloneProto(s.SchedulerState),
				cacheConflictToken: s.cacheConflictToken,
			}

			invoker := s.Invoker.Get(ctx)
			runningWorkflows = invoker.runningWorkflowExecutions()

			cb, err := chasm.GenerateNexusCallback(ctx, s)
			if err != nil {
				return struct{}{}, err
			}
			callback = common.CloneProto(cb)

			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read scheduler component: %w", err)
	}

	if len(runningWorkflows) == 0 {
		return nil
	}

	logger := newTaggedLogger(e.baseLogger, scheduler)
	metricsHandler := newTaggedMetricsHandler(e.metricsHandler, scheduler)

	// Attach completion callbacks to all running workflows concurrently.
	var wg sync.WaitGroup
	var errCount int
	var mu sync.Mutex

	for _, wf := range runningWorkflows {
		wg.Go(func() {
			if err := e.attachCallback(ctx, logger, scheduler, wf, callback); err != nil {
				logger.Warn("failed to attach migration callback to running workflow",
					tag.Error(err),
					tag.WorkflowID(wf.WorkflowId),
					tag.WorkflowRunID(wf.RunId),
				)
				mu.Lock()
				errCount++
				mu.Unlock()
				return
			}
			logger.Info("attached migration callback to running workflow",
				tag.WorkflowID(wf.WorkflowId),
				tag.WorkflowRunID(wf.RunId),
			)
		})
	}
	wg.Wait()

	if errCount > 0 {
		metricsHandler.Counter(metrics.ScheduleMigrationCallbackErrors.Name()).Record(int64(errCount))
		return fmt.Errorf("failed to attach callbacks to %d of %d running workflows", errCount, len(runningWorkflows))
	}

	return nil
}

// attachCallback attaches a CHASM completion callback to an already-running
// workflow by sending a StartWorkflowExecution request with USE_EXISTING
// conflict policy and AttachCompletionCallbacks set.
func (e *MigrationCallbackTaskExecutor) attachCallback(
	ctx context.Context,
	logger log.Logger,
	scheduler *Scheduler,
	wf *commonpb.WorkflowExecution,
	callback *commonpb.Callback,
) error {
	requestSpec := scheduler.Schedule.GetAction().GetStartWorkflow()

	request := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                scheduler.Namespace,
		WorkflowId:               wf.WorkflowId,
		WorkflowType:             requestSpec.GetWorkflowType(),
		TaskQueue:                requestSpec.GetTaskQueue(),
		Identity:                 scheduler.identity(),
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		RequestId:                fmt.Sprintf("migration-callback-%s-%s", scheduler.ScheduleId, wf.RunId),
		CompletionCallbacks:      []*commonpb.Callback{callback},
		OnConflictOptions: &workflowpb.OnConflictOptions{
			AttachRequestId:           true,
			AttachCompletionCallbacks: true,
		},
	}

	resp, err := e.frontendClient.StartWorkflowExecution(ctx, request)
	if err != nil {
		return err
	}
	if resp.Started {
		// The workflow had already completed, so USE_EXISTING started a new
		// one instead of attaching to the existing run. Terminate it
		// immediately.
		//
		// TODO: replace with an API that can attach callbacks without this
		// race (e.g. UpdateWorkflow with callback attachment).
		logger.Warn("migration callback started unintended workflow, terminating",
			tag.WorkflowID(wf.WorkflowId),
			tag.WorkflowRunID(resp.RunId),
		)
		_, _ = e.frontendClient.TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace: scheduler.Namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: wf.WorkflowId,
				RunId:      resp.RunId,
			},
			Reason:   "migration callback task: workflow started unintentionally",
			Identity: scheduler.identity(),
		})
	}
	return nil
}
