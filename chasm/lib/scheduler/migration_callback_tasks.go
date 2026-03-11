package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
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
	var workflows []runningWorkflow

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
			workflows = invoker.runningWorkflows()

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

	if len(workflows) == 0 {
		return nil
	}

	logger := newTaggedLogger(e.baseLogger, scheduler)
	metricsHandler := newTaggedMetricsHandler(e.metricsHandler, scheduler)

	logger.Info("migration callback task executing",
		tag.NewInt("running-workflow-count", len(workflows)),
		tag.NewStringTag("callback-url", callback.GetNexus().GetUrl()),
	)

	// Attach completion callbacks to all running workflows concurrently.
	var wg sync.WaitGroup
	var errCount int
	var mu sync.Mutex

	for _, wf := range workflows {
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
		return fmt.Errorf("failed to attach callbacks to %d of %d running workflows", errCount, len(workflows))
	}

	return nil
}

// attachCallback attaches a CHASM completion callback to an already-running
// workflow by sending a StartWorkflowExecution request with USE_EXISTING
// conflict policy, REJECT_DUPLICATE reuse policy, and AttachCompletionCallbacks
// set. It uses the BufferedStart's RequestId so that the callback completion
// can be matched back to the correct running workflow in HandleNexusCompletion.
func (e *MigrationCallbackTaskExecutor) attachCallback(
	ctx context.Context,
	logger log.Logger,
	scheduler *Scheduler,
	wf runningWorkflow,
	callback *commonpb.Callback,
) error {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: wf.WorkflowId,
		RunId:      wf.RunId,
	}

	// Verify the workflow exists before attempting to attach.
	_, err := e.frontendClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: scheduler.Namespace,
		Execution: execution,
	})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			logger.Warn("workflow not found, skipping callback attachment",
				tag.WorkflowID(wf.WorkflowId),
				tag.WorkflowRunID(wf.RunId),
			)
			return nil
		}
		return fmt.Errorf("failed to describe workflow %s/%s: %w", wf.WorkflowId, wf.RunId, err)
	}

	requestSpec := scheduler.Schedule.GetAction().GetStartWorkflow()

	logger.Info("attaching callback to workflow",
		tag.WorkflowID(wf.WorkflowId),
		tag.WorkflowRunID(wf.RunId),
		tag.NewStringTag("callback-url", callback.GetNexus().GetUrl()),
		tag.NewStringTag("request-id", wf.RequestId),
	)

	request := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                scheduler.Namespace,
		WorkflowId:               wf.WorkflowId,
		WorkflowType:             requestSpec.GetWorkflowType(),
		TaskQueue:                requestSpec.GetTaskQueue(),
		Identity:                 scheduler.identity(),
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		RequestId:                wf.RequestId,
		CompletionCallbacks:      []*commonpb.Callback{callback},
		OnConflictOptions: &workflowpb.OnConflictOptions{
			AttachRequestId:           true,
			AttachCompletionCallbacks: true,
		},
	}

	resp, err := e.frontendClient.StartWorkflowExecution(ctx, request)
	if err != nil {
		logger.Error("failed to attach callback via StartWorkflowExecution",
			tag.Error(err),
			tag.WorkflowID(wf.WorkflowId),
			tag.WorkflowRunID(wf.RunId),
		)
		return err
	}
	logger.Info("StartWorkflowExecution response for callback attachment",
		tag.WorkflowID(wf.WorkflowId),
		tag.WorkflowRunID(wf.RunId),
		tag.NewBoolTag("started", resp.GetStarted()),
	)
	return nil
}
