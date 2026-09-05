package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/fx"
)

type (
	SchedulerMigrateToWorkflowTaskHandlerOptions struct {
		fx.In

		Config           *Config
		MetricsHandler   metrics.Handler
		BaseLogger       log.Logger
		HistoryClient    resource.HistoryClient
		SaMapperProvider searchattribute.MapperProvider
	}

	SchedulerMigrateToWorkflowTaskHandler struct {
		chasm.SideEffectTaskHandlerBase[*schedulerpb.SchedulerMigrateToWorkflowTask]
		config           *Config
		metricsHandler   metrics.Handler
		baseLogger       log.Logger
		historyClient    resource.HistoryClient
		saMapperProvider searchattribute.MapperProvider
	}
)

func NewSchedulerMigrateToWorkflowTaskHandler(
	opts SchedulerMigrateToWorkflowTaskHandlerOptions,
) *SchedulerMigrateToWorkflowTaskHandler {
	return &SchedulerMigrateToWorkflowTaskHandler{
		config:           opts.Config,
		metricsHandler:   opts.MetricsHandler,
		baseLogger:       opts.BaseLogger,
		historyClient:    opts.HistoryClient,
		saMapperProvider: opts.SaMapperProvider,
	}
}

func (h *SchedulerMigrateToWorkflowTaskHandler) Validate(
	_ chasm.Context,
	scheduler *Scheduler,
	_ chasm.TaskInvocation,
	_ *schedulerpb.SchedulerMigrateToWorkflowTask,
) (bool, error) {
	if scheduler.Closed {
		return false, nil
	}
	return scheduler.WorkflowMigration != nil, nil
}

func (h *SchedulerMigrateToWorkflowTaskHandler) Execute(
	ctx context.Context,
	schedulerRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *schedulerpb.SchedulerMigrateToWorkflowTask,
) (retErr error) {
	metricsHandler := h.metricsHandler.WithTags(
		metrics.StringTag(metrics.ScheduleMigrationDirectionTag, metrics.ScheduleMigrationDirectionToWorkflow),
	)
	metricsHandler.Counter(metrics.ScheduleMigrationStarted.Name()).Record(1)

	// logger is initialized after ReadComponent, once namespace/scheduleID are known.
	var logger log.Logger
	defer func() {
		if retErr != nil {
			metricsHandler.Counter(metrics.ScheduleMigrationFailed.Name()).Record(1)
			if logger != nil {
				logger.Error("schedule migration to workflow failed", tag.Error(retErr))
			}
		} else {
			metricsHandler.Counter(metrics.ScheduleMigrationCompleted.Name()).Record(1)
			if logger != nil {
				logger.Info("schedule migration to workflow succeeded")
			}
		}
	}()

	// Read state and convert to V1 args inside the ReadComponent callback,
	// where we have access to the CHASM context for consistent time.
	type readResult struct {
		args             *schedulespb.StartScheduleArgs
		namespace        string
		namespaceID      string
		scheduleID       string
		searchAttributes map[string]*commonpb.Payload
		memo             map[string]*commonpb.Payload
		now              time.Time
		requestID        string
		receipt          string
	}
	var result readResult

	_, err := chasm.ReadComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, ctx chasm.Context, _ any) (struct{}, error) {
			now := ctx.Now(s)
			schedulerState := common.CloneProto(s.SchedulerState)
			generatorState := common.CloneProto(s.Generator.Get(ctx).GeneratorState)
			invokerState := common.CloneProto(s.Invoker.Get(ctx).InvokerState)

			bStates := make(map[string]*schedulerpb.BackfillerState, len(s.Backfillers))
			for id, field := range s.Backfillers {
				bStates[id] = common.CloneProto(field.Get(ctx).BackfillerState)
			}

			lastCompletionResult := common.CloneProto(s.LastCompletionResult.Get(ctx))

			visibility := s.Visibility.Get(ctx)
			searchAttributes := visibility.CustomSearchAttributes(ctx)
			memo := visibility.CustomMemo(ctx)

			// Restore the pre-migration paused state so the V1 workflow receives
			// the correct schedule state (not the migration-imposed pause).
			// Validation guarantees WorkflowMigration and State are always set
			// when this task runs.
			schedulerState.Schedule.State.Paused = schedulerState.WorkflowMigration.PreMigrationPaused
			schedulerState.Schedule.State.Notes = schedulerState.WorkflowMigration.PreMigrationNotes

			result = readResult{
				args: migration.CHASMToLegacyStartScheduleArgs(
					schedulerState,
					generatorState,
					invokerState,
					bStates,
					lastCompletionResult,
					searchAttributes,
					memo,
					now,
				),
				namespace:        schedulerState.GetNamespace(),
				namespaceID:      schedulerState.GetNamespaceId(),
				scheduleID:       schedulerState.GetScheduleId(),
				searchAttributes: searchAttributes,
				memo:             memo,
				now:              now,
				requestID:        schedulerState.GetWorkflowMigration().GetRequestId(),
				receipt:          schedulerState.GetWorkflowMigration().GetDestinationFirstRunId(),
			}
			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read scheduler state: %w", err)
	}
	if result.requestID == "" {
		return serviceerror.NewFailedPrecondition("workflow migration has no durable request ID")
	}

	logger = log.With(
		h.baseLogger,
		tag.WorkflowNamespace(result.namespace),
		tag.ScheduleID(result.scheduleID),
	)
	logger.Info("schedule migration to workflow started")

	if result.receipt != "" {
		return h.closeSource(ctx, schedulerRef, result.requestID)
	}

	// Serialize the V1 workflow input.
	inputPayloads, err := sdk.PreferProtoDataConverter.ToPayloads(result.args)
	if err != nil {
		return fmt.Errorf("failed to serialize schedule args: %w", err)
	}

	// Build the start request to match createScheduleWorkflow in the frontend
	// as closely as possible. Include TemporalNamespaceDivision so the V1
	// workflow is discoverable via ListSchedules.
	saMap := payload.MergeMapOfPayload(
		result.searchAttributes,
		map[string]*commonpb.Payload{
			sadefs.TemporalNamespaceDivision: payload.EncodeString(legacyscheduler.NamespaceDivision),
		},
	)

	// The CHASM scheduler stores custom search attributes by their alias (the frontend
	// passes the original request through unchanged), whereas V1 scheduler workflows
	// store them unaliased/resolved. Mirror how V1 unaliases search attributes before
	// starting the system scheduler workflow.
	sa, err := searchattribute.UnaliasFields(
		h.saMapperProvider,
		&commonpb.SearchAttributes{IndexedFields: saMap},
		result.namespace,
	)
	if err != nil {
		return fmt.Errorf("failed to unalias search attributes: %w", err)
	}
	workflowID := legacyscheduler.WorkflowIDPrefix + result.scheduleID
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                result.requestID,
		Namespace:                result.namespace,
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: legacyscheduler.WorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    inputPayloads,
		Identity:                 fmt.Sprintf("temporal-scheduler-migration-%s-%s", result.namespace, result.scheduleID),
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		Memo:                     &commonpb.Memo{Fields: result.memo},
		SearchAttributes:         sa,
		Priority:                 &commonpb.Priority{},
	}

	claimed, _, err := chasm.UpdateComponent(ctx, schedulerRef,
		func(s *Scheduler, _ chasm.MutableContext, _ any) (bool, error) {
			if s.Closed {
				return false, ErrClosed
			}
			if s.WorkflowMigration.GetRequestId() != result.requestID {
				return false, ErrMigrationPending
			}
			pending := s.WorkflowMigration.StartPending
			s.WorkflowMigration.StartPending = false
			return pending, nil
		}, nil)
	if err != nil {
		return fmt.Errorf("failed to claim migration start: %w", err)
	}
	var firstRunID string
	if claimed {
		response, startErr := h.historyClient.StartWorkflowExecution(ctx,
			common.CreateHistoryStartWorkflowRequest(result.namespaceID, startReq, nil, nil, result.now))
		if startErr != nil {
			alreadyStarted, ok := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](startErr)
			if !ok {
				return fmt.Errorf("failed to start V1 scheduler workflow: %w", startErr)
			}
			if alreadyStarted.StartRequestId == result.requestID {
				firstRunID = alreadyStarted.RunId
			} else {
				firstRunID, err = h.verifyDestinationChain(ctx, result.namespaceID, workflowID, alreadyStarted.RunId, result.requestID)
			}
		} else {
			firstRunID = response.GetFirstExecutionRunId()
			if firstRunID == "" {
				firstRunID = response.GetRunId()
			}
		}
	} else {
		// A previous start may have committed and continued as new or closed. Never restart its snapshot.
		firstRunID, err = h.verifyDestinationChain(ctx, result.namespaceID, workflowID, "", result.requestID)
	}
	if err != nil {
		return err
	}
	if firstRunID == "" {
		return serviceerror.NewFailedPrecondition("migration destination returned no run identity")
	}

	_, _, err = chasm.UpdateComponent(ctx, schedulerRef,
		func(s *Scheduler, _ chasm.MutableContext, _ any) (chasm.NoValue, error) {
			if s.Closed {
				return nil, nil
			}
			if s.WorkflowMigration.GetRequestId() != result.requestID {
				return nil, ErrMigrationPending
			}
			s.WorkflowMigration.DestinationFirstRunId = firstRunID
			return nil, nil
		}, nil)
	if err != nil {
		return fmt.Errorf("failed to persist migration destination receipt: %w", err)
	}
	return h.closeSource(ctx, schedulerRef, result.requestID)
}

func (h *SchedulerMigrateToWorkflowTaskHandler) closeSource(ctx context.Context, ref chasm.ComponentRef, requestID string) error {
	_, _, err := chasm.UpdateComponent(ctx, ref,
		func(s *Scheduler, _ chasm.MutableContext, _ any) (chasm.NoValue, error) {
			if s.Closed {
				return nil, nil
			}
			if s.WorkflowMigration.GetRequestId() != requestID {
				return nil, ErrMigrationPending
			}
			s.Closed = true
			s.WorkflowMigration = nil
			return nil, nil
		}, nil)
	if err != nil {
		return fmt.Errorf("failed to close CHASM scheduler after migration: %w", err)
	}
	return nil
}

func (h *SchedulerMigrateToWorkflowTaskHandler) verifyDestinationChain(
	ctx context.Context, namespaceID, workflowID, runID, requestID string,
) (string, error) {
	current, err := h.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceID, Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	if err != nil {
		return "", fmt.Errorf("failed to resolve migration destination chain: %w", err)
	}
	firstRunID := current.GetFirstExecutionRunId()
	if firstRunID == "" {
		return "", serviceerror.NewFailedPrecondition("migration destination has no first-run identity")
	}
	first, err := h.historyClient.DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
		NamespaceId: namespaceID, Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: firstRunID},
	})
	if err != nil {
		return "", fmt.Errorf("failed to verify migration destination owner: %w", err)
	}
	state := first.GetDatabaseMutableState().GetExecutionState()
	if state.GetRunId() != firstRunID || state.GetCreateRequestId() != requestID {
		return "", serviceerror.NewAlreadyExistsf("V1 scheduler workflow %q belongs to another migration", workflowID)
	}
	return firstRunID, nil
}
