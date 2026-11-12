package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
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
	}
	var result readResult

	_, err := chasm.ReadComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, ctx chasm.Context, _ any) (struct{}, error) {
			// The task retries until the invoker has drained.
			if !s.Invoker.Get(ctx).canMigrateToWorkflow() {
				return struct{}{}, ErrMigrationActionsPending
			}
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
			}
			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read scheduler state: %w", err)
	}

	if result.requestID == "" {
		// The migration was persisted before request IDs existed, so an earlier
		// attempt may have started a workflow under a random ID. Assign an ID
		// now; if such a workflow exists, the ownership check below rejects it
		// rather than accepting it unproven.
		result.requestID, err = h.assignRequestID(ctx, schedulerRef)
		if err != nil {
			return err
		}
	}

	logger = log.With(
		h.baseLogger,
		tag.WorkflowNamespace(result.namespace),
		tag.ScheduleID(result.scheduleID),
	)
	logger.Info("schedule migration to workflow started")

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

	_, err = h.historyClient.StartWorkflowExecution(
		ctx,
		common.CreateHistoryStartWorkflowRequest(result.namespaceID, startReq, nil, nil, result.now),
	)
	if err != nil {
		alreadyStarted, ok := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](err)
		if !ok {
			return fmt.Errorf("failed to start V1 scheduler workflow: %w", err)
		}
		// Only a workflow created by this migration is an idempotent success.
		if alreadyStarted.StartRequestId != result.requestID {
			err = h.verifyDestinationOwner(ctx, result.namespaceID, workflowID, alreadyStarted.RunId, result.requestID)
			if err != nil {
				return err
			}
		}
	}

	// Mark the CHASM scheduler as closed now that the V1 workflow is running.
	_, _, err = chasm.UpdateComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, ctx chasm.MutableContext, _ any) (chasm.NoValue, error) {
			s.Closed = true
			s.WorkflowMigration = nil
			return nil, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to close CHASM scheduler after migration: %w", err)
	}

	return nil
}

// assignRequestID persists a request ID for a migration that has none and
// returns the stored ID. It keeps an ID that a concurrent attempt already stored.
func (h *SchedulerMigrateToWorkflowTaskHandler) assignRequestID(
	ctx context.Context,
	schedulerRef chasm.ComponentRef,
) (string, error) {
	requestID, _, err := chasm.UpdateComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, _ chasm.MutableContext, _ any) (string, error) {
			if s.Closed || s.WorkflowMigration == nil {
				return "", ErrMigrationPending
			}
			if s.WorkflowMigration.RequestId == "" {
				s.WorkflowMigration.RequestId = uuid.NewString()
			}
			return s.WorkflowMigration.RequestId, nil
		},
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("failed to assign migration request ID: %w", err)
	}
	return requestID, nil
}

// verifyDestinationOwner checks that the V1 workflow chain containing runID was
// created by this migration. The current run's start request ID differs from the
// migration's after continue-as-new, so the check compares the chain's first
// run against the migration's request ID. It returns an error unless that
// ownership is proven.
func (h *SchedulerMigrateToWorkflowTaskHandler) verifyDestinationOwner(
	ctx context.Context,
	namespaceID, workflowID, runID, requestID string,
) error {
	current, err := h.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceID,
		Execution:   &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	if err != nil {
		return fmt.Errorf("failed to resolve V1 scheduler workflow chain: %w", err)
	}
	firstRunID := current.GetFirstExecutionRunId()
	if firstRunID == "" {
		return serviceerror.NewFailedPreconditionf("first run of V1 scheduler workflow %q is unknown", workflowID)
	}
	first, err := h.historyClient.DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
		NamespaceId: namespaceID,
		Execution:   &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: firstRunID},
	})
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		// Without the first run, ownership cannot be proven.
		return serviceerror.NewFailedPreconditionf("first run %q of V1 scheduler workflow %q is gone; cannot verify it was created by this migration", firstRunID, workflowID)
	}
	if err != nil {
		return fmt.Errorf("failed to verify V1 scheduler workflow owner: %w", err)
	}
	state := first.GetDatabaseMutableState().GetExecutionState()
	if state.GetRunId() != firstRunID || state.GetCreateRequestId() != requestID {
		return serviceerror.NewAlreadyExistsf("V1 scheduler workflow %q was not created by this migration", workflowID)
	}
	return nil
}
