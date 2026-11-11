package frontend

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/worker/dummy"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMigrationSentinelEvidence_BlocksOnlyWhileRunning(t *testing.T) {
	for _, status := range []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	} {
		t.Run(status.String(), func(t *testing.T) {
			if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING &&
				os.Getenv("TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES") == "" {
				t.Skip("set TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1 to run known failing migration repros")
			}

			ctrl := gomock.NewController(t)
			registry := namespace.NewMockRegistry(ctrl)
			historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
			registry.EXPECT().GetNamespaceID(namespace.Name("ns")).Return(namespace.ID("ns-id"), nil)
			historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
				&historyservice.DescribeWorkflowExecutionResponse{
					WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
						Type:      &commonpb.WorkflowType{Name: dummy.DummyWFTypeName},
						Status:    status,
						StartTime: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
					},
				}, nil)
			calls := 0
			handler := &AdminHandler{
				logger:            log.NewNoopLogger(),
				namespaceRegistry: registry,
				historyClient:     historyClient,
				schedulerClient: &fakeSchedulerClient{
					migrateToWorkflowFn: func(context.Context, *schedulerpb.MigrateToWorkflowRequest) (*schedulerpb.MigrateToWorkflowResponse, error) {
						calls++
						return &schedulerpb.MigrateToWorkflowResponse{}, nil
					},
				},
			}
			_, err := handler.MigrateSchedule(context.Background(), &adminservice.MigrateScheduleRequest{
				Namespace:  "ns",
				ScheduleId: "schedule-id",
				Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW,
				Identity:   "test",
				RequestId:  "rollback-operation-1",
			})
			if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				require.ErrorIs(t, err, chasmscheduler.ErrSentinelBlocked)
				require.Zero(t, calls)
				return
			}
			require.NoError(t, err, "a closed sentinel no longer reserves its workflow ID")
			require.Equal(t, 1, calls)
		})
	}
}
