package scheduler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func TestMigrationCounterexampleRollbackChainOwnership(t *testing.T) {
	requireMigrationCounterexamples(t)
	for _, scenario := range []string{"owned_descendant", "foreign_chain", "first_run_removed"} {
		t.Run(scenario, func(t *testing.T) {
			e, handler, history := newRollbackScenario(t)
			history.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil,
				serviceerror.NewWorkflowExecutionAlreadyStarted("continued-as-new destination", "descendant-request", "descendant-run"))
			history.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(
				&historyservice.GetMutableStateResponse{FirstExecutionRunId: "first-run"}, nil).AnyTimes()
			verified := false
			history.EXPECT().DescribeMutableState(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *historyservice.DescribeMutableStateRequest, _ ...grpc.CallOption) (*historyservice.DescribeMutableStateResponse, error) {
					require.Equal(t, "first-run", req.GetExecution().GetRunId())
					verified = true
					if scenario == "first_run_removed" {
						return nil, serviceerror.NewNotFound("original run expired")
					}
					owner := "rollback"
					if scenario == "foreign_chain" {
						owner = "foreign"
					}
					return &historyservice.DescribeMutableStateResponse{DatabaseMutableState: &persistencespb.WorkflowMutableState{
						ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "first-run", CreateRequestId: owner},
					}}, nil
				}).AnyTimes()
			_, err := executeRollbackTask(t, e, handler, e.engine)
			if scenario == "owned_descendant" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.True(t, verified, "a descendant's start request does not establish ownership of the original migration")
			require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
				require.Equal(t, scenario == "owned_descendant", s.Closed)
				return nil
			}))
		})
	}
}
