package scheduler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.uber.org/mock/gomock"
)

type rollbackReceiptFaultEngine struct {
	chasm.Engine
	skip        int
	afterCommit bool
	armed       bool
}

func (e *rollbackReceiptFaultEngine) UpdateComponent(ctx context.Context, ref chasm.ComponentRef, update func(chasm.MutableContext, chasm.Component) error, opts ...chasm.TransitionOption) ([]byte, error) {
	if e.armed && e.skip == 0 {
		e.armed = false
		if e.afterCommit {
			if _, err := e.Engine.UpdateComponent(ctx, ref, update, opts...); err != nil {
				return nil, err
			}
		}
		return nil, serviceerror.NewUnavailable("injected receipt/close failure")
	}
	e.skip--
	return e.Engine.UpdateComponent(ctx, ref, update, opts...)
}

func TestRollbackChainOwnership(t *testing.T) {
	for _, scenario := range []string{"owned_current", "owned_descendant", "foreign_chain", "first_run_removed", "response_loss", "source_close_loss", "receipt_response_loss"} {
		t.Run(scenario, func(t *testing.T) {
			e, handler, history := newRollbackScenario(t)
			engine := &rollbackReceiptFaultEngine{Engine: e.engine, armed: scenario == "source_close_loss" || scenario == "receipt_response_loss", afterCommit: scenario == "receipt_response_loss"}
			if scenario == "source_close_loss" {
				engine.skip = 2
			}
			if scenario == "receipt_response_loss" {
				engine.skip = 1
			}
			if scenario == "response_loss" {
				history.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("committed start response lost")).Times(1)
				_, err := executeRollbackTask(t, e, handler, engine)
				require.Error(t, err)
			}
			if scenario == "source_close_loss" || scenario == "receipt_response_loss" {
				history.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(&historyservice.StartWorkflowExecutionResponse{RunId: "first-run", FirstExecutionRunId: "first-run"}, nil).Times(1)
			} else {
				owner, run := "descendant-request", "descendant-run"
				if scenario == "owned_current" {
					owner, run = "rollback", "first-run"
				}
				if scenario != "response_loss" {
					history.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("collision", owner, run)).Times(1)
				} else {
					run = ""
				}
				if scenario != "owned_current" {
					history.EXPECT().GetMutableState(gomock.Any(), &historyservice.GetMutableStateRequest{NamespaceId: namespaceID, Execution: &commonpb.WorkflowExecution{WorkflowId: "temporal-sys-scheduler:" + scheduleID, RunId: run}}).Return(&historyservice.GetMutableStateResponse{FirstExecutionRunId: "first-run"}, nil).Times(1)
					call := history.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{NamespaceId: namespaceID, Execution: &commonpb.WorkflowExecution{WorkflowId: "temporal-sys-scheduler:" + scheduleID, RunId: "first-run"}}).Times(1)
					if scenario == "first_run_removed" {
						call.Return(nil, serviceerror.NewNotFound("first run expired"))
					} else {
						firstOwner := "rollback"
						if scenario == "foreign_chain" {
							firstOwner = "foreign-owner"
						}
						call.Return(&historyservice.DescribeMutableStateResponse{DatabaseMutableState: &persistencespb.WorkflowMutableState{ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "first-run", CreateRequestId: firstOwner}}}, nil)
					}
				}
			}
			_, err := executeRollbackTask(t, e, handler, engine)
			if scenario == "source_close_loss" || scenario == "receipt_response_loss" {
				require.Error(t, err)
				_, err = executeRollbackTask(t, e, handler, engine)
			}
			conflict := scenario == "foreign_chain" || scenario == "first_run_removed"
			if conflict {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
				require.Equal(t, !conflict, s.Closed)
				if conflict {
					require.NotNil(t, s.WorkflowMigration)
				}
				return nil
			}))
		})
	}
}
