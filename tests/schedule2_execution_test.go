package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type workflowStartOutcome struct {
	firstRunID string
	err        error
}

type observedWorkflowStarter struct {
	workflowservice.WorkflowServiceClient
	outcomes           chan workflowStartOutcome
	holdAcknowledgment <-chan struct{}
}

func (c *observedWorkflowStarter) StartWorkflowExecution(ctx context.Context, r *workflowservice.StartWorkflowExecutionRequest, opts ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
	response, err := c.WorkflowServiceClient.StartWorkflowExecution(ctx, r, opts...)
	select {
	case c.outcomes <- workflowStartOutcome{firstRunID: response.GetFirstExecutionRunId(), err: err}:
	default:
	}
	if err == nil && c.holdAcknowledgment != nil {
		select {
		case <-c.holdAcknowledgment:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return response, err
}

// This verifies public Create/Patch through physical workflow/callback transport.
func TestSchedule2WorkflowCallbackTransport(t *testing.T) {
	for _, continues := range []bool{false, true} {
		name := "complete"
		if continues {
			name = "continue-as-new"
		}
		t.Run(name, func(t *testing.T) {
			outcomes := make(chan workflowStartOutcome, 64)
			ackGate := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(ackGate) }) }
			defer release()
			backend := scheduleBackend{name: "scheduler2", context: func(ctx context.Context) context.Context { return ctx }, options: []testcore.TestOption{testcore.WithScheduleFrontend(func(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
				runtime.FrontendClient = &observedWorkflowStarter{WorkflowServiceClient: runtime.FrontendClient, outcomes: outcomes, holdAcknowledgment: ackGate}
				return scheduler2.NewPublicExecutionTestFrontend(runtime)
			}, scheduler2.NewNilLibrary())}}
			env := newScheduleEnv(t, []scheduleBackend{backend})
			ctx := t.Context()
			env.SdkWorker().RegisterWorkflowWithOptions(func(workflow.Context) (string, error) { return "done", nil }, workflow.RegisterOptions{Name: "transport-final"})
			env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) (string, error) {
				return "", workflow.NewContinueAsNewError(ctx, "transport-final")
			}, workflow.RegisterOptions{Name: "transport-first"})
			wt := "transport-final"
			if continues {
				wt = "transport-first"
			}
			sid := "transport-" + uuid.NewString()
			createSchedule(ctx, t, env, sid, &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, Action: startWorkflowAction(env, "workflow-"+sid, wt), State: &schedulepb.ScheduleState{Paused: true}})
			ref := chasm.NewComponentRef[*scheduler2.Scheduler](chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: sid})
			patchSchedule(ctx, t, env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))
			type observation struct {
				started, unfinished int64
				workflowID, chainID string
				result              []byte
			}
			readObservation := func(readCtx context.Context) (observation, error) {
				readEngineCtx, err := env.GetTestCluster().Host().ChasmContext(readCtx)
				if err != nil {
					return observation{}, err
				}
				return chasm.ReadComponent(readEngineCtx, ref, func(s *scheduler2.Scheduler, cc chasm.Context, _ struct{}) (observation, error) {
					o := observation{started: s.Started, unfinished: s.Unfinished}
					if field, ok := s.Operations[s.OperationId]; ok {
						op := field.Get(cc)
						o.workflowID, o.chainID = op.WorkflowId, op.ChainId
					}
					if s.CarryoverSet {
						carryover, err := scheduler2.ReadCarryoverForTest(s, cc)
						if err != nil {
							return observation{}, err
						}
						o.result = carryover.Result
					}
					return o, nil
				}, struct{}{})
			}
			var completed observation
			var latestOutcome workflowStartOutcome
			await.Require(ctx, t, func(t *await.T) {
				got, err := readObservation(t.Context())
				require.NoError(t, err)
				for {
					select {
					case outcome := <-outcomes:
						latestOutcome = outcome
					default:
						goto outcomesRead
					}
				}
			outcomesRead:
				require.EqualValues(t, 1, got.started, "root: %+v; start: %+v", got, latestOutcome)
				require.Zero(t, got.unfinished)
				require.NotEmpty(t, got.chainID)
				require.NotEmpty(t, got.result)
				completed = got
			}, 30*time.Second, 100*time.Millisecond)
			// The real callback must finish the admission while its successful start
			// response is still withheld from StartHandler.
			require.NoError(t, latestOutcome.err)
			require.Equal(t, latestOutcome.firstRunID, completed.chainID)
			release()
			result := &commonpb.Payload{}
			require.NoError(t, proto.Unmarshal(completed.result, result))
			var value string
			require.NoError(t, payloads.Decode(&commonpb.Payloads{Payloads: []*commonpb.Payload{result}}, &value))
			require.Equal(t, "done", value)
			described, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: completed.workflowID}})
			require.NoError(t, err)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, described.WorkflowExecutionInfo.Status)
			if continues {
				require.NotEqual(t, completed.chainID, described.WorkflowExecutionInfo.Execution.RunId)
			} else {
				require.Equal(t, completed.chainID, described.WorkflowExecutionInfo.Execution.RunId)
			}
			// The library's single issuance permit is released only after the first
			// handler reconciles its late response. A second physical start is a
			// completion barrier for that reconciliation, without timing sleeps.
			patchSchedule(ctx, t, env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))
			await.Require(ctx, t, func(t *await.T) {
				got, err := readObservation(t.Context())
				require.NoError(t, err)
				require.EqualValues(t, 2, got.started)
				require.Zero(t, got.unfinished)
				require.Equal(t, completed.result, got.result)
				require.NotEqual(t, completed.chainID, got.chainID)
			}, 30*time.Second, 100*time.Millisecond)
		})
	}
}
