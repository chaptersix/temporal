package schedulertest

import (
	"context"
	"maps"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// startFaults counts the upcoming StartWorkflowExecution RPCs that fail.
type startFaults struct {
	rejected  int // the RPC fails without the start being accepted
	lostReply int // the start is accepted but the response is lost
}

// fakeServices is the world outside the scheduler: a frontend that accepts
// starts into an external ledger, a history service that accepts terminate and
// cancel requests, and the counters and fault injection the invariants read.
type fakeServices struct {
	Frontend *workflowservicemock.MockWorkflowServiceClient
	History  *historyservicemock.MockHistoryServiceClient

	// external records the starts the fake frontend accepted, outside the
	// scheduler's transactions.
	external *externalLedger
	// startCalls is non-nil under WithControlledStarts, where a test decides
	// each start RPC's outcome.
	startCalls chan *StartCall

	// rpcMu guards the fields below: InvokerExecuteTask issues starts from
	// concurrent goroutines.
	rpcMu sync.Mutex
	// actionRPCs counts every action RPC (StartWorkflowExecution,
	// TerminateWorkflowExecution, RequestCancelWorkflowExecution), so a step can
	// measure how many actions a single ExecuteTask took.
	actionRPCs int
	// startRequests records the RequestId of every StartWorkflowExecution RPC.
	startRequests map[string]struct{}
	// faults injects failures into uncontrolled start RPCs.
	faults startFaults
}

// newFakeServices wires happy-path stubs; tests can layer more specific
// expectations on Frontend and History.
func newFakeServices(t chasmtest.TB, ctrl *gomock.Controller, ts clock.TimeSource, controlledStarts bool) *fakeServices {
	f := &fakeServices{
		Frontend:      workflowservicemock.NewMockWorkflowServiceClient(ctrl),
		History:       historyservicemock.NewMockHistoryServiceClient(ctrl),
		external:      &externalLedger{starts: make(map[string]ExternalStart)},
		startRequests: make(map[string]struct{}),
	}
	if controlledStarts {
		f.startCalls = make(chan *StartCall)
	}

	f.Frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			f.rpcMu.Lock()
			f.actionRPCs++
			f.startRequests[req.GetRequestId()] = struct{}{}
			f.rpcMu.Unlock()
			if f.startCalls != nil {
				return f.controlledStart(ctx, t, req)
			}
			f.rpcMu.Lock()
			rejected, lostReply := f.faults.rejected > 0, f.faults.rejected == 0 && f.faults.lostReply > 0
			if rejected {
				f.faults.rejected--
			} else if lostReply {
				f.faults.lostReply--
			}
			f.rpcMu.Unlock()
			if rejected {
				return nil, serviceerror.NewUnavailable("start rejected by driver")
			}
			accepted, err := f.external.accept(req)
			if err != nil {
				return nil, err
			}
			if lostReply {
				return nil, serviceerror.NewUnavailable("start accepted, response lost by driver")
			}
			return &workflowservice.StartWorkflowExecutionResponse{RunId: accepted.RunID}, nil
		}).AnyTimes()
	f.History.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *historyservice.TerminateWorkflowExecutionRequest, ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			f.countAction()
			return &historyservice.TerminateWorkflowExecutionResponse{}, nil
		}).AnyTimes()
	f.History.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *historyservice.RequestCancelWorkflowExecutionRequest, ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
			f.countAction()
			return &historyservice.RequestCancelWorkflowExecutionResponse{}, nil
		}).AnyTimes()
	// The callbacks task (used for migrated running workflows) describes each
	// running workflow; report it completed so watchers resolve deterministically.
	f.History.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *historyservice.DescribeWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.DescribeWorkflowExecutionResponse, error) {
			return &historyservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					CloseTime: timestamppb.New(ts.Now()),
				},
			}, nil
		}).AnyTimes()
	return f
}

// controlledStart hands the RPC to the test and waits for its Respond.
func (f *fakeServices) controlledStart(ctx context.Context, t chasmtest.TB, req *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
	call := &StartCall{Request: common.CloneProto(req), t: t, result: make(chan startResponse, 1), ledger: f.external}
	select {
	case f.startCalls <- call:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case result := <-call.result:
		return result.response, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeServices) countAction() {
	f.rpcMu.Lock()
	defer f.rpcMu.Unlock()
	f.actionRPCs++
}

func (f *fakeServices) actionCount() int {
	f.rpcMu.Lock()
	defer f.rpcMu.Unlock()
	return f.actionRPCs
}

func (f *fakeServices) startRequestsCopy() map[string]struct{} {
	f.rpcMu.Lock()
	defer f.rpcMu.Unlock()
	return maps.Clone(f.startRequests)
}

// FailNextStarts makes the next n StartWorkflowExecution RPCs fail before the
// start is accepted, then lets later RPCs (the scheduler's retries) succeed.
// It does not apply under WithControlledStarts, where the test decides each
// RPC's outcome.
func (f *fakeServices) FailNextStarts(n int) {
	f.rpcMu.Lock()
	defer f.rpcMu.Unlock()
	f.faults.rejected += n
}

// LoseNextStartResponses makes the next n StartWorkflowExecution RPCs (after
// any FailNextStarts) accept the start but return an error, so the scheduler
// must retry with the same request ID and rely on deduplication.
func (f *fakeServices) LoseNextStartResponses(n int) {
	f.rpcMu.Lock()
	defer f.rpcMu.Unlock()
	f.faults.lostReply += n
}
