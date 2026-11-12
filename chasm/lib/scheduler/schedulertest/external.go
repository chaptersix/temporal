package schedulertest

import (
	"context"
	"fmt"
	"maps"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ExternalStart records acceptance outside the scheduler's transaction.
type ExternalStart struct {
	WorkflowID string
	RunID      string
}

type externalLedger struct {
	mu     sync.Mutex
	starts map[string]ExternalStart
}

func (l *externalLedger) accept(req *workflowservice.StartWorkflowExecutionRequest) (ExternalStart, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if start, ok := l.starts[req.GetRequestId()]; ok {
		if start.WorkflowID != req.GetWorkflowId() {
			return ExternalStart{}, fmt.Errorf("request %s changed workflow identity", req.GetRequestId())
		}
		return start, nil
	}
	start := ExternalStart{WorkflowID: req.GetWorkflowId(), RunID: "run-" + req.GetRequestId()}
	l.starts[req.GetRequestId()] = start
	return start, nil
}

func (l *externalLedger) snapshot() map[string]ExternalStart {
	l.mu.Lock()
	defer l.mu.Unlock()
	return maps.Clone(l.starts)
}

// StartCall is an outstanding RPC. Accept and Respond are distinct events so a
// test can deliver a callback or lose the response after external acceptance.
type StartCall struct {
	Request *workflowservice.StartWorkflowExecutionRequest
	t       chasmtest.TB
	result  chan startResponse
	ledger  *externalLedger
}

type startResponse struct {
	response *workflowservice.StartWorkflowExecutionResponse
	err      error
}

func (c *StartCall) Accept() (ExternalStart, error) { return c.ledger.accept(c.Request) }

func (c *StartCall) Respond(start ExternalStart, err error) {
	select {
	case c.result <- startResponse{response: &workflowservice.StartWorkflowExecutionResponse{RunId: start.RunID}, err: err}:
	default:
		c.t.Fatal("StartCall.Respond called more than once")
	}
}

// WithControlledStarts makes start RPCs wait for explicit Accept/Respond events.
func WithControlledStarts() DriverOption {
	return func(c *driverConfig) { c.controlledStarts = true }
}

// BeginSideEffect runs until the handler blocks at a start RPC or finishes.
// FinishSideEffect must be called before starting another task. Engine access
// between these calls is allowed only while all start RPCs are blocked or the
// handler is waiting at NextRecord.
func (d *Driver) BeginSideEffect() bool {
	d.T.Helper()
	if d.startCalls == nil || d.sideEffectDone != nil {
		d.T.Fatal("controlled starts required; only one task may be in flight")
	}
	next := d.nextSideEffect()
	if next == nil {
		return false
	}
	d.actionsBefore = d.actionCount()
	done := make(chan error, 1)
	d.sideEffectDone = done
	ctx, cancel := context.WithCancel(d.T.Context())
	d.T.Cleanup(func() {
		cancel()
		if d.sideEffectDone == done {
			<-done
			d.sideEffectDone = nil
		}
	})
	node := d.node()
	go func() {
		engine := &recordingEngine{Engine: d.Engine, calls: d.recordCalls}
		done <- node.ExecuteSideEffectTask(chasm.NewEngineContext(ctx, engine), d.execKey, next,
			func(chasm.NodeBackend, chasm.Context, chasm.Component) error { return nil })
	}()
	return true
}

func (d *Driver) NextStart() *StartCall {
	d.T.Helper()
	select {
	case call := <-d.startCalls:
		return call
	case err := <-d.sideEffectDone:
		d.sideEffectDone = nil
		d.T.Fatalf("task finished before expected start: %v", err)
	case <-d.T.Context().Done():
		d.T.Fatal("waiting for start: test canceled")
	}
	return nil
}

func (d *Driver) FinishSideEffect() {
	d.T.Helper()
	for {
		select {
		case call := <-d.recordCalls:
			if err := call.Record(); err != nil {
				d.T.Fatalf("record start result: %v", err)
			}
		case call := <-d.startCalls:
			d.T.Fatalf("unconsumed start RPC for request %s; call NextStart before FinishSideEffect", call.Request.GetRequestId())
		case err := <-d.sideEffectDone:
			d.sideEffectDone = nil
			d.finishSideEffect(d.actionsBefore, err)
			return
		case <-d.T.Context().Done():
			d.T.Fatal("waiting for side effect: test canceled")
		}
	}
}

// NextRecord waits until responses have been collected and the handler is ready
// to record them. The caller can deliver other events before calling Record.
func (d *Driver) NextRecord() *RecordCall {
	d.T.Helper()
	select {
	case call := <-d.recordCalls:
		return call
	case err := <-d.sideEffectDone:
		d.sideEffectDone = nil
		d.T.Fatalf("task finished before result recording: %v", err)
	case <-d.T.Context().Done():
		d.T.Fatal("waiting for recording: test canceled")
	}
	return nil
}

type recordingEngine struct {
	chasm.Engine
	calls chan *RecordCall
}

// RecordCall separates successful RPC response delivery from local recording.
// It uses the actual handler's update closure; it is not a durable storage commit.
type RecordCall struct {
	engine chasm.Engine
	ctx    context.Context
	ref    chasm.ComponentRef
	update func(chasm.MutableContext, chasm.Component) error
	opts   []chasm.TransitionOption
	result chan recordResponse
}

type recordResponse struct {
	ref []byte
	err error
}

func (e *recordingEngine) UpdateComponent(ctx context.Context, ref chasm.ComponentRef,
	update func(chasm.MutableContext, chasm.Component) error, opts ...chasm.TransitionOption) ([]byte, error) {
	call := &RecordCall{engine: e.Engine, ctx: ctx, ref: ref, update: update, opts: opts, result: make(chan recordResponse, 1)}
	select {
	case e.calls <- call:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case result := <-call.result:
		return result.ref, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *RecordCall) Record() error {
	ref, err := c.engine.UpdateComponent(c.ctx, c.ref, c.update, c.opts...)
	c.result <- recordResponse{ref: ref, err: err}
	return err
}

// DeliverCompletion also supports accepted starts whose RPC results have not
// been recorded locally, and duplicate or obsolete callback request identities.
func (d *Driver) DeliverCompletion(requestID string) error {
	_, err := d.Engine.UpdateComponent(chasm.NewEngineContext(context.Background(), d.Engine), d.ref,
		func(ctx chasm.MutableContext, c chasm.Component) error {
			return c.(*scheduler.Scheduler).HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: requestID, CloseTime: timestamppb.New(d.Now()),
				Outcome: &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{}},
			})
		})
	return err
}

func (d *Driver) observeRecordedStarts() {
	accepted := d.external.snapshot()
	d.ReadScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) {
		if s.IsSentinel() {
			return
		}
		for _, start := range s.Invoker.Get(ctx).GetBufferedStarts() {
			if start.GetRunId() != "" {
				if _, ok := accepted[start.GetRequestId()]; ok {
					d.recorded[start.GetRequestId()] = ExternalStart{WorkflowID: start.GetWorkflowId(), RunID: start.GetRunId()}
				}
			}
		}
		for _, result := range s.Info.GetRecentActions() {
			execution := result.GetStartWorkflowResult()
			for id, start := range accepted {
				if execution.GetWorkflowId() == start.WorkflowID && execution.GetRunId() == start.RunID {
					d.recorded[id] = start
				}
			}
		}
	})
}
