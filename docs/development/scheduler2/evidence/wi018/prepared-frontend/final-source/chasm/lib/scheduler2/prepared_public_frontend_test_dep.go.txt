//go:build test_dep

package scheduler2

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	legacy "go.temporal.io/server/service/worker/scheduler"
)

// NewPreparedPublicExecutionTestFrontend activates prepared definitions only in
// an explicitly selected local test cluster. One source service is shared by
// ingress and the registered source task handler; production defaults are intact.
func NewPreparedPublicExecutionTestFrontend(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
	return newPublicExecutionTestFrontend(runtime, true, true)
}

type preparedUpdateFence struct {
	revision int64
	ref      []byte
}

var errPreparedUpdateRevisionChanged = errors.New("prepared update definition revision changed")

func (f *publicTestFrontend) createPreparedSchedule(ctx context.Context, namespaceID namespace.ID, request *workflowservice.CreateScheduleRequest) (*workflowservice.CreateScheduleResponse, error) {
	stage, err := f.preparation.stagePublicCreate(request, namespaceID.String(), int64(legacy.CurrentTweakablePolicies.SpecFieldLengthLimit))
	if err != nil {
		return nil, err
	}
	constructor := func(cc chasm.MutableContext, input *stagedPublicCreate) (*Scheduler, error) {
		if f.packedPrepared {
			return newPackedPreparedPublicSchedule(cc, input, f.tweaks(input.request.Namespace))
		}
		return newPreparedPublicSchedule(cc, input, f.tweaks(input.request.Namespace))
	}
	result, err := chasm.StartExecution(f.bind(ctx), chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: request.ScheduleId}, constructor, stage, chasm.WithStartRequestID(request.RequestId))
	if _, exists := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); exists {
		return nil, serviceerror.NewWorkflowExecutionAlreadyStarted(fmt.Sprintf("schedule %q is already registered", request.ScheduleId), "", "")
	}
	if err != nil {
		return nil, err
	}
	return &workflowservice.CreateScheduleResponse{ConflictToken: publicConflictToken(result.ExecutionKey.RunID, 1)}, nil
}

func (f *publicTestFrontend) updatePreparedSchedule(ctx context.Context, namespaceID namespace.ID, request *workflowservice.UpdateScheduleRequest) (*workflowservice.UpdateScheduleResponse, error) {
	// Capture the concrete run and revision under a read lease. Later attempts use
	// that same run ref, preventing a concurrent delete/recreate from retargeting it.
	fence, err := chasm.ReadComponent(f.bind(ctx), chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: request.ScheduleId}), readPreparedUpdateFence, struct{}{})
	if err != nil {
		return nil, err
	}
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		stage, stageErr := f.preparation.stagePublicUpdate(request, namespaceID.String(), fence.revision, int64(legacy.CurrentTweakablePolicies.SpecFieldLengthLimit))
		if stageErr != nil {
			return nil, stageErr
		}
		_, _, err = chasm.UpdateComponent(f.bind(ctx), fence.ref, func(s *Scheduler, cc chasm.MutableContext, input *stagedPublicUpdate) (chasm.NoValue, error) {
			if s.Revision != input.definition.expectedRevision {
				return nil, errPreparedUpdateRevisionChanged
			}
			return nil, s.UpdatePreparedPublic(cc, input, f.tweaks(input.request.Namespace))
		}, stage, chasm.WithRequestID(request.RequestId))
		if err == nil || errors.Is(err, chasm.ErrRequestIDAlreadyUsed) {
			return &workflowservice.UpdateScheduleResponse{}, nil
		}
		if !errors.Is(err, errPreparedUpdateRevisionChanged) {
			return nil, err
		}
		fence, err = chasm.ReadComponent(f.bind(ctx), fence.ref, readPreparedUpdateFence, struct{}{})
		if err != nil {
			return nil, err
		}
	}
}

func readPreparedUpdateFence(s *Scheduler, ctx chasm.Context, _ struct{}) (preparedUpdateFence, error) {
	ref, err := ctx.Ref(s)
	return preparedUpdateFence{revision: s.Revision, ref: ref}, err
}

// NewPackedPreparedPublicExecutionTestFrontend adds the explicit fresh packed
// selector to the prepared test profile. It is never chosen by default.
func NewPackedPreparedPublicExecutionTestFrontend(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
	frontend, err := newPublicExecutionTestFrontend(runtime, true, true)
	if err != nil {
		return nil, err
	}
	frontend.packedPrepared = true
	return frontend, nil
}
