//go:build test_dep

package scheduler2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

func TestPreparedFrontendUpdateRestagesRevisionAndDedups(t *testing.T) {
	t.Parallel()
	h, service, _, created := stagedPublicIntegrationTree(t)
	ctrl := gomock.NewController(t)
	engine := chasm.NewMockEngine(ctrl)
	frontend := &publicTestFrontend{engine: engine, visibility: chasm.NewMockVisibilityManager(ctrl), preparation: service, tweaks: func(string) schedulerconfig.Tweakables { return schedulerconfig.DefaultTweakables }}
	request := &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", RequestId: "dedup", Schedule: common.CloneProto(created.request.Schedule)}
	reads, updates := 0, 0
	engine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(_ context.Context, ref chasm.ComponentRef, read func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
		s, cc := h.root(false)
		if reads > 0 {
			require.Equal(t, s.Incarnation, ref.RunID, "retry remains pinned to captured run")
		}
		reads++
		return read(cc, s)
	})
	engine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(2).DoAndReturn(func(_ context.Context, ref chasm.ComponentRef, update func(chasm.MutableContext, chasm.Component) error, options ...chasm.TransitionOption) ([]byte, error) {
		updates++
		s, raw := h.root(true)
		cc := raw.(chasm.MutableContext)
		require.Equal(t, s.Incarnation, ref.RunID)
		opts := chasm.TransitionOptions{}
		for _, option := range options {
			option(&opts)
		}
		require.Equal(t, request.RequestId, opts.RequestID)
		// The compiled next-revision stage already exists before the write callback.
		expected, err := StageDefinitionSource(request.Schedule.Spec, "namespace-schedule", s.Revision+1)
		require.NoError(t, err)
		_, hit := service.Definition(expected.owner)
		require.True(t, hit)
		if updates == 1 {
			s.Revision++
			err = update(cc, s)
			require.ErrorIs(t, err, errPreparedUpdateRevisionChanged)
			return nil, err
		}
		// Framework request-ID rejection precedes user mutation and normalizes to success.
		return nil, chasm.ErrRequestIDAlreadyUsed
	})
	response, err := frontend.updatePreparedSchedule(t.Context(), namespace.ID("namespace"), request)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, 2, reads)
	require.Equal(t, 2, updates)
}
