package queues

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestTimerInterceptorPreservesDispatchResultAndScope(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, active, scope string
		timer, observed     bool
	}{
		{"active timer", "current", "ns", true, true},
		{"standby timer", "other", "ns", true, true},
		{"other namespace", "current", "other", true, false},
		{"transfer unchanged", "current", "ns", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			registry := namespace.NewMockRegistry(ctrl)
			registry.EXPECT().GetNamespaceByID(namespace.ID("ns")).Return(namespace.NewGlobalNamespaceForTest(nil, nil, &persistencespb.NamespaceReplicationConfig{ActiveClusterName: tc.active, Clusters: []string{"current", "other"}}, 1), nil)
			active, standby := NewMockExecutor(ctrl), NewMockExecutor(ctrl)
			executable := NewMockExecutable(ctrl)
			var task tasks.Task = &tasks.WorkflowTask{WorkflowKey: definition.NewWorkflowKey("ns", "workflow", "run")}
			if tc.timer {
				task = &tasks.UserTimerTask{WorkflowKey: definition.NewWorkflowKey("ns", "workflow", "run")}
			}
			executable.EXPECT().GetTask().Return(task).AnyTimes()
			executable.EXPECT().GetNamespaceID().Return("ns").AnyTimes()
			executable.EXPECT().GetWorkflowID().Return("workflow").AnyTimes()
			expected := ExecuteResponse{ExecutedAsActive: tc.active == "current", ExecutionErr: errors.New("delegate result")}
			if tc.active == "current" {
				active.EXPECT().Execute(gomock.Any(), executable).Return(expected)
			} else {
				standby.EXPECT().Execute(gomock.Any(), executable).Return(expected)
			}
			hooks := testhooks.NewTestHooks()
			observed := false
			cleanup := testhooks.Set(hooks, testhooks.HistoryTimerTaskInterceptor, func(received tasks.Task, execute func()) { require.Same(t, task, received); observed = true; execute() }, namespace.ID(tc.scope))
			defer cleanup()
			executor := NewActiveStandbyExecutor("current", registry, active, standby, log.NewNoopLogger(), metrics.NoopMetricsHandler, hooks)
			require.Equal(t, expected, executor.Execute(context.Background(), executable))
			require.Equal(t, tc.observed, observed)
		})
	}
}
