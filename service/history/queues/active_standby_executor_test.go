package queues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

const (
	currentCluster    = "current"
	nonCurrentCluster = "nonCurrent"
)

func TestExecute_Active(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	activeExecutor := NewMockExecutor(ctrl)
	standbyExecutor := NewMockExecutor(ctrl)
	executor := NewActiveStandbyExecutor(
		currentCluster,
		registry,
		activeExecutor,
		standbyExecutor,
		log.NewNoopLogger(),
	)

	executable := NewMockExecutable(ctrl)
	executable.EXPECT().GetNamespaceID().Return("namespace_id")
	executable.EXPECT().GetTask().Return(nil)
	ns := namespace.NewGlobalNamespaceForTest(nil, nil, &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: currentCluster,
		Clusters:          []string{currentCluster},
	}, 1)
	registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)
	activeExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    true,
		ExecutionErr:        nil,
	}).Times(1)
	resp := executor.Execute(context.Background(), executable)
	require.NoError(t, resp.ExecutionErr)
	require.True(t, resp.ExecutedAsActive)
}

func TestExecute_Standby(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	activeExecutor := NewMockExecutor(ctrl)
	standbyExecutor := NewMockExecutor(ctrl)
	executor := NewActiveStandbyExecutor(
		currentCluster,
		registry,
		activeExecutor,
		standbyExecutor,
		log.NewNoopLogger(),
	)

	executable := NewMockExecutable(ctrl)
	executable.EXPECT().GetNamespaceID().Return("namespace_id")
	executable.EXPECT().GetTask().Return(nil)
	ns := namespace.NewGlobalNamespaceForTest(nil, nil, &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: nonCurrentCluster,
		Clusters:          []string{currentCluster, nonCurrentCluster},
	}, 1)
	registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)
	standbyExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        nil,
	}).Times(1)
	resp := executor.Execute(context.Background(), executable)
	require.NoError(t, resp.ExecutionErr)
	require.False(t, resp.ExecutedAsActive)
}
