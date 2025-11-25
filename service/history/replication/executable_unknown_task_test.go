package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"go.uber.org/mock/gomock"
)

func setupExecutableUnknownTask(t *testing.T) (*gomock.Controller, *ExecutableUnknownTask) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clientBean := client.NewMockBean(controller)
	shardController := shard.NewMockController(controller)
	namespaceCache := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NoopMetricsHandler
	logger := log.NewNoopLogger()
	eagerNamespaceRefresher := NewMockEagerNamespaceRefresher(controller)

	taskID := rand.Int63()
	task := NewExecutableUnknownTask(
		ProcessToolBox{
			ClusterMetadata:         clusterMetadata,
			ClientBean:              clientBean,
			ShardController:         shardController,
			NamespaceCache:          namespaceCache,
			MetricsHandler:          metricsHandler,
			Logger:                  logger,
			EagerNamespaceRefresher: eagerNamespaceRefresher,
			DLQWriter:               NoopDLQWriter{},
		},
		taskID,
		time.Unix(0, rand.Int63()),
		nil,
	)
	return controller, task
}

func TestExecutableUnknownTask_Execute(t *testing.T) {
	controller, task := setupExecutableUnknownTask(t)
	defer controller.Finish()

	err := task.Execute()
	require.IsType(t, serviceerror.NewInvalidArgument(""), err)
}

func TestExecutableUnknownTask_HandleErr(t *testing.T) {
	controller, task := setupExecutableUnknownTask(t)
	defer controller.Finish()

	err := errors.New("OwO")
	require.Equal(t, err, task.HandleErr(err))

	err = serviceerror.NewUnavailable("")
	require.Equal(t, err, task.HandleErr(err))
}

func TestExecutableUnknownTask_MarkPoisonPill(t *testing.T) {
	controller, task := setupExecutableUnknownTask(t)
	defer controller.Finish()

	err := task.MarkPoisonPill()
	require.NoError(t, err)
}
