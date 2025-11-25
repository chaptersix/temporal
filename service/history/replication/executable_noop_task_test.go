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

func setupExecutableNoopTask(t *testing.T) (*gomock.Controller, *ExecutableNoopTask) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clientBean := client.NewMockBean(controller)
	shardController := shard.NewMockController(controller)
	namespaceCache := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NoopMetricsHandler
	logger := log.NewNoopLogger()
	eagerNamespaceRefresher := NewMockEagerNamespaceRefresher(controller)

	task := NewExecutableNoopTask(
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
		rand.Int63(),
		time.Unix(0, rand.Int63()),
		"sourceCluster",
		ClusterShardKey{
			ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
			ShardID:   rand.Int31(),
		},
	)
	return controller, task
}

func TestExecute(t *testing.T) {
	controller, task := setupExecutableNoopTask(t)
	defer controller.Finish()

	err := task.Execute()
	require.NoError(t, err)
}

func TestHandleErr(t *testing.T) {
	controller, task := setupExecutableNoopTask(t)
	defer controller.Finish()

	err := errors.New("OwO")
	require.Equal(t, err, task.HandleErr(err))

	err = serviceerror.NewUnavailable("")
	require.Equal(t, err, task.HandleErr(err))
}

func TestMarkPoisonPill(t *testing.T) {
	controller, task := setupExecutableNoopTask(t)
	defer controller.Finish()

	err := task.MarkPoisonPill()
	require.NoError(t, err)
}
