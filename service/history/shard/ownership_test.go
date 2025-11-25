package shard

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func newController(t *testing.T, controller *gomock.Controller, resource *resourcetest.Test, config *configs.Config, contextFactory ContextFactory) *ControllerImpl {
	return ControllerProvider(
		config,
		resource.GetLogger(),
		resource.GetHistoryServiceResolver(),
		resource.GetMetricsHandler(),
		resource.GetHostInfoProvider(),
		contextFactory,
	)
}

func TestAcquireViaMembershipUpdate(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	resource := resourcetest.NewTest(controller, primitives.HistoryService)
	config := tests.NewDynamicConfig()
	config.NumberOfShards = 1
	shardID := int32(1)

	resource.HostInfoProvider.EXPECT().HostInfo().Return(resource.GetHostInfo()).AnyTimes()

	shard := historyi.NewMockControllableContext(controller)
	shard.EXPECT().GetEngine(gomock.Any()).Return(nil, nil).AnyTimes()
	shard.EXPECT().AssertOwnership(gomock.Any()).Return(nil).AnyTimes()
	shard.EXPECT().IsValid().Return(true).AnyTimes()

	cf := NewMockContextFactory(controller)
	cf.EXPECT().CreateContext(shardID, gomock.Any()).
		DoAndReturn(func(_ int32, _ CloseCallback) (historyi.ControllableContext, error) {
			return shard, nil
		})

	resource.HistoryServiceResolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(resource.GetHostInfo(), nil).AnyTimes()

	resource.HistoryServiceResolver.EXPECT().
		AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).
		Return(nil).Times(1)

	shardController := newController(t, controller, resource, config, cf)
	shardController.Start()

	require.Zero(t, len(shardController.ShardIDs()))

	shardController.ownership.membershipUpdateCh <- &membership.ChangedEvent{}

	require.Eventually(t, func() bool {
		shardIDs := shardController.ShardIDs()
		return len(shardIDs) == 1 && shardIDs[0] == shardID
	}, 5*time.Second, 100*time.Millisecond)

	resource.HistoryServiceResolver.EXPECT().
		RemoveListener(shardControllerMembershipUpdateListenerName).
		Return(nil).Times(1)

	shard.EXPECT().FinishStop().Times(1)
	shardController.Stop()
}

func TestAcquireOnDemand(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	resource := resourcetest.NewTest(controller, primitives.HistoryService)
	config := tests.NewDynamicConfig()
	config.NumberOfShards = 1
	shardID := int32(1)

	resource.HostInfoProvider.EXPECT().HostInfo().Return(resource.GetHostInfo()).AnyTimes()

	shard := historyi.NewMockControllableContext(controller)
	cf := NewMockContextFactory(controller)
	cf.EXPECT().CreateContext(shardID, gomock.Any()).Return(shard, nil).Times(1)

	resource.HistoryServiceResolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(resource.GetHostInfo(), nil).Times(1)

	resource.HistoryServiceResolver.EXPECT().
		AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).
		Return(nil).Times(1)

	shardController := newController(t, controller, resource, config, cf)
	shardController.Start()

	_, err := shardController.GetShardByID(shardID)
	require.NoError(t, err)

	resource.HistoryServiceResolver.EXPECT().
		RemoveListener(shardControllerMembershipUpdateListenerName).
		Return(nil).Times(1)

	shard.EXPECT().FinishStop().Times(1)
	shardController.Stop()
}

func TestAcquireViaTicker(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	resource := resourcetest.NewTest(controller, primitives.HistoryService)
	config := tests.NewDynamicConfig()
	config.NumberOfShards = 1
	config.AcquireShardInterval = func() time.Duration {
		return 100 * time.Millisecond
	}

	shardID := int32(1)

	resource.HostInfoProvider.EXPECT().HostInfo().Return(resource.GetHostInfo()).AnyTimes()

	shard := historyi.NewMockControllableContext(controller)
	shard.EXPECT().GetEngine(gomock.Any()).Return(nil, nil).AnyTimes()
	shard.EXPECT().AssertOwnership(gomock.Any()).Return(nil).AnyTimes()
	shard.EXPECT().IsValid().Return(true).AnyTimes()

	cf := NewMockContextFactory(controller)
	cf.EXPECT().CreateContext(shardID, gomock.Any()).Return(shard, nil).Times(1)

	resource.HistoryServiceResolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(resource.GetHostInfo(), nil).AnyTimes()

	resource.HistoryServiceResolver.EXPECT().
		AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).
		Return(nil).Times(1)

	shardController := newController(t, controller, resource, config, cf)
	shardController.Start()

	time.Sleep(500 * time.Millisecond)
	shardIDs := shardController.ShardIDs()
	require.Len(t, shardIDs, 1)
	require.Equal(t, shardID, shardIDs[0])

	resource.HistoryServiceResolver.EXPECT().
		RemoveListener(shardControllerMembershipUpdateListenerName).
		Return(nil).Times(1)

	shard.EXPECT().FinishStop().Times(1)
	shardController.Stop()
}

func TestAttemptAcquireUnowned(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	resource := resourcetest.NewTest(controller, primitives.HistoryService)
	config := tests.NewDynamicConfig()
	config.NumberOfShards = 1
	shardID := int32(1)

	resource.HostInfoProvider.EXPECT().HostInfo().Return(resource.GetHostInfo()).AnyTimes()

	otherHost := "otherHost"
	resource.HistoryServiceResolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(otherHost), nil).Times(1)

	resource.HistoryServiceResolver.EXPECT().
		AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).
		Return(nil).Times(1)

	cf := NewMockContextFactory(controller)
	shardController := newController(t, controller, resource, config, cf)
	shardController.Start()

	_, err := shardController.GetShardByID(shardID)
	require.Error(t, err)

	solErr, ok := err.(*serviceerrors.ShardOwnershipLost)
	require.True(t, ok)
	require.Equal(t, otherHost, solErr.OwnerHost)
	require.Equal(t, resource.GetHostInfo().Identity(), solErr.CurrentHost)

	resource.HistoryServiceResolver.EXPECT().
		RemoveListener(shardControllerMembershipUpdateListenerName).
		Return(nil).Times(1)

	shardController.Stop()
}
