package history

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type chasmEngineTestFixture struct {
	controller            *gomock.Controller
	mockShard             *shard.ContextTest
	mockEngine            *historyi.MockEngine
	mockShardController   *shard.MockController
	mockExecutionManager  *persistence.MockExecutionManager
	mockNamespaceRegistry *namespace.MockRegistry
	mockClusterMetadata   *cluster.MockMetadata

	namespaceEntry *namespace.Namespace
	executionCache wcache.Cache
	registry       *chasm.Registry
	config         *configs.Config

	engine *ChasmEngine
}

func setupChasmEngineTest(t *testing.T) *chasmEngineTestFixture {
	controller := gomock.NewController(t)
	mockShardController := shard.NewMockController(controller)
	mockEngine := historyi.NewMockEngine(controller)

	config := tests.NewDynamicConfig()
	config.EnableChasm = dynamicconfig.GetBoolPropertyFn(true)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
	)
	executionCache := wcache.NewHostLevelCache(
		mockShard.GetConfig(),
		mockShard.GetLogger(),
		metrics.NoopMetricsHandler,
	)
	namespaceEntry := tests.GlobalNamespaceEntry

	mockExecutionManager := mockShard.Resource.ExecutionMgr
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockNamespaceRegistry := mockShard.Resource.NamespaceCache
	mockShardController.EXPECT().GetShardByID(gomock.Any()).Return(mockShard, nil).AnyTimes()
	mockClusterMetadata.EXPECT().IsVersionFromSameCluster(cluster.TestCurrentClusterInitialFailoverVersion, tests.Version).Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
	mockNamespaceRegistry.EXPECT().GetNamespace(namespaceEntry.Name()).Return(namespaceEntry, nil).AnyTimes()

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	mockShard.SetStateMachineRegistry(reg)

	registry := chasm.NewRegistry(mockShard.GetLogger())
	err = registry.Register(&testChasmLibrary{})
	require.NoError(t, err)
	mockShard.SetChasmRegistry(registry)

	mockShard.SetEngineForTesting(mockEngine)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()

	engine := newChasmEngine(
		executionCache,
		registry,
		config,
	)
	engine.SetShardController(mockShardController)

	return &chasmEngineTestFixture{
		controller:            controller,
		mockShard:             mockShard,
		mockEngine:            mockEngine,
		mockShardController:   mockShardController,
		mockExecutionManager:  mockExecutionManager,
		mockNamespaceRegistry: mockNamespaceRegistry,
		mockClusterMetadata:   mockClusterMetadata,
		namespaceEntry:        namespaceEntry,
		executionCache:        executionCache,
		registry:              registry,
		config:                config,
		engine:                engine,
	}
}

func TestNewExecution_BrandNew(t *testing.T) {
	f := setupChasmEngineTest(t)
	tv := testvars.New(t)

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
		},
	)
	newActivityID := tv.ActivityID()

	var runID string
	f.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *persistence.CreateWorkflowExecutionRequest,
		) (*persistence.CreateWorkflowExecutionResponse, error) {
			validateCreateRequest(t, f, request, newActivityID, "", 0)
			runID = request.NewWorkflowSnapshot.ExecutionState.RunId
			return tests.CreateWorkflowExecutionResponse, nil
		},
	).Times(1)

	executionKey, serializedRef, err := f.engine.NewExecution(
		context.Background(),
		ref,
		newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyRejectDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	require.NoError(t, err)
	expectedExecutionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       runID,
	}
	require.Equal(t, expectedExecutionKey, executionKey)
	validateNewExecutionResponseRef(t, f, serializedRef, expectedExecutionKey)
}

func TestNewExecution_RequestIDDedup(t *testing.T) {
	f := setupChasmEngineTest(t)
	tv := testvars.New(t)
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
		},
	)
	newActivityID := tv.ActivityID()

	f.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		currentRunConditionFailedErr(
			f,
			tv,
			enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		),
	).Times(1)

	executionKey, serializedRef, err := f.engine.NewExecution(
		context.Background(),
		ref,
		newTestExecutionFn(newActivityID),
		chasm.WithRequestID(tv.RequestID()),
	)
	require.NoError(t, err)

	expectedExecutionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       tv.RunID(),
	}
	require.Equal(t, expectedExecutionKey, executionKey)
	validateNewExecutionResponseRef(t, f, serializedRef, expectedExecutionKey)
}

func TestNewExecution_ReusePolicy_AllowDuplicate(t *testing.T) {
	f := setupChasmEngineTest(t)
	tv := testvars.New(t)
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
		},
	)
	newActivityID := tv.ActivityID()
	currentRunConditionFailedErr := currentRunConditionFailedErr(
		f,
		tv,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	)

	var runID string
	f.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		currentRunConditionFailedErr,
	).Times(1)
	f.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *persistence.CreateWorkflowExecutionRequest,
		) (*persistence.CreateWorkflowExecutionResponse, error) {
			validateCreateRequest(t, f, request, newActivityID, tv.RunID(), currentRunConditionFailedErr.LastWriteVersion)
			runID = request.NewWorkflowSnapshot.ExecutionState.RunId
			return tests.CreateWorkflowExecutionResponse, nil
		},
	).Times(1)

	executionKey, serializedRef, err := f.engine.NewExecution(
		context.Background(),
		ref,
		newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	require.NoError(t, err)

	expectedExecutionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       runID,
	}
	require.Equal(t, expectedExecutionKey, executionKey)
	validateNewExecutionResponseRef(t, f, serializedRef, expectedExecutionKey)
}

func TestNewExecution_ReusePolicy_FailedOnly_Success(t *testing.T) {
	f := setupChasmEngineTest(t)
	tv := testvars.New(t)
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
		},
	)
	newActivityID := tv.ActivityID()
	currentRunConditionFailedErr := currentRunConditionFailedErr(
		f,
		tv,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	)

	var runID string
	f.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		currentRunConditionFailedErr,
	).Times(1)
	f.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *persistence.CreateWorkflowExecutionRequest,
		) (*persistence.CreateWorkflowExecutionResponse, error) {
			validateCreateRequest(t, f, request, newActivityID, tv.RunID(), currentRunConditionFailedErr.LastWriteVersion)
			runID = request.NewWorkflowSnapshot.ExecutionState.RunId
			return tests.CreateWorkflowExecutionResponse, nil
		},
	).Times(1)

	executionKey, serializedRef, err := f.engine.NewExecution(
		context.Background(),
		ref,
		newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	require.NoError(t, err)

	expectedExecutionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       runID,
	}
	require.Equal(t, expectedExecutionKey, executionKey)
	validateNewExecutionResponseRef(t, f, serializedRef, expectedExecutionKey)
}

func TestNewExecution_ReusePolicy_FailedOnly_Fail(t *testing.T) {
	f := setupChasmEngineTest(t)
	tv := testvars.New(t)
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
		},
	)
	newActivityID := tv.ActivityID()

	f.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		currentRunConditionFailedErr(
			f,
			tv,
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		),
	).Times(1)

	_, _, err := f.engine.NewExecution(
		context.Background(),
		ref,
		newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	require.ErrorAs(t, err, new(*chasm.ExecutionAlreadyStartedError))
}

func TestNewExecution_ReusePolicy_RejectDuplicate(t *testing.T) {
	f := setupChasmEngineTest(t)
	tv := testvars.New(t)
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
		},
	)
	newActivityID := tv.ActivityID()

	f.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		currentRunConditionFailedErr(
			f,
			tv,
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		),
	).Times(1)

	_, _, err := f.engine.NewExecution(
		context.Background(),
		ref,
		newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyRejectDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	require.ErrorAs(t, err, new(*chasm.ExecutionAlreadyStartedError))
}

func TestNewExecution_ConflictPolicy_UseExisting(t *testing.T) {
	f := setupChasmEngineTest(t)
	tv := testvars.New(t)
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
		},
	)
	newActivityID := tv.ActivityID()
	// Current run is still running, conflict policy will be used.
	currentRunConditionFailedErr := currentRunConditionFailedErr(
		f,
		tv,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	)

	f.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		currentRunConditionFailedErr,
	).Times(1)

	executionKey, serializedRef, err := f.engine.NewExecution(
		context.Background(),
		ref,
		newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyUseExisting,
		),
	)
	require.NoError(t, err)

	expectedExecutionKey := chasm.ExecutionKey{
		NamespaceID: string(tests.NamespaceID),
		BusinessID:  tv.WorkflowID(),
		RunID:       tv.RunID(),
	}
	require.Equal(t, expectedExecutionKey, executionKey)
	validateNewExecutionResponseRef(t, f, serializedRef, expectedExecutionKey)
}

func TestNewExecution_ConflictPolicy_TerminateExisting(t *testing.T) {
	f := setupChasmEngineTest(t)
	tv := testvars.New(t)
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       "",
		},
	)
	newActivityID := tv.ActivityID()
	// Current run is still running, conflict policy will be used.
	currentRunConditionFailedErr := currentRunConditionFailedErr(
		f,
		tv,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	)

	f.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		currentRunConditionFailedErr,
	).Times(1)

	_, _, err := f.engine.NewExecution(
		context.Background(),
		ref,
		newTestExecutionFn(newActivityID),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyTerminateExisting,
		),
	)
	require.ErrorAs(t, err, new(*serviceerror.Unimplemented))
}

func TestUpdateComponent_Success(t *testing.T) {
	f := setupChasmEngineTest(t)
	tv := testvars.New(t)
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       tv.RunID(),
		},
	)
	newActivityID := tv.ActivityID()

	f.mockExecutionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{
			State: buildPersistenceMutableState(t, f, ref.ExecutionKey, &persistencespb.ActivityInfo{
				ActivityId: "",
			}),
		}, nil).Times(1)
	f.mockExecutionManager.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *persistence.UpdateWorkflowExecutionRequest,
		) (*persistence.UpdateWorkflowExecutionResponse, error) {
			require.Len(t, request.UpdateWorkflowMutation.UpsertChasmNodes, 1)
			updatedNode, ok := request.UpdateWorkflowMutation.UpsertChasmNodes[""]
			require.True(t, ok)

			activityInfo := &persistencespb.ActivityInfo{}
			err := serialization.Decode(updatedNode.Data, activityInfo)
			require.NoError(t, err)
			require.Equal(t, newActivityID, activityInfo.ActivityId)
			return tests.UpdateWorkflowExecutionResponse, nil
		},
	).Times(1)

	// TODO: validate returned component once Ref() method of chasm tree is implememented.
	_, err := f.engine.UpdateComponent(
		context.Background(),
		ref,
		func(
			ctx chasm.MutableContext,
			component chasm.Component,
		) error {
			tc, ok := component.(*testComponent)
			require.True(t, ok)
			tc.ActivityInfo.ActivityId = newActivityID
			return nil
		},
	)
	require.NoError(t, err)
}

func TestReadComponent_Success(t *testing.T) {
	f := setupChasmEngineTest(t)
	tv := testvars.New(t)
	tv = tv.WithRunID(tv.Any().RunID())

	ref := chasm.NewComponentRef[*testComponent](
		chasm.ExecutionKey{
			NamespaceID: string(tests.NamespaceID),
			BusinessID:  tv.WorkflowID(),
			RunID:       tv.RunID(),
		},
	)
	expectedActivityID := tv.ActivityID()

	f.mockExecutionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{
			State: buildPersistenceMutableState(t, f, ref.ExecutionKey, &persistencespb.ActivityInfo{
				ActivityId: expectedActivityID,
			}),
		}, nil).Times(1)

	err := f.engine.ReadComponent(
		context.Background(),
		ref,
		func(
			ctx chasm.Context,
			component chasm.Component,
		) error {
			tc, ok := component.(*testComponent)
			require.True(t, ok)
			require.Equal(t, expectedActivityID, tc.ActivityInfo.ActivityId)
			return nil
		},
	)
	require.NoError(t, err)
}

// Helper functions

func newTestExecutionFn(
	activityID string,
) func(ctx chasm.MutableContext) (chasm.Component, error) {
	return func(ctx chasm.MutableContext) (chasm.Component, error) {
		return &testComponent{
			ActivityInfo: &persistencespb.ActivityInfo{
				ActivityId: activityID,
			},
		}, nil
	}
}

func validateCreateRequest(
	t *testing.T,
	f *chasmEngineTestFixture,
	request *persistence.CreateWorkflowExecutionRequest,
	expectedActivityID string,
	expectedPreviousRunID string,
	expectedPreviousLastWriteVersion int64,
) {
	if expectedPreviousRunID == "" && expectedPreviousLastWriteVersion == 0 {
		require.Equal(t, persistence.CreateWorkflowModeBrandNew, request.Mode)
	} else {
		require.Equal(t, persistence.CreateWorkflowModeUpdateCurrent, request.Mode)
		require.Equal(t, expectedPreviousRunID, request.PreviousRunID)
		require.Equal(t, expectedPreviousLastWriteVersion, request.PreviousLastWriteVersion)
	}

	require.Len(t, request.NewWorkflowSnapshot.ChasmNodes, 1)
	updatedNode, ok := request.NewWorkflowSnapshot.ChasmNodes[""]
	require.True(t, ok)

	activityInfo := &persistencespb.ActivityInfo{}
	err := serialization.Decode(updatedNode.Data, activityInfo)
	require.NoError(t, err)
	require.Equal(t, expectedActivityID, activityInfo.ActivityId)
}

func validateNewExecutionResponseRef(
	t *testing.T,
	f *chasmEngineTestFixture,
	serializedRef []byte,
	expectedExecutionKey chasm.ExecutionKey,
) {
	deserializedRef, err := chasm.DeserializeComponentRef(serializedRef)
	require.NoError(t, err)
	require.Equal(t, expectedExecutionKey, deserializedRef.ExecutionKey)

	archetypeID, err := deserializedRef.ArchetypeID(f.registry)
	require.NoError(t, err)
	fqn, ok := f.registry.ComponentFqnByID(archetypeID)
	require.True(t, ok)
	require.Equal(t, "TestLibrary.test_component", fqn)
}

func currentRunConditionFailedErr(
	f *chasmEngineTestFixture,
	tv *testvars.TestVars,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) *persistence.CurrentWorkflowConditionFailedError {
	return &persistence.CurrentWorkflowConditionFailedError{
		RequestIDs: map[string]*persistencespb.RequestIDInfo{
			tv.RequestID(): {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				EventId:   0,
			},
		},
		RunID:            tv.RunID(),
		State:            state,
		Status:           status,
		LastWriteVersion: f.namespaceEntry.FailoverVersion() - 1,
	}
}

func buildPersistenceMutableState(
	t *testing.T,
	f *chasmEngineTestFixture,
	key chasm.ExecutionKey,
	componentState proto.Message,
) *persistencespb.WorkflowMutableState {

	testComponentTypeID, ok := f.mockShard.ChasmRegistry().ComponentIDFor(&testComponent{})
	require.True(t, ok)

	return &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: key.NamespaceID,
			WorkflowId:  key.BusinessID,
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{},
				},
			},
			TransitionHistory: []*persistencespb.VersionedTransition{
				{
					NamespaceFailoverVersion: f.namespaceEntry.FailoverVersion(),
					TransitionCount:          10,
				},
			},
			ExecutionStats: &persistencespb.ExecutionStats{},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:     key.RunID,
			State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(f.mockShard.GetTimeSource().Now().Add(-1 * time.Minute)),
		},
		ChasmNodes: map[string]*persistencespb.ChasmNode{
			"": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: f.namespaceEntry.FailoverVersion(),
						TransitionCount:          1,
					},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: f.namespaceEntry.FailoverVersion(),
						TransitionCount:          10,
					},
					Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{
							TypeId: testComponentTypeID,
						},
					},
				},
				Data: serializeComponentState(t, componentState),
			},
		},
	}
}

func serializeComponentState(
	t *testing.T,
	state proto.Message,
) *commonpb.DataBlob {
	blob, err := serialization.ProtoEncode(state)
	require.NoError(t, err)
	return blob
}

const (
	testComponentPausedSAName   = "PausedSA"
	testComponentPausedMemoName = "PausedMemo"
)

var (
	testComponentPausedSearchAttribute = chasm.NewSearchAttributeBool(testComponentPausedSAName, chasm.SearchAttributeFieldBool01)

	_ chasm.VisibilitySearchAttributesProvider = (*testComponent)(nil)
	_ chasm.VisibilityMemoProvider             = (*testComponent)(nil)
)

type testComponent struct {
	chasm.UnimplementedComponent

	ActivityInfo *persistencespb.ActivityInfo
}

func (l *testComponent) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (l *testComponent) SearchAttributes(_ chasm.Context) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		testComponentPausedSearchAttribute.Value(l.ActivityInfo.Paused),
	}
}

func (l *testComponent) Memo(_ chasm.Context) map[string]chasm.VisibilityValue {
	return map[string]chasm.VisibilityValue{
		testComponentPausedMemoName: chasm.VisibilityValueBool(l.ActivityInfo.Paused),
	}
}

func newTestComponentStateBlob(info *persistencespb.ActivityInfo) *commonpb.DataBlob {
	data, _ := info.Marshal()
	return &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         data,
	}
}

type testChasmLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *testChasmLibrary) Name() string {
	return "TestLibrary"
}

func (l *testChasmLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*testComponent]("test_component",
			chasm.WithSearchAttributes(testComponentPausedSearchAttribute)),
	}
}
