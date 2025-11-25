package updateworkflowoptions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var (
	emptyOptions            = &workflowpb.WorkflowExecutionOptions{}
	unpinnedOverrideOptions = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		},
	}
	pinnedOverrideOptionsA = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: "X.A",
		},
	}
	pinnedOverrideOptionsB = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: "X.B",
		},
	}
)

func TestMergeOptions_VersionOverrideMask(t *testing.T) {
	updateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}}
	input := emptyOptions

	// Merge unpinned into empty options
	merged, err := mergeWorkflowExecutionOptions(input, unpinnedOverrideOptions, updateMask)
	require.NoError(t, err)
	require.EqualExportedValues(t, unpinnedOverrideOptions, merged)

	// Merge pinned_A into unpinned options
	merged, err = mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsA, updateMask)
	require.NoError(t, err)
	require.EqualExportedValues(t, pinnedOverrideOptionsA, merged)

	// Merge pinned_B into pinned_A options
	merged, err = mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsB, updateMask)
	require.NoError(t, err)
	require.EqualExportedValues(t, pinnedOverrideOptionsB, merged)

	// Unset versioning override
	merged, err = mergeWorkflowExecutionOptions(input, emptyOptions, updateMask)
	require.NoError(t, err)
	require.EqualExportedValues(t, emptyOptions, merged)
}

func TestMergeOptions_PartialMask(t *testing.T) {
	bothUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.behavior", "versioning_override.deployment"}}
	behaviorOnlyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.behavior"}}
	deploymentOnlyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.deployment"}}

	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, behaviorOnlyUpdateMask)
	require.Error(t, err)

	_, err = mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, deploymentOnlyUpdateMask)
	require.Error(t, err)

	merged, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, bothUpdateMask)
	require.NoError(t, err)
	require.EqualExportedValues(t, unpinnedOverrideOptions, merged)
}

func TestMergeOptions_EmptyMask(t *testing.T) {
	emptyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{}}
	input := pinnedOverrideOptionsB

	// Don't merge anything
	merged, err := mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsA, emptyUpdateMask)
	require.NoError(t, err)
	require.EqualExportedValues(t, input, merged)

	// Don't merge anything
	merged, err = mergeWorkflowExecutionOptions(input, nil, emptyUpdateMask)
	require.NoError(t, err)
	require.EqualExportedValues(t, input, merged)
}

func TestMergeOptions_AsteriskMask(t *testing.T) {
	asteriskUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"*"}}
	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, asteriskUpdateMask)
	require.Error(t, err)
}

func TestMergeOptions_FooMask(t *testing.T) {
	fooUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"foo"}}
	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, fooUpdateMask)
	require.Error(t, err)
}

type updateWorkflowOptionsTestDeps struct {
	controller                 *gomock.Controller
	shardContext               *historyi.MockShardContext
	namespaceRegistry          *namespace.MockRegistry
	workflowCache              *wcache.MockCache
	workflowConsistencyChecker api.WorkflowConsistencyChecker
	currentContext             *historyi.MockWorkflowContext
	currentMutableState        *historyi.MockMutableState
}

func setupUpdateWorkflowOptionsTest(t *testing.T) *updateWorkflowOptionsTestDeps {
	controller := gomock.NewController(t)
	namespaceRegistry := namespace.NewMockRegistry(controller)
	namespaceRegistry.EXPECT().GetNamespaceByID(tests.GlobalNamespaceEntry.ID()).Return(tests.GlobalNamespaceEntry, nil)

	shardContext := historyi.NewMockShardContext(controller)
	shardContext.EXPECT().GetNamespaceRegistry().Return(namespaceRegistry)
	shardContext.EXPECT().GetClusterMetadata().Return(clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true)))

	// mock a mutable state with an existing versioning override
	currentMutableState := historyi.NewMockMutableState(controller)
	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
		VersioningInfo: &workflowpb.WorkflowExecutionVersioningInfo{
			VersioningOverride: &workflowpb.VersioningOverride{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
				PinnedVersion: "X.123",
			},
		},
	}).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: tests.RunID,
	}).AnyTimes()

	currentContext := historyi.NewMockWorkflowContext(controller)
	currentContext.EXPECT().LoadMutableState(gomock.Any(), shardContext).Return(currentMutableState, nil)

	workflowCache := wcache.NewMockCache(controller)
	workflowCache.EXPECT().GetOrCreateChasmExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), chasm.WorkflowArchetype, locks.PriorityHigh).
		Return(currentContext, wcache.NoopReleaseFn, nil)

	workflowConsistencyChecker := api.NewWorkflowConsistencyChecker(
		shardContext,
		workflowCache,
	)

	return &updateWorkflowOptionsTestDeps{
		controller:                 controller,
		shardContext:               shardContext,
		namespaceRegistry:          namespaceRegistry,
		workflowCache:              workflowCache,
		workflowConsistencyChecker: workflowConsistencyChecker,
		currentContext:             currentContext,
		currentMutableState:        currentMutableState,
	}
}

func TestInvoke_Success(t *testing.T) {
	deps := setupUpdateWorkflowOptionsTest(t)
	defer deps.controller.Finish()

	expectedOverrideOptions := &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: "X.A",
		},
	}
	deps.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	deps.currentMutableState.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(expectedOverrideOptions.VersioningOverride, false, "", nil, nil, "").Return(&historypb.HistoryEvent{}, nil)
	deps.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), deps.shardContext).Return(nil)

	updateReq := &historyservice.UpdateWorkflowExecutionOptionsRequest{
		NamespaceId: tests.NamespaceID.String(),
		UpdateRequest: &workflowservice.UpdateWorkflowExecutionOptionsRequest{
			Namespace: tests.Namespace.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: tests.WorkflowID,
				RunId:      tests.RunID,
			},
			WorkflowExecutionOptions: expectedOverrideOptions,
			UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
		},
	}

	resp, err := Invoke(
		context.Background(),
		updateReq,
		deps.shardContext,
		deps.workflowConsistencyChecker,
	)
	require.NoError(t, err)
	require.NotNil(t, resp)
	proto.Equal(expectedOverrideOptions, resp.GetWorkflowExecutionOptions())
}
