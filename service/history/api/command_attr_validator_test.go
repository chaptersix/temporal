package api

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	nonTerminalCommands = []*commandpb.Command{
		{CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK},
		{CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK},
		{CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER},
		{CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER},
		{CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER},
		{CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES},
		{CommandType: enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES},
	}

	terminalCommands = []*commandpb.Command{
		{CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION},
	}
)

const (
	testNamespaceID       = "test namespace ID"
	testTargetNamespaceID = "test target namespace ID"
)

type commandAttrValidatorTestDeps struct {
	controller            *gomock.Controller
	mockNamespaceCache    *namespace.MockRegistry
	mockVisibilityManager *manager.MockVisibilityManager
	validator             *CommandAttrValidator
}

func setupCommandAttrValidatorTest(t *testing.T) *commandAttrValidatorTestDeps {
	controller := gomock.NewController(t)
	mockNamespaceCache := namespace.NewMockRegistry(controller)

	mockVisibilityManager := manager.NewMockVisibilityManager(controller)
	mockVisibilityManager.EXPECT().GetIndexName().Return("index-name").AnyTimes()
	mockVisibilityManager.EXPECT().
		ValidateCustomSearchAttributes(gomock.Any()).
		DoAndReturn(
			func(searchAttributes map[string]any) (map[string]any, error) {
				return searchAttributes, nil
			},
		).
		AnyTimes()

	config := &configs.Config{
		MaxIDLengthLimit:                  dynamicconfig.GetIntPropertyFn(1000),
		SearchAttributesNumberOfKeysLimit: dynamicconfig.GetIntPropertyFnFilteredByNamespace(100),
		SearchAttributesSizeOfValueLimit:  dynamicconfig.GetIntPropertyFnFilteredByNamespace(2 * 1024),
		SearchAttributesTotalSizeLimit:    dynamicconfig.GetIntPropertyFnFilteredByNamespace(40 * 1024),
		DefaultActivityRetryPolicy:        func(string) retrypolicy.DefaultRetrySettings { return retrypolicy.DefaultDefaultRetrySettings },
		DefaultWorkflowRetryPolicy:        func(string) retrypolicy.DefaultRetrySettings { return retrypolicy.DefaultDefaultRetrySettings },
		EnableCrossNamespaceCommands:      dynamicconfig.GetBoolPropertyFn(true),
		DefaultWorkflowTaskTimeout:        dynamicconfig.GetDurationPropertyFnFilteredByNamespace(primitives.DefaultWorkflowTaskTimeout),
	}
	validator := NewCommandAttrValidator(
		mockNamespaceCache,
		config,
		searchattribute.NewValidator(
			searchattribute.NewTestProvider(),
			searchattribute.NewTestMapperProvider(nil),
			config.SearchAttributesNumberOfKeysLimit,
			config.SearchAttributesSizeOfValueLimit,
			config.SearchAttributesTotalSizeLimit,
			mockVisibilityManager,
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		))

	t.Cleanup(func() {
		controller.Finish()
	})

	return &commandAttrValidatorTestDeps{
		controller:            controller,
		mockNamespaceCache:    mockNamespaceCache,
		mockVisibilityManager: mockVisibilityManager,
		validator:             validator,
	}
}

func TestValidateSignalExternalWorkflowExecutionAttributes(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
	)
	targetNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil).AnyTimes()
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil).AnyTimes()

	var attributes *commandpb.SignalExternalWorkflowExecutionCommandAttributes

	fc, err := deps.validator.ValidateSignalExternalWorkflowExecutionAttributes(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID), attributes)
	require.EqualError(t, err, "SignalExternalWorkflowExecutionCommandAttributes is not set on SignalExternalWorkflowExecutionCommand.")
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes = &commandpb.SignalExternalWorkflowExecutionCommandAttributes{}
	fc, err = deps.validator.ValidateSignalExternalWorkflowExecutionAttributes(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID), attributes)
	require.EqualError(t, err, "Execution is not set on SignalExternalWorkflowExecutionCommand.")
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes.Execution = &commonpb.WorkflowExecution{}
	attributes.Execution.WorkflowId = "workflow-id"
	fc, err = deps.validator.ValidateSignalExternalWorkflowExecutionAttributes(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID), attributes)
	require.EqualError(t, err, "SignalName is not set on SignalExternalWorkflowExecutionCommand. WorkflowId=workflow-id Namespace= RunId=")
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes.Execution.RunId = "run-id"
	fc, err = deps.validator.ValidateSignalExternalWorkflowExecutionAttributes(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID), attributes)
	require.EqualError(t, err, "Invalid RunId set on SignalExternalWorkflowExecutionCommand. WorkflowId=workflow-id Namespace= RunId=run-id SignalName=")
	attributes.Execution.RunId = tests.RunID
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, fc)

	attributes.SignalName = "my signal name"
	fc, err = deps.validator.ValidateSignalExternalWorkflowExecutionAttributes(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID), attributes)
	require.NoError(t, err)
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)

	attributes.Input = payloads.EncodeString("test input")
	fc, err = deps.validator.ValidateSignalExternalWorkflowExecutionAttributes(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID), attributes)
	require.NoError(t, err)
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)
}

func TestValidateUpsertWorkflowSearchAttributes(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceName := namespace.Name("tests.Namespace")
	var attributes *commandpb.UpsertWorkflowSearchAttributesCommandAttributes

	fc, err := deps.validator.ValidateUpsertWorkflowSearchAttributes(namespaceName, attributes)
	require.EqualError(t, err, "UpsertWorkflowSearchAttributesCommandAttributes is not set on UpsertWorkflowSearchAttributesCommand.")
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)

	attributes = &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{}
	fc, err = deps.validator.ValidateUpsertWorkflowSearchAttributes(namespaceName, attributes)
	require.EqualError(t, err, "SearchAttributes is not set on UpsertWorkflowSearchAttributesCommand.")
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)

	attributes.SearchAttributes = &commonpb.SearchAttributes{}
	fc, err = deps.validator.ValidateUpsertWorkflowSearchAttributes(namespaceName, attributes)
	require.EqualError(t, err, "IndexedFields is not set on UpsertWorkflowSearchAttributesCommand.")
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)

	saPayload, err := searchattribute.EncodeValue("bytes", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	require.NoError(t, err)
	attributes.SearchAttributes.IndexedFields = map[string]*commonpb.Payload{
		"Keyword01": saPayload,
	}
	fc, err = deps.validator.ValidateUpsertWorkflowSearchAttributes(namespaceName, attributes)
	require.NoError(t, err)
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)

	// Predefined Worker-Deployment related SA's should be rejected when they are attempted to be upserted
	deploymentRestrictedAttributes := []string{
		sadefs.TemporalWorkerDeploymentVersion,
		sadefs.TemporalWorkerDeployment,
		sadefs.TemporalWorkflowVersioningBehavior,
	}

	for _, attr := range deploymentRestrictedAttributes {
		attributes.SearchAttributes.IndexedFields = map[string]*commonpb.Payload{
			attr: saPayload,
		}
		fc, err = deps.validator.ValidateUpsertWorkflowSearchAttributes(namespaceName, attributes)
		require.EqualError(t, err, fmt.Sprintf("%s attribute can't be set in SearchAttributes", attr))
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)
	}
}

func TestValidateContinueAsNewWorkflowExecutionAttributes(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	executionTimeout := time.Hour
	workflowTypeName := "workflowType"
	taskQueue := "taskQueue"

	attributes := &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
		// workflow type name and task queue name should be retrieved from existing workflow info

		// WorkflowRunTimeout should be shorten to execution timeout
		WorkflowRunTimeout: durationpb.New(executionTimeout * 2),
		// WorkflowTaskTimeout should be shorten to max workflow task timeout
		WorkflowTaskTimeout: durationpb.New(maxWorkflowTaskStartToCloseTimeout * 2),
	}

	executionInfo := &persistencespb.WorkflowExecutionInfo{
		WorkflowTypeName:         workflowTypeName,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: durationpb.New(executionTimeout),
	}

	fc, err := deps.validator.ValidateContinueAsNewWorkflowExecutionAttributes(
		tests.Namespace,
		attributes,
		executionInfo,
	)
	require.NoError(t, err)
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED, fc)

	require.Equal(t, workflowTypeName, attributes.GetWorkflowType().GetName())
	require.Equal(t, taskQueue, attributes.GetTaskQueue().GetName())
	require.Equal(t, executionTimeout, attributes.GetWorkflowRunTimeout().AsDuration())
	require.Equal(t, maxWorkflowTaskStartToCloseTimeout, attributes.GetWorkflowTaskTimeout().AsDuration())

	// Predefined Worker-Deployment related SA's should be rejected when they are attempted to be set during CAN
	saPayload, _ := searchattribute.EncodeValue([]string{"a"}, enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	attributes.SearchAttributes = &commonpb.SearchAttributes{}

	deploymentRestrictedAttributes := []string{
		sadefs.TemporalWorkerDeploymentVersion,
		sadefs.TemporalWorkerDeployment,
		sadefs.TemporalWorkflowVersioningBehavior,
	}

	for _, attr := range deploymentRestrictedAttributes {
		attributes.SearchAttributes.IndexedFields = map[string]*commonpb.Payload{
			attr: saPayload,
		}
		fc, err = deps.validator.ValidateContinueAsNewWorkflowExecutionAttributes(
			tests.Namespace,
			attributes,
			executionInfo,
		)
		require.EqualError(t, err, fmt.Sprintf("invalid SearchAttributes on ContinueAsNewWorkflowExecutionCommand: %s attribute "+
			"can't be set in SearchAttributes. WorkflowType=%s TaskQueue=%s",
			attr, workflowTypeName, attributes.TaskQueue))
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, fc)
	}
}

func TestValidateModifyWorkflowProperties(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	var attributes *commandpb.ModifyWorkflowPropertiesCommandAttributes

	fc, err := deps.validator.ValidateModifyWorkflowProperties(attributes)
	require.EqualError(t, err, "ModifyWorkflowPropertiesCommandAttributes is not set on ModifyWorkflowPropertiesCommand.")
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, fc)

	// test attributes has at least one non-nil attribute
	attributes = &commandpb.ModifyWorkflowPropertiesCommandAttributes{}
	fc, err = deps.validator.ValidateModifyWorkflowProperties(attributes)
	require.EqualError(t, err, "UpsertedMemo is not set on ModifyWorkflowPropertiesCommand.")
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, fc)

	// test UpsertedMemo cannot be an empty map
	attributes = &commandpb.ModifyWorkflowPropertiesCommandAttributes{
		UpsertedMemo: &commonpb.Memo{},
	}
	fc, err = deps.validator.ValidateModifyWorkflowProperties(attributes)
	require.EqualError(t, err, "UpsertedMemo.Fields is not set on ModifyWorkflowPropertiesCommand.")
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, fc)
}

func TestValidateCrossNamespaceCall_LocalToLocal(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
	)
	targetNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.Nil(t, err)
}

func TestValidateCrossNamespaceCall_LocalToEffectiveLocal_SameCluster(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.Nil(t, err)
}

func TestValidateCrossNamespaceCall_LocalToEffectiveLocal_DiffCluster(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []string{cluster.TestAlternativeClusterName},
		},
		1234,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestValidateCrossNamespaceCall_LocalToGlobal(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestValidateCrossNamespaceCall_EffectiveLocalToLocal_SameCluster(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.Nil(t, err)
}

func TestValidateCrossNamespaceCall_EffectiveLocalToLocal_DiffCluster(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []string{cluster.TestAlternativeClusterName},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestValidateCrossNamespaceCall_EffectiveLocalToEffectiveLocal_SameCluster(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		5678,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.Nil(t, err)
}

func TestValidateCrossNamespaceCall_EffectiveLocalToEffectiveLocal_DiffCluster(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters:          []string{cluster.TestAlternativeClusterName},
		},
		5678,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestValidateCrossNamespaceCall_EffectiveLocalToGlobal(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
			},
		},
		5678,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestValidateCrossNamespaceCall_GlobalToLocal(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		cluster.TestCurrentClusterName,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestValidateCrossNamespaceCall_GlobalToEffectiveLocal(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		5678,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
			},
		},
		1234,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestValidateCrossNamespaceCall_GlobalToGlobal_DiffNamespace(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestAlternativeClusterName,
				cluster.TestCurrentClusterName,
			},
		},
		1234,
	)
	targetNamespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: testTargetNamespaceID},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(namespaceEntry, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(testTargetNamespaceID)).Return(targetNamespaceEntry, nil)

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(testTargetNamespaceID))
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestValidateCrossNamespaceCall_GlobalToGlobal_SameNamespace(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	targetNamespaceID := testNamespaceID

	err := deps.validator.validateCrossNamespaceCall(namespace.ID(testNamespaceID), namespace.ID(targetNamespaceID))
	require.Nil(t, err)
}

func TestValidateActivityRetryPolicy(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	testCases := []struct {
		name  string
		input *commonpb.RetryPolicy
		want  *commonpb.RetryPolicy
	}{
		{
			name:  "override non-set policy",
			input: &commonpb.RetryPolicy{},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(1 * time.Second),
				BackoffCoefficient: 2,
				MaximumInterval:    durationpb.New(100 * time.Second),
				MaximumAttempts:    0,
			},
		},
		{
			name: "do not override fully set policy",
			input: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(5 * time.Second),
				BackoffCoefficient: 10,
				MaximumInterval:    durationpb.New(20 * time.Second),
				MaximumAttempts:    8,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(5 * time.Second),
				BackoffCoefficient: 10,
				MaximumInterval:    durationpb.New(20 * time.Second),
				MaximumAttempts:    8,
			},
		},
		{
			name: "partial override of fields",
			input: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(0 * time.Second),
				BackoffCoefficient: 1.2,
				MaximumInterval:    durationpb.New(0 * time.Second),
				MaximumAttempts:    7,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(1 * time.Second),
				BackoffCoefficient: 1.2,
				MaximumInterval:    durationpb.New(100 * time.Second),
				MaximumAttempts:    7,
			},
		},
		{
			name: "set expected max interval if only init interval set",
			input: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(3 * time.Second),
				MaximumInterval: durationpb.New(0 * time.Second),
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(3 * time.Second),
				BackoffCoefficient: 2,
				MaximumInterval:    durationpb.New(300 * time.Second),
				MaximumAttempts:    0,
			},
		},
		{
			name: "override all defaults",
			input: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(0 * time.Second),
				BackoffCoefficient: 0,
				MaximumInterval:    durationpb.New(0 * time.Second),
				MaximumAttempts:    0,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(1 * time.Second),
				BackoffCoefficient: 2,
				MaximumInterval:    durationpb.New(100 * time.Second),
				MaximumAttempts:    0,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			attr := &commandpb.ScheduleActivityTaskCommandAttributes{
				RetryPolicy: tt.input,
			}

			err := deps.validator.validateActivityRetryPolicy(namespace.ID(testNamespaceID), attr.GetRetryPolicy())
			require.Nil(t, err, "expected no error")
			require.Equal(t, tt.want, attr.RetryPolicy, "unexpected retry policy")
		})
	}
}

func TestValidateCommandSequence_NoTerminalCommand(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	err := deps.validator.ValidateCommandSequence(nonTerminalCommands)
	require.NoError(t, err)
}

func TestValidateCommandSequence_ValidTerminalCommand(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	for _, terminalCommand := range terminalCommands {
		err := deps.validator.ValidateCommandSequence(append(nonTerminalCommands, terminalCommand))
		require.NoError(t, err)
	}
}

func TestValidateCommandSequence_InvalidTerminalCommand(t *testing.T) {
	deps := setupCommandAttrValidatorTest(t)

	for _, terminalCommand := range terminalCommands {
		err := deps.validator.ValidateCommandSequence(append(
			[]*commandpb.Command{terminalCommand},
			nonTerminalCommands[int(rand.Int31n(int32(len(nonTerminalCommands))))],
		))
		require.Error(t, err)
		require.IsType(t, &serviceerror.InvalidArgument{}, err)
	}
}
