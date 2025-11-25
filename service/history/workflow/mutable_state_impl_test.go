package workflow

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	serviceerror2 "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	mutableStateTestDeps struct {
		controller      *gomock.Controller
		mockConfig      *configs.Config
		mockShard       *shard.ContextTest
		mockEventsCache *events.MockCache
		namespaceEntry  *namespace.Namespace
		mutableState    *MutableStateImpl
		logger          log.Logger
		testScope       tally.TestScope
		cleanup         func()
	}
)

var (
	testPayload = &commonpb.Payload{
		Metadata: map[string][]byte{
			"random metadata key": []byte("random metadata value"),
		},
		Data: []byte("random data"),
	}
	testPayloads                 = &commonpb.Payloads{Payloads: []*commonpb.Payload{testPayload}}
	workflowTaskCompletionLimits = historyi.WorkflowTaskCompletionLimits{
		MaxResetPoints:              10,
		MaxSearchAttributeValueSize: 1024,
	}
	deployment1 = &deploymentpb.Deployment{
		SeriesName: "my_app",
		BuildId:    "build_1",
	}
	deployment2 = &deploymentpb.Deployment{
		SeriesName: "my_app",
		BuildId:    "build_2",
	}
	deployment3 = &deploymentpb.Deployment{
		SeriesName: "my_app",
		BuildId:    "build_3",
	}
	versionOverrideMask = &fieldmaskpb.FieldMask{Paths: []string{"versioning_behavior_override"}}
	pinnedOptions1      = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment1)),
		},
	}
	pinnedOptions2 = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment2)),
		},
	}
	pinnedOptions3 = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment3)),
		},
	}
	unpinnedOptions = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		},
	}
	emptyOptions = &workflowpb.WorkflowExecutionOptions{}
)

func setupMutableStateTest(t *testing.T, replicationMultipleBatches bool) *mutableStateTestDeps {
	controller := gomock.NewController(t)
	mockEventsCache := events.NewMockCache(controller)

	mockConfig := tests.NewDynamicConfig()
	mockConfig.ReplicationMultipleBatches = dynamicconfig.GetBoolPropertyFn(replicationMultipleBatches)
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		mockConfig,
	)
	reg := hsm.NewRegistry()
	require.NoError(t, RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterStateMachine(reg))
	mockShard.SetStateMachineRegistry(reg)

	mockConfig.MutableStateActivityFailureSizeLimitWarn = func(namespace string) int { return 1 * 1024 }
	mockConfig.MutableStateActivityFailureSizeLimitError = func(namespace string) int { return 2 * 1024 }
	mockConfig.EnableTransitionHistory = func() bool { return true }
	mockShard.SetEventsCacheForTesting(mockEventsCache)

	namespaceEntry := tests.GlobalNamespaceEntry
	mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(namespaceEntry.IsGlobalNamespace(), namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	testScope := mockShard.Resource.MetricsScope.(tally.TestScope)
	logger := mockShard.GetLogger()

	mutableState := NewMutableState(mockShard, mockEventsCache, logger, namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC())

	return &mutableStateTestDeps{
		controller:      controller,
		mockConfig:      mockConfig,
		mockShard:       mockShard,
		mockEventsCache: mockEventsCache,
		namespaceEntry:  namespaceEntry,
		mutableState:    mutableState,
		logger:          logger,
		testScope:       testScope,
		cleanup: func() {
			controller.Finish()
			mockShard.StopForTest()
		},
	}
}

func (d *mutableStateTestDeps) newMutableState() {
	d.mutableState = NewMutableState(d.mockShard, d.mockEventsCache, d.logger, d.mutableState.GetNamespaceEntry(), tests.WorkflowID, tests.RunID, time.Now().UTC())
}

func TestTransientWorkflowTaskCompletionFirstBatchApplied_ApplyWorkflowTaskCompleted(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			version := int64(12)
			workflowID := "some random workflow ID"
			runID := uuid.New()
			deps.mutableState = TestGlobalMutableState(
				deps.mockShard,
				deps.mockEventsCache,
				deps.logger,
				version,
				workflowID,
				runID,
			)

			newWorkflowTaskScheduleEvent, newWorkflowTaskStartedEvent := deps.prepareTransientWorkflowTaskCompletionFirstBatchApplied(t, version, workflowID, runID)

			newWorkflowTaskCompletedEvent := &historypb.HistoryEvent{
				Version:   version,
				EventId:   newWorkflowTaskStartedEvent.GetEventId() + 1,
				EventTime: timestamppb.New(time.Now().UTC()),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: newWorkflowTaskScheduleEvent.GetEventId(),
					StartedEventId:   newWorkflowTaskStartedEvent.GetEventId(),
					Identity:         "some random identity",
				}},
			}
			deps.mutableState.SetHistoryBuilder(historybuilder.NewImmutable([]*historypb.HistoryEvent{
				newWorkflowTaskCompletedEvent,
			}))
			err := deps.mutableState.ApplyWorkflowTaskCompletedEvent(newWorkflowTaskCompletedEvent)
			require.NoError(t, err)
			require.Equal(t, 0, deps.mutableState.hBuilder.NumBufferedEvents())

		})
	}
}

func TestTransientWorkflowTaskCompletionFirstBatchApplied_FailoverWorkflowTaskTimeout(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			version := int64(12)
			workflowID := "some random workflow ID"
			runID := uuid.New()
			deps.mutableState = TestGlobalMutableState(
				deps.mockShard,
				deps.mockEventsCache,
				deps.logger,
				version,
				workflowID,
				runID,
			)

			newWorkflowTaskScheduleEvent, _ := deps.prepareTransientWorkflowTaskCompletionFirstBatchApplied(t, version, workflowID, runID)

			newWorkflowTask := deps.mutableState.GetWorkflowTaskByID(newWorkflowTaskScheduleEvent.GetEventId())
			require.NotNil(t, newWorkflowTask)

			_, err := deps.mutableState.AddWorkflowTaskTimedOutEvent(
				newWorkflowTask,
			)
			require.NoError(t, err)
			require.Equal(t, 0, deps.mutableState.hBuilder.NumBufferedEvents())

		})
	}
}

func TestTransientWorkflowTaskCompletionFirstBatchApplied_FailoverWorkflowTaskFailed(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			version := int64(12)
			workflowID := "some random workflow ID"
			runID := uuid.New()
			deps.mutableState = TestGlobalMutableState(
				deps.mockShard,
				deps.mockEventsCache,
				deps.logger,
				version,
				workflowID,
				runID,
			)

			newWorkflowTaskScheduleEvent, _ := deps.prepareTransientWorkflowTaskCompletionFirstBatchApplied(t, version, workflowID, runID)

			newWorkflowTask := deps.mutableState.GetWorkflowTaskByID(newWorkflowTaskScheduleEvent.GetEventId())
			require.NotNil(t, newWorkflowTask)

			_, err := deps.mutableState.AddWorkflowTaskFailedEvent(
				newWorkflowTask,
				enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
				failure.NewServerFailure("some random workflow task failure details", false),
				"some random workflow task failure identity",
				nil,
				"",
				"",
				"",
				0,
			)
			require.NoError(t, err)
			require.Equal(t, 0, deps.mutableState.hBuilder.NumBufferedEvents())

		})
	}
}

func TestRedirectInfoValidation_Valid(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			deps.createVersionedMutableStateWithCompletedWFT(t, tq)

			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			e, wft, err := deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				tq,
				"",
				worker_versioning.StampFromBuildId("b2"),
				&taskqueuespb.BuildIdRedirectInfo{AssignedBuildId: "b1"},
				nil,
				false,
			)
			require.NoError(t, err)
			require.Equal(t, "b2", wft.BuildId)
			require.Equal(t, "b2", e.GetWorkflowTaskStartedEventAttributes().GetWorkerVersion().GetBuildId())
			require.Equal(t, "b2", deps.mutableState.GetAssignedBuildId())
			require.Equal(t, int64(1), wft.BuildIdRedirectCounter)
			require.Equal(t, int64(1), deps.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())

		})
	}
}

func TestRedirectInfoValidation_Invalid(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			deps.createVersionedMutableStateWithCompletedWFT(t, tq)

			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			_, _, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				tq,
				"",
				worker_versioning.StampFromBuildId("b2"),
				&taskqueuespb.BuildIdRedirectInfo{AssignedBuildId: "b0"},
				nil,
				false,
			)
			expectedErr := &serviceerror2.ObsoleteDispatchBuildId{}
			require.ErrorAs(t, err, &expectedErr)
			require.Equal(t, "b1", deps.mutableState.GetAssignedBuildId())
			require.Equal(t, int64(0), deps.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())

		})
	}
}

func TestRedirectInfoValidation_EmptyRedirectInfo(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			deps.createVersionedMutableStateWithCompletedWFT(t, tq)

			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			_, _, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				tq,
				"",
				worker_versioning.StampFromBuildId("b2"),
				nil,
				nil,
				false,
			)
			expectedErr := &serviceerror2.ObsoleteDispatchBuildId{}
			require.ErrorAs(t, err, &expectedErr)
			require.Equal(t, "b1", deps.mutableState.GetAssignedBuildId())
			require.Equal(t, int64(0), deps.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())

		})
	}
}

func TestRedirectInfoValidation_EmptyStamp(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			deps.createVersionedMutableStateWithCompletedWFT(t, tq)

			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			_, _, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				tq,
				"",
				nil,
				&taskqueuespb.BuildIdRedirectInfo{AssignedBuildId: "b1"},
				nil,
				false,
			)
			expectedErr := &serviceerror2.ObsoleteDispatchBuildId{}
			require.ErrorAs(t, err, &expectedErr)
			require.Equal(t, "b1", deps.mutableState.GetAssignedBuildId())
			require.Equal(t, int64(0), deps.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())

		})
	}
}

func TestRedirectInfoValidation_Sticky(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			sticky := &taskqueuepb.TaskQueue{Name: "sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY}
			deps.createVersionedMutableStateWithCompletedWFT(t, tq)

			deps.mutableState.SetStickyTaskQueue(sticky.Name, durationpb.New(time.Second))
			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			e, wft, err := deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				sticky,
				"",
				nil,
				nil,
				nil,
				false,
			)
			require.NoError(t, err)
			require.Equal(t, "", wft.BuildId)
			require.Equal(t, "", e.GetWorkflowTaskStartedEventAttributes().GetWorkerVersion().GetBuildId())
			require.Equal(t, "b1", deps.mutableState.GetAssignedBuildId())
			require.Equal(t, int64(0), wft.BuildIdRedirectCounter)
			require.Equal(t, int64(0), deps.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())

		})
	}
}

func TestRedirectInfoValidation_StickyInvalid(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			sticky := &taskqueuepb.TaskQueue{Name: "sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY}
			deps.createVersionedMutableStateWithCompletedWFT(t, tq)

			deps.mutableState.SetStickyTaskQueue(sticky.Name, durationpb.New(time.Second))
			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			_, _, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				&taskqueuepb.TaskQueue{Name: "another-sticky-tq"},
				"",
				nil,
				nil,
				nil,
				false,
			)
			expectedErr := &serviceerror2.ObsoleteDispatchBuildId{}
			require.ErrorAs(t, err, &expectedErr)
			require.Equal(t, "b1", deps.mutableState.GetAssignedBuildId())
			require.Equal(t, int64(0), deps.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())

		})
	}
}

func TestRedirectInfoValidation_UnexpectedSticky(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			sticky := &taskqueuepb.TaskQueue{Name: "sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY}
			deps.createVersionedMutableStateWithCompletedWFT(t, tq)

			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			_, _, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				sticky,
				"",
				nil,
				nil,
				nil,
				false,
			)
			expectedErr := &serviceerror2.ObsoleteDispatchBuildId{}
			require.ErrorAs(t, err, &expectedErr)
			require.Equal(t, "b1", deps.mutableState.GetAssignedBuildId())
			require.Equal(t, int64(0), deps.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())

		})
	}
}

func TestPopulateDeleteTasks_WithWorkflowTaskTimeouts(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			// Test that workflow task timeout task references are added to BestEffortDeleteTasks when present.
			version := int64(1)
			workflowID := "wf-timeout-delete"
			runID := uuid.New()
			deps.mutableState = TestGlobalMutableState(
				deps.mockShard,
				deps.mockEventsCache,
				deps.logger,
				version,
				workflowID,
				runID,
			)

			// Create mock timeout tasks that meet the criteria
			now := time.Now().UTC()
			mockScheduleToStartTask := &tasks.WorkflowTaskTimeoutTask{
				WorkflowKey: definition.NewWorkflowKey(
					deps.mutableState.GetExecutionInfo().NamespaceId,
					workflowID,
					runID,
				),
				VisibilityTimestamp: now.Add(10 * time.Second), // < 120s
				TaskID:              123,
				TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				InMemory:            false, // Persisted task
			}

			mockStartToCloseTask := &tasks.WorkflowTaskTimeoutTask{
				WorkflowKey: definition.NewWorkflowKey(
					deps.mutableState.GetExecutionInfo().NamespaceId,
					workflowID,
					runID,
				),
				VisibilityTimestamp: now.Add(30 * time.Second), // < 120s
				TaskID:              456,
				TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				InMemory:            false, // Persisted task
			}

			// Schedule and start a workflow task
			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)

			sticky := &taskqueuepb.TaskQueue{Name: "sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY}
			_, wft, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				sticky,
				"",
				nil,
				nil,
				nil,
				false,
			)
			require.NoError(t, err)

			// Set timeout tasks directly in mutable state (simulating what task_generator does)
			deps.mutableState.SetWorkflowTaskScheduleToStartTimeoutTask(mockScheduleToStartTask)
			deps.mutableState.SetWorkflowTaskStartToCloseTimeoutTask(mockStartToCloseTask)
			// Call UpdateWorkflowTask to persist the workflow task info to ExecutionInfo
			deps.mutableState.workflowTaskManager.UpdateWorkflowTask(wft)

			// Complete the workflow task
			_, err = deps.mutableState.AddWorkflowTaskCompletedEvent(
				wft,
				&workflowservice.RespondWorkflowTaskCompletedRequest{},
				workflowTaskCompletionLimits,
			)
			require.NoError(t, err)

			// Verify that BestEffortDeleteTasks contains the timeout task keys
			del := deps.mutableState.BestEffortDeleteTasks
			require.Contains(t, del, tasks.CategoryTimer)
			require.Equal(t, 2, len(del[tasks.CategoryTimer]), "Should have both ScheduleToStart and StartToClose timeout tasks")
			require.Contains(t, del[tasks.CategoryTimer], mockScheduleToStartTask.GetKey())
			require.Contains(t, del[tasks.CategoryTimer], mockStartToCloseTask.GetKey())

		})
	}
}

func TestPopulateDeleteTasks_LongTimeout_NotIncluded(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			// Test that timeout tasks with very long timeouts (> 120s) are NOT added to BestEffortDeleteTasks.
			version := int64(1)
			workflowID := "wf-long-timeout"
			runID := uuid.New()
			deps.mutableState = TestGlobalMutableState(
				deps.mockShard,
				deps.mockEventsCache,
				deps.logger,
				version,
				workflowID,
				runID,
			)

			// Schedule a workflow task - this sets the scheduled time
			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)

			sticky := &taskqueuepb.TaskQueue{Name: "sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY}
			_, wft, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				sticky,
				"",
				nil,
				nil,
				nil,
				false,
			)
			require.NoError(t, err)

			// Get the actual scheduled time from wft (this is what will be used in the calculation)
			scheduledTime := wft.ScheduledTime
			if scheduledTime.IsZero() {
				// If scheduled time is not set in wft, use current time
				scheduledTime = time.Now().UTC()
			}

			// Create a timeout task with timeout > 120s relative to the actual scheduled time
			mockLongTimeoutTask := &tasks.WorkflowTaskTimeoutTask{
				WorkflowKey: definition.NewWorkflowKey(
					deps.mutableState.GetExecutionInfo().NamespaceId,
					workflowID,
					runID,
				),
				VisibilityTimestamp: scheduledTime.Add(200 * time.Second), // > 120s from scheduled time
				TaskID:              123,
				TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				InMemory:            false, // Persisted task
			}

			// Set the long timeout task directly in mutable state
			deps.mutableState.SetWorkflowTaskScheduleToStartTimeoutTask(mockLongTimeoutTask)
			// Clear the StartToClose task so it doesn't interfere with the test
			deps.mutableState.SetWorkflowTaskStartToCloseTimeoutTask(nil)

			// Complete the workflow task
			_, err = deps.mutableState.AddWorkflowTaskCompletedEvent(
				wft,
				&workflowservice.RespondWorkflowTaskCompletedRequest{},
				workflowTaskCompletionLimits,
			)
			require.NoError(t, err)

			// Verify that BestEffortDeleteTasks does NOT contain the long timeout task
			del := deps.mutableState.BestEffortDeleteTasks
			if timerTasks, exists := del[tasks.CategoryTimer]; exists {
				require.Equal(t, 0, len(timerTasks), "Tasks with timeout > 120s should not be added to BestEffortDeleteTasks")
			}

		})
	}
}

// creates a mutable state with first WFT completed on Build ID "b1"
func (d *mutableStateTestDeps) createVersionedMutableStateWithCompletedWFT(t *testing.T, tq *taskqueuepb.TaskQueue) {
	version := int64(12)
	workflowID := "some random workflow ID"
	runID := uuid.New()
	d.mutableState = TestGlobalMutableState(
		d.mockShard,
		d.mockEventsCache,
		d.logger,
		version,
		workflowID,
		runID,
	)

	wft, err := d.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	require.NoError(t, err)
	e, wft, err := d.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		worker_versioning.StampFromBuildId("b1"),
		nil,
		nil,
		false,
	)
	require.NoError(t, err)
	require.Equal(t, "b1", wft.BuildId)
	require.Equal(t, "b1", e.GetWorkflowTaskStartedEventAttributes().GetWorkerVersion().GetBuildId())
	require.Equal(t, "b1", d.mutableState.GetAssignedBuildId())
	require.Equal(t, int64(0), wft.BuildIdRedirectCounter)
	require.Equal(t, int64(0), d.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())
	_, err = d.mutableState.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		workflowTaskCompletionLimits,
	)
	require.NoError(t, err)
}

func TestEffectiveDeployment(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tv := testvars.New(t)
			ms := TestGlobalMutableState(
				deps.mockShard,
				deps.mockEventsCache,
				deps.logger,
				int64(12),
				tv.WorkflowID(),
				tv.RunID(),
			)
			require.Nil(t, ms.executionInfo.VersioningInfo)
			deps.mutableState = ms
			deps.verifyEffectiveDeployment(t, nil, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)

			versioningInfo := &workflowpb.WorkflowExecutionVersioningInfo{}
			ms.executionInfo.VersioningInfo = versioningInfo
			deps.verifyEffectiveDeployment(t, nil, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)

			dv1 := worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment1))
			dv2 := worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment2))
			dv3 := worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment3))

			deploymentVersion1 := worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment1)
			deploymentVersion2 := worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment2)
			deploymentVersion3 := worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment3)

			for _, useV32 := range []bool{true, false} {
				// ------- Without override, without transition

				// deployment is set but behavior is not -> unversioned
				if useV32 {
					versioningInfo.DeploymentVersion = deploymentVersion1
				} else {
					versioningInfo.Version = dv1 //nolint:staticcheck // SA1019: worker versioning v0.31
				}
				versioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
				deps.verifyEffectiveDeployment(t, nil, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)

				if useV32 {
					versioningInfo.DeploymentVersion = deploymentVersion1
				} else {
					versioningInfo.Version = dv1 //nolint:staticcheck // SA1019: worker versioning v0.31
				}
				versioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_PINNED
				deps.verifyEffectiveDeployment(t, deployment1, enumspb.VERSIONING_BEHAVIOR_PINNED)

				if useV32 {
					versioningInfo.DeploymentVersion = deploymentVersion1
				} else {
					versioningInfo.Version = dv1 //nolint:staticcheck // SA1019: worker versioning v0.31
				}
				versioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
				deps.verifyEffectiveDeployment(t, deployment1, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

				// ------- With override, without transition

				// deployment and behavior are not set, but override behavior is AUTO_UPGRADE -> AUTO_UPGRADE
				if useV32 {
					versioningInfo.DeploymentVersion = nil
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true},
					}
				} else {
					versioningInfo.Version = "" //nolint:staticcheck // SA1019: worker versioning v0.31
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, //nolint:staticcheck // SA1019: worker versioning v0.31
					}
				}
				versioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
				deps.verifyEffectiveDeployment(t, nil, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

				// deployment is set, behavior is not, but override behavior is AUTO_UPGRADE -> AUTO_UPGRADE
				if useV32 {
					versioningInfo.DeploymentVersion = deploymentVersion1
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true},
					}
				} else {
					versioningInfo.Version = dv1 //nolint:staticcheck // SA1019: worker versioning v0.31
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, //nolint:staticcheck // SA1019: worker versioning v0.31
					}
				}
				versioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
				deps.verifyEffectiveDeployment(t, deployment1, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

				// worker says PINNED, but override behavior is AUTO_UPGRADE -> AUTO_UPGRADE
				if useV32 {
					versioningInfo.DeploymentVersion = deploymentVersion1
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true},
					}
				} else {
					versioningInfo.Version = dv1 //nolint:staticcheck // SA1019: worker versioning v0.31
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, //nolint:staticcheck // SA1019: worker versioning v0.31
						// Technically, API should not allow deployment to be set for AUTO_UPGRADE override, but we
						// test it this way to make sure it is ignored.
						PinnedVersion: dv2, //nolint:staticcheck // SA1019: worker versioning v0.31
					}
				}
				versioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_PINNED
				deps.verifyEffectiveDeployment(t, deployment1, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

				// deployment and behavior are not set, but override behavior is PINNED -> PINNED
				if useV32 {
					versioningInfo.DeploymentVersion = nil
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Override: &workflowpb.VersioningOverride_Pinned{Pinned: &workflowpb.VersioningOverride_PinnedOverride{
							Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
							Version:  deploymentVersion2,
						}},
					}
				} else {
					versioningInfo.Version = "" //nolint:staticcheck // SA1019: worker versioning v0.31
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED, //nolint:staticcheck // SA1019: worker versioning v0.31
						PinnedVersion: dv2,                                //nolint:staticcheck // SA1019: worker versioning v0.31
					}
				}
				versioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
				deps.verifyEffectiveDeployment(t, deployment2, enumspb.VERSIONING_BEHAVIOR_PINNED)

				// deployment is set, behavior is not, but override behavior is PINNED --> PINNED
				if useV32 {
					versioningInfo.DeploymentVersion = deploymentVersion1
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Override: &workflowpb.VersioningOverride_Pinned{Pinned: &workflowpb.VersioningOverride_PinnedOverride{
							Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
							Version:  deploymentVersion2,
						}},
					}
				} else {
					versioningInfo.Version = dv1 //nolint:staticcheck // SA1019: worker versioning v0.31
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED, //nolint:staticcheck // SA1019: worker versioning v0.31
						PinnedVersion: dv2,                                //nolint:staticcheck // SA1019: worker versioning v0.31
					}
				}
				versioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
				deps.verifyEffectiveDeployment(t, deployment2, enumspb.VERSIONING_BEHAVIOR_PINNED)

				// worker says AUTO_UPGRADE, but override behavior is PINNED --> PINNED
				if useV32 {
					versioningInfo.DeploymentVersion = deploymentVersion1
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Override: &workflowpb.VersioningOverride_Pinned{Pinned: &workflowpb.VersioningOverride_PinnedOverride{
							Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
							Version:  deploymentVersion2,
						}},
					}
				} else {
					versioningInfo.Version = dv1 //nolint:staticcheck // SA1019: worker versioning v0.31
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED, //nolint:staticcheck // SA1019: worker versioning v0.31
						PinnedVersion: dv2,                                //nolint:staticcheck // SA1019: worker versioning v0.31
					}
				}
				versioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
				deps.verifyEffectiveDeployment(t, deployment2, enumspb.VERSIONING_BEHAVIOR_PINNED)

				// ------- With transition

				if useV32 {
					versioningInfo.DeploymentVersion = deploymentVersion1
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true},
					}
					versioningInfo.VersionTransition = &workflowpb.DeploymentVersionTransition{
						DeploymentVersion: deploymentVersion3,
					}
				} else {
					versioningInfo.Version = dv1 //nolint:staticcheck // SA1019: worker versioning v0.31
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Behavior:      enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, //nolint:staticcheck // SA1019: worker versioning v0.31
						PinnedVersion: dv2,                                      //nolint:staticcheck // SA1019: worker versioning v0.31
					}
					versioningInfo.VersionTransition = &workflowpb.DeploymentVersionTransition{
						Version: dv3, //nolint:staticcheck // SA1019: worker versioning v0.31
					}
				}
				versioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_PINNED
				deps.verifyEffectiveDeployment(t, deployment3, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

				if useV32 {
					versioningInfo.DeploymentVersion = deploymentVersion1
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true},
					}
					versioningInfo.VersionTransition = &workflowpb.DeploymentVersionTransition{
						DeploymentVersion: nil,
					}
				} else {
					versioningInfo.Version = dv1 //nolint:staticcheck // SA1019: worker versioning v0.31
					versioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
						// Transitioning to unversioned
						Behavior:      enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, //nolint:staticcheck // SA1019: worker versioning v0.31
						PinnedVersion: dv2,                                      //nolint:staticcheck // SA1019: worker versioning v0.31
					}
					versioningInfo.VersionTransition = &workflowpb.DeploymentVersionTransition{
						Version: "", //nolint:staticcheck // SA1019: worker versioning v0.31
					}
				}
				versioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_PINNED
				deps.verifyEffectiveDeployment(t, nil, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

				// clear for next round
				versioningInfo.VersioningOverride = nil
				versioningInfo.VersionTransition = nil
				versioningInfo.DeploymentVersion = nil
				versioningInfo.Version = "" //nolint:staticcheck // SA1019: worker versioning v0.31
			}

		})
	}
}

func (d *mutableStateTestDeps) verifyEffectiveDeployment(t *testing.T,
	expectedDeployment *deploymentpb.Deployment,
	expectedBehavior enumspb.VersioningBehavior,
) {
	if !d.mutableState.GetEffectiveDeployment().Equal(expectedDeployment) {
		require.Fail(t, fmt.Sprintf("expected: {%s}, actual: {%s}",
			expectedDeployment.String(),
			d.mutableState.GetEffectiveDeployment().String(),
		),
		)
	}
	require.Equal(t, expectedBehavior, d.mutableState.GetEffectiveVersioningBehavior())
}

// Creates a mutable state with first WFT completed on the given deployment and behavior set
// to the given behavior, testing expected output after Add, Start, and Complete Workflow Task.
func (d *mutableStateTestDeps) createMutableStateWithVersioningBehavior(t *testing.T,
	behavior enumspb.VersioningBehavior,
	deployment *deploymentpb.Deployment,
	tq *taskqueuepb.TaskQueue,
) {
	version := int64(12)
	workflowID := "some random workflow ID"
	runID := uuid.New()

	d.mutableState = TestGlobalMutableState(
		d.mockShard,
		d.mockEventsCache,
		d.logger,
		version,
		workflowID,
		runID,
	)
	require.EqualValues(t, 0, d.mutableState.executionInfo.WorkflowTaskStamp)
	d.mutableState.executionInfo.Attempt = 5 // pretend we are in the middle workflow task retries

	wft, err := d.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	require.NoError(t, err)
	d.verifyEffectiveDeployment(t, nil, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)

	err = d.mutableState.StartDeploymentTransition(deployment, 0)
	require.NoError(t, err)
	d.verifyEffectiveDeployment(t, deployment, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

	_, wft, err = d.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		nil,
		nil,
		nil,
		false,
	)
	require.NoError(t, err)
	d.verifyEffectiveDeployment(t, deployment, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

	_, err = d.mutableState.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			VersioningBehavior: behavior,
			Deployment:         deployment, //nolint:staticcheck // SA1019: worker versioning v0.30
		},
		workflowTaskCompletionLimits,
	)
	d.verifyEffectiveDeployment(t, deployment, behavior)
	require.NoError(t, err)
}

func TestPinnedFirstWorkflowTask(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			deps.createMutableStateWithVersioningBehavior(t, enumspb.VERSIONING_BEHAVIOR_PINNED, deployment1, tq)
			deps.verifyEffectiveDeployment(t, deployment1, enumspb.VERSIONING_BEHAVIOR_PINNED)

		})
	}
}

func TestUnpinnedFirstWorkflowTask(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			deps.createMutableStateWithVersioningBehavior(t, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, deployment1, tq)
			deps.verifyEffectiveDeployment(t, deployment1, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

		})
	}
}

func TestUnpinnedTransition(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			behavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
			deps.createMutableStateWithVersioningBehavior(t, behavior, deployment1, tq)

			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment1, behavior)

			err = deps.mutableState.StartDeploymentTransition(deployment2, 0)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment2, behavior)

			_, wft, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				tq,
				"",
				nil,
				nil,
				nil,
				false,
			)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment2, behavior)

			_, err = deps.mutableState.AddWorkflowTaskCompletedEvent(
				wft,
				&workflowservice.RespondWorkflowTaskCompletedRequest{
					// wf is pinned in the new build
					VersioningBehavior: enumspb.VERSIONING_BEHAVIOR_PINNED,
					Deployment:         deployment2, //nolint:staticcheck // SA1019: worker versioning v0.30
				},
				workflowTaskCompletionLimits,
			)
			deps.verifyEffectiveDeployment(t, deployment2, enumspb.VERSIONING_BEHAVIOR_PINNED)
			require.NoError(t, err)

		})
	}
}

func TestUnpinnedTransitionFailed(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			behavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
			deps.createMutableStateWithVersioningBehavior(t, behavior, deployment1, tq)

			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment1, behavior)

			err = deps.mutableState.StartDeploymentTransition(deployment2, 0)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment2, behavior)

			_, wft, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				tq,
				"",
				nil,
				nil,
				nil,
				false,
			)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment2, behavior)

			_, err = deps.mutableState.AddWorkflowTaskFailedEvent(
				wft,
				enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
				failure.NewServerFailure("some random workflow task failure details", false),
				"some random workflow task failure identity",
				nil,
				"",
				"",
				"",
				0,
			)
			require.NoError(t, err)
			// WFT failure does not fail transition
			deps.verifyEffectiveDeployment(t, deployment2, behavior)

		})
	}
}

func TestUnpinnedTransitionTimeout(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			behavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
			deps.createMutableStateWithVersioningBehavior(t, behavior, deployment1, tq)

			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment1, behavior)

			err = deps.mutableState.StartDeploymentTransition(deployment2, 0)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment2, behavior)

			_, wft, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				tq,
				"",
				nil,
				nil,
				nil,
				false,
			)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment2, behavior)

			_, err = deps.mutableState.AddWorkflowTaskTimedOutEvent(wft)
			require.NoError(t, err)
			// WFT timeout does not fail transition
			deps.verifyEffectiveDeployment(t, deployment2, behavior)

		})
	}
}

func (d *mutableStateTestDeps) verifyWorkflowOptionsUpdatedEventAttr(t *testing.T,
	actualAttr *historypb.WorkflowExecutionOptionsUpdatedEventAttributes,
	expectedAttr *historypb.WorkflowExecutionOptionsUpdatedEventAttributes,
) {
	expectedOverride := worker_versioning.ConvertOverrideToV32(expectedAttr.GetVersioningOverride())
	actualOverride := actualAttr.GetVersioningOverride()
	require.Equal(t, expectedOverride.GetPinned().GetBehavior(), actualOverride.GetPinned().GetBehavior())
	require.Equal(t, expectedOverride.GetPinned().GetVersion().GetDeploymentName(), actualOverride.GetPinned().GetVersion().GetDeploymentName())
	require.Equal(t, expectedOverride.GetPinned().GetVersion().GetBuildId(), actualOverride.GetPinned().GetVersion().GetBuildId())
	require.Equal(t, expectedOverride.GetAutoUpgrade(), actualOverride.GetAutoUpgrade())

	require.Equal(t, expectedOverride.GetBehavior(), actualOverride.GetBehavior())                                       //nolint:staticcheck // SA1019: worker versioning v0.31
	require.Equal(t, expectedOverride.GetDeployment().GetSeriesName(), expectedOverride.GetDeployment().GetSeriesName()) //nolint:staticcheck // SA1019: worker versioning v0.30
	require.Equal(t, expectedOverride.GetDeployment().GetBuildId(), expectedOverride.GetDeployment().GetBuildId())       //nolint:staticcheck // SA1019: worker versioning v0.30
	require.Equal(t, expectedOverride.GetPinnedVersion(), actualOverride.GetPinnedVersion())                             //nolint:staticcheck // SA1019: worker versioning v0.31

	require.Equal(t, actualAttr.GetUnsetVersioningOverride(), expectedAttr.GetUnsetVersioningOverride())
	require.Equal(t, actualAttr.GetIdentity(), expectedAttr.GetIdentity())
}

func (d *mutableStateTestDeps) verifyOverrides(t *testing.T,
	expectedBehavior, expectedBehaviorOverride enumspb.VersioningBehavior,
	expectedDeployment, expectedDeploymentOverride *deploymentpb.Deployment,
) {
	versioningInfo := d.mutableState.GetExecutionInfo().GetVersioningInfo()
	require.Equal(t, expectedBehavior, versioningInfo.GetBehavior())
	if versioningInfo.GetVersioningOverride().GetAutoUpgrade() {
		require.Equal(t, expectedBehaviorOverride, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)
	} else if versioningInfo.GetVersioningOverride().GetPinned().GetBehavior() == workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED {
		require.Equal(t, expectedBehaviorOverride, enumspb.VERSIONING_BEHAVIOR_PINNED)
	}
	expectedVersion := ""
	if expectedDeployment != nil {
		expectedVersion = worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(expectedDeployment))
	}
	require.Equal(t, expectedVersion, versioningInfo.GetVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
	var expectedPinnedDeploymentVersion *deploymentpb.WorkerDeploymentVersion
	if expectedDeploymentOverride != nil {
		expectedPinnedDeploymentVersion = worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(expectedDeploymentOverride)
	}
	require.Equal(t, expectedPinnedDeploymentVersion.GetDeploymentName(), versioningInfo.GetVersioningOverride().GetPinned().GetVersion().GetDeploymentName())
	require.Equal(t, expectedPinnedDeploymentVersion.GetBuildId(), versioningInfo.GetVersioningOverride().GetPinned().GetVersion().GetBuildId())
}

func TestOverride_UnpinnedBase_SetPinnedAndUnsetWithEmptyOptions(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			baseBehavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
			overrideBehavior := enumspb.VERSIONING_BEHAVIOR_PINNED
			id := uuid.New()
			deps.createMutableStateWithVersioningBehavior(t, baseBehavior, deployment1, tq)

			// set pinned override
			event, err := deps.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(pinnedOptions2.GetVersioningOverride(), false, "", nil, nil, id)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment2, overrideBehavior)
			deps.verifyWorkflowOptionsUpdatedEventAttr(t,
				event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
				&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					VersioningOverride:      pinnedOptions2.GetVersioningOverride(),
					UnsetVersioningOverride: false,
					Identity:                id,
				},
			)
			deps.verifyOverrides(t, baseBehavior, overrideBehavior, deployment1, deployment2)

			// unset pinned override with boolean
			id = uuid.New()
			event, err = deps.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(nil, true, "", nil, nil, id)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment1, baseBehavior)
			deps.verifyWorkflowOptionsUpdatedEventAttr(t,
				event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
				&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					VersioningOverride:      nil,
					UnsetVersioningOverride: true,
					Identity:                id,
				},
			)
			deps.verifyOverrides(t, baseBehavior, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED, deployment1, nil)

		})
	}
}

func TestOverride_PinnedBase_SetUnpinnedAndUnsetWithEmptyOptions(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			baseBehavior := enumspb.VERSIONING_BEHAVIOR_PINNED
			overrideBehavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
			id := uuid.New()
			deps.createMutableStateWithVersioningBehavior(t, baseBehavior, deployment1, tq)

			// set unpinned override
			event, err := deps.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(unpinnedOptions.GetVersioningOverride(), false, "", nil, nil, id)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment1, overrideBehavior)
			deps.verifyWorkflowOptionsUpdatedEventAttr(t,
				event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
				&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					VersioningOverride:      unpinnedOptions.GetVersioningOverride(),
					UnsetVersioningOverride: false,
					Identity:                id,
				},
			)
			deps.verifyOverrides(t, baseBehavior, overrideBehavior, deployment1, nil)

			// unset pinned override with empty
			id = uuid.New()
			event, err = deps.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(nil, true, "", nil, nil, id)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment1, baseBehavior)
			deps.verifyWorkflowOptionsUpdatedEventAttr(t,
				event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
				&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					VersioningOverride:      nil,
					UnsetVersioningOverride: true,
					Identity:                id,
				},
			)
			deps.verifyOverrides(t, baseBehavior, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED, deployment1, nil)

		})
	}
}

func TestOverride_RedirectFails(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			baseBehavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
			overrideBehavior := enumspb.VERSIONING_BEHAVIOR_PINNED
			id := uuid.New()
			deps.createMutableStateWithVersioningBehavior(t, baseBehavior, deployment1, tq)

			event, err := deps.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(pinnedOptions3.GetVersioningOverride(), false, "", nil, nil, id)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment3, overrideBehavior)
			deps.verifyWorkflowOptionsUpdatedEventAttr(t,
				event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
				&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					VersioningOverride:      pinnedOptions3.GetVersioningOverride(),
					UnsetVersioningOverride: false,
					Identity:                id,
				},
			)
			deps.verifyOverrides(t, baseBehavior, overrideBehavior, deployment1, deployment3)

			// assert that transition fails
			err = deps.mutableState.StartDeploymentTransition(deployment2, 0)
			require.ErrorIs(t, err, ErrPinnedWorkflowCannotTransition)
			deps.verifyEffectiveDeployment(t, deployment3, overrideBehavior)
			deps.verifyOverrides(t, baseBehavior, overrideBehavior, deployment1, deployment3)

		})
	}
}

func TestOverride_BaseDeploymentUpdatedOnCompletion(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			baseBehavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
			overrideBehavior := enumspb.VERSIONING_BEHAVIOR_PINNED
			id := uuid.New()
			deps.createMutableStateWithVersioningBehavior(t, baseBehavior, deployment1, tq)

			event, err := deps.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(pinnedOptions3.GetVersioningOverride(), false, "", nil, nil, id)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment3, overrideBehavior)
			deps.verifyWorkflowOptionsUpdatedEventAttr(t,
				event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
				&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					VersioningOverride:      pinnedOptions3.GetVersioningOverride(),
					UnsetVersioningOverride: false,
					Identity:                id,
				},
			)
			deps.verifyOverrides(t, baseBehavior, overrideBehavior, deployment1, deployment3)

			// assert that redirect fails - should be its own test
			err = deps.mutableState.StartDeploymentTransition(deployment2, 0)
			require.ErrorIs(t, err, ErrPinnedWorkflowCannotTransition)
			deps.verifyEffectiveDeployment(t, deployment3, overrideBehavior)
			deps.verifyOverrides(t, baseBehavior, overrideBehavior, deployment1, deployment3) // base deployment still deployment1 here -- good

			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment3, overrideBehavior)

			_, wft, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				tq,
				"",
				nil,
				nil,
				nil,
				false,
			)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment3, overrideBehavior)
			deps.verifyOverrides(t, baseBehavior, overrideBehavior, deployment1, deployment3)

			_, err = deps.mutableState.AddWorkflowTaskCompletedEvent(
				wft,
				&workflowservice.RespondWorkflowTaskCompletedRequest{
					// sdk says wf is unpinned, but that does not take effect due to the override
					VersioningBehavior: baseBehavior,
					Deployment:         deployment2,
				},
				workflowTaskCompletionLimits,
			)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment3, overrideBehavior)
			deps.verifyOverrides(t, baseBehavior, overrideBehavior, deployment2, deployment3)

			// now we unset the override and check that the base deployment/behavior is in effect
			id = uuid.New()
			event, err = deps.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(nil, true, "", nil, nil, id)
			require.NoError(t, err)
			deps.verifyEffectiveDeployment(t, deployment2, baseBehavior)
			deps.verifyWorkflowOptionsUpdatedEventAttr(t,
				event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
				&historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					VersioningOverride:      nil,
					UnsetVersioningOverride: true,
					Identity:                id,
				},
			)
			deps.verifyOverrides(t, baseBehavior, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED, deployment2, nil)

		})
	}
}

func TestChecksum(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			// set the checksum probabilities to 100% for exercising during test
			deps.mockConfig.MutableStateChecksumGenProbability = func(namespace string) int { return 100 }
			deps.mockConfig.MutableStateChecksumVerifyProbability = func(namespace string) int { return 100 }

			testCases := []struct {
				name                 string
				enableBufferedEvents bool
				closeTxFunc          func(ms *MutableStateImpl) (*persistencespb.Checksum, error)
			}{
				{
					name: "closeTransactionAsSnapshot",
					closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.Checksum, error) {
						snapshot, _, err := ms.CloseTransactionAsSnapshot(historyi.TransactionPolicyPassive)
						if err != nil {
							return nil, err
						}
						return snapshot.Checksum, err
					},
				},
				{
					name:                 "closeTransactionAsMutation",
					enableBufferedEvents: true,
					closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.Checksum, error) {
						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyPassive)
						if err != nil {
							return nil, err
						}
						return mutation.Checksum, err
					},
				},
			}

			loadErrorsFunc := func() int64 {
				counter := deps.testScope.Snapshot().Counters()["test.mutable_state_checksum_mismatch+operation=WorkflowContext,service_name=history"]
				if counter != nil {
					return counter.Value()
				}
				return 0
			}

			var loadErrors int64

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					dbState := deps.buildWorkflowMutableState(t)
					if !tc.enableBufferedEvents {
						dbState.BufferedEvents = nil
					}

					// create mutable state and verify checksum is generated on close
					loadErrors = loadErrorsFunc()
					var err error
					deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, dbState, 123)
					require.NoError(t, err)
					require.Equal(t, loadErrors, loadErrorsFunc()) // no errors expected
					require.EqualValues(t, dbState.Checksum, deps.mutableState.checksum)
					deps.mutableState.namespaceEntry = deps.newNamespaceCacheEntry(t)
					csum, err := tc.closeTxFunc(deps.mutableState)
					require.Nil(t, err)
					require.NotNil(t, csum.Value)
					require.Equal(t, enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY, csum.Flavor)
					require.Equal(t, mutableStateChecksumPayloadV1, csum.Version)
					require.EqualValues(t, csum, deps.mutableState.checksum)

					// verify checksum is verified on Load
					dbState.Checksum = csum
					deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, dbState, 123)
					require.NoError(t, err)
					require.Equal(t, loadErrors, loadErrorsFunc())

					// generate checksum again and verify its the same
					csum, err = tc.closeTxFunc(deps.mutableState)
					require.Nil(t, err)
					require.NotNil(t, csum.Value)
					require.Equal(t, dbState.Checksum.Value, csum.Value)

					// modify checksum and verify Load fails
					dbState.Checksum.Value[0]++
					deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, dbState, 123)
					require.NoError(t, err)
					require.Equal(t, loadErrors+1, loadErrorsFunc())
					require.EqualValues(t, dbState.Checksum, deps.mutableState.checksum)

					// test checksum is invalidated
					loadErrors = loadErrorsFunc()
					deps.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
						return float64((deps.mutableState.executionInfo.LastUpdateTime.AsTime().UnixNano() / int64(time.Second)) + 1)
					}
					deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, dbState, 123)
					require.NoError(t, err)
					require.Equal(t, loadErrors, loadErrorsFunc())
					require.Nil(t, deps.mutableState.checksum)

					// revert the config value for the next test case
					deps.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
						return float64(0)
					}
				})
			}

		})
	}
}

func TestChecksumProbabilities(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			for _, prob := range []int{0, 100} {
				deps.mockConfig.MutableStateChecksumGenProbability = func(namespace string) int { return prob }
				deps.mockConfig.MutableStateChecksumVerifyProbability = func(namespace string) int { return prob }
				for i := 0; i < 100; i++ {
					shouldGenerate := deps.mutableState.shouldGenerateChecksum()
					shouldVerify := deps.mutableState.shouldVerifyChecksum()
					require.Equal(t, prob == 100, shouldGenerate)
					require.Equal(t, prob == 100, shouldVerify)
				}
			}

		})
	}
}

func TestChecksumShouldInvalidate(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			deps.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 { return 0 }
			require.False(t, deps.mutableState.shouldInvalidateCheckum())
			deps.mutableState.executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
			deps.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
				return float64((deps.mutableState.executionInfo.LastUpdateTime.AsTime().UnixNano() / int64(time.Second)) + 1)
			}
			require.True(t, deps.mutableState.shouldInvalidateCheckum())
			deps.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
				return float64((deps.mutableState.executionInfo.LastUpdateTime.AsTime().UnixNano() / int64(time.Second)) - 1)
			}
			require.False(t, deps.mutableState.shouldInvalidateCheckum())

		})
	}
}

func TestUpdateWorkflowStateStatus_Table(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			deps.newMutableState()
			cases := []struct {
				name          string
				currentState  enumsspb.WorkflowExecutionState
				currentStatus enumspb.WorkflowExecutionStatus
				toState       enumsspb.WorkflowExecutionState
				toStatus      enumspb.WorkflowExecutionStatus
				wantErr       bool
			}{
				{
					name:         "created-> {running, running}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					wantErr:      false,
				},
				{
					name:         "created-> {running, paused}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
					wantErr:      false,
				},
				{
					name:         "created-> {running, completed}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					wantErr:      true,
				},
				// CREATED -> CREATED (allowed for RUNNING/PAUSED)
				{
					name:         "created-> {created, running}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					wantErr:      false,
				},
				{
					name:         "created-> {created, paused}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
					wantErr:      true,
				},
				{
					name:         "created-> {created, completed} (invalid)",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					wantErr:      true,
				},
				// CREATED -> COMPLETED (allowed only for TERMINATED/TIMED_OUT/CONTINUED_AS_NEW)
				{
					name:         "created-> {completed, terminated}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
					wantErr:      false,
				},
				{
					name:         "created-> {completed, timed_out}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
					wantErr:      false,
				},
				{
					name:         "created-> {completed, continued_as_new}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
					wantErr:      false,
				},
				// CREATED -> ZOMBIE (allowed for RUNNING/PAUSED)
				{
					name:         "created-> {zombie, running}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					wantErr:      false,
				},
				{
					name:         "created-> {zombie, paused}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
					wantErr:      false,
				},
				// RUNNING state transitions
				{
					name:         "running-> {created, running} (invalid)",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					wantErr:      true,
				},
				{
					name:         "running-> {running, paused}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
					wantErr:      false,
				},
				{
					name:         "running-> {running, terminated} (invalid)",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
					wantErr:      true,
				},
				{
					name:         "running-> {completed, completed}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					wantErr:      false,
				},
				{
					name:         "running-> {completed, paused} (invalid)",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
					wantErr:      true,
				},
				{
					name:         "running-> {zombie, running}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					wantErr:      false,
				},
				{
					name:         "running-> {zombie, paused}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
					wantErr:      false,
				},
				{
					name:         "running-> {zombie, terminated} (invalid)",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
					wantErr:      true,
				},
				// COMPLETED state transitions
				{
					name:          "completed-> {completed, sameStatus} (no-op)",
					currentState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					currentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					toState:       enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					toStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					wantErr:       false,
				},
				{
					name:          "completed-> {created, running} (invalid)",
					currentState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					currentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					toState:       enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					wantErr:       true,
				},
				{
					name:          "completed-> {running, running} (invalid)",
					currentState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					currentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					toState:       enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					wantErr:       true,
				},
				{
					name:          "completed-> {zombie, running} (invalid)",
					currentState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					currentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					toState:       enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					wantErr:       true,
				},
				{
					name:          "completed-> {completed, differentStatus} (invalid)",
					currentState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					currentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					toState:       enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					toStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
					wantErr:       true,
				},
				// ZOMBIE state transitions
				{
					name:         "zombie-> {created, running}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					wantErr:      false,
				},
				{
					name:         "zombie-> {created, paused}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
					wantErr:      true,
				},
				{
					name:         "zombie-> {running, paused}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
					wantErr:      false,
				},
				{
					name:         "zombie-> {completed, terminated}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
					wantErr:      false,
				},
				{
					name:         "zombie-> {completed, paused} (invalid)",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
					wantErr:      true,
				},
				{
					name:         "zombie-> {zombie, running}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					wantErr:      false,
				},
				{
					name:         "zombie-> {zombie, terminated} (invalid)",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
					wantErr:      true,
				},
				// VOID state (no validation)
				{
					name:         "void-> {running, running}",
					currentState: enumsspb.WORKFLOW_EXECUTION_STATE_VOID,
					toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					wantErr:      false,
				},
			}

			for _, c := range cases {
				t.Run(c.name, func() {
					deps.newMutableState()
					deps.mutableState.executionState.State = c.currentState
					// default current status to RUNNING unless specified
					curStatus := c.currentStatus
					if curStatus == 0 {
						curStatus = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
					}
					deps.mutableState.executionState.Status = curStatus
					_, err := deps.mutableState.UpdateWorkflowStateStatus(c.toState, c.toStatus)
					if c.wantErr {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
					if !c.wantErr { // if the transition was successful, verify the state and status are updated.
						require.Equal(t, c.toState, deps.mutableState.executionState.State)
						require.Equal(t, c.toStatus, deps.mutableState.executionState.Status)
					}
				})
			}

		})
	}
}

func TestAddWorkflowExecutionPausedEvent(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			deps.newMutableState()
			deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			deps.createVersionedMutableStateWithCompletedWFT(t, tq)

			// Complete another WFT to obtain a valid completed event id for scheduling an activity.
			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			_, wft, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				tq,
				"",
				worker_versioning.StampFromBuildId("b1"),
				nil,
				nil,
				false,
			)
			require.NoError(t, err)
			completedEvent, err := deps.mutableState.AddWorkflowTaskCompletedEvent(
				wft,
				&workflowservice.RespondWorkflowTaskCompletedRequest{},
				workflowTaskCompletionLimits,
			)
			require.NoError(t, err)

			// Schedule an activity (pending) using the completed WFT event id.
			_, activityInfo, err := deps.mutableState.AddActivityTaskScheduledEvent(
				completedEvent.GetEventId(),
				&commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:   "act-1",
					ActivityType: &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:    tq,
				},
				false,
			)
			require.NoError(t, err)
			prevActivityStamp := activityInfo.Stamp

			// Create a pending workflow task.
			pendingWFT, err := deps.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			prevWFTStamp := pendingWFT.Stamp

			// Pause and assert stamps incremented.
			pausedEvent, err := deps.mutableState.AddWorkflowExecutionPausedEvent("tester", "reason", uuid.New())
			require.NoError(t, err)

			updatedActivityInfo, ok := deps.mutableState.GetActivityInfo(activityInfo.ScheduledEventId)
			require.True(t, ok)
			require.Greater(t, updatedActivityInfo.Stamp, prevActivityStamp)

			wftInfo := deps.mutableState.GetPendingWorkflowTask()
			require.NotNil(t, wftInfo)
			require.Greater(t, wftInfo.Stamp, prevWFTStamp)

			// assert the event is marked as 'worker may ignore' so that older SDKs can safely ignore it.
			require.True(t, pausedEvent.GetWorkerMayIgnore())

		})
	}
}

func TestAddWorkflowExecutionUnpausedEvent(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			deps.newMutableState()
			deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

			tq := &taskqueuepb.TaskQueue{Name: "tq"}
			deps.createVersionedMutableStateWithCompletedWFT(t, tq)

			// Complete another WFT to obtain a valid completed event id for scheduling an activity.
			wft, err := deps.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			_, wft, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				wft.ScheduledEventID,
				"",
				tq,
				"",
				worker_versioning.StampFromBuildId("b1"),
				nil,
				nil,
				false,
			)
			require.NoError(t, err)
			completedEvent, err := deps.mutableState.AddWorkflowTaskCompletedEvent(
				wft,
				&workflowservice.RespondWorkflowTaskCompletedRequest{},
				workflowTaskCompletionLimits,
			)
			require.NoError(t, err)

			// Schedule an activity (pending) using the completed WFT event id.
			_, activityInfo, err := deps.mutableState.AddActivityTaskScheduledEvent(
				completedEvent.GetEventId(),
				&commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:   "act-1",
					ActivityType: &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:    tq,
				},
				false,
			)
			require.NoError(t, err)
			// Create a pending workflow task.
			pendingWFT, err := deps.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)

			// Pause first to simulate paused workflow state.
			_, err = deps.mutableState.AddWorkflowExecutionPausedEvent("tester", "reason", uuid.New())
			require.NoError(t, err)

			// Capture stamps after pause.
			pausedActivityInfo, ok := deps.mutableState.GetActivityInfo(activityInfo.ScheduledEventId)
			require.True(t, ok)
			pausedActivityStamp := pausedActivityInfo.Stamp
			pausedWFT := deps.mutableState.GetPendingWorkflowTask()
			require.NotNil(t, pausedWFT)
			pausedWFTStamp := pausedWFT.Stamp

			// Unpause and verify.
			unpausedEvent, err := deps.mutableState.AddWorkflowExecutionUnpausedEvent("tester", "reason", uuid.New())
			require.NoError(t, err)

			// PauseInfo should be cleared and status should be RUNNING.
			require.Nil(t, deps.mutableState.executionInfo.PauseInfo)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, deps.mutableState.executionState.Status)

			// Stamps should be incremented again (only for activities) on unpause.
			updatedActivityInfo, ok := deps.mutableState.GetActivityInfo(activityInfo.ScheduledEventId)
			require.True(t, ok)
			require.Greater(t, updatedActivityInfo.Stamp, pausedActivityStamp)

			currentWFT := deps.mutableState.GetPendingWorkflowTask()
			require.NotNil(t, currentWFT)
			require.Equal(t, currentWFT.Stamp, pausedWFTStamp) // workflow task stamp should not change between pause and unpause.

			// assert the event is marked as 'worker may ignore' so that older SDKs can safely ignore it.
			require.True(t, unpausedEvent.GetWorkerMayIgnore())

			// Ensure the pending workflow task we created earlier still exists (no unexpected removal).
			require.Equal(t, pendingWFT.ScheduledEventID, currentWFT.ScheduledEventID)

		})
	}
}

func TestPauseWorkflowExecution_FailStateValidation(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			deps.newMutableState()
			deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

			// Simulate a completed workflow where transitioning status to PAUSED is invalid.
			deps.mutableState.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
			deps.mutableState.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			prevStatus := deps.mutableState.executionState.Status

			_, err := deps.mutableState.AddWorkflowExecutionPausedEvent("tester", "test_reason", uuid.New())
			require.Error(t, err)
			// Status should remain unchanged and PauseInfo should not be set when validation fails.
			require.Equal(t, prevStatus, deps.mutableState.executionState.Status)
			require.Nil(t, deps.mutableState.executionInfo.PauseInfo)

		})
	}
}

func TestContinueAsNewMinBackoff(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			// set ContinueAsNew min interval to 5s
			deps.mockConfig.WorkflowIdReuseMinimalInterval = func(namespace string) time.Duration {
				return 5 * time.Second
			}

			// with no backoff, verify min backoff is in [3s, 5s]
			minBackoff := deps.mutableState.ContinueAsNewMinBackoff(nil).AsDuration()
			require.NotZero(t, minBackoff)
			require.True(t, minBackoff >= 3*time.Second)
			require.True(t, minBackoff <= 5*time.Second)

			// with 2s backoff, verify min backoff is in [3s, 5s]
			minBackoff = deps.mutableState.ContinueAsNewMinBackoff(durationpb.New(time.Second * 2)).AsDuration()
			require.NotZero(t, minBackoff)
			require.True(t, minBackoff >= 3*time.Second)
			require.True(t, minBackoff <= 5*time.Second)

			// with 6s backoff, verify min backoff unchanged
			backoff := time.Second * 6
			minBackoff = deps.mutableState.ContinueAsNewMinBackoff(durationpb.New(backoff)).AsDuration()
			require.NotZero(t, minBackoff)
			require.True(t, minBackoff == backoff)

			// set start time to be 3s ago
			startTime := timestamppb.New(time.Now().Add(-time.Second * 3))
			deps.mutableState.executionInfo.StartTime = startTime
			deps.mutableState.executionInfo.ExecutionTime = startTime
			deps.mutableState.executionState.StartTime = startTime
			// with no backoff, verify min backoff is in [0, 2s]
			minBackoff = deps.mutableState.ContinueAsNewMinBackoff(nil).AsDuration()
			require.NotNil(t, minBackoff)
			require.True(t, minBackoff >= 0)
			require.True(t, minBackoff <= 2*time.Second, "%v\n", minBackoff)

			// with 2s backoff, verify min backoff not changed
			backoff = time.Second * 2
			minBackoff = deps.mutableState.ContinueAsNewMinBackoff(durationpb.New(backoff)).AsDuration()
			require.True(t, minBackoff == backoff)

			// set start time to be 5s ago
			startTime = timestamppb.New(time.Now().Add(-time.Second * 5))
			deps.mutableState.executionInfo.StartTime = startTime
			deps.mutableState.executionInfo.ExecutionTime = startTime
			deps.mutableState.executionState.StartTime = startTime
			// with no backoff, verify backoff unchanged (no backoff needed)
			minBackoff = deps.mutableState.ContinueAsNewMinBackoff(nil).AsDuration()
			require.Zero(t, minBackoff)

			// with 2s backoff, verify backoff unchanged
			backoff = time.Second * 2
			minBackoff = deps.mutableState.ContinueAsNewMinBackoff(durationpb.New(backoff)).AsDuration()
			require.True(t, minBackoff == backoff)

		})
	}
}

func TestEventReapplied(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			runID := uuid.New()
			eventID := int64(1)
			version := int64(2)
			dedupResource := definition.NewEventReappliedID(runID, eventID, version)
			isReapplied := deps.mutableState.IsResourceDuplicated(dedupResource)
			require.False(t, isReapplied)
			deps.mutableState.UpdateDuplicatedResource(dedupResource)
			isReapplied = deps.mutableState.IsResourceDuplicated(dedupResource)
			require.True(t, isReapplied)

		})
	}
}

func TestTransientWorkflowTaskSchedule_CurrentVersionChanged(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			version := int64(2000)
			workflowID := "some random workflow ID"
			runID := uuid.New()
			deps.mutableState = TestGlobalMutableState(
				deps.mockShard,
				deps.mockEventsCache,
				deps.logger,
				version,
				workflowID,
				runID,
			)
			_, _ = deps.prepareTransientWorkflowTaskCompletionFirstBatchApplied(t, version, workflowID, runID)
			err := deps.mutableState.ApplyWorkflowTaskFailedEvent()
			require.NoError(t, err)

			err = deps.mutableState.UpdateCurrentVersion(version+1, true)
			require.NoError(t, err)
			versionHistories := deps.mutableState.GetExecutionInfo().GetVersionHistories()
			versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
			require.NoError(t, err)
			err = versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, &historyspb.VersionHistoryItem{
				EventId: deps.mutableState.GetNextEventID() - 1,
				Version: version,
			})
			require.NoError(t, err)

			wt, err := deps.mutableState.AddWorkflowTaskScheduledEventAsHeartbeat(true, timestamp.TimeNowPtrUtc(), enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			require.NotNil(t, wt)

			require.Equal(t, int32(1), deps.mutableState.GetExecutionInfo().WorkflowTaskAttempt)
			require.Equal(t, 0, deps.mutableState.hBuilder.NumBufferedEvents())

		})
	}
}

func TestTransientWorkflowTaskStart_CurrentVersionChanged(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			version := int64(2000)
			workflowID := "some random workflow ID"
			runID := uuid.New()
			deps.mutableState = TestGlobalMutableState(
				deps.mockShard,
				deps.mockEventsCache,
				deps.logger,
				version,
				workflowID,
				runID,
			)
			_, _ = deps.prepareTransientWorkflowTaskCompletionFirstBatchApplied(t, version, workflowID, runID)
			err := deps.mutableState.ApplyWorkflowTaskFailedEvent()
			require.NoError(t, err)

			versionHistories := deps.mutableState.GetExecutionInfo().GetVersionHistories()
			versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
			require.NoError(t, err)
			err = versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, &historyspb.VersionHistoryItem{
				EventId: deps.mutableState.GetNextEventID() - 1,
				Version: version,
			})
			require.NoError(t, err)

			wt, err := deps.mutableState.AddWorkflowTaskScheduledEventAsHeartbeat(true, timestamp.TimeNowPtrUtc(), enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			require.NotNil(t, wt)

			err = deps.mutableState.UpdateCurrentVersion(version+1, true)
			require.NoError(t, err)

			f, err := tqid.NewTaskQueueFamily("", "tq")
			require.NoError(t, err)

			_, _, err = deps.mutableState.AddWorkflowTaskStartedEvent(
				deps.mutableState.GetNextEventID(),
				uuid.New(),
				&taskqueuepb.TaskQueue{Name: f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(5).RpcName()},
				"random identity",
				nil,
				nil,
				nil,
				false,
			)
			require.NoError(t, err)
			require.Equal(t, 0, deps.mutableState.hBuilder.NumBufferedEvents())

			mutation, err := deps.mutableState.hBuilder.Finish(true)
			require.NoError(t, err)
			require.Equal(t, 1, len(mutation.DBEventsBatches))
			require.Equal(t, 2, len(mutation.DBEventsBatches[0]))
			attrs := mutation.DBEventsBatches[0][0].GetWorkflowTaskScheduledEventAttributes()
			require.NotNil(t, attrs)
			require.Equal(t, "tq", attrs.TaskQueue.Name)

		})
	}
}

func TestNewMutableStateInChain(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			executionTimerTaskStatuses := []int32{
				TimerTaskStatusNone,
				TimerTaskStatusCreated,
			}

			for _, taskStatus := range executionTimerTaskStatuses {
				t.Run(
					fmt.Sprintf("TimerTaskStatus: %v", taskStatus),
					func(t *testing.T) {
						currentMutableState := TestGlobalMutableState(
							deps.mockShard,
							deps.mockEventsCache,
							deps.logger,
							1000,
							tests.WorkflowID,
							uuid.New(),
						)
						currentMutableState.GetExecutionInfo().WorkflowExecutionTimerTaskStatus = taskStatus

						newMutableState, err := NewMutableStateInChain(
							deps.mockShard,
							deps.mockEventsCache,
							deps.logger,
							tests.GlobalNamespaceEntry,
							tests.WorkflowID,
							uuid.New(),
							deps.mockShard.GetTimeSource().Now(),
							currentMutableState,
						)
						require.NoError(t, err)
						require.Equal(t, taskStatus, newMutableState.GetExecutionInfo().WorkflowExecutionTimerTaskStatus)
					},
				)
			}

		})
	}
}

func TestSanitizedMutableState(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			txnID := int64(2000)
			runID := uuid.New()
			mutableState := TestGlobalMutableState(
				deps.mockShard,
				deps.mockEventsCache,
				deps.logger,
				1000,
				tests.WorkflowID,
				runID,
			)

			mutableState.executionInfo.LastFirstEventTxnId = txnID
			mutableState.executionInfo.ParentClock = &clockspb.VectorClock{
				ShardId: 1,
				Clock:   1,
			}
			mutableState.pendingChildExecutionInfoIDs = map[int64]*persistencespb.ChildExecutionInfo{1: {
				Clock: &clockspb.VectorClock{
					ShardId: 1,
					Clock:   1,
				},
			}}
			mutableState.executionInfo.WorkflowExecutionTimerTaskStatus = TimerTaskStatusCreated
			mutableState.executionInfo.TaskGenerationShardClockTimestamp = 1000

			stateMachineDef := hsmtest.NewDefinition("test")
			err := deps.mockShard.StateMachineRegistry().RegisterMachine(stateMachineDef)
			require.NoError(t, err)
			_, err = mutableState.HSM().AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1"}, hsmtest.NewData(hsmtest.State1))
			require.NoError(t, err)

			mutableStateProto := mutableState.CloneToProto()
			sanitizedMutableState, err := NewSanitizedMutableState(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, mutableStateProto, 0)
			require.NoError(t, err)
			require.Equal(t, int64(0), sanitizedMutableState.executionInfo.LastFirstEventTxnId)
			require.Nil(t, sanitizedMutableState.executionInfo.ParentClock)
			for _, childInfo := range sanitizedMutableState.pendingChildExecutionInfoIDs {
				require.Nil(t, childInfo.Clock)
			}
			require.Equal(t, int32(TimerTaskStatusNone), sanitizedMutableState.executionInfo.WorkflowExecutionTimerTaskStatus)
			require.Zero(t, sanitizedMutableState.executionInfo.TaskGenerationShardClockTimestamp)
			err = sanitizedMutableState.HSM().Walk(func(node *hsm.Node) error {
				if node.Parent != nil {
					require.Equal(t, int64(1), node.InternalRepr().TransitionCount)
				}
				return nil
			})
			require.NoError(t, err)

		})
	}
}

func (d *mutableStateTestDeps) prepareTransientWorkflowTaskCompletionFirstBatchApplied(t *testing.T, version int64, workflowID, runID string) (*historypb.HistoryEvent, *historypb.HistoryEvent) {
	namespaceID := tests.NamespaceID
	execution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}

	now := time.Now().UTC()
	workflowType := "some random workflow type"
	taskqueue := "some random taskqueue"
	workflowTimeout := 222 * time.Second
	runTimeout := 111 * time.Second
	workflowTaskTimeout := 11 * time.Second
	workflowTaskAttempt := int32(1)

	eventID := int64(1)
	workflowStartEvent := &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskqueue},
			Input:                    nil,
			WorkflowExecutionTimeout: durationpb.New(workflowTimeout),
			WorkflowRunTimeout:       durationpb.New(runTimeout),
			WorkflowTaskTimeout:      durationpb.New(workflowTaskTimeout),
		}},
	}
	eventID++

	workflowTaskScheduleEvent := &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue},
			StartToCloseTimeout: durationpb.New(workflowTaskTimeout),
			Attempt:             workflowTaskAttempt,
		}},
	}
	eventID++

	workflowTaskStartedEvent := &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: workflowTaskScheduleEvent.GetEventId(),
			RequestId:        uuid.New(),
		}},
	}
	eventID++

	_ = &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: workflowTaskScheduleEvent.GetEventId(),
			StartedEventId:   workflowTaskStartedEvent.GetEventId(),
		}},
	}
	eventID++

	d.mockEventsCache.EXPECT().PutEvent(
		events.EventKey{
			NamespaceID: namespaceID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			EventID:     workflowStartEvent.GetEventId(),
			Version:     version,
		},
		workflowStartEvent,
	)
	err := d.mutableState.ApplyWorkflowExecutionStartedEvent(
		nil,
		execution,
		uuid.New(),
		workflowStartEvent,
	)
	require.Nil(t, err)

	// setup transient workflow task
	wt, err := d.mutableState.ApplyWorkflowTaskScheduledEvent(
		workflowTaskScheduleEvent.GetVersion(),
		workflowTaskScheduleEvent.GetEventId(),
		workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetTaskQueue(),
		workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetStartToCloseTimeout(),
		workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt(),
		nil,
		nil,
		enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	)
	require.Nil(t, err)
	require.NotNil(t, wt)

	wt, err = d.mutableState.ApplyWorkflowTaskStartedEvent(
		nil,
		workflowTaskStartedEvent.GetVersion(),
		workflowTaskScheduleEvent.GetEventId(),
		workflowTaskStartedEvent.GetEventId(),
		workflowTaskStartedEvent.GetWorkflowTaskStartedEventAttributes().GetRequestId(),
		timestamp.TimeValue(workflowTaskStartedEvent.GetEventTime()),
		false,
		123678,
		nil,
		int64(0),
	)
	require.Nil(t, err)
	require.NotNil(t, wt)

	err = d.mutableState.ApplyWorkflowTaskFailedEvent()
	require.Nil(t, err)

	workflowTaskAttempt = int32(123)
	newWorkflowTaskScheduleEvent := &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue},
			StartToCloseTimeout: durationpb.New(workflowTaskTimeout),
			Attempt:             workflowTaskAttempt,
		}},
	}
	eventID++

	newWorkflowTaskStartedEvent := &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: workflowTaskScheduleEvent.GetEventId(),
			RequestId:        uuid.New(),
		}},
	}
	eventID++

	wt, err = d.mutableState.ApplyWorkflowTaskScheduledEvent(
		newWorkflowTaskScheduleEvent.GetVersion(),
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetTaskQueue(),
		newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetStartToCloseTimeout(),
		newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt(),
		nil,
		nil,
		enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	)
	require.Nil(t, err)
	require.NotNil(t, wt)

	wt, err = d.mutableState.ApplyWorkflowTaskStartedEvent(
		nil,
		newWorkflowTaskStartedEvent.GetVersion(),
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetWorkflowTaskStartedEventAttributes().GetRequestId(),
		timestamp.TimeValue(newWorkflowTaskStartedEvent.GetEventTime()),
		false,
		123678,
		nil,
		int64(0),
	)
	require.Nil(t, err)
	require.NotNil(t, wt)

	d.mutableState.SetHistoryBuilder(historybuilder.NewImmutable([]*historypb.HistoryEvent{
		newWorkflowTaskScheduleEvent,
		newWorkflowTaskStartedEvent,
	}))
	_, _, err = d.mutableState.CloseTransactionAsMutation(historyi.TransactionPolicyPassive)
	require.NoError(t, err)

	return newWorkflowTaskScheduleEvent, newWorkflowTaskStartedEvent
}

func (d *mutableStateTestDeps) newNamespaceCacheEntry(t *testing.T) *namespace.Namespace {
	return namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "mutableStateTest"},
		&persistencespb.NamespaceConfig{},
		true,
		&persistencespb.NamespaceReplicationConfig{},
		1,
	)
}

func (d *mutableStateTestDeps) buildWorkflowMutableState(t *testing.T) *persistencespb.WorkflowMutableState {

	namespaceID := d.namespaceEntry.ID()
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	failoverVersion := d.namespaceEntry.FailoverVersion()

	startTime := timestamppb.New(time.Date(2020, 8, 22, 1, 2, 3, 4, time.UTC))
	info := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:                             namespaceID.String(),
		WorkflowId:                              we.GetWorkflowId(),
		TaskQueue:                               tl,
		WorkflowTypeName:                        "wType",
		WorkflowRunTimeout:                      timestamp.DurationFromSeconds(200),
		DefaultWorkflowTaskTimeout:              timestamp.DurationFromSeconds(100),
		LastCompletedWorkflowTaskStartedEventId: int64(99),
		LastUpdateTime:                          timestamp.TimeNowPtrUtc(),
		ExecutionTime:                           startTime,
		WorkflowTaskVersion:                     failoverVersion,
		WorkflowTaskScheduledEventId:            101,
		WorkflowTaskStartedEventId:              102,
		WorkflowTaskTimeout:                     timestamp.DurationFromSeconds(100),
		WorkflowTaskAttempt:                     1,
		WorkflowTaskType:                        enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 102, Version: failoverVersion},
					},
				},
			},
		},
		TransitionHistory: []*persistencespb.VersionedTransition{
			{
				NamespaceFailoverVersion: failoverVersion,
				TransitionCount:          1024,
			},
		},
		FirstExecutionRunId:              uuid.New(),
		WorkflowExecutionTimerTaskStatus: TimerTaskStatusCreated,
	}

	state := &persistencespb.WorkflowExecutionState{
		RunId:     we.GetRunId(),
		State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		StartTime: startTime,
	}

	activityInfos := map[int64]*persistencespb.ActivityInfo{
		90: {
			Version:                failoverVersion,
			ScheduledEventId:       int64(90),
			ScheduledTime:          timestamppb.New(time.Now().UTC()),
			StartedEventId:         common.EmptyEventID,
			StartedTime:            timestamppb.New(time.Now().UTC()),
			ActivityId:             "activityID_5",
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			ScheduleToCloseTimeout: timestamp.DurationFromSeconds(200),
			StartToCloseTimeout:    timestamp.DurationFromSeconds(300),
			HeartbeatTimeout:       timestamp.DurationFromSeconds(50),
		},
	}

	expiryTime := timestamp.TimeNowPtrUtcAddDuration(time.Hour)
	timerInfos := map[string]*persistencespb.TimerInfo{
		"25": {
			Version:        failoverVersion,
			TimerId:        "25",
			StartedEventId: 85,
			ExpiryTime:     expiryTime,
		},
	}

	childInfos := map[int64]*persistencespb.ChildExecutionInfo{
		80: {
			Version:               failoverVersion,
			InitiatedEventId:      80,
			InitiatedEventBatchId: 20,
			StartedEventId:        common.EmptyEventID,
			CreateRequestId:       uuid.New(),
			Namespace:             tests.Namespace.String(),
			WorkflowTypeName:      "code.uber.internal/test/foobar",
		},
	}

	requestCancelInfo := map[int64]*persistencespb.RequestCancelInfo{
		70: {
			Version:               failoverVersion,
			InitiatedEventBatchId: 20,
			CancelRequestId:       uuid.New(),
			InitiatedEventId:      70,
		},
	}

	signalInfos := map[int64]*persistencespb.SignalInfo{
		75: {
			Version:               failoverVersion,
			InitiatedEventId:      75,
			InitiatedEventBatchId: 17,
			RequestId:             uuid.New(),
		},
	}

	signalRequestIDs := []string{
		"signal_request_id_1",
	}

	chasmNodes := map[string]*persistencespb.ChasmNode{
		"component-path": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{NamespaceFailoverVersion: failoverVersion, TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: failoverVersion, TransitionCount: 90},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{},
				},
			},
			Data: &commonpb.DataBlob{Data: []byte("test-data")},
		},
		"component-path/collection": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{NamespaceFailoverVersion: failoverVersion, TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: failoverVersion, TransitionCount: 90},
				Attributes: &persistencespb.ChasmNodeMetadata_CollectionAttributes{
					CollectionAttributes: &persistencespb.ChasmCollectionAttributes{},
				},
			},
		},
	}

	bufferedEvents := []*historypb.HistoryEvent{
		{
			EventId:   common.BufferedEventID,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			Version:   failoverVersion,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: "test-signal-buffered",
				Input:      payloads.EncodeString("test-signal-buffered-input"),
			}},
		},
	}

	return &persistencespb.WorkflowMutableState{
		ExecutionInfo:       info,
		ExecutionState:      state,
		NextEventId:         int64(103),
		ActivityInfos:       activityInfos,
		TimerInfos:          timerInfos,
		ChildExecutionInfos: childInfos,
		RequestCancelInfos:  requestCancelInfo,
		SignalInfos:         signalInfos,
		ChasmNodes:          chasmNodes,
		SignalRequestedIds:  signalRequestIDs,
		BufferedEvents:      bufferedEvents,
	}
}

func TestUpdateInfos(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			ctx := context.Background()
			cacheStore := map[events.EventKey]*historypb.HistoryEvent{}
			dbstate := deps.buildWorkflowMutableState(t)
			var err error

			namespaceEntry := tests.GlobalNamespaceEntry
			deps.mutableState, err = NewMutableStateFromDB(
				deps.mockShard,
				NewMapEventCache(t, cacheStore),
				deps.logger,
				namespaceEntry,
				dbstate,
				123,
			)
			require.NoError(t, err)
			err = deps.mutableState.UpdateCurrentVersion(namespaceEntry.FailoverVersion(), false)
			require.NoError(t, err)

			// 1st accepted update (without acceptedRequest)
			updateID1 := fmt.Sprintf("%s-1-accepted-update-id", t.Name())
			acptEvent1, err := deps.mutableState.AddWorkflowExecutionUpdateAcceptedEvent(
				updateID1,
				fmt.Sprintf("%s-1-accepted-msg-id", t.Name()),
				1,
				nil) // no acceptedRequest!
			require.NoError(t, err)
			require.NotNil(t, acptEvent1)

			// 2nd accepted update (with acceptedRequest)
			updateID2 := fmt.Sprintf("%s-2-accepted-update-id", t.Name())
			acptEvent2, err := deps.mutableState.AddWorkflowExecutionUpdateAcceptedEvent(
				updateID2,
				fmt.Sprintf("%s-2-accepted-msg-id", t.Name()),
				1,
				&updatepb.Request{Meta: &updatepb.Meta{UpdateId: updateID2}})
			require.NoError(t, err)
			require.NotNil(t, acptEvent2)

			_, err = deps.mutableState.AddWorkflowExecutionUpdateCompletedEvent(
				1234,
				&updatepb.Response{
					Meta: &updatepb.Meta{UpdateId: t.Name() + "-completed-update-without-accepted-event"},
					Outcome: &updatepb.Outcome{
						Value: &updatepb.Outcome_Success{Success: testPayloads},
					},
				},
			)
			require.Error(t, err)

			completedEvent, err := deps.mutableState.AddWorkflowExecutionUpdateCompletedEvent(
				acptEvent1.EventId,
				&updatepb.Response{
					Meta: &updatepb.Meta{UpdateId: updateID1},
					Outcome: &updatepb.Outcome{
						Value: &updatepb.Outcome_Success{Success: testPayloads},
					},
				},
			)
			require.NoError(t, err)
			require.NotNil(t, completedEvent)

			require.Len(t, cacheStore, 3, "expected 1 UpdateCompleted event + 2 UpdateAccepted events in cache")

			numCompleted := 0
			numAccepted := 0
			deps.mutableState.VisitUpdates(func(updID string, updInfo *persistencespb.UpdateInfo) {
				require.Contains(t, []string{updateID1, updateID2}, updID)
				switch {
				case updInfo.GetCompletion() != nil:
					numCompleted++
				case updInfo.GetAcceptance() != nil:
					numAccepted++
				}
			})
			require.Equal(t, numCompleted, 1, "expected 1 completed")
			require.Equal(t, numAccepted, 1, "expected 1 accepted")

			deps.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
				namespaceEntry.IsGlobalNamespace(),
				namespaceEntry.FailoverVersion(),
			).Return(cluster.TestCurrentClusterName).AnyTimes()
			deps.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

			mutation, _, err := deps.mutableState.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
			require.NoError(t, err)
			require.Len(t, mutation.ExecutionInfo.UpdateInfos, 2,
				"expected 1 completed update + 1 accepted in mutation")

			// this must be done after the transaction is closed
			// as GetUpdateOutcome relies on event version history which is only updated when closing the transaction
			outcome, err := deps.mutableState.GetUpdateOutcome(ctx, completedEvent.GetWorkflowExecutionUpdateCompletedEventAttributes().GetMeta().GetUpdateId())
			require.NoError(t, err)
			require.Equal(t, completedEvent.GetWorkflowExecutionUpdateCompletedEventAttributes().GetOutcome(), outcome)

			_, err = deps.mutableState.GetUpdateOutcome(ctx, "not_an_update_id")
			require.Error(t, err)
			require.IsType(t, (*serviceerror.NotFound)(nil), err)

		})
	}
}

func TestApplyActivityTaskStartedEvent(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			state := deps.buildWorkflowMutableState(t)

			var err error
			deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, state, 123)
			require.NoError(t, err)

			var scheduledEventID int64
			var ai *persistencespb.ActivityInfo
			for scheduledEventID, ai = range deps.mutableState.GetPendingActivityInfos() {
				break
			}
			require.Nil(t, ai.LastHeartbeatDetails)

			now := time.Now().UTC()
			version := int64(101)
			requestID := "102"
			eventID := int64(104)
			attributes := &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: scheduledEventID,
				RequestId:        requestID,
			}
			err = deps.mutableState.ApplyActivityTaskStartedEvent(&historypb.HistoryEvent{
				EventId:   eventID,
				EventTime: timestamppb.New(now),
				Version:   version,
				Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
					ActivityTaskStartedEventAttributes: attributes,
				},
			})
			require.NoError(t, err)
			require.Equal(t, version, ai.Version)
			require.Equal(t, eventID, ai.StartedEventId)
			require.NotNil(t, ai.StartedTime)
			require.Equal(t, now, ai.StartedTime.AsTime())
			require.Equal(t, requestID, ai.RequestId)
			s.Assert().Nil(ai.LastHeartbeatDetails)

		})
	}
}

func TestAddContinueAsNewEvent_Default(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			dbState := deps.buildWorkflowMutableState(t)
			dbState.BufferedEvents = nil

			var err error
			deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, dbState, 123)
			require.NoError(t, err)

			workflowTaskInfo := deps.mutableState.GetStartedWorkflowTask()
			workflowTaskCompletedEvent, err := deps.mutableState.AddWorkflowTaskCompletedEvent(
				workflowTaskInfo,
				&workflowservice.RespondWorkflowTaskCompletedRequest{},
				workflowTaskCompletionLimits,
			)
			require.NoError(t, err)

			coll := callbacks.MachineCollection(deps.mutableState.HSM())
			_, err = coll.Add(
				"test-callback-carryover",
				callbacks.NewCallback(
					"random-request-id",
					timestamppb.Now(),
					callbacks.NewWorkflowClosedTrigger(),
					&persistencespb.Callback{
						Variant: &persistencespb.Callback_Nexus_{
							Nexus: &persistencespb.Callback_Nexus{
								Url: "test-callback-carryover-url",
							},
						},
					},
				),
			)
			require.NoError(t, err)

			deps.mockEventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&historypb.HistoryEvent{}, nil)
			deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).Times(2)
			_, newRunMutableState, err := deps.mutableState.AddContinueAsNewEvent(
				context.Background(),
				workflowTaskCompletedEvent.GetEventId(),
				workflowTaskCompletedEvent.GetEventId(),
				"",
				&commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					// All other fields will default to those in the current run.
					WorkflowRunTimeout: deps.mutableState.GetExecutionInfo().WorkflowRunTimeout,
				},
				nil,
			)
			require.NoError(t, err)

			newColl := callbacks.MachineCollection(newRunMutableState.HSM())
			require.Equal(t, 1, newColl.Size())

			currentRunExecutionInfo := deps.mutableState.GetExecutionInfo()
			newRunExecutionInfo := newRunMutableState.GetExecutionInfo()
			require.Equal(t, currentRunExecutionInfo.TaskQueue, newRunExecutionInfo.TaskQueue)
			require.Equal(t, currentRunExecutionInfo.WorkflowTypeName, newRunExecutionInfo.WorkflowTypeName)
			protorequire.ProtoEqual(t, currentRunExecutionInfo.DefaultWorkflowTaskTimeout, newRunExecutionInfo.DefaultWorkflowTaskTimeout)
			protorequire.ProtoEqual(t, currentRunExecutionInfo.WorkflowRunTimeout, newRunExecutionInfo.WorkflowRunTimeout)
			protorequire.ProtoEqual(t, currentRunExecutionInfo.WorkflowExecutionExpirationTime, newRunExecutionInfo.WorkflowExecutionExpirationTime)
			require.Equal(t, currentRunExecutionInfo.WorkflowExecutionTimerTaskStatus, newRunExecutionInfo.WorkflowExecutionTimerTaskStatus)
			require.Equal(t, currentRunExecutionInfo.FirstExecutionRunId, newRunExecutionInfo.FirstExecutionRunId)

			// Add more checks here if needed.

		})
	}
}

func TestTotalEntitiesCount(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

			// scheduling, starting & completing workflow task is omitted here

			workflowTaskCompletedEventID := int64(4)
			_, _, err := deps.mutableState.AddActivityTaskScheduledEvent(
				workflowTaskCompletedEventID,
				&commandpb.ScheduleActivityTaskCommandAttributes{},
				false,
			)
			require.NoError(t, err)

			_, _, err = deps.mutableState.AddStartChildWorkflowExecutionInitiatedEvent(
				workflowTaskCompletedEventID,
				&commandpb.StartChildWorkflowExecutionCommandAttributes{},
				namespace.ID(uuid.New()),
			)
			require.NoError(t, err)

			_, _, err = deps.mutableState.AddTimerStartedEvent(
				workflowTaskCompletedEventID,
				&commandpb.StartTimerCommandAttributes{},
			)
			require.NoError(t, err)

			_, _, err = deps.mutableState.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
				workflowTaskCompletedEventID,
				uuid.New(),
				&commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{},
				namespace.ID(uuid.New()),
			)
			require.NoError(t, err)

			_, _, err = deps.mutableState.AddSignalExternalWorkflowExecutionInitiatedEvent(
				workflowTaskCompletedEventID,
				uuid.New(),
				&commandpb.SignalExternalWorkflowExecutionCommandAttributes{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: tests.WorkflowID,
						RunId:      tests.RunID,
					},
				},
				namespace.ID(uuid.New()),
			)
			require.NoError(t, err)

			updateID := "random-updateId"
			accptEvent, err := deps.mutableState.AddWorkflowExecutionUpdateAcceptedEvent(
				updateID, "random", 0, nil)
			require.NoError(t, err)
			require.NotNil(t, accptEvent)

			completedEvent, err := deps.mutableState.AddWorkflowExecutionUpdateCompletedEvent(
				accptEvent.EventId, &updatepb.Response{Meta: &updatepb.Meta{UpdateId: updateID}})
			require.NoError(t, err)
			require.NotNil(t, completedEvent)

			_, err = deps.mutableState.AddWorkflowExecutionSignaled(
				"signalName",
				&commonpb.Payloads{},
				"identity",
				&commonpb.Header{},
				nil,
			)
			require.NoError(t, err)

			mutation, _, err := deps.mutableState.CloseTransactionAsMutation(
				historyi.TransactionPolicyActive,
			)
			require.NoError(t, err)

			require.Equal(t, int64(1), mutation.ExecutionInfo.ActivityCount)
			require.Equal(t, int64(1), mutation.ExecutionInfo.ChildExecutionCount)
			require.Equal(t, int64(1), mutation.ExecutionInfo.UserTimerCount)
			require.Equal(t, int64(1), mutation.ExecutionInfo.RequestCancelExternalCount)
			require.Equal(t, int64(1), mutation.ExecutionInfo.SignalExternalCount)
			require.Equal(t, int64(1), mutation.ExecutionInfo.SignalCount)
			require.Equal(t, int64(1), mutation.ExecutionInfo.UpdateCount)

		})
	}
}

func TestSpeculativeWorkflowTaskNotPersisted(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			testCases := []struct {
				name                 string
				enableBufferedEvents bool
				closeTxFunc          func(ms *MutableStateImpl) (*persistencespb.WorkflowExecutionInfo, error)
			}{
				{
					name: "CloseTransactionAsSnapshot",
					closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.WorkflowExecutionInfo, error) {
						snapshot, _, err := ms.CloseTransactionAsSnapshot(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return snapshot.ExecutionInfo, err
					},
				},
				{
					name:                 "CloseTransactionAsMutation",
					enableBufferedEvents: true,
					closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.WorkflowExecutionInfo, error) {
						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					dbState := deps.buildWorkflowMutableState(t)
					if !tc.enableBufferedEvents {
						dbState.BufferedEvents = nil
					}

					var err error
					namespaceEntry := tests.GlobalNamespaceEntry
					deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, namespaceEntry, dbState, 123)
					require.NoError(t, err)
					err = deps.mutableState.UpdateCurrentVersion(namespaceEntry.FailoverVersion(), false)
					require.NoError(t, err)

					deps.mutableState.executionInfo.WorkflowTaskScheduledEventId = deps.mutableState.GetNextEventID()
					deps.mutableState.executionInfo.WorkflowTaskStartedEventId = deps.mutableState.GetNextEventID() + 1

					deps.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
						namespaceEntry.IsGlobalNamespace(),
						namespaceEntry.FailoverVersion(),
					).Return(cluster.TestCurrentClusterName).AnyTimes()
					deps.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

					// Normal WT is persisted as is.
					execInfo, err := tc.closeTxFunc(deps.mutableState)
					require.Nil(t, err)
					require.Equal(t, enumsspb.WORKFLOW_TASK_TYPE_NORMAL, execInfo.WorkflowTaskType)
					require.NotEqual(t, common.EmptyEventID, execInfo.WorkflowTaskScheduledEventId)
					require.NotEqual(t, common.EmptyEventID, execInfo.WorkflowTaskStartedEventId)

					deps.mutableState.executionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE

					// Speculative WT is converted to normal.
					execInfo, err = tc.closeTxFunc(deps.mutableState)
					require.Nil(t, err)
					require.Equal(t, enumsspb.WORKFLOW_TASK_TYPE_NORMAL, execInfo.WorkflowTaskType)
					require.NotEqual(t, common.EmptyEventID, execInfo.WorkflowTaskScheduledEventId)
					require.NotEqual(t, common.EmptyEventID, execInfo.WorkflowTaskStartedEventId)
				})
			}

		})
	}
}

func TestRetryWorkflowTask_WithNextRetryDelay(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			expectedDelayDuration := time.Minute
			deps.mutableState.executionInfo.HasRetryPolicy = true
			applicationFailure := &failurepb.Failure{
				Message: "application failure with customized next retry delay",
				Source:  "application",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					Type:           "application-failure-type",
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(expectedDelayDuration),
				}},
			}

			duration, retryState := deps.mutableState.GetRetryBackoffDuration(applicationFailure)
			require.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, retryState)
			require.Equal(t, duration, expectedDelayDuration)

		})
	}
}

func TestRetryActivity_TruncateRetryableFailure(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
			deps.mockConfig.EnableActivityRetryStampIncrement = dynamicconfig.GetBoolPropertyFn(true)

			// scheduling, starting & completing workflow task is omitted here

			workflowTaskCompletedEventID := int64(4)
			_, activityInfo, err := deps.mutableState.AddActivityTaskScheduledEvent(
				workflowTaskCompletedEventID,
				&commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:   "5",
					ActivityType: &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "task-queue"},
					RetryPolicy: &commonpb.RetryPolicy{
						InitialInterval: timestamp.DurationFromSeconds(1),
					},
				},
				false,
			)
			require.NoError(t, err)

			_, err = deps.mutableState.AddActivityTaskStartedEvent(
				activityInfo,
				activityInfo.ScheduledEventId,
				uuid.New(),
				"worker-identity",
				nil,
				nil,
				nil,
			)
			require.NoError(t, err)

			failureSizeErrorLimit := deps.mockConfig.MutableStateActivityFailureSizeLimitError(
				deps.mutableState.namespaceEntry.Name().String(),
			)

			activityFailure := &failurepb.Failure{
				Message: "activity failure with large details",
				Source:  "application",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					Type:         "application-failure-type",
					NonRetryable: false,
					Details: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							{
								Data: make([]byte, failureSizeErrorLimit*2),
							},
						},
					},
				}},
			}
			require.Greater(t, activityFailure.Size(), failureSizeErrorLimit)

			prevStamp := activityInfo.Stamp

			retryState, err := deps.mutableState.RetryActivity(activityInfo, activityFailure)
			require.NoError(t, err)
			require.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, retryState)

			activityInfo, ok := deps.mutableState.GetActivityInfo(activityInfo.ScheduledEventId)
			require.True(t, ok)
			require.Greater(t, activityInfo.Stamp, prevStamp)
			require.Equal(t, int32(2), activityInfo.Attempt)
			require.LessOrEqual(t, activityInfo.RetryLastFailure.Size(), failureSizeErrorLimit)
			require.Equal(t, activityFailure.GetMessage(), activityInfo.RetryLastFailure.Cause.GetMessage())

		})
	}
}

func TestRetryActivity_PausedIncrementsStamp(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
			deps.mockConfig.EnableActivityRetryStampIncrement = dynamicconfig.GetBoolPropertyFn(true)

			workflowTaskCompletedEventID := int64(4)
			_, activityInfo, err := deps.mutableState.AddActivityTaskScheduledEvent(
				workflowTaskCompletedEventID,
				&commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:   "6",
					ActivityType: &commonpb.ActivityType{Name: "activity-type"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "task-queue"},
					RetryPolicy: &commonpb.RetryPolicy{
						InitialInterval: timestamp.DurationFromSeconds(1),
					},
				},
				false,
			)
			require.NoError(t, err)

			_, err = deps.mutableState.AddActivityTaskStartedEvent(
				activityInfo,
				activityInfo.ScheduledEventId,
				uuid.New(),
				"worker-identity",
				nil,
				nil,
				nil,
			)
			require.NoError(t, err)

			activityInfo.Paused = true
			prevStamp := activityInfo.Stamp

			retryState, err := deps.mutableState.RetryActivity(activityInfo, &failurepb.Failure{Message: "activity failure"})
			require.NoError(t, err)
			require.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, retryState)

			updatedActivityInfo, ok := deps.mutableState.GetActivityInfo(activityInfo.ScheduledEventId)
			require.True(t, ok)
			require.Greater(t, updatedActivityInfo.Stamp, prevStamp)
			require.Equal(t, int32(2), updatedActivityInfo.Attempt)

		})
	}
}

func TestupdateBuildIdsAndDeploymentSearchAttributes(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			versioned := func(buildId string) *commonpb.WorkerVersionStamp {
				return &commonpb.WorkerVersionStamp{BuildId: buildId, UseVersioning: true}
			}
			versionedSearchAttribute := func(buildIds ...string) []string {
				attrs := []string{}
				for _, buildId := range buildIds {
					attrs = append(attrs, worker_versioning.VersionedBuildIdSearchAttribute(buildId))
				}
				return attrs
			}
			unversioned := func(buildId string) *commonpb.WorkerVersionStamp {
				return &commonpb.WorkerVersionStamp{BuildId: buildId, UseVersioning: false}
			}
			unversionedSearchAttribute := func(buildIds ...string) []string {
				// assumed limit is 2
				attrs := []string{worker_versioning.UnversionedSearchAttribute, worker_versioning.UnversionedBuildIdSearchAttribute(buildIds[len(buildIds)-1])}
				return attrs
			}

			type testCase struct {
				name            string
				searchAttribute func(buildIds ...string) []string
				stamp           func(buildId string) *commonpb.WorkerVersionStamp
			}
			matrix := []testCase{
				{name: "unversioned", searchAttribute: unversionedSearchAttribute, stamp: unversioned},
				{name: "versioned", searchAttribute: versionedSearchAttribute, stamp: versioned},
			}
			for _, c := range matrix {
				t.Run(c.name, func(t *testing.T) {
					dbState := deps.buildWorkflowMutableState(t)
					var err error
					deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, dbState, 123)
					require.NoError(t, err)

					// Max 0
					err = deps.mutableState.updateBuildIdsAndDeploymentSearchAttributes(c.stamp("0.1"), 0)
					require.NoError(t, err)
					require.Equal(t, []string{}, deps.getBuildIdsFromMutableState(t))

					err = deps.mutableState.updateBuildIdsAndDeploymentSearchAttributes(c.stamp("0.1"), 40)
					require.NoError(t, err)
					require.Equal(t, c.searchAttribute("0.1"), deps.getBuildIdsFromMutableState(t))

					// Add the same build ID
					err = deps.mutableState.updateBuildIdsAndDeploymentSearchAttributes(c.stamp("0.1"), 40)
					require.NoError(t, err)
					require.Equal(t, c.searchAttribute("0.1"), deps.getBuildIdsFromMutableState(t))

					err = deps.mutableState.updateBuildIdsAndDeploymentSearchAttributes(c.stamp("0.2"), 40)
					require.NoError(t, err)
					require.Equal(t, c.searchAttribute("0.1", "0.2"), deps.getBuildIdsFromMutableState(t))

					// Limit applies
					err = deps.mutableState.updateBuildIdsAndDeploymentSearchAttributes(c.stamp("0.3"), 40)
					require.NoError(t, err)
					require.Equal(t, c.searchAttribute("0.2", "0.3"), deps.getBuildIdsFromMutableState(t))
				})
			}

		})
	}
}

func TestAddResetPointFromCompletion(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			dbState := deps.buildWorkflowMutableState(t)
			var err error
			deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, dbState, 123)
			require.NoError(t, err)

			require.Nil(t, deps.cleanedResetPoints(t).GetPoints())

			deps.mutableState.addResetPointFromCompletion("checksum1", "buildid1", 32, 10)
			p1 := &workflowpb.ResetPointInfo{
				BuildId:                      "buildid1",
				BinaryChecksum:               "checksum1",
				RunId:                        deps.mutableState.executionState.RunId,
				FirstWorkflowTaskCompletedId: 32,
			}
			require.Equal(t, []*workflowpb.ResetPointInfo{p1}, deps.cleanedResetPoints(t).GetPoints())

			// new checksum + buildid
			deps.mutableState.addResetPointFromCompletion("checksum2", "buildid2", 35, 10)
			p2 := &workflowpb.ResetPointInfo{
				BuildId:                      "buildid2",
				BinaryChecksum:               "checksum2",
				RunId:                        deps.mutableState.executionState.RunId,
				FirstWorkflowTaskCompletedId: 35,
			}
			require.Equal(t, []*workflowpb.ResetPointInfo{p1, p2}, deps.cleanedResetPoints(t).GetPoints())

			// same checksum + buildid, does not add new point
			deps.mutableState.addResetPointFromCompletion("checksum2", "buildid2", 42, 10)
			require.Equal(t, []*workflowpb.ResetPointInfo{p1, p2}, deps.cleanedResetPoints(t).GetPoints())

			// back to 1, does not add new point
			deps.mutableState.addResetPointFromCompletion("checksum1", "buildid1", 48, 10)
			require.Equal(t, []*workflowpb.ResetPointInfo{p1, p2}, deps.cleanedResetPoints(t).GetPoints())

			// buildid changes
			deps.mutableState.addResetPointFromCompletion("checksum2", "buildid3", 53, 10)
			p3 := &workflowpb.ResetPointInfo{
				BuildId:                      "buildid3",
				BinaryChecksum:               "checksum2",
				RunId:                        deps.mutableState.executionState.RunId,
				FirstWorkflowTaskCompletedId: 53,
			}
			require.Equal(t, []*workflowpb.ResetPointInfo{p1, p2, p3}, deps.cleanedResetPoints(t).GetPoints())

			// limit to 3, p1 gets dropped
			deps.mutableState.addResetPointFromCompletion("checksum2", "buildid4", 55, 3)
			p4 := &workflowpb.ResetPointInfo{
				BuildId:                      "buildid4",
				BinaryChecksum:               "checksum2",
				RunId:                        deps.mutableState.executionState.RunId,
				FirstWorkflowTaskCompletedId: 55,
			}
			require.Equal(t, []*workflowpb.ResetPointInfo{p2, p3, p4}, deps.cleanedResetPoints(t).GetPoints())

		})
	}
}

func TestRolloverAutoResetPointsWithExpiringTime(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			runId1 := uuid.New()
			runId2 := uuid.New()
			runId3 := uuid.New()

			retention := 3 * time.Hour
			base := time.Now()
			t1 := timestamppb.New(base)
			now := timestamppb.New(base.Add(1 * time.Hour))
			t2 := timestamppb.New(base.Add(2 * time.Hour))
			t3 := timestamppb.New(base.Add(4 * time.Hour))

			points := []*workflowpb.ResetPointInfo{
				{
					BuildId:                      "buildid1",
					RunId:                        runId1,
					FirstWorkflowTaskCompletedId: 32,
					ExpireTime:                   t1,
				},
				{
					BuildId:                      "buildid2",
					RunId:                        runId1,
					FirstWorkflowTaskCompletedId: 63,
					ExpireTime:                   t1,
				},
				{
					BuildId:                      "buildid3",
					RunId:                        runId2,
					FirstWorkflowTaskCompletedId: 94,
					ExpireTime:                   t2,
				},
				{
					BuildId:                      "buildid4",
					RunId:                        runId3,
					FirstWorkflowTaskCompletedId: 125,
				},
			}

			newPoints := rolloverAutoResetPointsWithExpiringTime(&workflowpb.ResetPoints{Points: points}, runId3, now.AsTime(), retention)
			expected := []*workflowpb.ResetPointInfo{
				{
					BuildId:                      "buildid3",
					RunId:                        runId2,
					FirstWorkflowTaskCompletedId: 94,
					ExpireTime:                   t2,
				},
				{
					BuildId:                      "buildid4",
					RunId:                        runId3,
					FirstWorkflowTaskCompletedId: 125,
					ExpireTime:                   t3,
				},
			}
			require.Equal(t, expected, newPoints.Points)

		})
	}
}

func TestCloseTransactionUpdateTransition(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			namespaceEntry := tests.GlobalNamespaceEntry

			completWorkflowTaskFn := func(ms historyi.MutableState) {
				workflowTaskInfo := ms.GetStartedWorkflowTask()
				_, err := ms.AddWorkflowTaskCompletedEvent(
					workflowTaskInfo,
					&workflowservice.RespondWorkflowTaskCompletedRequest{},
					workflowTaskCompletionLimits,
				)
				require.NoError(t, err)
			}

			testCases := []struct {
				name                       string
				dbStateMutationFn          func(dbState *persistencespb.WorkflowMutableState)
				txFunc                     func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error)
				versionedTransitionUpdated bool
			}{
				{
					name: "CloseTransactionAsMutation_HistoryEvents",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						completWorkflowTaskFn(ms)

						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
					versionedTransitionUpdated: true,
				},
				{
					name: "CloseTransactionAsMutation_BufferedEvents",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						var activityScheduleEventID int64
						for activityScheduleEventID = range deps.mutableState.GetPendingActivityInfos() {
							break
						}
						_, err := deps.mutableState.AddActivityTaskTimedOutEvent(
							activityScheduleEventID,
							common.EmptyEventID,
							failure.NewTimeoutFailure("test-timeout", enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START),
							enumspb.RETRY_STATE_TIMEOUT,
						)
						require.NoError(t, err)

						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
					versionedTransitionUpdated: true,
				},
				{
					name: "CloseTransactionAsMutation_SyncActivity",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						for _, ai := range ms.GetPendingActivityInfos() {
							ms.UpdateActivityProgress(ai, &workflowservice.RecordActivityTaskHeartbeatRequest{})
							break
						}

						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
					versionedTransitionUpdated: true,
				},
				{
					name: "CloseTransactionAsMutation_DirtyStateMachine",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						root := ms.HSM()
						err := hsm.MachineTransition(root, func(*MutableStateImpl) (hsm.TransitionOutput, error) {
							return hsm.TransitionOutput{}, nil
						})
						require.NoError(t, err)
						require.True(t, root.Dirty())

						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
					versionedTransitionUpdated: true,
				},
				{
					name: "CloseTransactionAsMutation_SignalWorkflow",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						_, err := ms.AddWorkflowExecutionSignaledEvent(
							"signalName",
							&commonpb.Payloads{},
							"identity",
							&commonpb.Header{},
							nil,
							nil,
						)
						if err != nil {
							return nil, err
						}

						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
					versionedTransitionUpdated: true,
				},
				{
					name: "CloseTransactionAsMutation_ChasmTree",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						mockChasmTree := historyi.NewMockChasmTree(deps.controller)
						mockChasmTree.EXPECT().ArchetypeID().Return(chasm.ArchetypeID(1234)).AnyTimes()
						gomock.InOrder(
							mockChasmTree.EXPECT().IsStateDirty().Return(true).AnyTimes(),
							mockChasmTree.EXPECT().CloseTransaction().Return(chasm.NodesMutation{
								UpdatedNodes: map[string]*persistencespb.ChasmNode{
									"node-path": {
										Metadata: &persistencespb.ChasmNodeMetadata{
											Attributes: &persistencespb.ChasmNodeMetadata_DataAttributes{
												DataAttributes: &persistencespb.ChasmDataAttributes{},
											},
										},
										Data: &commonpb.DataBlob{Data: []byte("test-data")},
									},
								},
							}, nil),
						)
						ms.(*MutableStateImpl).chasmTree = mockChasmTree

						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
					versionedTransitionUpdated: true,
				},
				{
					name: "CloseTransactionAsSnapshot",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						completWorkflowTaskFn(ms)

						mutation, _, err := ms.CloseTransactionAsSnapshot(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
					versionedTransitionUpdated: true,
				},
				{
					name: "CloseTransactionAsSnapshot, from unknown to enable",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						ms.GetExecutionInfo().PreviousTransitionHistory = ms.GetExecutionInfo().TransitionHistory
						ms.GetExecutionInfo().TransitionHistory = nil
						completWorkflowTaskFn(ms)

						mutation, _, err := ms.CloseTransactionAsSnapshot(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
					versionedTransitionUpdated: true,
				},
				// TODO: add a test for flushing buffered events using last event version.
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					dbState := deps.buildWorkflowMutableState(t)
					if tc.dbStateMutationFn != nil {
						tc.dbStateMutationFn(dbState)
					}

					var err error
					deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, namespaceEntry, dbState, 123)
					require.NoError(t, err)
					err = deps.mutableState.UpdateCurrentVersion(namespaceEntry.FailoverVersion(), false)
					require.NoError(t, err)

					deps.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
						namespaceEntry.IsGlobalNamespace(),
						namespaceEntry.FailoverVersion(),
					).Return(cluster.TestCurrentClusterName).AnyTimes()
					deps.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
					var expectedTransitionHistory []*persistencespb.VersionedTransition
					if deps.mutableState.executionInfo.TransitionHistory == nil {
						expectedTransitionHistory = transitionhistory.CopyVersionedTransitions(deps.mutableState.executionInfo.PreviousTransitionHistory)
					} else {
						expectedTransitionHistory = transitionhistory.CopyVersionedTransitions(deps.mutableState.executionInfo.TransitionHistory)
					}

					if tc.versionedTransitionUpdated {
						expectedTransitionHistory = UpdatedTransitionHistory(expectedTransitionHistory, namespaceEntry.FailoverVersion())
					}

					execInfo, err := tc.txFunc(deps.mutableState)
					require.Nil(t, err)

					protorequire.ProtoSliceEqual(t, expectedTransitionHistory, execInfo.TransitionHistory)
				})
			}

		})
	}
}

func TestCloseTransactionTrackLastUpdateVersionedTransition(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			namespaceEntry := tests.GlobalNamespaceEntry
			deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

			stateMachineDef := hsmtest.NewDefinition("test")
			err := deps.mockShard.StateMachineRegistry().RegisterMachine(stateMachineDef)
			require.NoError(t, err)

			completWorkflowTaskFn := func(ms historyi.MutableState) *historypb.HistoryEvent {
				workflowTaskInfo := ms.GetStartedWorkflowTask()
				completedEvent, err := ms.AddWorkflowTaskCompletedEvent(
					workflowTaskInfo,
					&workflowservice.RespondWorkflowTaskCompletedRequest{},
					workflowTaskCompletionLimits,
				)
				require.NoError(t, err)
				return completedEvent
			}

			buildHSMFn := func(ms historyi.MutableState) {
				hsmRoot := ms.HSM()
				child1, err := hsmRoot.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1"}, hsmtest.NewData(hsmtest.State1))
				require.NoError(t, err)
				_, err = child1.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1_1"}, hsmtest.NewData(hsmtest.State2))
				require.NoError(t, err)
				_, err = hsmRoot.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_2"}, hsmtest.NewData(hsmtest.State3))
				require.NoError(t, err)
			}

			testCases := []struct {
				name   string
				testFn func(ms historyi.MutableState)
			}{
				{
					name: "Activity",
					testFn: func(ms historyi.MutableState) {
						completedEvent := completWorkflowTaskFn(ms)
						scheduledEvent, _, err := ms.AddActivityTaskScheduledEvent(
							completedEvent.GetEventId(),
							&commandpb.ScheduleActivityTaskCommandAttributes{},
							false,
						)
						require.NoError(t, err)

						_, _, err = ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()

						require.Len(t, ms.GetPendingActivityInfos(), 2)
						for _, ai := range ms.GetPendingActivityInfos() {
							if ai.ScheduledEventId == scheduledEvent.EventId {
								protorequire.ProtoEqual(t, currentVersionedTransition, ai.LastUpdateVersionedTransition)
							} else {
								protorequire.NotProtoEqual(t, currentVersionedTransition, ai.LastUpdateVersionedTransition)
							}
						}
					},
				},
				{
					name: "UserTimer",
					testFn: func(ms historyi.MutableState) {
						completedEvent := completWorkflowTaskFn(ms)
						newTimerID := "new-timer-id"
						_, _, err := ms.AddTimerStartedEvent(
							completedEvent.GetEventId(),
							&commandpb.StartTimerCommandAttributes{
								TimerId: newTimerID,
							},
						)
						require.NoError(t, err)

						_, _, err = ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()

						require.Len(t, ms.GetPendingTimerInfos(), 2)
						for _, ti := range ms.GetPendingTimerInfos() {
							if ti.TimerId == newTimerID {
								protorequire.ProtoEqual(t, currentVersionedTransition, ti.LastUpdateVersionedTransition)
							} else {
								protorequire.NotProtoEqual(t, currentVersionedTransition, ti.LastUpdateVersionedTransition)
							}
						}
					},
				},
				{
					name: "ChildExecution",
					testFn: func(ms historyi.MutableState) {
						completedEvent := completWorkflowTaskFn(ms)
						initiatedEvent, _, err := ms.AddStartChildWorkflowExecutionInitiatedEvent(
							completedEvent.GetEventId(),
							&commandpb.StartChildWorkflowExecutionCommandAttributes{},
							ms.GetNamespaceEntry().ID(),
						)
						require.NoError(t, err)

						_, _, err = ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()

						require.Len(t, ms.GetPendingChildExecutionInfos(), 2)
						for _, ci := range ms.GetPendingChildExecutionInfos() {
							if ci.InitiatedEventId == initiatedEvent.EventId {
								protorequire.ProtoEqual(t, currentVersionedTransition, ci.LastUpdateVersionedTransition)
							} else {
								protorequire.NotProtoEqual(t, currentVersionedTransition, ci.LastUpdateVersionedTransition)
							}
						}
					},
				},
				{
					name: "RequestCancelExternal",
					testFn: func(ms historyi.MutableState) {
						completedEvent := completWorkflowTaskFn(ms)
						initiatedEvent, _, err := ms.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
							completedEvent.GetEventId(),
							uuid.New(),
							&commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{},
							ms.GetNamespaceEntry().ID(),
						)
						require.NoError(t, err)

						_, _, err = ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()

						require.Len(t, ms.GetPendingRequestCancelExternalInfos(), 2)
						for _, ci := range ms.GetPendingRequestCancelExternalInfos() {
							if ci.InitiatedEventId == initiatedEvent.EventId {
								protorequire.ProtoEqual(t, currentVersionedTransition, ci.LastUpdateVersionedTransition)
							} else {
								protorequire.NotProtoEqual(t, currentVersionedTransition, ci.LastUpdateVersionedTransition)
							}
						}
					},
				},
				{
					name: "SignalExternal",
					testFn: func(ms historyi.MutableState) {
						completedEvent := completWorkflowTaskFn(ms)
						initiatedEvent, _, err := ms.AddSignalExternalWorkflowExecutionInitiatedEvent(
							completedEvent.GetEventId(),
							uuid.New(),
							&commandpb.SignalExternalWorkflowExecutionCommandAttributes{
								Execution: &commonpb.WorkflowExecution{
									WorkflowId: "target-workflow-id",
									RunId:      "target-run-id",
								},
							},
							ms.GetNamespaceEntry().ID(),
						)
						require.NoError(t, err)

						_, _, err = ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()

						require.Len(t, ms.GetPendingSignalExternalInfos(), 2)
						for _, ci := range ms.GetPendingSignalExternalInfos() {
							if ci.InitiatedEventId == initiatedEvent.EventId {
								protorequire.ProtoEqual(t, currentVersionedTransition, ci.LastUpdateVersionedTransition)
							} else {
								protorequire.NotProtoEqual(t, currentVersionedTransition, ci.LastUpdateVersionedTransition)
							}
						}
					},
				},
				{
					name: "SignalRequestedID",
					testFn: func(ms historyi.MutableState) {
						ms.AddSignalRequested(uuid.New())

						_, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()
						protorequire.ProtoEqual(t, currentVersionedTransition, ms.GetExecutionInfo().SignalRequestIdsLastUpdateVersionedTransition)
					},
				},
				{
					name: "UpdateInfo",
					testFn: func(ms historyi.MutableState) {
						updateID := "test-updateId"
						_, err := ms.AddWorkflowExecutionUpdateAcceptedEvent(
							updateID,
							"update-message-id",
							65,
							nil, // this is an optional field
						)
						require.NoError(t, err)

						_, _, err = ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()
						require.Len(t, ms.GetExecutionInfo().UpdateInfos, 1)
						protorequire.ProtoEqual(t, currentVersionedTransition, ms.GetExecutionInfo().UpdateInfos[updateID].LastUpdateVersionedTransition)
					},
				},
				{
					name: "WorkflowTask/Completed",
					testFn: func(ms historyi.MutableState) {
						completWorkflowTaskFn(ms)

						_, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()
						protorequire.ProtoEqual(t, currentVersionedTransition, ms.GetExecutionInfo().WorkflowTaskLastUpdateVersionedTransition)
					},
				},
				{
					name: "WorkflowTask/Scheduled",
					testFn: func(ms historyi.MutableState) {
						completWorkflowTaskFn(ms)
						_, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
						require.NoError(t, err)

						_, _, err = ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()
						protorequire.ProtoEqual(t, currentVersionedTransition, ms.GetExecutionInfo().WorkflowTaskLastUpdateVersionedTransition)
					},
				},
				{
					name: "Visibility",
					testFn: func(ms historyi.MutableState) {
						completedEvent := completWorkflowTaskFn(ms)
						_, err := ms.AddUpsertWorkflowSearchAttributesEvent(
							completedEvent.EventId,
							&commandpb.UpsertWorkflowSearchAttributesCommandAttributes{},
						)
						require.NoError(t, err)

						_, _, err = ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()
						protorequire.ProtoEqual(t, currentVersionedTransition, ms.GetExecutionInfo().VisibilityLastUpdateVersionedTransition)
					},
				},
				{
					name: "ExecutionState",
					testFn: func(ms historyi.MutableState) {
						completedEvent := completWorkflowTaskFn(ms)
						_, err := ms.AddCompletedWorkflowEvent(
							completedEvent.EventId,
							&commandpb.CompleteWorkflowExecutionCommandAttributes{},
							"",
						)
						require.NoError(t, err)

						_, _, err = ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()
						protorequire.ProtoEqual(t, currentVersionedTransition, ms.GetExecutionState().LastUpdateVersionedTransition)
					},
				},
				{
					name: "HSM/CloseAsMutation",
					testFn: func(ms historyi.MutableState) {
						completWorkflowTaskFn(ms)
						buildHSMFn(ms)

						_, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()
						err = ms.HSM().Walk(func(n *hsm.Node) error {
							if n.Parent == nil {
								// skip root which is entire mutable state
								return nil
							}
							protorequire.ProtoEqual(t, currentVersionedTransition, n.InternalRepr().LastUpdateVersionedTransition)
							return nil
						})
						require.NoError(t, err)
					},
				},
				{
					name: "HSM/CloseAsSnapshot",
					testFn: func(ms historyi.MutableState) {
						completWorkflowTaskFn(ms)
						buildHSMFn(ms)

						_, _, err := ms.CloseTransactionAsSnapshot(historyi.TransactionPolicyActive)
						require.NoError(t, err)

						currentVersionedTransition := ms.CurrentVersionedTransition()
						err = ms.HSM().Walk(func(n *hsm.Node) error {
							if n.Parent == nil {
								// skip root which is entire mutable state
								return nil
							}
							protorequire.ProtoEqual(t, currentVersionedTransition, n.InternalRepr().LastUpdateVersionedTransition)
							return nil
						})
						require.NoError(t, err)
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {

					dbState := deps.buildWorkflowMutableState(t)
					dbState.BufferedEvents = nil

					var err error
					deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, namespaceEntry, dbState, 123)
					require.NoError(t, err)
					err = deps.mutableState.UpdateCurrentVersion(namespaceEntry.FailoverVersion(), false)
					require.NoError(t, err)

					deps.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
						namespaceEntry.IsGlobalNamespace(),
						namespaceEntry.FailoverVersion(),
					).Return(cluster.TestCurrentClusterName).AnyTimes()
					deps.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

					tc.testFn(deps.mutableState)
				})
			}

		})
	}
}

func TestCloseTransactionHandleUnknownVersionedTransition(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			namespaceEntry := tests.GlobalNamespaceEntry

			completWorkflowTaskFn := func(ms historyi.MutableState) {
				workflowTaskInfo := ms.GetStartedWorkflowTask()
				_, err := ms.AddWorkflowTaskCompletedEvent(
					workflowTaskInfo,
					&workflowservice.RespondWorkflowTaskCompletedRequest{},
					historyi.WorkflowTaskCompletionLimits{
						MaxResetPoints:              10,
						MaxSearchAttributeValueSize: 1024,
					},
				)
				require.NoError(t, err)
			}

			testCases := []struct {
				name              string
				dbStateMutationFn func(dbState *persistencespb.WorkflowMutableState)
				txFunc            func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error)
			}{
				{
					name: "CloseTransactionAsPassive", // this scenario simulate the case to clear the transition history (non state-based transition happened at passive side)
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						completWorkflowTaskFn(ms)

						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyPassive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
				},
				{
					name: "CloseTransactionAsMutation_HistoryEvents",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						completWorkflowTaskFn(ms)

						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
				},
				{
					name: "CloseTransactionAsMutation_BufferedEvents",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						var activityScheduleEventID int64
						for activityScheduleEventID = range deps.mutableState.GetPendingActivityInfos() {
							break
						}
						_, err := deps.mutableState.AddActivityTaskTimedOutEvent(
							activityScheduleEventID,
							common.EmptyEventID,
							failure.NewTimeoutFailure("test-timeout", enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START),
							enumspb.RETRY_STATE_TIMEOUT,
						)
						require.NoError(t, err)

						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
				},
				{
					name: "CloseTransactionAsMutation_SyncActivity",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						for _, ai := range ms.GetPendingActivityInfos() {
							ms.UpdateActivityProgress(ai, &workflowservice.RecordActivityTaskHeartbeatRequest{})
							break
						}

						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
				},
				{
					name: "CloseTransactionAsMutation_DirtyStateMachine",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						root := ms.HSM()
						err := hsm.MachineTransition(root, func(*MutableStateImpl) (hsm.TransitionOutput, error) {
							return hsm.TransitionOutput{}, nil
						})
						require.NoError(t, err)
						require.True(t, root.Dirty())

						mutation, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
				},
				{
					name: "CloseTransactionAsSnapshot",
					dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
						dbState.BufferedEvents = nil
					},
					txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
						completWorkflowTaskFn(ms)

						mutation, _, err := ms.CloseTransactionAsSnapshot(historyi.TransactionPolicyActive)
						if err != nil {
							return nil, err
						}
						return mutation.ExecutionInfo, err
					},
				},
				// TODO: add a test for flushing buffered events using last event version.
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					dbState := deps.buildWorkflowMutableState(t)
					if tc.dbStateMutationFn != nil {
						tc.dbStateMutationFn(dbState)
					}

					var err error
					deps.mutableState, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, namespaceEntry, dbState, 123)
					require.NoError(t, err)
					err = deps.mutableState.UpdateCurrentVersion(namespaceEntry.FailoverVersion(), false)
					require.NoError(t, err)
					deps.mutableState.transitionHistoryEnabled = false

					deps.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
						namespaceEntry.IsGlobalNamespace(),
						namespaceEntry.FailoverVersion(),
					).Return(cluster.TestCurrentClusterName).AnyTimes()
					deps.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

					require.NotNil(t, deps.mutableState.executionInfo.TransitionHistory)
					execInfo, err := tc.txFunc(deps.mutableState)
					require.NotNil(t, execInfo.PreviousTransitionHistory)
					require.Nil(t, execInfo.TransitionHistory)
					require.Nil(t, err)
				})
			}

		})
	}
}

func (d *mutableStateTestDeps) getBuildIdsFromMutableState(t *testing.T) []string {
	payload, found := deps.mutableState.executionInfo.SearchAttributes[sadefs.BuildIds]
	if !found {
		return []string{}
	}
	decoded, err := searchattribute.DecodeValue(payload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
	require.NoError(t, err)
	buildIDs, ok := decoded.([]string)
	require.True(t, ok)
	return buildIDs
}

// return reset points minus a few fields that are hard to check for equality
func (d *mutableStateTestDeps) cleanedResetPoints(t *testing.T) *workflowpb.ResetPoints {
	out := common.CloneProto(d.mutableState.executionInfo.GetAutoResetPoints())
	for _, point := range out.GetPoints() {
		point.CreateTime = nil // current time
		point.ExpireTime = nil
	}
	return out
}

func TestCollapseVisibilityTasks(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			testCases := []struct {
				name  string
				tasks []tasks.Task
				res   []enumsspb.TaskType
			}{
				{
					name: "start upsert close delete",
					tasks: []tasks.Task{
						&tasks.StartExecutionVisibilityTask{},
						&tasks.UpsertExecutionVisibilityTask{},
						&tasks.UpsertExecutionVisibilityTask{},
						&tasks.CloseExecutionVisibilityTask{},
						&tasks.DeleteExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
					},
				},
				{
					name: "upsert close delete",
					tasks: []tasks.Task{
						&tasks.UpsertExecutionVisibilityTask{},
						&tasks.UpsertExecutionVisibilityTask{},
						&tasks.CloseExecutionVisibilityTask{},
						&tasks.DeleteExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
					},
				},
				{
					name: "close delete",
					tasks: []tasks.Task{
						&tasks.CloseExecutionVisibilityTask{},
						&tasks.DeleteExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
					},
				},
				{
					name: "delete",
					tasks: []tasks.Task{
						&tasks.DeleteExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
					},
				},
				{
					name: "start upsert close",
					tasks: []tasks.Task{
						&tasks.StartExecutionVisibilityTask{},
						&tasks.UpsertExecutionVisibilityTask{},
						&tasks.UpsertExecutionVisibilityTask{},
						&tasks.CloseExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
					},
				},
				{
					name: "upsert close",
					tasks: []tasks.Task{
						&tasks.UpsertExecutionVisibilityTask{},
						&tasks.UpsertExecutionVisibilityTask{},
						&tasks.CloseExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
					},
				},
				{
					name: "close",
					tasks: []tasks.Task{
						&tasks.CloseExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
					},
				},
				{
					name: "start upsert",
					tasks: []tasks.Task{
						&tasks.StartExecutionVisibilityTask{},
						&tasks.UpsertExecutionVisibilityTask{},
						&tasks.UpsertExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION,
					},
				},
				{
					name: "upsert",
					tasks: []tasks.Task{
						&tasks.UpsertExecutionVisibilityTask{},
						&tasks.UpsertExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION,
					},
				},
				{
					name: "start",
					tasks: []tasks.Task{
						&tasks.StartExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION,
					},
				},
				{
					name: "upsert start delete close",
					tasks: []tasks.Task{
						&tasks.UpsertExecutionVisibilityTask{},
						&tasks.StartExecutionVisibilityTask{},
						&tasks.DeleteExecutionVisibilityTask{},
						&tasks.CloseExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
					},
				},
				{
					name: "close upsert",
					tasks: []tasks.Task{
						&tasks.CloseExecutionVisibilityTask{},
						&tasks.UpsertExecutionVisibilityTask{},
					},
					res: []enumsspb.TaskType{
						enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
					},
				},
			}

			ms := deps.mutableState

			for _, tc := range testCases {
				t.Run(
					tc.name,
					func() {
						ms.InsertTasks[tasks.CategoryVisibility] = []tasks.Task{}
						ms.AddTasks(tc.tasks...)
						ms.closeTransactionCollapseVisibilityTasks()
						visTasks := ms.InsertTasks[tasks.CategoryVisibility]
						require.Equal(t, len(tc.res), len(visTasks))
						for i, expectTaskType := range tc.res {
							require.Equal(t, expectTaskType, visTasks[i].GetType())
						}
					},
				)
			}

		})
	}
}

func TestStartChildWorkflowRequestID(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			workflowTaskCompletionEventID := rand.Int63()
			attributes := &commandpb.StartChildWorkflowExecutionCommandAttributes{}
			event := deps.mutableState.hBuilder.AddStartChildWorkflowExecutionInitiatedEvent(
				workflowTaskCompletionEventID,
				attributes,
				tests.NamespaceID,
			)
			createRequestID := fmt.Sprintf("%s:%d:%d", deps.mutableState.executionState.RunId, event.GetEventId(), event.GetVersion())
			deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

			ci, err := deps.mutableState.ApplyStartChildWorkflowExecutionInitiatedEvent(
				workflowTaskCompletionEventID,
				event,
			)
			require.NoError(t, err)
			require.Equal(t, createRequestID, ci.CreateRequestId)

		})
	}
}

func TestGetCloseVersion(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

			_, err := deps.mutableState.AddWorkflowExecutionStartedEvent(
				&commonpb.WorkflowExecution{
					WorkflowId: tests.WorkflowID,
					RunId:      tests.RunID,
				},
				&historyservice.StartWorkflowExecutionRequest{
					StartRequest: &workflowservice.StartWorkflowExecutionRequest{},
				},
			)
			require.NoError(t, err)
			_, err = deps.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
			require.NoError(t, err)
			_, _, err = deps.mutableState.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
			require.NoError(t, err)

			_, err = deps.mutableState.GetCloseVersion()
			require.Error(t, err) // workflow still open

			namespaceEntry, err := deps.mockShard.GetNamespaceRegistry().GetNamespaceByID(tests.NamespaceID)
			require.NoError(t, err)
			expectedVersion := namespaceEntry.FailoverVersion()

			_, err = deps.mutableState.AddCompletedWorkflowEvent(
				5,
				&commandpb.CompleteWorkflowExecutionCommandAttributes{},
				"",
			)
			require.NoError(t, err)
			// get close version in the transaction that closes the workflow
			closeVersion, err := deps.mutableState.GetCloseVersion()
			require.NoError(t, err)
			require.Equal(t, expectedVersion, closeVersion)

			_, _, err = deps.mutableState.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
			require.NoError(t, err)

			// get close version after workflow is closed
			closeVersion, err = deps.mutableState.GetCloseVersion()
			require.NoError(t, err)
			require.Equal(t, expectedVersion, closeVersion)

			// verify close version doesn't change after workflow is closed
			err = deps.mutableState.UpdateCurrentVersion(12345, true)
			require.NoError(t, err)
			closeVersion, err = deps.mutableState.GetCloseVersion()
			require.NoError(t, err)
			require.Equal(t, expectedVersion, closeVersion)

		})
	}
}

func TestCloseTransactionPrepareReplicationTasks_HistoryTask(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			version := int64(777)
			firstEventID := int64(2)
			lastEventID := int64(3)
			now := time.Now().UTC()
			taskqueue := "taskqueue for test"
			workflowTaskTimeout := 11 * time.Second
			workflowTaskAttempt := int32(1)
			eventBatches := [][]*historypb.HistoryEvent{
				{
					&historypb.HistoryEvent{
						Version:   version,
						EventId:   firstEventID,
						EventTime: timestamppb.New(now),
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue},
							StartToCloseTimeout: durationpb.New(workflowTaskTimeout),
							Attempt:             workflowTaskAttempt,
						}},
					},
				},
				{
					&historypb.HistoryEvent{
						Version:   version,
						EventId:   lastEventID,
						EventTime: timestamppb.New(now),
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: firstEventID,
							RequestId:        uuid.New(),
						}},
					},
				},
			}

			testCases := []struct {
				name                       string
				replicationMultipleBatches bool
				tasks                      []tasks.Task
			}{
				{
					name:                       "multiple event batches disabled",
					replicationMultipleBatches: false,
					tasks: []tasks.Task{
						&tasks.HistoryReplicationTask{
							WorkflowKey:  deps.mutableState.GetWorkflowKey(),
							FirstEventID: firstEventID,
							NextEventID:  firstEventID + 1,
							Version:      version,
						},
						&tasks.HistoryReplicationTask{
							WorkflowKey:  deps.mutableState.GetWorkflowKey(),
							FirstEventID: lastEventID,
							NextEventID:  lastEventID + 1,
							Version:      version,
						},
					},
				},
				{
					name:                       "multiple event batches enabled",
					replicationMultipleBatches: true,
					tasks: []tasks.Task{
						&tasks.HistoryReplicationTask{
							WorkflowKey:  deps.mutableState.GetWorkflowKey(),
							FirstEventID: firstEventID,
							NextEventID:  lastEventID + 1,
							Version:      version,
						},
					},
				},
			}

			ms := deps.mutableState
			ms.transitionHistoryEnabled = false
			for _, tc := range testCases {
				t.Run(
					tc.name,
					func() {
						if s.replicationMultipleBatches != tc.replicationMultipleBatches {
							return
						}
						ms.InsertTasks[tasks.CategoryReplication] = []tasks.Task{}
						err := ms.closeTransactionPrepareReplicationTasks(historyi.TransactionPolicyActive, eventBatches, false)
						if err != nil {
							require.Fail(t, "closeTransactionPrepareReplicationTasks failed", err)
						}
						repicationTasks := ms.InsertTasks[tasks.CategoryReplication]
						require.Equal(t, len(tc.tasks), len(repicationTasks))
						for i, task := range tc.tasks {
							require.Equal(t, task, repicationTasks[i])
						}
					},
				)
			}

		})
	}
}

func TestCloseTransactionPrepareReplicationTasks_SyncVersionedTransitionTask(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			if s.replicationMultipleBatches == true {
				return
			}
			version := int64(777)
			firstEventID := int64(2)
			lastEventID := int64(3)
			now := time.Now().UTC()
			taskqueue := "taskqueue for test"
			workflowTaskTimeout := 11 * time.Second
			workflowTaskAttempt := int32(1)
			eventBatches := [][]*historypb.HistoryEvent{
				{
					&historypb.HistoryEvent{
						Version:   version,
						EventId:   firstEventID,
						EventTime: timestamppb.New(now),
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue},
							StartToCloseTimeout: durationpb.New(workflowTaskTimeout),
							Attempt:             workflowTaskAttempt,
						}},
					},
				},
				{
					&historypb.HistoryEvent{
						Version:   version,
						EventId:   lastEventID,
						EventTime: timestamppb.New(now),
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: firstEventID,
							RequestId:        uuid.New(),
						}},
					},
				},
			}

			ms := deps.mutableState
			ms.transitionHistoryEnabled = true
			ms.syncActivityTasks[1] = struct{}{}
			ms.pendingActivityInfoIDs[1] = &persistencespb.ActivityInfo{
				Version:          version,
				ScheduledEventId: 1,
			}
			ms.InsertTasks[tasks.CategoryReplication] = []tasks.Task{}
			transitionHistory := []*persistencespb.VersionedTransition{
				{
					NamespaceFailoverVersion: 1,
					TransitionCount:          10,
				},
			}
			ms.executionInfo.TransitionHistory = transitionHistory
			err := ms.closeTransactionPrepareReplicationTasks(historyi.TransactionPolicyActive, eventBatches, false)
			require.NoError(t, err)
			replicationTasks := ms.InsertTasks[tasks.CategoryReplication]
			require.Equal(t, 1, len(replicationTasks))
			historyTasks := []tasks.Task{
				&tasks.HistoryReplicationTask{
					WorkflowKey:  deps.mutableState.GetWorkflowKey(),
					FirstEventID: firstEventID,
					NextEventID:  firstEventID + 1,
					Version:      version,
				},
				&tasks.HistoryReplicationTask{
					WorkflowKey:  deps.mutableState.GetWorkflowKey(),
					FirstEventID: lastEventID,
					NextEventID:  lastEventID + 1,
					Version:      version,
				},
			}
			expectedTask := &tasks.SyncVersionedTransitionTask{
				WorkflowKey:         deps.mutableState.GetWorkflowKey(),
				VisibilityTimestamp: now,
				Priority:            enumsspb.TASK_PRIORITY_HIGH,
				VersionedTransition: transitionHistory[0],
				FirstEventID:        firstEventID,
				NextEventID:         lastEventID + 1,
			}
			require.Equal(t, enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION, replicationTasks[0].GetType())
			actualTask, ok := replicationTasks[0].(*tasks.SyncVersionedTransitionTask)
			require.True(t, ok)
			require.Equal(t, expectedTask.WorkflowKey, actualTask.WorkflowKey)
			require.Equal(t, expectedTask.VersionedTransition, actualTask.VersionedTransition)
			require.Equal(t, 3, len(actualTask.TaskEquivalents))
			require.Equal(t, historyTasks[0], actualTask.TaskEquivalents[0])
			require.Equal(t, historyTasks[1], actualTask.TaskEquivalents[1])
			require.Equal(t, enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY, actualTask.TaskEquivalents[2].GetType())

		})
	}
}

func TestMaxAllowedTimer(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			testCases := []struct {
				name                   string
				runTimeout             time.Duration
				runTimeoutTimerDropped bool
			}{
				{
					name:                   "run timeout timer preserved",
					runTimeout:             time.Hour * 24 * 365,
					runTimeoutTimerDropped: false,
				},
				{
					name:                   "run timeout timer dropped",
					runTimeout:             time.Hour * 24 * 365 * 100,
					runTimeoutTimerDropped: true,
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).Times(1)
					deps.mutableState = NewMutableState(deps.mockShard, deps.mockEventsCache, deps.logger, deps.namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC())

					workflowKey := deps.mutableState.GetWorkflowKey()
					_, err := deps.mutableState.AddWorkflowExecutionStartedEvent(
						&commonpb.WorkflowExecution{
							WorkflowId: workflowKey.WorkflowID,
							RunId:      workflowKey.RunID,
						},
						&historyservice.StartWorkflowExecutionRequest{
							NamespaceId: workflowKey.NamespaceID,
							StartRequest: &workflowservice.StartWorkflowExecutionRequest{
								Namespace:  deps.mutableState.GetNamespaceEntry().Name().String(),
								WorkflowId: workflowKey.WorkflowID,
								WorkflowType: &commonpb.WorkflowType{
									Name: "test-workflow-type",
								},
								TaskQueue: &taskqueuepb.TaskQueue{
									Name: "test-task-queue",
								},
								WorkflowRunTimeout: durationpb.New(tc.runTimeout),
							},
						},
					)
					require.NoError(t, err)

					snapshot, _, err := deps.mutableState.CloseTransactionAsSnapshot(historyi.TransactionPolicyActive)
					require.NoError(t, err)

					timerTasks := snapshot.Tasks[tasks.CategoryTimer]
					if tc.runTimeoutTimerDropped {
						require.Empty(t, timerTasks)
					} else {
						require.Len(t, timerTasks, 1)
						require.Equal(t, enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT, timerTasks[0].GetType())
					}
				})
			}

		})
	}
}

func TestCloseTransactionPrepareReplicationTasks_SyncHSMTask(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			version := deps.mutableState.GetCurrentVersion()
			stateMachineDef := hsmtest.NewDefinition("test")
			err := deps.mockShard.StateMachineRegistry().RegisterMachine(stateMachineDef)
			require.NoError(t, err)

			testCases := []struct {
				name                    string
				hsmEmpty                bool
				hsmDirty                bool
				eventBatches            [][]*historypb.HistoryEvent
				clearBufferEvents       bool
				expectedReplicationTask tasks.Task
			}{
				{
					name:     "WithEvents",
					hsmEmpty: false,
					hsmDirty: true,
					eventBatches: [][]*historypb.HistoryEvent{
						{
							&historypb.HistoryEvent{
								Version:   version,
								EventId:   5,
								EventTime: timestamppb.New(time.Now()),
								EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
								Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
									NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{},
								},
							},
						},
					},
					clearBufferEvents: false,
					expectedReplicationTask: &tasks.HistoryReplicationTask{
						WorkflowKey:  deps.mutableState.GetWorkflowKey(),
						FirstEventID: 5,
						NextEventID:  6,
						Version:      version,
					},
				},
				{
					name:              "NoEvents",
					hsmEmpty:          false,
					hsmDirty:          true,
					eventBatches:      nil,
					clearBufferEvents: false,
					expectedReplicationTask: &tasks.SyncHSMTask{
						WorkflowKey: deps.mutableState.GetWorkflowKey(),
					},
				},
				{
					name:                    "NoChildren/ClearBufferFalse",
					hsmEmpty:                true,
					hsmDirty:                false,
					eventBatches:            nil,
					clearBufferEvents:       false,
					expectedReplicationTask: nil,
				},
				{
					name:                    "NoChildren/ClearBufferTrue",
					hsmEmpty:                true,
					hsmDirty:                false,
					eventBatches:            nil,
					clearBufferEvents:       true,
					expectedReplicationTask: nil,
				},
				{
					name:                    "CleanChildren/ClearBufferFalse",
					hsmEmpty:                false,
					hsmDirty:                false,
					clearBufferEvents:       false,
					expectedReplicationTask: nil,
				},
				{
					name:              "CleanChildren/ClearBufferTrue",
					hsmEmpty:          false,
					hsmDirty:          false,
					clearBufferEvents: true,
					expectedReplicationTask: &tasks.SyncHSMTask{
						WorkflowKey: deps.mutableState.GetWorkflowKey(),
					},
				},
			}

			for _, tc := range testCases {
				t.Run(
					tc.name,
					func() {
						if !tc.hsmEmpty {
							_, err = deps.mutableState.HSM().AddChild(
								hsm.Key{Type: stateMachineDef.Type(), ID: "child_1"},
								hsmtest.NewData(hsmtest.State1),
							)
							require.NoError(t, err)

							if !tc.hsmDirty {
								deps.mutableState.HSM().ClearTransactionState()
							}
						}
						deps.mutableState.transitionHistoryEnabled = false
						err := deps.mutableState.closeTransactionPrepareReplicationTasks(historyi.TransactionPolicyActive, tc.eventBatches, tc.clearBufferEvents)
						require.NoError(t, err)

						repicationTasks := deps.mutableState.PopTasks()[tasks.CategoryReplication]

						if tc.expectedReplicationTask != nil {
							require.Len(t, repicationTasks, 1)
							require.Equal(t, tc.expectedReplicationTask, repicationTasks[0])
						} else {
							require.Empty(t, repicationTasks)
						}
					},
				)
			}

		})
	}
}

func (d *mutableStateTestDeps) setDisablingTransitionHistory(t *testing.T, ms *MutableStateImpl) {
	ms.versionedTransitionInDB = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: d.namespaceEntry.FailoverVersion(),
		TransitionCount:          1025,
	}
	ms.executionInfo.TransitionHistory = nil
}

func TestCloseTransactionPrepareReplicationTasks_SyncActivityTask(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			testCases := []struct {
				name                       string
				disablingTransitionHistory bool
				expectedReplicationTask    []tasks.SyncActivityTask
			}{
				{
					name:                       "NoDisablingTransitionHistory",
					disablingTransitionHistory: false,
					expectedReplicationTask: []tasks.SyncActivityTask{
						{
							ScheduledEventID: 100,
						},
					},
				},
				{
					name:                       "DisablingTransitionHistory",
					disablingTransitionHistory: true,
					expectedReplicationTask: []tasks.SyncActivityTask{
						{
							ScheduledEventID: 90,
						},
						{
							ScheduledEventID: 100,
						},
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					dbState := deps.buildWorkflowMutableState(t)
					dbState.ActivityInfos[100] = &persistencespb.ActivityInfo{
						ScheduledEventId: 100,
					}
					ms, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, deps.namespaceEntry, dbState, 123)
					require.NoError(t, err)

					if tc.disablingTransitionHistory {
						deps.setDisablingTransitionHistory(t, ms)
					}

					ms.UpdateActivityProgress(ms.pendingActivityInfoIDs[100], &workflowservice.RecordActivityTaskHeartbeatRequest{})

					repicationTasks := ms.syncActivityToReplicationTask(historyi.TransactionPolicyActive)
					require.Len(t, repicationTasks, len(tc.expectedReplicationTask))
					sort.Slice(repicationTasks, func(i, j int) bool {
						return repicationTasks[i].(*tasks.SyncActivityTask).ScheduledEventID < repicationTasks[j].(*tasks.SyncActivityTask).ScheduledEventID
					})
					for i, task := range tc.expectedReplicationTask {
						require.Equal(t, task.ScheduledEventID, repicationTasks[i].(*tasks.SyncActivityTask).ScheduledEventID)
					}
				})
			}

		})
	}
}

func TestVersionedTransitionInDB(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			// case 1: versionedTransitionInDB is not nil
			dbState := deps.buildWorkflowMutableState(t)
			ms, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, deps.namespaceEntry, dbState, 123)
			require.NoError(t, err)

			require.True(t, proto.Equal(ms.CurrentVersionedTransition(), ms.versionedTransitionInDB))

			require.NoError(t, ms.cleanupTransaction())
			require.True(t, proto.Equal(ms.CurrentVersionedTransition(), ms.versionedTransitionInDB))

			ms.executionInfo.TransitionHistory = nil
			require.NoError(t, ms.cleanupTransaction())
			require.Nil(t, ms.versionedTransitionInDB)

			// case 2: versionedTransitionInDB is nil
			dbState = deps.buildWorkflowMutableState(t)
			dbState.ExecutionInfo.TransitionHistory = nil
			ms, err = NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, deps.namespaceEntry, dbState, 123)
			require.NoError(t, err)

			require.Nil(t, ms.versionedTransitionInDB)

			ms.executionInfo.TransitionHistory = UpdatedTransitionHistory(ms.executionInfo.TransitionHistory, deps.namespaceEntry.FailoverVersion())
			require.NoError(t, ms.cleanupTransaction())
			require.True(t, proto.Equal(ms.CurrentVersionedTransition(), ms.versionedTransitionInDB))

		})
	}
}

func TestCloseTransactionTrackTombstones(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			testCases := []struct {
				name        string
				tombstoneFn func(ms historyi.MutableState) (*persistencespb.StateMachineTombstone, error)
			}{
				{
					name: "Activity",
					tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
						var activityScheduleEventID int64
						for activityScheduleEventID = range mutableState.GetPendingActivityInfos() {
							break
						}
						_, err := mutableState.AddActivityTaskTimedOutEvent(
							activityScheduleEventID,
							common.EmptyEventID,
							failure.NewTimeoutFailure("test-timeout", enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START),
							enumspb.RETRY_STATE_TIMEOUT,
						)
						return &persistencespb.StateMachineTombstone{
							StateMachineKey: &persistencespb.StateMachineTombstone_ActivityScheduledEventId{
								ActivityScheduledEventId: activityScheduleEventID,
							},
						}, err
					},
				},
				{
					name: "UserTimer",
					tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
						var timerID string
						for timerID = range mutableState.GetPendingTimerInfos() {
							break
						}
						_, err := mutableState.AddTimerFiredEvent(timerID)
						return &persistencespb.StateMachineTombstone{
							StateMachineKey: &persistencespb.StateMachineTombstone_TimerId{
								TimerId: timerID,
							},
						}, err
					},
				},
				{
					name: "ChildWorkflow",
					tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
						var initiatedEventId int64
						var ci *persistencespb.ChildExecutionInfo
						for initiatedEventId, ci = range mutableState.GetPendingChildExecutionInfos() {
							break
						}
						childExecution := &commonpb.WorkflowExecution{
							WorkflowId: uuid.New(),
							RunId:      uuid.New(),
						}
						_, err := mutableState.AddChildWorkflowExecutionStartedEvent(
							childExecution,
							&commonpb.WorkflowType{Name: ci.WorkflowTypeName},
							initiatedEventId,
							nil,
							nil,
						)
						if err != nil {
							return nil, err
						}
						_, err = mutableState.AddChildWorkflowExecutionTerminatedEvent(
							initiatedEventId,
							childExecution,
						)
						return &persistencespb.StateMachineTombstone{
							StateMachineKey: &persistencespb.StateMachineTombstone_ChildExecutionInitiatedEventId{
								ChildExecutionInitiatedEventId: initiatedEventId,
							},
						}, err
					},
				},
				{
					name: "RequestCancelExternal",
					tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
						var initiatedEventId int64
						for initiatedEventId = range mutableState.GetPendingRequestCancelExternalInfos() {
							break
						}
						_, err := mutableState.AddRequestCancelExternalWorkflowExecutionFailedEvent(
							initiatedEventId,
							deps.namespaceEntry.Name(),
							deps.namespaceEntry.ID(),
							uuid.New(),
							uuid.New(),
							enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
						)
						return &persistencespb.StateMachineTombstone{
							StateMachineKey: &persistencespb.StateMachineTombstone_RequestCancelInitiatedEventId{
								RequestCancelInitiatedEventId: initiatedEventId,
							},
						}, err
					},
				},
				{
					name: "SignalExternal",
					tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
						var initiatedEventId int64
						for initiatedEventId = range mutableState.GetPendingSignalExternalInfos() {
							break
						}
						_, err := mutableState.AddSignalExternalWorkflowExecutionFailedEvent(
							initiatedEventId,
							deps.namespaceEntry.Name(),
							deps.namespaceEntry.ID(),
							uuid.New(),
							uuid.New(),
							"",
							enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
						)
						return &persistencespb.StateMachineTombstone{
							StateMachineKey: &persistencespb.StateMachineTombstone_SignalExternalInitiatedEventId{
								SignalExternalInitiatedEventId: initiatedEventId,
							},
						}, err
					},
				},
				{
					name: "CHASM",
					tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
						deletedNodePath := "deleted-node-path"
						tombstone := &persistencespb.StateMachineTombstone{
							StateMachineKey: &persistencespb.StateMachineTombstone_ChasmNodePath{
								ChasmNodePath: deletedNodePath,
							},
						}

						mockChasmTree := historyi.NewMockChasmTree(deps.controller)
						mockChasmTree.EXPECT().ArchetypeID().Return(chasm.ArchetypeID(1234)).AnyTimes()
						gomock.InOrder(
							mockChasmTree.EXPECT().IsStateDirty().Return(true).AnyTimes(),
							mockChasmTree.EXPECT().CloseTransaction().Return(chasm.NodesMutation{
								DeletedNodes: map[string]struct{}{deletedNodePath: {}},
							}, nil),
						)
						mutableState.(*MutableStateImpl).chasmTree = mockChasmTree

						return tombstone, nil
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					dbState := deps.buildWorkflowMutableState(t)

					mutableState, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, deps.namespaceEntry, dbState, 123)
					require.NoError(t, err)

					currentVersionedTransition := mutableState.CurrentVersionedTransition()
					newVersionedTranstion := common.CloneProto(currentVersionedTransition)
					newVersionedTranstion.TransitionCount += 1

					_, err = mutableState.StartTransaction(deps.namespaceEntry)
					require.NoError(t, err)

					expectedTombstone, err := tc.tombstoneFn(mutableState)
					require.NoError(t, err)

					_, _, err = mutableState.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
					require.NoError(t, err)

					tombstoneBatches := mutableState.GetExecutionInfo().SubStateMachineTombstoneBatches
					require.Len(t, tombstoneBatches, 1)
					tombstoneBatch := tombstoneBatches[0]
					protorequire.ProtoEqual(t, newVersionedTranstion, tombstoneBatch.VersionedTransition)
					require.True(t, tombstoneExists(tombstoneBatch.StateMachineTombstones, expectedTombstone))
				})
			}

		})
	}
}

func tombstoneExists(
	tombstones []*persistencespb.StateMachineTombstone,
	expectedTombstone *persistencespb.StateMachineTombstone,
) bool {
	for _, tombstone := range tombstones {
		if tombstone.Equal(expectedTombstone) {
			return true
		}
	}
	return false
}

func TestCloseTransactionTrackTombstones_CapIfLargerThanLimit(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			dbState := deps.buildWorkflowMutableState(t)

			mutableState, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, deps.namespaceEntry, dbState, 123)
			require.NoError(t, err)
			mutableState.executionInfo.SubStateMachineTombstoneBatches = []*persistencespb.StateMachineTombstoneBatch{
				{
					VersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
						TransitionCount:          1,
					},
				},
			}

			currentVersionedTransition := mutableState.CurrentVersionedTransition()
			newVersionedTranstion := common.CloneProto(currentVersionedTransition)
			newVersionedTranstion.TransitionCount += 1
			signalMap := mutableState.GetPendingSignalExternalInfos()
			for i := 0; i < deps.mockConfig.MutableStateTombstoneCountLimit(); i++ {
				signalMap[int64(76+i)] = &persistencespb.SignalInfo{

					Version:               deps.namespaceEntry.FailoverVersion(),
					InitiatedEventId:      int64(76 + i),
					InitiatedEventBatchId: 17,
					RequestId:             uuid.New(),
				}
			}

			_, err = mutableState.StartTransaction(deps.namespaceEntry)
			require.NoError(t, err)
			var initiatedEventId int64
			for initiatedEventId = range signalMap {
				_, err := mutableState.AddSignalExternalWorkflowExecutionFailedEvent(
					initiatedEventId,
					deps.namespaceEntry.Name(),
					deps.namespaceEntry.ID(),
					uuid.New(),
					uuid.New(),
					"",
					enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
				)
				require.NoError(t, err)
			}

			_, _, err = mutableState.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
			require.NoError(t, err)

			tombstoneBatches := mutableState.GetExecutionInfo().SubStateMachineTombstoneBatches
			require.Len(t, tombstoneBatches, 0)

		})
	}
}

func TestCloseTransactionTrackTombstones_OnlyTrackFirstEmpty(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			dbState := deps.buildWorkflowMutableState(t)

			mutableState, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, deps.namespaceEntry, dbState, 123)
			require.NoError(t, err)
			mutableState.executionInfo.SubStateMachineTombstoneBatches = []*persistencespb.StateMachineTombstoneBatch{
				{
					VersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
						TransitionCount:          1,
					},
				},
			}

			currentVersionedTransition := mutableState.CurrentVersionedTransition()
			newVersionedTranstion := common.CloneProto(currentVersionedTransition)
			newVersionedTranstion.TransitionCount += 1

			_, err = mutableState.StartTransaction(deps.namespaceEntry)
			require.NoError(t, err)

			_, _, err = mutableState.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
			require.NoError(t, err)

			tombstoneBatches := mutableState.GetExecutionInfo().SubStateMachineTombstoneBatches
			require.Len(t, tombstoneBatches, 1)
			require.Equal(t, int64(1), tombstoneBatches[0].VersionedTransition.TransitionCount)

		})
	}
}

func TestCloseTransactionGenerateCHASMRetentionTask(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			dbState := deps.buildWorkflowMutableState(t)

			mutableState, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, deps.namespaceEntry, dbState, 123)
			require.NoError(t, err)

			// First close transaction once to get rid of unrelated tasks like UserTimer and ActivityTimeout
			_, err = mutableState.StartTransaction(deps.namespaceEntry)
			require.NoError(t, err)
			_, _, err = mutableState.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
			require.NoError(t, err)

			// Switch to a mock CHASM tree
			mockChasmTree := historyi.NewMockChasmTree(deps.controller)
			mutableState.chasmTree = mockChasmTree

			// Is workflow, should not generate retention task
			mockChasmTree.EXPECT().IsStateDirty().Return(true).AnyTimes()
			mockChasmTree.EXPECT().ArchetypeID().Return(chasm.WorkflowArchetypeID).Times(1)
			mockChasmTree.EXPECT().CloseTransaction().Return(chasm.NodesMutation{}, nil).AnyTimes()
			mutation, _, err := mutableState.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
			require.NoError(t, err)
			require.Empty(t, mutation.Tasks[tasks.CategoryTimer])

			// Now make the mutable state non-workflow.
			mockChasmTree.EXPECT().ArchetypeID().Return(chasm.WorkflowArchetypeID + 101).Times(2) // One time for each CloseTransactionAsMutation call
			_, err = mutableState.UpdateWorkflowStateStatus(
				enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			)
			require.NoError(t, err)
			mutation, _, err = mutableState.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
			require.NoError(t, err)
			require.Len(t, mutation.Tasks[tasks.CategoryTimer], 1)
			require.Equal(t, enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT, mutation.Tasks[tasks.CategoryTimer][0].GetType())

			// Already closed before, should not generate retention task again.
			mutation, _, err = mutableState.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
			require.NoError(t, err)
			require.Empty(t, mutation.Tasks[tasks.CategoryTimer])

		})
	}
}

func TestExecutionInfoClone(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			newInstance := reflect.New(reflect.TypeOf(deps.mutableState.executionInfo).Elem()).Interface()
			clone, ok := newInstance.(*persistencespb.WorkflowExecutionInfo)
			if !ok {
				t.Fatal("type assertion to *persistencespb.WorkflowExecutionInfo failed")
			}
			clone.NamespaceId = "namespace-id"
			clone.WorkflowId = "workflow-id"
			err := common.MergeProtoExcludingFields(deps.mutableState.executionInfo, clone, func(v any) []interface{} {
				info, ok := v.(*persistencespb.WorkflowExecutionInfo)
				if !ok || info == nil {
					return nil
				}
				return []interface{}{
					&info.NamespaceId,
				}
			})
			require.Nil(t, err)

		})
	}
}

func (d *mutableStateTestDeps) addChangesForStateReplication(t *testing.T, state *persistencespb.WorkflowMutableState) {
	// These fields will be updated during ApplySnapshot
	proto.Merge(state.ExecutionInfo, &persistencespb.WorkflowExecutionInfo{
		LastUpdateTime: timestamp.TimeNowPtrUtc(),
	})
	proto.Merge(state.ExecutionState, &persistencespb.WorkflowExecutionState{
		State: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	})

	state.ActivityInfos[90].TimerTaskStatus = TimerTaskStatusCreated
	state.TimerInfos["25"].ExpiryTime = timestamp.TimeNowPtrUtcAddDuration(time.Hour)
	state.ChildExecutionInfos[80].StartedEventId = 84
	state.RequestCancelInfos[70].CancelRequestId = uuid.New()
	state.SignalInfos[75].RequestId = uuid.New()

	// These infos will be deleted during ApplySnapshot
	state.ActivityInfos[89] = &persistencespb.ActivityInfo{}
	state.TimerInfos["to-be-deleted"] = &persistencespb.TimerInfo{}
	state.ChildExecutionInfos[79] = &persistencespb.ChildExecutionInfo{}
	state.RequestCancelInfos[69] = &persistencespb.RequestCancelInfo{}
	state.SignalInfos[74] = &persistencespb.SignalInfo{}
	state.SignalRequestedIds = []string{"to-be-deleted"}
}

func compareMapOfProto[K comparable, V proto.Message](t *testing.T, expected, actual map[K]V) {
	require.Equal(t, len(expected), len(actual))
	for k, v := range expected {
		require.True(t, proto.Equal(v, actual[k]))
	}
}

func (d *mutableStateTestDeps) verifyChildExecutionInfos(t *testing.T, expectedMap, actualMap, originMap map[int64]*persistencespb.ChildExecutionInfo) {
	require.Equal(t, len(expectedMap), len(actualMap))
	for k, expected := range expectedMap {
		actual, ok := actualMap[k]
		require.True(t, ok)
		origin := originMap[k]

		require.Equal(t, expected.Version, actual.Version, "Version mismatch")
		require.Equal(t, expected.InitiatedEventBatchId, actual.InitiatedEventBatchId, "InitiatedEventBatchId mismatch")
		require.Equal(t, expected.StartedEventId, actual.StartedEventId, "StartedEventId mismatch")
		require.Equal(t, expected.StartedWorkflowId, actual.StartedWorkflowId, "StartedWorkflowId mismatch")
		require.Equal(t, expected.StartedRunId, actual.StartedRunId, "StartedRunId mismatch")
		require.Equal(t, expected.CreateRequestId, actual.CreateRequestId, "CreateRequestId mismatch")
		require.Equal(t, expected.Namespace, actual.Namespace, "Namespace mismatch")
		require.Equal(t, expected.WorkflowTypeName, actual.WorkflowTypeName, "WorkflowTypeName mismatch")
		require.Equal(t, expected.ParentClosePolicy, actual.ParentClosePolicy, "ParentClosePolicy mismatch")
		require.Equal(t, expected.InitiatedEventId, actual.InitiatedEventId, "InitiatedEventId mismatch")
		require.Equal(t, expected.NamespaceId, actual.NamespaceId, "NamespaceId mismatch")
		require.True(t, proto.Equal(expected.LastUpdateVersionedTransition, actual.LastUpdateVersionedTransition), "LastUpdateVersionedTransition mismatch")

		// special handled fields
		if origin != nil {
			require.Equal(t, origin.Clock, actual.Clock, "Clock mismatch")
		}
	}
}

func (d *mutableStateTestDeps) verifyActivityInfos(t *testing.T, expectedMap, actualMap map[int64]*persistencespb.ActivityInfo) {
	require.Equal(t, len(expectedMap), len(actualMap))
	for k, expected := range expectedMap {
		actual, ok := actualMap[k]
		require.True(t, ok)

		require.Equal(t, expected.Version, actual.Version, "Version mismatch")
		require.Equal(t, expected.ScheduledEventBatchId, actual.ScheduledEventBatchId, "ScheduledEventBatchId mismatch")
		require.True(t, proto.Equal(expected.ScheduledTime, actual.ScheduledTime), "ScheduledTime mismatch")
		require.Equal(t, expected.StartedEventId, actual.StartedEventId, "StartedEventId mismatch")
		require.True(t, proto.Equal(expected.StartedTime, actual.StartedTime), "StartedTime mismatch")
		require.Equal(t, expected.ActivityId, actual.ActivityId, "ActivityId mismatch")
		require.Equal(t, expected.RequestId, actual.RequestId, "RequestId mismatch")
		require.True(t, proto.Equal(expected.ScheduleToStartTimeout, actual.ScheduleToStartTimeout), "ScheduleToStartTimeout mismatch")
		require.True(t, proto.Equal(expected.ScheduleToCloseTimeout, actual.ScheduleToCloseTimeout), "ScheduleToCloseTimeout mismatch")
		require.True(t, proto.Equal(expected.StartToCloseTimeout, actual.StartToCloseTimeout), "StartToCloseTimeout mismatch")
		require.True(t, proto.Equal(expected.HeartbeatTimeout, actual.HeartbeatTimeout), "HeartbeatTimeout mismatch")
		require.Equal(t, expected.CancelRequested, actual.CancelRequested, "CancelRequested mismatch")
		require.Equal(t, expected.CancelRequestId, actual.CancelRequestId, "CancelRequestId mismatch")
		require.Equal(t, expected.Attempt, actual.Attempt, "Attempt mismatch")
		require.Equal(t, expected.TaskQueue, actual.TaskQueue, "TaskQueue mismatch")
		require.Equal(t, expected.StartedIdentity, actual.StartedIdentity, "StartedIdentity mismatch")
		require.Equal(t, expected.HasRetryPolicy, actual.HasRetryPolicy, "HasRetryPolicy mismatch")
		require.True(t, proto.Equal(expected.RetryInitialInterval, actual.RetryInitialInterval), "RetryInitialInterval mismatch")
		require.True(t, proto.Equal(expected.RetryMaximumInterval, actual.RetryMaximumInterval), "RetryMaximumInterval mismatch")
		require.Equal(t, expected.RetryMaximumAttempts, actual.RetryMaximumAttempts, "RetryMaximumAttempts mismatch")
		require.True(t, proto.Equal(expected.RetryExpirationTime, actual.RetryExpirationTime), "RetryExpirationTime mismatch")
		require.Equal(t, expected.RetryBackoffCoefficient, actual.RetryBackoffCoefficient, "RetryBackoffCoefficient mismatch")
		require.Equal(t, expected.RetryNonRetryableErrorTypes, actual.RetryNonRetryableErrorTypes, "RetryNonRetryableErrorTypes mismatch")
		require.True(t, proto.Equal(expected.RetryLastFailure, actual.RetryLastFailure), "RetryLastFailure mismatch")
		require.Equal(t, expected.RetryLastWorkerIdentity, actual.RetryLastWorkerIdentity, "RetryLastWorkerIdentity mismatch")
		require.Equal(t, expected.ScheduledEventId, actual.ScheduledEventId, "ScheduledEventId mismatch")
		require.True(t, proto.Equal(expected.LastHeartbeatDetails, actual.LastHeartbeatDetails), "LastHeartbeatDetails mismatch")
		require.True(t, proto.Equal(expected.LastHeartbeatUpdateTime, actual.LastHeartbeatUpdateTime), "LastHeartbeatUpdateTime mismatch")
		require.Equal(t, expected.UseCompatibleVersion, actual.UseCompatibleVersion, "UseCompatibleVersion mismatch")
		require.True(t, proto.Equal(expected.ActivityType, actual.ActivityType), "ActivityType mismatch")
		require.True(t, proto.Equal(expected.LastWorkerVersionStamp, actual.LastWorkerVersionStamp), "LastWorkerVersionStamp mismatch")
		require.True(t, proto.Equal(expected.LastStartedDeployment, actual.LastStartedDeployment), "LastStartedDeployment mismatch")
		require.True(t, proto.Equal(expected.LastUpdateVersionedTransition, actual.LastUpdateVersionedTransition), "LastUpdateVersionedTransition mismatch")

		// special handled fields
		require.Equal(t, int32(TimerTaskStatusNone), actual.TimerTaskStatus, "TimerTaskStatus mismatch")
	}
}

func (d *mutableStateTestDeps) verifyExecutionInfo(t *testing.T, current, target, origin *persistencespb.WorkflowExecutionInfo) {
	// These fields should not change.
	require.Equal(t, origin.WorkflowTaskVersion, current.WorkflowTaskVersion, "WorkflowTaskVersion mismatch")
	require.Equal(t, origin.WorkflowTaskScheduledEventId, current.WorkflowTaskScheduledEventId, "WorkflowTaskScheduledEventId mismatch")
	require.Equal(t, origin.WorkflowTaskStartedEventId, current.WorkflowTaskStartedEventId, "WorkflowTaskStartedEventId mismatch")
	require.Equal(t, origin.WorkflowTaskRequestId, current.WorkflowTaskRequestId, "WorkflowTaskRequestId mismatch")
	require.Equal(t, origin.WorkflowTaskTimeout, current.WorkflowTaskTimeout, "WorkflowTaskTimeout mismatch")
	require.Equal(t, origin.WorkflowTaskAttempt, current.WorkflowTaskAttempt, "WorkflowTaskAttempt mismatch")
	require.Equal(t, origin.WorkflowTaskStartedTime, current.WorkflowTaskStartedTime, "WorkflowTaskStartedTime mismatch")
	require.Equal(t, origin.WorkflowTaskScheduledTime, current.WorkflowTaskScheduledTime, "WorkflowTaskScheduledTime mismatch")
	require.Equal(t, origin.WorkflowTaskOriginalScheduledTime, current.WorkflowTaskOriginalScheduledTime, "WorkflowTaskOriginalScheduledTime mismatch")
	require.Equal(t, origin.WorkflowTaskType, current.WorkflowTaskType, "WorkflowTaskType mismatch")
	require.Equal(t, origin.WorkflowTaskSuggestContinueAsNew, current.WorkflowTaskSuggestContinueAsNew, "WorkflowTaskSuggestContinueAsNew mismatch")
	require.Equal(t, origin.WorkflowTaskHistorySizeBytes, current.WorkflowTaskHistorySizeBytes, "WorkflowTaskHistorySizeBytes mismatch")
	require.Equal(t, origin.WorkflowTaskBuildId, current.WorkflowTaskBuildId, "WorkflowTaskBuildId mismatch")
	require.Equal(t, origin.WorkflowTaskBuildIdRedirectCounter, current.WorkflowTaskBuildIdRedirectCounter, "WorkflowTaskBuildIdRedirectCounter mismatch")
	require.True(t, proto.Equal(origin.VersioningInfo, current.VersioningInfo), "VersioningInfo mismatch")
	require.True(t, proto.Equal(origin.VersionHistories, current.VersionHistories), "VersionHistories mismatch")
	require.True(t, proto.Equal(origin.ExecutionStats, current.ExecutionStats), "ExecutionStats mismatch")
	require.Equal(t, origin.LastFirstEventTxnId, current.LastFirstEventTxnId, "LastFirstEventTxnId mismatch")
	require.True(t, proto.Equal(origin.ParentClock, current.ParentClock), "ParentClock mismatch")
	require.Equal(t, origin.CloseTransferTaskId, current.CloseTransferTaskId, "CloseTransferTaskId mismatch")
	require.Equal(t, origin.CloseVisibilityTaskId, current.CloseVisibilityTaskId, "CloseVisibilityTaskId mismatch")
	require.Equal(t, origin.RelocatableAttributesRemoved, current.RelocatableAttributesRemoved, "RelocatableAttributesRemoved mismatch")
	require.Equal(t, origin.WorkflowExecutionTimerTaskStatus, current.WorkflowExecutionTimerTaskStatus, "WorkflowExecutionTimerTaskStatus mismatch")
	require.Equal(t, origin.SubStateMachinesByType, current.SubStateMachinesByType, "SubStateMachinesByType mismatch")
	require.Equal(t, origin.StateMachineTimers, current.StateMachineTimers, "StateMachineTimers mismatch")
	require.Equal(t, origin.TaskGenerationShardClockTimestamp, current.TaskGenerationShardClockTimestamp, "TaskGenerationShardClockTimestamp mismatch")
	require.Equal(t, origin.UpdateInfos, current.UpdateInfos, "UpdateInfos mismatch")

	// These fields should be updated.
	require.Equal(t, target.NamespaceId, current.NamespaceId, "NamespaceId mismatch")
	require.Equal(t, target.WorkflowId, current.WorkflowId, "WorkflowId mismatch")
	require.Equal(t, target.ParentNamespaceId, current.ParentNamespaceId, "ParentNamespaceId mismatch")
	require.Equal(t, target.ParentWorkflowId, current.ParentWorkflowId, "ParentWorkflowId mismatch")
	require.Equal(t, target.ParentRunId, current.ParentRunId, "ParentRunId mismatch")
	require.Equal(t, target.ParentInitiatedId, current.ParentInitiatedId, "ParentInitiatedId mismatch")
	require.Equal(t, target.CompletionEventBatchId, current.CompletionEventBatchId, "CompletionEventBatchId mismatch")
	require.Equal(t, target.TaskQueue, current.TaskQueue, "TaskQueue mismatch")
	require.Equal(t, target.WorkflowTypeName, current.WorkflowTypeName, "WorkflowTypeName mismatch")
	require.True(t, proto.Equal(target.WorkflowExecutionTimeout, current.WorkflowExecutionTimeout), "WorkflowExecutionTimeout mismatch")
	require.True(t, proto.Equal(target.WorkflowRunTimeout, current.WorkflowRunTimeout), "WorkflowRunTimeout mismatch")
	require.True(t, proto.Equal(target.DefaultWorkflowTaskTimeout, current.DefaultWorkflowTaskTimeout), "DefaultWorkflowTaskTimeout mismatch")
	require.Equal(t, target.LastRunningClock, current.LastRunningClock, "LastRunningClock mismatch")
	require.Equal(t, target.LastFirstEventId, current.LastFirstEventId, "LastFirstEventId mismatch")
	require.Equal(t, target.LastCompletedWorkflowTaskStartedEventId, current.LastCompletedWorkflowTaskStartedEventId, "LastCompletedWorkflowTaskStartedEventId mismatch")
	require.True(t, proto.Equal(target.StartTime, current.StartTime), "StartTime mismatch")
	require.True(t, proto.Equal(target.LastUpdateTime, current.LastUpdateTime), "LastUpdateTime mismatch")
	require.Equal(t, target.CancelRequested, current.CancelRequested, "CancelRequested mismatch")
	require.Equal(t, target.CancelRequestId, current.CancelRequestId, "CancelRequestId mismatch")
	require.Equal(t, target.StickyTaskQueue, current.StickyTaskQueue, "StickyTaskQueue mismatch")
	require.True(t, proto.Equal(target.StickyScheduleToStartTimeout, current.StickyScheduleToStartTimeout), "StickyScheduleToStartTimeout mismatch")
	require.Equal(t, target.Attempt, current.Attempt, "Attempt mismatch")
	require.Equal(t, target.WorkflowTaskStamp, current.WorkflowTaskStamp, "WorkflowTaskStamp mismatch")
	require.True(t, proto.Equal(target.RetryInitialInterval, current.RetryInitialInterval), "RetryInitialInterval mismatch")
	require.True(t, proto.Equal(target.RetryMaximumInterval, current.RetryMaximumInterval), "RetryMaximumInterval mismatch")
	require.Equal(t, target.RetryMaximumAttempts, current.RetryMaximumAttempts, "RetryMaximumAttempts mismatch")
	require.Equal(t, target.RetryBackoffCoefficient, current.RetryBackoffCoefficient, "RetryBackoffCoefficient mismatch")
	require.True(t, proto.Equal(target.WorkflowExecutionExpirationTime, current.WorkflowExecutionExpirationTime), "WorkflowExecutionExpirationTime mismatch")
	require.Equal(t, target.RetryNonRetryableErrorTypes, current.RetryNonRetryableErrorTypes, "RetryNonRetryableErrorTypes mismatch")
	require.Equal(t, target.HasRetryPolicy, current.HasRetryPolicy, "HasRetryPolicy mismatch")
	require.Equal(t, target.CronSchedule, current.CronSchedule, "CronSchedule mismatch")
	require.Equal(t, target.SignalCount, current.SignalCount, "SignalCount mismatch")
	require.Equal(t, target.ActivityCount, current.ActivityCount, "ActivityCount mismatch")
	require.Equal(t, target.ChildExecutionCount, current.ChildExecutionCount, "ChildExecutionCount mismatch")
	require.Equal(t, target.UserTimerCount, current.UserTimerCount, "UserTimerCount mismatch")
	require.Equal(t, target.RequestCancelExternalCount, current.RequestCancelExternalCount, "RequestCancelExternalCount mismatch")
	require.Equal(t, target.SignalExternalCount, current.SignalExternalCount, "SignalExternalCount mismatch")
	require.Equal(t, target.UpdateCount, current.UpdateCount, "UpdateCount mismatch")
	require.True(t, proto.Equal(target.AutoResetPoints, current.AutoResetPoints), "AutoResetPoints mismatch")
	require.Equal(t, target.SearchAttributes, current.SearchAttributes, "SearchAttributes mismatch")
	require.Equal(t, target.Memo, current.Memo, "Memo mismatch")
	require.Equal(t, target.FirstExecutionRunId, current.FirstExecutionRunId, "FirstExecutionRunId mismatch")
	require.True(t, proto.Equal(target.WorkflowRunExpirationTime, current.WorkflowRunExpirationTime), "WorkflowRunExpirationTime mismatch")
	require.Equal(t, target.StateTransitionCount, current.StateTransitionCount, "StateTransitionCount mismatch")
	require.True(t, proto.Equal(target.ExecutionTime, current.ExecutionTime), "ExecutionTime mismatch")
	require.Equal(t, target.NewExecutionRunId, current.NewExecutionRunId, "NewExecutionRunId mismatch")
	require.Equal(t, target.ParentInitiatedVersion, current.ParentInitiatedVersion, "ParentInitiatedVersion mismatch")
	require.True(t, proto.Equal(target.CloseTime, current.CloseTime), "CloseTime mismatch")
	require.True(t, proto.Equal(target.BaseExecutionInfo, current.BaseExecutionInfo), "BaseExecutionInfo mismatch")
	require.True(t, proto.Equal(target.MostRecentWorkerVersionStamp, current.MostRecentWorkerVersionStamp), "MostRecentWorkerVersionStamp mismatch")
	require.Equal(t, target.AssignedBuildId, current.AssignedBuildId, "AssignedBuildId mismatch")
	require.Equal(t, target.InheritedBuildId, current.InheritedBuildId, "InheritedBuildId mismatch")
	require.Equal(t, target.BuildIdRedirectCounter, current.BuildIdRedirectCounter, "BuildIdRedirectCounter mismatch")
	require.Equal(t, target.SubStateMachinesByType, current.SubStateMachinesByType, "SubStateMachinesByType mismatch")
	require.Equal(t, target.RootWorkflowId, current.RootWorkflowId, "RootWorkflowId mismatch")
	require.Equal(t, target.RootRunId, current.RootRunId, "RootRunId mismatch")
	require.Equal(t, target.StateMachineTimers, current.StateMachineTimers, "StateMachineTimers mismatch")
	require.True(t, proto.Equal(target.WorkflowTaskLastUpdateVersionedTransition, current.WorkflowTaskLastUpdateVersionedTransition), "WorkflowTaskLastUpdateVersionedTransition mismatch")
	require.True(t, proto.Equal(target.VisibilityLastUpdateVersionedTransition, current.VisibilityLastUpdateVersionedTransition), "VisibilityLastUpdateVersionedTransition mismatch")
	require.True(t, proto.Equal(target.SignalRequestIdsLastUpdateVersionedTransition, current.SignalRequestIdsLastUpdateVersionedTransition), "SignalRequestIdsLastUpdateVersionedTransition mismatch")
	require.Equal(t, target.SubStateMachineTombstoneBatches, current.SubStateMachineTombstoneBatches, "SubStateMachineTombstoneBatches mismatch")
}

func (d *mutableStateTestDeps) verifyMutableState(t *testing.T, current, target, origin *MutableStateImpl) {
	deps.verifyExecutionInfo(t, current.executionInfo, target.executionInfo, origin.executionInfo)
	require.True(t, proto.Equal(target.executionState, current.executionState), "executionState mismatch")

	require.Equal(t, target.pendingActivityTimerHeartbeats, current.pendingActivityTimerHeartbeats, "pendingActivityTimerHeartbeats mismatch")
	d.verifyActivityInfos(t, target.pendingActivityInfoIDs, current.pendingActivityInfoIDs)
	require.Equal(t, target.pendingActivityIDToEventID, current.pendingActivityIDToEventID, "pendingActivityIDToEventID mismatch")
	compareMapOfProto(s, current.pendingActivityInfoIDs, current.updateActivityInfos)
	require.Equal(t, map[int64]struct{}{89: {}}, current.deleteActivityInfos, "deleteActivityInfos mismatch")

	compareMapOfProto(s, target.pendingTimerInfoIDs, current.pendingTimerInfoIDs)
	require.Equal(t, target.pendingTimerEventIDToID, current.pendingTimerEventIDToID, "pendingTimerEventIDToID mismatch")
	compareMapOfProto(s, target.pendingTimerInfoIDs, current.updateTimerInfos)
	require.Equal(t, map[string]struct{}{"to-be-deleted": {}}, current.deleteTimerInfos, "deleteTimerInfos mismatch")

	deps.verifyChildExecutionInfos(t, target.pendingChildExecutionInfoIDs, current.pendingChildExecutionInfoIDs, origin.pendingChildExecutionInfoIDs)
	deps.verifyChildExecutionInfos(t, target.pendingChildExecutionInfoIDs, current.updateChildExecutionInfos, origin.pendingChildExecutionInfoIDs)
	require.Equal(t, map[int64]struct{}{79: {}}, current.deleteChildExecutionInfos, "deleteChildExecutionInfos mismatch")

	compareMapOfProto(s, target.pendingRequestCancelInfoIDs, current.pendingRequestCancelInfoIDs)
	compareMapOfProto(s, target.pendingRequestCancelInfoIDs, current.updateRequestCancelInfos)
	require.Equal(t, map[int64]struct{}{69: {}}, current.deleteRequestCancelInfos, "deleteRequestCancelInfos mismatch")

	compareMapOfProto(s, target.pendingSignalInfoIDs, current.pendingSignalInfoIDs)
	compareMapOfProto(s, target.pendingSignalInfoIDs, current.updateSignalInfos)
	require.Equal(t, map[int64]struct{}{74: {}}, current.deleteSignalInfos, "deleteSignalInfos mismatch")

	require.Equal(t, target.pendingSignalRequestedIDs, current.pendingSignalRequestedIDs, "pendingSignalRequestedIDs mismatch")
	require.Equal(t, target.pendingSignalRequestedIDs, current.updateSignalRequestedIDs, "updateSignalRequestedIDs mismatch")
	require.Equal(t, map[string]struct{}{"to-be-deleted": {}}, current.deleteSignalRequestedIDs, "deleteSignalRequestedIDs mismatch")

	require.Equal(t, target.currentVersion, current.currentVersion, "currentVersion mismatch")
	require.Equal(t, target.totalTombstones, current.totalTombstones, "totalTombstones mismatch")
	require.Equal(t, target.dbRecordVersion, current.dbRecordVersion, "dbRecordVersion mismatch")
	require.True(t, proto.Equal(target.checksum, current.checksum), "checksum mismatch")
}

func (d *mutableStateTestDeps) buildSnapshot(t *testing.T, state *MutableStateImpl) *persistencespb.WorkflowMutableState {
	snapshot := &persistencespb.WorkflowMutableState{
		ActivityInfos: map[int64]*persistencespb.ActivityInfo{
			90: {
				Version:                       1234,
				ScheduledTime:                 state.pendingActivityInfoIDs[90].ScheduledTime,
				StartedTime:                   state.pendingActivityInfoIDs[90].StartedTime,
				ActivityId:                    "activityID_5",
				ScheduleToStartTimeout:        timestamp.DurationPtr(time.Second * 100),
				ScheduleToCloseTimeout:        timestamp.DurationPtr(time.Second * 200),
				StartToCloseTimeout:           timestamp.DurationPtr(time.Second * 300),
				HeartbeatTimeout:              timestamp.DurationPtr(time.Second * 50),
				ScheduledEventId:              90,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
			},
			91: {
				ActivityId:                    "activity_id_91",
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
			},
		},
		TimerInfos: map[string]*persistencespb.TimerInfo{
			"25": {
				Version:                       1234,
				StartedEventId:                85,
				ExpiryTime:                    state.pendingTimerInfoIDs["25"].ExpiryTime,
				TimerId:                       "25",
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
			},
			"26": {
				TimerId:                       "26",
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
			},
		},
		ChildExecutionInfos: map[int64]*persistencespb.ChildExecutionInfo{
			80: {
				Version:                       1234,
				InitiatedEventBatchId:         20,
				CreateRequestId:               state.pendingChildExecutionInfoIDs[80].CreateRequestId,
				Namespace:                     "mock namespace name",
				WorkflowTypeName:              "code.uber.internal/test/foobar",
				InitiatedEventId:              80,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
			},
			81: {
				InitiatedEventBatchId:         81,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
			},
		},
		RequestCancelInfos: map[int64]*persistencespb.RequestCancelInfo{
			70: {
				Version:                       1234,
				InitiatedEventBatchId:         20,
				CancelRequestId:               state.pendingRequestCancelInfoIDs[70].CancelRequestId,
				InitiatedEventId:              70,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
			},
			71: {
				InitiatedEventBatchId:         71,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
			},
		},
		SignalInfos: map[int64]*persistencespb.SignalInfo{
			75: {
				Version:                       1234,
				InitiatedEventBatchId:         17,
				RequestId:                     state.pendingSignalInfoIDs[75].RequestId,
				InitiatedEventId:              75,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
			},
			76: {
				InitiatedEventBatchId:         76,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
			},
		},
		ChasmNodes:         state.chasmTree.Snapshot(nil).Nodes,
		SignalRequestedIds: []string{"signal_request_id_1", "signal_requested_id_2"},
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:                             "deadbeef-0123-4567-890a-bcdef0123456",
			WorkflowId:                              "wId",
			TaskQueue:                               "testTaskQueue",
			WorkflowTypeName:                        "wType",
			WorkflowRunTimeout:                      timestamp.DurationPtr(time.Second * 200),
			DefaultWorkflowTaskTimeout:              timestamp.DurationPtr(time.Second * 100),
			LastCompletedWorkflowTaskStartedEventId: 99,
			LastUpdateTime:                          state.executionInfo.LastUpdateTime,
			WorkflowTaskVersion:                     1234,
			WorkflowTaskScheduledEventId:            101,
			WorkflowTaskStartedEventId:              102,
			WorkflowTaskTimeout:                     timestamp.DurationPtr(time.Second * 100),
			WorkflowTaskAttempt:                     1,
			WorkflowTaskType:                        enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("token#1"),
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 102, Version: 1234},
						},
					},
				},
			},
			FirstExecutionRunId: state.executionInfo.FirstExecutionRunId,
			ExecutionTime:       state.executionInfo.ExecutionTime,
			TransitionHistory: []*persistencespb.VersionedTransition{
				{NamespaceFailoverVersion: 1234, TransitionCount: 1024},
				{TransitionCount: 1025},
			},
			SignalRequestIdsLastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
			WorkflowTaskLastUpdateVersionedTransition:     state.executionInfo.WorkflowTaskLastUpdateVersionedTransition,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:     state.executionState.RunId,
			State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: state.executionState.StartTime,
		},
		NextEventId: 103,
	}
	return snapshot
}

func TestApplySnapshot(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			testCases := []struct {
				name                        string
				updateWorkflowTask          bool
				speculativeTask             bool
				expectedWorkflowTaskUpdated bool
			}{
				{
					name:                        "update workflow task",
					updateWorkflowTask:          true,
					speculativeTask:             false,
					expectedWorkflowTaskUpdated: true,
				},
				{
					name:                        "not update workflow task",
					updateWorkflowTask:          false,
					speculativeTask:             false,
					expectedWorkflowTaskUpdated: false,
				},
				{
					name:                        "update speculative workflow task",
					updateWorkflowTask:          true,
					speculativeTask:             true,
					expectedWorkflowTaskUpdated: true,
				},
				{
					name:                        "not update speculative workflow task",
					updateWorkflowTask:          false,
					speculativeTask:             true,
					expectedWorkflowTaskUpdated: false,
				},
			}
			for _, tc := range testCases {
				t.Run(tc.name, func() {
					state := deps.buildWorkflowMutableState(t)
					deps.addChangesForStateReplication(t, state)

					chasmNodesSnapshot := chasm.NodesSnapshot{
						Nodes: state.ChasmNodes,
					}

					originMS, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, state, 123)
					require.NoError(t, err)

					currentMS, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, state, 123)
					require.NoError(t, err)
					currentMockChasmTree := historyi.NewMockChasmTree(deps.controller)
					currentMockChasmTree.EXPECT().ApplySnapshot(chasmNodesSnapshot).Return(nil).Times(1)
					currentMS.chasmTree = currentMockChasmTree

					state = deps.buildWorkflowMutableState(t)
					state.ActivityInfos[91] = &persistencespb.ActivityInfo{
						ActivityId: "activity_id_91",
					}
					state.TimerInfos["26"] = &persistencespb.TimerInfo{
						TimerId: "26",
					}
					state.ChildExecutionInfos[81] = &persistencespb.ChildExecutionInfo{
						InitiatedEventBatchId: 81,
					}
					state.RequestCancelInfos[71] = &persistencespb.RequestCancelInfo{
						InitiatedEventBatchId: 71,
					}
					state.SignalInfos[76] = &persistencespb.SignalInfo{
						InitiatedEventBatchId: 76,
					}
					state.SignalRequestedIds = append(state.SignalRequestedIds, "signal_requested_id_2")

					targetMS, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, state, 123)
					require.NoError(t, err)
					targetMockChasmTree := historyi.NewMockChasmTree(deps.controller)
					targetMockChasmTree.EXPECT().Snapshot(nil).Return(chasmNodesSnapshot).Times(1)
					targetMS.chasmTree = targetMockChasmTree

					targetMS.GetExecutionInfo().TransitionHistory = UpdatedTransitionHistory(targetMS.GetExecutionInfo().TransitionHistory, targetMS.GetCurrentVersion())

					// set updateXXX so LastUpdateVersionedTransition will be updated
					targetMS.updateActivityInfos = targetMS.pendingActivityInfoIDs
					for key := range targetMS.updateActivityInfos {
						targetMS.activityInfosUserDataUpdated[key] = struct{}{}
					}
					targetMS.updateTimerInfos = targetMS.pendingTimerInfoIDs
					for key := range targetMS.updateTimerInfos {
						targetMS.timerInfosUserDataUpdated[key] = struct{}{}
					}
					targetMS.updateChildExecutionInfos = targetMS.pendingChildExecutionInfoIDs
					targetMS.updateRequestCancelInfos = targetMS.pendingRequestCancelInfoIDs
					targetMS.updateSignalInfos = targetMS.pendingSignalInfoIDs
					targetMS.updateSignalRequestedIDs = targetMS.pendingSignalRequestedIDs

					if tc.updateWorkflowTask {
						// test mutation with workflow task update
						workflowTask := &historyi.WorkflowTaskInfo{
							ScheduledEventID: 1234,
						}
						targetMS.workflowTaskManager.UpdateWorkflowTask(workflowTask)
					}

					if tc.speculativeTask {
						targetMS.executionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE
					}

					targetMS.closeTransactionTrackLastUpdateVersionedTransition(historyi.TransactionPolicyActive)

					snapshot := deps.buildSnapshot(t, targetMS)
					require.Nil(t, snapshot.ExecutionInfo.SubStateMachinesByType)
					err = currentMS.ApplySnapshot(snapshot)
					require.NoError(t, err)
					require.NotNil(t, currentMS.GetExecutionInfo().SubStateMachinesByType)

					deps.verifyMutableState(t, currentMS, targetMS, originMS)
					require.Equal(t, tc.expectedWorkflowTaskUpdated, currentMS.workflowTaskUpdated)
				})
			}

		})
	}
}

func (d *mutableStateTestDeps) buildMutation(t *testing.T,
	state *MutableStateImpl,
	tombstones []*persistencespb.StateMachineTombstoneBatch,
) *persistencespb.WorkflowMutableStateMutation {
	executionInfoClone := common.CloneProto(state.executionInfo)
	executionInfoClone.SubStateMachineTombstoneBatches = nil
	mutation := &persistencespb.WorkflowMutableStateMutation{
		UpdatedActivityInfos:            state.pendingActivityInfoIDs,
		UpdatedTimerInfos:               state.pendingTimerInfoIDs,
		UpdatedChildExecutionInfos:      state.pendingChildExecutionInfoIDs,
		UpdatedRequestCancelInfos:       state.pendingRequestCancelInfoIDs,
		UpdatedSignalInfos:              state.pendingSignalInfoIDs,
		UpdatedChasmNodes:               state.chasmTree.Snapshot(nil).Nodes,
		SignalRequestedIds:              state.GetPendingSignalRequestedIds(),
		SubStateMachineTombstoneBatches: tombstones,
		ExecutionInfo:                   executionInfoClone,
		ExecutionState:                  state.executionState,
	}
	return mutation
}

func TestApplyMutation(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			testCases := []struct {
				name                        string
				updateWorkflowTask          bool
				speculativeTask             bool
				expectedWorkflowTaskUpdated bool
			}{
				{
					name:                        "update workflow task",
					updateWorkflowTask:          true,
					speculativeTask:             false,
					expectedWorkflowTaskUpdated: true,
				},
				{
					name:                        "not update workflow task",
					updateWorkflowTask:          false,
					speculativeTask:             false,
					expectedWorkflowTaskUpdated: false,
				},
				{
					name:                        "update speculative workflow task",
					updateWorkflowTask:          true,
					speculativeTask:             true,
					expectedWorkflowTaskUpdated: true,
				},
				{
					name:                        "not update speculative workflow task",
					updateWorkflowTask:          false,
					speculativeTask:             true,
					expectedWorkflowTaskUpdated: false,
				},
			}
			for _, tc := range testCases {
				t.Run(tc.name, func() {
					state := deps.buildWorkflowMutableState(t)
					deps.addChangesForStateReplication(t, state)

					originMS, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, state, 123)
					require.NoError(t, err)
					tombstones := []*persistencespb.StateMachineTombstoneBatch{
						{
							VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1},
							StateMachineTombstones: []*persistencespb.StateMachineTombstone{
								{
									StateMachineKey: &persistencespb.StateMachineTombstone_ActivityScheduledEventId{
										ActivityScheduledEventId: 10,
									},
								},
							},
						},
					}
					originMS.GetExecutionInfo().SubStateMachineTombstoneBatches = tombstones

					currentMS, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, state, 123)
					require.NoError(t, err)
					currentMockChasmTree := historyi.NewMockChasmTree(deps.controller)
					currentMS.chasmTree = currentMockChasmTree

					currentMS.GetExecutionInfo().SubStateMachineTombstoneBatches = tombstones

					state = deps.buildWorkflowMutableState(t)

					targetMS, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, tests.LocalNamespaceEntry, state, 123)
					require.NoError(t, err)

					targetMockChasmTree := historyi.NewMockChasmTree(deps.controller)
					updateChasmNodes := map[string]*persistencespb.ChasmNode{
						"node-path": {
							Metadata: &persistencespb.ChasmNodeMetadata{
								InitialVersionedTransition:    &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1},
								LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1},
								Attributes:                    &persistencespb.ChasmNodeMetadata_DataAttributes{},
							},
							Data: &commonpb.DataBlob{Data: []byte("test-data")},
						},
						"node-path/collection-node": {
							Metadata: &persistencespb.ChasmNodeMetadata{
								InitialVersionedTransition:    &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1},
								LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1},
								Attributes:                    &persistencespb.ChasmNodeMetadata_CollectionAttributes{},
							},
						},
					}
					targetMockChasmTree.EXPECT().Snapshot(nil).Return(chasm.NodesSnapshot{
						Nodes: updateChasmNodes,
					})
					targetMS.chasmTree = targetMockChasmTree

					transitionHistory := targetMS.executionInfo.TransitionHistory
					failoverVersion := transitionhistory.LastVersionedTransition(transitionHistory).NamespaceFailoverVersion
					targetMS.executionInfo.TransitionHistory = UpdatedTransitionHistory(transitionHistory, failoverVersion)

					// set updateXXX so LastUpdateVersionedTransition will be updated
					targetMS.updateActivityInfos = targetMS.pendingActivityInfoIDs
					for key := range targetMS.updateActivityInfos {
						targetMS.activityInfosUserDataUpdated[key] = struct{}{}
					}
					targetMS.updateTimerInfos = targetMS.pendingTimerInfoIDs
					for key := range targetMS.updateTimerInfos {
						targetMS.timerInfosUserDataUpdated[key] = struct{}{}
					}
					targetMS.updateChildExecutionInfos = targetMS.pendingChildExecutionInfoIDs
					targetMS.updateRequestCancelInfos = targetMS.pendingRequestCancelInfoIDs
					targetMS.updateSignalInfos = targetMS.pendingSignalInfoIDs
					targetMS.updateSignalRequestedIDs = targetMS.pendingSignalRequestedIDs

					if tc.updateWorkflowTask {
						// test mutation with workflow task update
						workflowTask := &historyi.WorkflowTaskInfo{
							ScheduledEventID: 1234,
						}
						targetMS.workflowTaskManager.UpdateWorkflowTask(workflowTask)
					}

					if tc.speculativeTask {
						targetMS.executionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE
					}

					targetMS.closeTransactionTrackLastUpdateVersionedTransition(historyi.TransactionPolicyActive)

					tombstonesToAdd := []*persistencespb.StateMachineTombstoneBatch{
						{
							VersionedTransition: targetMS.CurrentVersionedTransition(),
							StateMachineTombstones: []*persistencespb.StateMachineTombstone{
								{
									StateMachineKey: &persistencespb.StateMachineTombstone_ActivityScheduledEventId{
										ActivityScheduledEventId: 89,
									},
								},
								{
									StateMachineKey: &persistencespb.StateMachineTombstone_ActivityScheduledEventId{
										ActivityScheduledEventId: 9999, // not exist
									},
								},
								{
									StateMachineKey: &persistencespb.StateMachineTombstone_TimerId{
										TimerId: "to-be-deleted",
									},
								},
								{

									StateMachineKey: &persistencespb.StateMachineTombstone_TimerId{
										TimerId: "not-exist",
									},
								},
								{
									StateMachineKey: &persistencespb.StateMachineTombstone_ChildExecutionInitiatedEventId{
										ChildExecutionInitiatedEventId: 79,
									},
								},
								{
									StateMachineKey: &persistencespb.StateMachineTombstone_ChildExecutionInitiatedEventId{
										ChildExecutionInitiatedEventId: 9998, // not exist
									},
								},
								{
									StateMachineKey: &persistencespb.StateMachineTombstone_RequestCancelInitiatedEventId{
										RequestCancelInitiatedEventId: 69,
									},
								},
								{
									StateMachineKey: &persistencespb.StateMachineTombstone_RequestCancelInitiatedEventId{
										RequestCancelInitiatedEventId: 9997, // not exist
									},
								},
								{
									StateMachineKey: &persistencespb.StateMachineTombstone_SignalExternalInitiatedEventId{
										SignalExternalInitiatedEventId: 74,
									},
								},
								{
									StateMachineKey: &persistencespb.StateMachineTombstone_SignalExternalInitiatedEventId{
										SignalExternalInitiatedEventId: 9996, // not exist
									},
								},
								{
									StateMachineKey: &persistencespb.StateMachineTombstone_ChasmNodePath{
										ChasmNodePath: "deleted-node-path",
									},
								},
							},
						},
					}
					targetMS.GetExecutionInfo().SubStateMachineTombstoneBatches = append(tombstones, tombstonesToAdd...)
					targetMS.totalTombstones = len(tombstones[0].StateMachineTombstones) + len(tombstonesToAdd[0].StateMachineTombstones)
					mutation := deps.buildMutation(t, targetMS, tombstonesToAdd)

					currentMockChasmTree.EXPECT().ApplyMutation(chasm.NodesMutation{
						DeletedNodes: map[string]struct{}{"deleted-node-path": {}},
					}).Return(nil).Times(1)
					currentMockChasmTree.EXPECT().ApplyMutation(chasm.NodesMutation{
						UpdatedNodes: updateChasmNodes,
					}).Return(nil).Times(1)

					err = currentMS.ApplyMutation(mutation)
					require.NoError(t, err)
					deps.verifyMutableState(t, currentMS, targetMS, originMS)
					require.Equal(t, tc.expectedWorkflowTaskUpdated, currentMS.workflowTaskUpdated)
				})
			}

		})
	}
}

func TestRefreshTask_DiffCluster(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			version := int64(99)
			attempt := int32(1)
			incomingActivityInfo := &persistencespb.ActivityInfo{
				Version: version,
				Attempt: attempt,
			}
			localActivityInfo := &persistencespb.ActivityInfo{
				Version: int64(100),
				Attempt: incomingActivityInfo.Attempt,
			}

			deps.mockShard.Resource.ClusterMetadata.EXPECT().IsVersionFromSameCluster(localActivityInfo.Version, version).Return(false)

			shouldReset := deps.mutableState.ShouldResetActivityTimerTaskMask(
				localActivityInfo,
				incomingActivityInfo,
			)
			require.True(t, shouldReset)

		})
	}
}

func TestRefreshTask_SameCluster_DiffAttempt(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			version := int64(99)
			attempt := int32(1)
			incomingActivityInfo := &persistencespb.ActivityInfo{
				Version: version,
				Attempt: attempt,
			}
			localActivityInfo := &persistencespb.ActivityInfo{
				Version: version,
				Attempt: attempt + 1,
			}

			deps.mockShard.Resource.ClusterMetadata.EXPECT().IsVersionFromSameCluster(version, version).Return(true)

			shouldReset := deps.mutableState.ShouldResetActivityTimerTaskMask(
				localActivityInfo,
				incomingActivityInfo,
			)
			require.True(t, shouldReset)

		})
	}
}

func TestRefreshTask_SameCluster_SameAttempt(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			version := int64(99)
			attempt := int32(1)
			incomingActivityInfo := &persistencespb.ActivityInfo{
				Version: version,
				Attempt: attempt,
			}
			localActivityInfo := &persistencespb.ActivityInfo{
				Version: version,
				Attempt: attempt,
			}

			deps.mockShard.Resource.ClusterMetadata.EXPECT().IsVersionFromSameCluster(version, version).Return(true)

			shouldReset := deps.mutableState.ShouldResetActivityTimerTaskMask(
				localActivityInfo,
				incomingActivityInfo,
			)
			require.False(t, shouldReset)

		})
	}
}

func TestUpdateActivityTaskStatusWithTimerHeartbeat(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			dbState := deps.buildWorkflowMutableState(t)
			scheduleEventId := int64(781)
			dbState.ActivityInfos[scheduleEventId] = &persistencespb.ActivityInfo{
				Version:                5,
				ScheduledEventId:       int64(90),
				ScheduledTime:          timestamppb.New(time.Now().UTC()),
				StartedEventId:         common.EmptyEventID,
				StartedTime:            timestamppb.New(time.Now().UTC()),
				ActivityId:             "activityID_5",
				TimerTaskStatus:        0,
				ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
				ScheduleToCloseTimeout: timestamp.DurationFromSeconds(200),
				StartToCloseTimeout:    timestamp.DurationFromSeconds(300),
				HeartbeatTimeout:       timestamp.DurationFromSeconds(50),
			}
			mutableState, err := NewMutableStateFromDB(deps.mockShard, deps.mockEventsCache, deps.logger, deps.namespaceEntry, dbState, 123)
			require.NoError(t, err)
			originalTime := time.Now().UTC().Add(time.Second * 60)
			mutableState.pendingActivityTimerHeartbeats[scheduleEventId] = originalTime
			status := int32(1)
			err = mutableState.UpdateActivityTaskStatusWithTimerHeartbeat(scheduleEventId, status, nil)
			require.NoError(t, err)
			require.Equal(t, status, dbState.ActivityInfos[scheduleEventId].TimerTaskStatus)
			require.Equal(t, originalTime, mutableState.pendingActivityTimerHeartbeats[scheduleEventId])

		})
	}
}

func TestHasRequestID(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			testCases := []struct {
				name          string
				requestID     string
				setupFunc     func(ms *MutableStateImpl) // Setup function to prepare the mutable state
				expectedFound bool
			}{
				{
					name:      "empty_request_id",
					requestID: "",
					setupFunc: func(ms *MutableStateImpl) {
						// No setup needed
					},
					expectedFound: false,
				},
				{
					name:      "request_id_not_found",
					requestID: "non-existent-request-id",
					setupFunc: func(ms *MutableStateImpl) {
						// No setup needed
					},
					expectedFound: false,
				},
				{
					name:      "request_id_found",
					requestID: "existing-request-id",
					setupFunc: func(ms *MutableStateImpl) {
						// Add request ID to execution state
						ms.AttachRequestID("existing-request-id", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 100)
					},
					expectedFound: true,
				},
				{
					name:      "multiple_request_ids_found_target",
					requestID: "target-request-id",
					setupFunc: func(ms *MutableStateImpl) {
						// Add multiple request IDs
						ms.AttachRequestID("request-id-1", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 101)
						ms.AttachRequestID("target-request-id", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 102)
						ms.AttachRequestID("request-id-3", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 103)
					},
					expectedFound: true,
				},
				{
					name:      "multiple_request_ids_not_found_target",
					requestID: "missing-request-id",
					setupFunc: func(ms *MutableStateImpl) {
						// Add multiple request IDs, but not the target one
						ms.AttachRequestID("request-id-1", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 104)
						ms.AttachRequestID("request-id-2", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 105)
						ms.AttachRequestID("request-id-3", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 106)
					},
					expectedFound: false,
				},
				{
					name:      "request_id_case_sensitive",
					requestID: "Case-Sensitive-ID",
					setupFunc: func(ms *MutableStateImpl) {
						// Add request ID with different case
						ms.AttachRequestID("case-sensitive-id", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 107)
					},
					expectedFound: false,
				},
				{
					name:      "request_id_exact_match",
					requestID: "exact-match-id",
					setupFunc: func(ms *MutableStateImpl) {
						// Add exact match
						ms.AttachRequestID("exact-match-id", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 108)
					},
					expectedFound: true,
				},
				{
					name:      "request_id_with_special_characters",
					requestID: "request-id-with-$pecial-ch@racters_123",
					setupFunc: func(ms *MutableStateImpl) {
						ms.AttachRequestID("request-id-with-$pecial-ch@racters_123", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 109)
					},
					expectedFound: true,
				},
				{
					name:      "uuid_style_request_id",
					requestID: "12345678-1234-1234-1234-123456789abc",
					setupFunc: func(ms *MutableStateImpl) {
						ms.AttachRequestID("12345678-1234-1234-1234-123456789abc", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 110)
					},
					expectedFound: true,
				},
				{
					name:      "very_long_request_id",
					requestID: "very-long-request-id-" + strings.Repeat("x", 100),
					setupFunc: func(ms *MutableStateImpl) {
						ms.AttachRequestID("very-long-request-id-"+strings.Repeat("x", 100), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 111)
					},
					expectedFound: true,
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func() {
					deps.newMutableState()

					// Setup the mutable state
					tc.setupFunc(deps.mutableState)

					// Test HasRequestID
					found := deps.mutableState.HasRequestID(tc.requestID)
					require.Equal(t, tc.expectedFound, found, "HasRequestID result should match expected")

					// Verify the request ID existence directly in the execution state if expected to be found
					if tc.expectedFound && tc.requestID != "" {
						_, exists := deps.mutableState.executionState.RequestIds[tc.requestID]
						require.True(t, exists, "Request ID should exist in execution state RequestIds map")
					}
				})
			}

		})
	}
}

func TestHasRequestID_StateConsistency(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			// Test that HasRequestID is consistent with AttachRequestID
			requestID := "consistency-test-request-id"

			// Initially should not exist
			require.False(t, deps.mutableState.HasRequestID(requestID))

			// After attaching, should exist
			deps.mutableState.AttachRequestID(requestID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 200)
			require.True(t, deps.mutableState.HasRequestID(requestID))

			// Should still exist after multiple calls
			require.True(t, deps.mutableState.HasRequestID(requestID))
			require.True(t, deps.mutableState.HasRequestID(requestID))

		})
	}
}

func TestHasRequestID_EmptyExecutionState(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			// Ensure execution state has no request IDs initially
			if deps.mutableState.executionState.RequestIds == nil {
				deps.mutableState.executionState.RequestIds = make(map[string]*persistencespb.RequestIDInfo)
			}

			// Clear any existing request IDs
			for k := range deps.mutableState.executionState.RequestIds {
				delete(deps.mutableState.executionState.RequestIds, k)
			}

			// Test various request IDs on empty state
			testRequestIDs := []string{
				"",
				"test-id",
				"another-id",
			}

			for _, requestID := range testRequestIDs {
				require.False(t, deps.mutableState.HasRequestID(requestID), "Should return false for request ID: %s", requestID)
			}

		})
	}
}

func TestAddTasks_CHASMPureTask(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			deps.mockConfig.ChasmMaxInMemoryPureTasks = dynamicconfig.GetIntPropertyFn(5)
			totalTasks := 2 * deps.mockConfig.ChasmMaxInMemoryPureTasks()

			visTimestamp := deps.mockShard.GetTimeSource().Now()
			for i := 0; i < totalTasks; i++ {
				task := &tasks.ChasmTaskPure{
					VisibilityTimestamp: visTimestamp,
					Category:            tasks.CategoryTimer,
				}
				deps.mutableState.AddTasks(task)
				require.LessOrEqual(t, len(deps.mutableState.chasmPureTasks), deps.mockConfig.ChasmMaxInMemoryPureTasks())

				visTimestamp = visTimestamp.Add(-time.Minute)
			}

			deps.mockConfig.ChasmMaxInMemoryPureTasks = dynamicconfig.GetIntPropertyFn(2)
			deps.mutableState.AddTasks(&tasks.ChasmTaskPure{
				VisibilityTimestamp: visTimestamp,
				Category:            tasks.CategoryTimer,
			})
			require.Len(t, deps.mutableState.chasmPureTasks, 2)

		})
	}
}

func TestDeleteCHASMPureTasks(t *testing.T) {
	for _, replicationMultipleBatches := range []bool{true, false} {
		t.Run(fmt.Sprintf("replicationMultipleBatches=%v", replicationMultipleBatches), func(t *testing.T) {
			deps := setupMutableStateTest(t, replicationMultipleBatches)
			defer deps.cleanup()

			now := deps.mockShard.GetTimeSource().Now()

			testCases := []struct {
				name              string
				maxScheduledTime  time.Time
				expectedRemaining int
			}{
				{
					name:              "none",
					maxScheduledTime:  now,
					expectedRemaining: 3,
				},
				{
					name:              "paritial",
					maxScheduledTime:  now.Add(2 * time.Minute),
					expectedRemaining: 2,
				},
				{
					name:              "all",
					maxScheduledTime:  now.Add(5 * time.Minute),
					expectedRemaining: 0,
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func() {
					deps.mutableState.chasmPureTasks = []*tasks.ChasmTaskPure{
						{
							VisibilityTimestamp: now.Add(3 * time.Minute),
							Category:            tasks.CategoryTimer,
						},
						{
							VisibilityTimestamp: now.Add(2 * time.Minute),
							Category:            tasks.CategoryTimer,
						},
						{
							VisibilityTimestamp: now.Add(time.Minute),
							Category:            tasks.CategoryTimer,
						},
					}
					deps.mutableState.BestEffortDeleteTasks = make(map[tasks.Category][]tasks.Key)

					deps.mutableState.DeleteCHASMPureTasks(tc.maxScheduledTime)

					require.Len(t, deps.mutableState.chasmPureTasks, tc.expectedRemaining)
					for _, task := range deps.mutableState.chasmPureTasks {
						require.False(t, task.VisibilityTimestamp.Before(tc.maxScheduledTime))
					}

					require.Len(t, deps.mutableState.BestEffortDeleteTasks[tasks.CategoryTimer], 3-tc.expectedRemaining)
				})
			}

		})
	}
}
