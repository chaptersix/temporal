package history

import (
	"context"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/consts"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type visibilityReadinessFixture struct {
	chasm.UnimplementedComponent
	ActivityInfo *persistencespb.ActivityInfo
}

func (*visibilityReadinessFixture) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
func (*visibilityReadinessFixture) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, nil
}
func (v *visibilityReadinessFixture) VisibilityReady(chasm.Context) bool {
	return !v.ActivityInfo.Paused
}
func (v *visibilityReadinessFixture) Memo(chasm.Context) proto.Message {
	return &persistencespb.WorkflowExecutionState{RunId: v.ActivityInfo.ActivityId}
}

type visibilityReadinessLibrary struct{ chasm.UnimplementedLibrary }

func (*visibilityReadinessLibrary) Name() string { return "VisibilityReadinessTest" }
func (*visibilityReadinessLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{chasm.NewRegistrableComponent[*visibilityReadinessFixture]("root")}
}

func (s *visibilityQueueTaskExecutorSuite) TestChasmVisibilityReadinessRetriesAcrossReload() {
	s.NoError(s.mockShard.ChasmRegistry().Register(&visibilityReadinessLibrary{}))
	kind, ok := s.mockShard.ChasmRegistry().ComponentIDFor(&visibilityReadinessFixture{})
	s.True(ok)
	key := definition.NewWorkflowKey(s.namespaceID.String(), "pending-projection", uuid.NewString())
	state := s.buildChasmMutableState(key, 5)
	state.ChasmNodes[""].Metadata.GetComponentAttributes().TypeId = kind
	task := s.buildChasmVisTask(key, 5)
	task.Info.ArchetypeId = kind
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: state}, nil)
	// No visibility manager expectation: an incomplete projection cannot create a row.
	s.ErrorIs(s.execute(task), consts.ErrDependencyTaskNotCompleted)
	wcache.ClearMutableState(s.workflowCache, wcache.Key{WorkflowKey: key, ArchetypeID: chasm.ArchetypeID(kind), ShardUUID: s.mockShard.GetOwner()})
	ready := proto.Clone(state).(*persistencespb.WorkflowMutableState)
	ready.ChasmNodes[""].Data = newTestComponentStateBlob(&persistencespb.ActivityInfo{ActivityId: key.RunID})
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: ready}, nil)
	s.mockVisibilityMgr.EXPECT().UpsertWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *manager.UpsertWorkflowExecutionRequest) error {
		s.NotNil(request.Memo.Fields[chasm.ChasmMemoKey])
		return nil
	})
	// The same durable task remains sufficient even if no newer task is delivered.
	s.NoError(s.execute(task))
	stale := s.buildChasmVisTask(key, 4)
	stale.Info.ArchetypeId = kind
	s.NoError(s.execute(stale))
}
func (s *visibilityQueueTaskExecutorSuite) TestChasmVisibilityReadinessNeverBlocksClosedExport() {
	s.NoError(s.mockShard.ChasmRegistry().Register(&visibilityReadinessLibrary{}))
	kind, ok := s.mockShard.ChasmRegistry().ComponentIDFor(&visibilityReadinessFixture{})
	s.True(ok)
	key := definition.NewWorkflowKey(s.namespaceID.String(), "closed-before-projection", uuid.NewString())
	state := s.buildChasmMutableState(key, 5)
	state.ChasmNodes[""].Metadata.GetComponentAttributes().TypeId = kind
	state.ExecutionInfo.CloseTime = timestamppb.New(s.now.Add(time.Minute))
	state.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	state.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	task := s.buildChasmVisTask(key, 5)
	task.Info.ArchetypeId = kind
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: state}, nil)
	s.mockVisibilityMgr.EXPECT().RecordWorkflowExecutionClosed(gomock.Any(), gomock.Any()).Return(nil)
	s.NoError(s.execute(task))
}
