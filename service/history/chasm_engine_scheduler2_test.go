package history

import (
	"context"
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

// Real ChasmEngine allocation, cache reload and persistence requests are exercised;
// the persistence manager remains an in-memory test double, not a database.
func (s *chasmEngineSuite) TestScheduler2DeleteRecreateFencesInFlightStart() {
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	entered := make(chan *workflowservice.StartWorkflowExecutionRequest, 1)
	release := make(chan struct{})
	starter := scheduler2.NewStartHandler(func(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		select {
		case entered <- request:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		select {
		case <-release:
			return "old-chain", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	s.Require().NoError(s.registry.Register(scheduler2.NewLibrary(starter, nil)))
	archetype, ok := s.registry.ComponentIDFor(&scheduler2.Scheduler{})
	s.Require().True(ok)
	records := make(map[string]*persistencespb.WorkflowMutableState)
	current := ""
	var published []tasks.Task
	s.mockExecutionManager.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, r *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
		if current != "" && r.Mode == persistence.CreateWorkflowModeBrandNew {
			old := records[current]
			return nil, &persistence.CurrentWorkflowConditionFailedError{RunID: current, State: old.ExecutionState.State, Status: old.ExecutionState.Status, LastWriteVersion: tests.Version}
		}
		if current != "" && r.PreviousRunID != current {
			return nil, errors.New("recreate did not condition on previous run")
		}
		snap := r.NewWorkflowSnapshot
		record := &persistencespb.WorkflowMutableState{ExecutionInfo: snap.ExecutionInfo, ExecutionState: snap.ExecutionState, ChasmNodes: snap.ChasmNodes, NextEventId: snap.NextEventID}
		current = snap.ExecutionState.RunId
		records[current] = common.CloneProto(record)
		for _, batch := range snap.Tasks {
			published = append(published, batch...)
		}
		return tests.CreateWorkflowExecutionResponse, nil
	}).AnyTimes()
	s.mockExecutionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, r *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
		record, found := records[r.RunID]
		if !found {
			return nil, serviceerror.NewNotFound("execution removed")
		}
		return &persistence.GetWorkflowExecutionResponse{State: common.CloneProto(record)}, nil
	}).AnyTimes()
	s.mockExecutionManager.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, r *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		m := r.UpdateWorkflowMutation
		record, found := records[m.ExecutionState.RunId]
		if !found {
			return nil, serviceerror.NewNotFound("execution removed")
		}
		record.ExecutionInfo = common.CloneProto(m.ExecutionInfo)
		record.ExecutionState = common.CloneProto(m.ExecutionState)
		record.NextEventId = m.NextEventID
		for path := range m.DeleteChasmNodes {
			delete(record.ChasmNodes, path)
		}
		for path, node := range m.UpsertChasmNodes {
			record.ChasmNodes[path] = common.CloneProto(node)
		}
		for _, batch := range m.Tasks {
			published = append(published, batch...)
		}
		return tests.UpdateWorkflowExecutionResponse, nil
	}).AnyTimes()
	s.mockEngine.EXPECT().NotifyChasmExecution(gomock.Any(), gomock.Any()).AnyTimes()
	key := chasm.ExecutionKey{NamespaceID: string(tests.NamespaceID), BusinessID: tv.WorkflowID()}
	create := func() chasm.StartExecutionResult {
		result, err := s.engine.StartExecution(ctx, chasm.NewComponentRef[*scheduler2.Scheduler](key), func(cc chasm.MutableContext) (chasm.RootComponent, error) {
			root := scheduler2.New(cc.ExecutionKey().RunID, 10)
			return root, nil
		}, chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyAllowDuplicate, chasm.BusinessIDConflictPolicyFail))
		s.Require().NoError(err)
		s.Require().True(result.Created)
		return result
	}
	a := create()
	aRef, err := chasm.DeserializeComponentRef(a.ExecutionRef)
	s.Require().NoError(err)
	s.Require().NotEmpty(a.ExecutionKey.RunID)
	reload := func(k chasm.ExecutionKey) {
		wcache.ClearMutableState(s.executionCache, wcache.Key{WorkflowKey: definition.NewWorkflowKey(k.NamespaceID, k.BusinessID, k.RunID), ArchetypeID: archetype, ShardUUID: s.mockShard.GetOwner()})
	}
	// Admit through the real coordinator before starting the external call.
	_, err = s.engine.UpdateComponent(ctx, aRef, func(cc chasm.MutableContext, c chasm.Component) error {
		root := c.(*scheduler2.Scheduler)
		if err := root.ConfigureGeneration(cc, scheduler2.GenerationConfig{Spec: &schedulepb.ScheduleSpec{}, Revision: 1, After: cc.Now(root), Policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, Request: &workflowservice.StartWorkflowExecutionRequest{WorkflowId: "child"}}); err != nil {
			return err
		}
		return root.AcceptManual(cc, &schedulepb.TriggerImmediatelyRequest{})
	})
	s.Require().NoError(err)
	var opID string
	var startAt int64
	for step := 0; step < 3 && opID == ""; step++ {
		_, err = s.engine.UpdateComponent(ctx, aRef, func(cc chasm.MutableContext, c chasm.Component) error {
			root := c.(*scheduler2.Scheduler)
			work := root.Coordination
			if work != nil && work.Armed {
				if err := (&scheduler2.WorkHandler{}).Execute(cc, root, chasm.TaskAttributes{ScheduledTime: time.Unix(0, work.At)}, &scheduler2pb.WorkTask{Token: work.Token}); err != nil {
					return err
				}
			}
			opID = root.OperationId
			startAt = root.PendingStarts[opID]
			return nil
		})
		s.Require().NoError(err)
	}
	s.Require().NotEmpty(opID)
	engineCtx := chasm.NewEngineContext(ctx, s.engine)
	result := make(chan error, 1)
	go func() {
		result <- starter.Execute(engineCtx, aRef, chasm.TaskAttributes{}, &scheduler2pb.StartTask{OperationId: opID})
	}()
	select {
	case request := <-entered:
		s.Require().NotEmpty(request.RequestId)
	case <-ctx.Done():
		s.T().Fatal(ctx.Err())
	}
	s.Require().NoError(s.engine.DeleteExecution(ctx, aRef, chasm.DeleteExecutionRequest{TerminateComponentRequest: chasm.TerminateComponentRequest{Reason: "recreate fencing regression"}}))
	s.Require().Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, records[a.ExecutionKey.RunID].ExecutionState.State)
	s.Require().Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, records[a.ExecutionKey.RunID].ExecutionState.Status)
	foundDelete := false
	for _, task := range published {
		if _, ok := task.(*tasks.DeleteExecutionTask); ok {
			foundDelete = true
		}
	}
	s.Require().True(foundDelete)
	reload(a.ExecutionKey)
	b := create()
	s.Require().NotEqual(a.ExecutionKey.RunID, b.ExecutionKey.RunID)
	s.Require().Equal(a.ExecutionKey.BusinessID, b.ExecutionKey.BusinessID)
	beforeB := common.CloneProto(records[b.ExecutionKey.RunID])
	publishedBeforeResult := len(published)
	close(release)
	select {
	case err := <-result:
		s.Require().NoError(err)
	case <-ctx.Done():
		s.T().Fatal(ctx.Err())
	}
	s.ProtoEqual(beforeB, records[b.ExecutionKey.RunID])
	s.Require().NoError(s.engine.ReadComponent(ctx, aRef, func(cc chasm.Context, c chasm.Component) error {
		root := c.(*scheduler2.Scheduler)
		s.Equal(a.ExecutionKey.RunID, root.Incarnation)
		s.Equal(chasm.LifecycleStateCompleted, root.LifecycleState(cc))
		valid, err := starter.Validate(cc, root, chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: time.Unix(0, startAt)}}, &scheduler2pb.StartTask{OperationId: opID})
		s.NoError(err)
		s.False(valid)
		return nil
	}))
	s.Require().NoError(starter.Execute(engineCtx, aRef, chasm.TaskAttributes{}, &scheduler2pb.StartTask{OperationId: opID}))
	s.Empty(entered, "closed request snapshot must not issue another RPC")
	for _, task := range published[publishedBeforeResult:] {
		switch task.(type) {
		case *tasks.ChasmTask, *tasks.ChasmTaskPure:
			s.Fail("late result and redelivery published scheduler work")
		default:
		}
	}
	// A late exact-ref ingress cannot redirect to B, even while A is still retained.
	err = scheduler2.Trigger(engineCtx, aRef, "late-trigger", &schedulepb.TriggerImmediatelyRequest{})
	s.Require().ErrorAs(err, new(*serviceerror.NotFound))
	s.ProtoEqual(beforeB, records[b.ExecutionKey.RunID])
	// Model completion of the asynchronous delete task, then force a cache reload.
	reload(a.ExecutionKey)
	delete(records, a.ExecutionKey.RunID)
	err = s.engine.ReadComponent(ctx, aRef, func(chasm.Context, chasm.Component) error { return errors.New("removed execution was read") })
	s.Require().ErrorAs(err, new(*serviceerror.NotFound))
	s.ProtoEqual(beforeB, records[b.ExecutionKey.RunID])
}
