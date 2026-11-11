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
	"go.temporal.io/server/chasm/lib/scheduler2/scheduler2test"
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
	s.scheduler2DeleteRecreateFences("start")
}

func (s *chasmEngineSuite) TestScheduler2DeleteRecreateFencesInFlightEffect() {
	s.scheduler2DeleteRecreateFences("effect")
}

func (s *chasmEngineSuite) scheduler2DeleteRecreateFences(kind string) {
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	entered := make(chan *workflowservice.StartWorkflowExecutionRequest, 1)
	effectEntered := make(chan *workflowservice.RequestCancelWorkflowExecutionRequest, 1)
	release := make(chan struct{})
	starter := scheduler2.NewStartHandler(func(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		if kind == "effect" {
			return "old-chain", nil
		}
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
	effects := scheduler2.NewEffectHandler(func(ctx context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) error {
		select {
		case effectEntered <- request:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, nil)
	s.Require().NoError(s.registry.Register(scheduler2.NewLibrary(starter, effects)))
	archetype, ok := s.registry.ComponentIDFor(&scheduler2.Scheduler{})
	s.Require().True(ok)
	records := make(map[string]*persistencespb.WorkflowMutableState)
	current := ""
	var published []tasks.Task
	failDelete := false
	deleteRejected := false
	// Shard handleWriteErrorLocked classifies this as definitely not committed.
	// An arbitrary error instead requires shard reacquisition because commit outcome
	// is unknown, which is a different failure protocol from this rollback fixture.
	deleteFailure := &persistence.InvalidPersistenceRequestError{Msg: "injected delete persistence rejection"}
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
		if failDelete && m.ExecutionState.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED {
			deleteRejected = true
			return nil, deleteFailure
		}
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
			return scheduler2.NewExecution(cc, 10)
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
	// Configure/exhaust the empty source through real engine transactions. Capture
	// an actual idle obligation before ingress revokes it.
	configure := func(ref chasm.ComponentRef) {
		_, err := s.engine.UpdateComponent(ctx, ref, func(cc chasm.MutableContext, c chasm.Component) error {
			root := c.(*scheduler2.Scheduler)
			if err := root.ConfigureGeneration(cc, scheduler2.GenerationConfig{Spec: &schedulepb.ScheduleSpec{}, Revision: 1, After: cc.Now(root), Policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, Request: &workflowservice.StartWorkflowExecutionRequest{WorkflowId: "child"}}); err != nil {
				return err
			}
			return root.ConfigureIdle(cc, time.Minute)
		})
		s.Require().NoError(err)
		_, err = s.engine.UpdateComponent(ctx, ref, func(cc chasm.MutableContext, c chasm.Component) error {
			root := c.(*scheduler2.Scheduler)
			work := root.Coordination
			return (&scheduler2.WorkHandler{}).Execute(cc, root, chasm.TaskAttributes{ScheduledTime: time.Unix(0, work.At)}, &scheduler2pb.WorkTask{Token: work.Token})
		})
		s.Require().NoError(err)
	}
	configure(aRef)
	var oldIdleAt int64
	var oldIdleToken uint64
	s.Require().NoError(s.engine.ReadComponent(ctx, aRef, func(_ chasm.Context, c chasm.Component) error {
		root := c.(*scheduler2.Scheduler)
		s.Require().True(root.IdleArmed)
		oldIdleAt, oldIdleToken = root.IdleAt, root.IdleToken
		return nil
	}))
	_, err = s.engine.UpdateComponent(ctx, aRef, func(cc chasm.MutableContext, c chasm.Component) error {
		return c.(*scheduler2.Scheduler).AcceptManual(cc, &schedulepb.TriggerImmediatelyRequest{})
	})
	s.Require().NoError(err)
	var oldWorkAt int64
	var oldWorkToken uint64
	s.Require().NoError(s.engine.ReadComponent(ctx, aRef, func(_ chasm.Context, c chasm.Component) error {
		root := c.(*scheduler2.Scheduler)
		oldWorkAt, oldWorkToken = root.Coordination.At, root.Coordination.Token
		return nil
	}))
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
	var effectID string
	var effectAt int64
	if kind == "effect" {
		// Bind the tracked chain with the real starter, then accept CANCEL_OTHER.
		s.Require().NoError(starter.Execute(engineCtx, aRef, chasm.TaskAttributes{}, &scheduler2pb.StartTask{OperationId: opID}))
		_, err = s.engine.UpdateComponent(ctx, aRef, func(cc chasm.MutableContext, c chasm.Component) error {
			return c.(*scheduler2.Scheduler).AcceptManual(cc, &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER})
		})
		s.Require().NoError(err)
		_, err = s.engine.UpdateComponent(ctx, aRef, func(cc chasm.MutableContext, c chasm.Component) error {
			root := c.(*scheduler2.Scheduler)
			work := root.Coordination
			if err := (&scheduler2.WorkHandler{}).Execute(cc, root, chasm.TaskAttributes{ScheduledTime: time.Unix(0, work.At)}, &scheduler2pb.WorkTask{Token: work.Token}); err != nil {
				return err
			}
			s.Require().Len(root.Effects, 1)
			effectID, effectAt = root.Effects[0].Id, root.Effects[0].RetryAt
			return nil
		})
		s.Require().NoError(err)
		go func() {
			result <- effects.Execute(engineCtx, aRef, chasm.TaskAttributes{}, &scheduler2pb.EffectTask{EffectId: effectID})
		}()
		select {
		case request := <-effectEntered:
			s.Require().Equal("old-chain", request.FirstExecutionRunId)
			s.Require().NotEmpty(request.RequestId)
		case <-ctx.Done():
			s.T().Fatal(ctx.Err())
		}
	} else {
		go func() {
			result <- starter.Execute(engineCtx, aRef, chasm.TaskAttributes{}, &scheduler2pb.StartTask{OperationId: opID})
		}()
		select {
		case request := <-entered:
			s.Require().NotEmpty(request.RequestId)
		case <-ctx.Done():
			s.T().Fatal(ctx.Err())
		}
	}
	// Persistence rejects the terminal mutation before applying either state or tasks.
	beforeDelete := common.CloneProto(records[a.ExecutionKey.RunID])
	tasksBeforeDelete := len(published)
	failDelete = true
	err = s.engine.DeleteExecution(ctx, aRef, chasm.DeleteExecutionRequest{TerminateComponentRequest: chasm.TerminateComponentRequest{Reason: "injected failed delete"}})
	s.Require().Error(err)
	s.Require().True(deleteRejected, "failure must reach persistence after preparing close")
	s.ProtoEqual(beforeDelete, records[a.ExecutionKey.RunID])
	s.Require().Len(published, tasksBeforeDelete, "failed delete must publish no deletion or scheduler tasks")
	failDelete = false
	reload(a.ExecutionKey)
	s.Require().NoError(s.engine.ReadComponent(ctx, aRef, func(cc chasm.Context, c chasm.Component) error {
		root := c.(*scheduler2.Scheduler)
		s.Require().False(root.Closed)
		s.Require().Equal(chasm.LifecycleStateRunning, root.LifecycleState(cc))
		if kind == "effect" {
			valid, err := effects.Validate(cc, root, chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: time.Unix(0, effectAt)}}, &scheduler2pb.EffectTask{EffectId: effectID})
			s.Require().NoError(err)
			s.Require().True(valid)
		} else {
			valid, err := starter.Validate(cc, root, chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: time.Unix(0, startAt)}}, &scheduler2pb.StartTask{OperationId: opID})
			s.Require().NoError(err)
			s.Require().True(valid)
		}
		return nil
	}))
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
	bRef, err := chasm.DeserializeComponentRef(b.ExecutionRef)
	s.Require().NoError(err)
	configure(bRef)
	// Numeric idle tokens collide naturally in freshly allocated runs. Dispatch
	// old task payload through A's exact ref, never B's current-run ref.
	s.Require().NoError(s.engine.ReadComponent(ctx, bRef, func(_ chasm.Context, c chasm.Component) error {
		root := c.(*scheduler2.Scheduler)
		s.Require().True(root.IdleArmed)
		s.Require().Equal(oldIdleToken, root.IdleToken)
		return nil
	}))
	beforeIdleB := common.CloneProto(records[b.ExecutionKey.RunID])
	_, err = s.engine.UpdateComponent(ctx, aRef, func(cc chasm.MutableContext, c chasm.Component) error {
		return (&scheduler2.IdleHandler{}).Execute(cc, c.(*scheduler2.Scheduler), chasm.TaskAttributes{ScheduledTime: time.Unix(0, oldIdleAt)}, &scheduler2pb.IdleTask{Token: oldIdleToken})
	})
	s.Require().NoError(err)
	s.ProtoEqual(beforeIdleB, records[b.ExecutionKey.RunID])
	_, err = s.engine.UpdateComponent(ctx, bRef, func(cc chasm.MutableContext, c chasm.Component) error {
		return c.(*scheduler2.Scheduler).AcceptManual(cc, &schedulepb.TriggerImmediatelyRequest{})
	})
	s.Require().NoError(err)
	s.Require().NoError(s.engine.ReadComponent(ctx, bRef, func(_ chasm.Context, c chasm.Component) error {
		root := c.(*scheduler2.Scheduler)
		s.Require().True(root.Coordination.Armed)
		s.Require().Equal(oldWorkToken, root.Coordination.Token)
		return nil
	}))
	beforeWorkB := common.CloneProto(records[b.ExecutionKey.RunID])
	_, err = s.engine.UpdateComponent(ctx, aRef, func(cc chasm.MutableContext, c chasm.Component) error {
		return (&scheduler2.WorkHandler{}).Execute(cc, c.(*scheduler2.Scheduler), chasm.TaskAttributes{ScheduledTime: time.Unix(0, oldWorkAt)}, &scheduler2pb.WorkTask{Token: oldWorkToken})
	})
	s.Require().NoError(err)
	s.ProtoEqual(beforeWorkB, records[b.ExecutionKey.RunID])
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
	if kind == "effect" {
		s.Require().NoError(s.engine.ReadComponent(ctx, aRef, func(cc chasm.Context, c chasm.Component) error {
			valid, err := effects.Validate(cc, c.(*scheduler2.Scheduler), chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: time.Unix(0, effectAt)}}, &scheduler2pb.EffectTask{EffectId: effectID})
			s.NoError(err)
			s.False(valid)
			return nil
		}))
		s.Require().NoError(effects.Execute(engineCtx, aRef, chasm.TaskAttributes{}, &scheduler2pb.EffectTask{EffectId: effectID}))
		s.Empty(effectEntered, "closed effect snapshot must not issue another RPC")
	}
	// Existing internal result events are routed through the real engine to A.
	// This test-only bridge makes no public callback/authentication claim.
	_, err = s.engine.UpdateComponent(ctx, aRef, func(cc chasm.MutableContext, c chasm.Component) error {
		return scheduler2test.ApplyLifecycleResults(cc, c.(*scheduler2.Scheduler), a.ExecutionKey.RunID, opID, "old-chain")
	})
	s.Require().NoError(err)
	s.ProtoEqual(beforeB, records[b.ExecutionKey.RunID])
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
