package scheduler_test

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

type differentialActionKind int

const (
	differentialAdvance differentialActionKind = iota
	differentialPause
	differentialUnpause
	differentialTrigger
	differentialUpdate
	differentialBackfill
	differentialComplete
	differentialListMatching
)

type differentialAction struct {
	kind    differentialActionKind
	elapsed time.Duration
	ordinal int
}

type differentialStart struct {
	WorkflowID   string
	ScheduleTime time.Time
}

type differentialRecentAction struct {
	StartOrdinal int
	ScheduleTime time.Time
	Status       enumspb.WorkflowExecutionStatus
}

type differentialSnapshot struct {
	Starts              []differentialStart
	Cancels             []int
	Terminates          []int
	Running             []int
	RecentActions       []differentialRecentAction
	ActionCount         int64
	MissedCatchupWindow int64
	OverlapSkipped      int64
	BufferDropped       int64
	BufferSize          int64
	LimitedActions      bool
	RemainingActions    int64
	Paused              bool
	Notes               string
	MatchingTimes       []time.Time
}

type differentialStartRecord struct {
	request *workflowservice.StartWorkflowExecutionRequest
	runID   string
	nominal time.Time
	index   int
}

type differentialV1Run struct {
	status    enumspb.WorkflowExecutionStatus
	closeTime time.Time
}

type differentialV1Services struct {
	mu         sync.Mutex
	env        *testsuite.TestWorkflowEnvironment
	starts     []differentialStartRecord
	runs       map[string]*differentialV1Run
	cancels    []string
	terminates []string
	nextRun    int
	err        error
}

func TestSchedulerDifferentialModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		trace := drawDifferentialTrace(t)
		chasmSnapshots := runCHASMDifferentialTrace(t, trace)
		v1Snapshots, err := runV1DifferentialTrace(trace)
		mustNoError(t, err)
		if len(chasmSnapshots) != len(v1Snapshots) {
			t.Fatalf("snapshot count: CHASM=%d V1=%d", len(chasmSnapshots), len(v1Snapshots))
		}
		for i := range chasmSnapshots {
			if !reflect.DeepEqual(chasmSnapshots[i], v1Snapshots[i]) {
				t.Fatalf("action %d (%v) differs:\nCHASM: %+v\nV1:    %+v",
					i, trace[i].kind, chasmSnapshots[i], v1Snapshots[i])
			}
		}
	})
}

func drawDifferentialTrace(t *rapid.T) []differentialAction {
	t.Helper()
	kinds := []differentialActionKind{
		differentialPause,
		differentialUnpause,
		differentialTrigger,
		differentialUpdate,
		differentialBackfill,
		differentialComplete,
		differentialListMatching,
		differentialAdvance,
	}
	for i := len(kinds) - 1; i > 0; i-- {
		j := rapid.IntRange(0, i).Draw(t, fmt.Sprintf("operation swap %d", i))
		kinds[i], kinds[j] = kinds[j], kinds[i]
	}

	trace := []differentialAction{{
		kind: differentialAdvance, elapsed: 5*time.Minute + time.Second,
	}}
	advances := []time.Duration{61 * time.Second, 121 * time.Second, 181 * time.Second}
	for i, kind := range kinds {
		trace = append(trace, differentialAction{
			kind: kind, elapsed: advances[rapid.IntRange(0, len(advances)-1).Draw(t, fmt.Sprintf("advance %d", i))],
			ordinal: i + 1,
		})
	}
	return trace
}

func runCHASMDifferentialTrace(t *rapid.T, trace []differentialAction) []differentialSnapshot {
	t.Helper()
	config := defaultModelEnvConfig()
	config.interval = 5 * time.Minute
	config.overlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	config.maxBufferSize = 1000
	config.generatorReserve = 0
	config.maxActions = 1000
	env := newSchedulerModelEnv(t, config)
	workflowBase := "scheduled-wf"
	var matching []time.Time
	result := make([]differentialSnapshot, 0, len(trace))

	for _, action := range trace {
		env.timeSource.Update(env.timeSource.Now().Add(action.elapsed))
		env.drain(t)

		switch action.kind {
		case differentialAdvance:
		case differentialPause, differentialUnpause:
			patch := differentialPausePatch(action)
			_, err := env.handler.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{
				NamespaceId: namespaceID,
				FrontendRequest: &workflowservice.PatchScheduleRequest{
					Namespace: namespace, ScheduleId: scheduleID, Patch: patch,
				},
			})
			mustNoError(t, err)
		case differentialTrigger:
			_, err := env.handler.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{
				NamespaceId: namespaceID,
				FrontendRequest: &workflowservice.PatchScheduleRequest{
					Namespace: namespace, ScheduleId: scheduleID,
					Patch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
						ScheduledTime: timestamppb.New(env.timeSource.Now()),
					}},
				},
			})
			mustNoError(t, err)
		case differentialUpdate:
			description := env.describe(t)
			workflowBase = fmt.Sprintf("updated-wf-%d", action.ordinal)
			_, err := env.handler.UpdateSchedule(env.engineCtx, &schedulerpb.UpdateScheduleRequest{
				NamespaceId: namespaceID,
				FrontendRequest: &workflowservice.UpdateScheduleRequest{
					Namespace: namespace, ScheduleId: scheduleID,
					ConflictToken: description.GetConflictToken(),
					Schedule: differentialSchedule(
						workflowBase,
						proto.CloneOf(description.GetSchedule().GetState()),
					),
				},
			})
			mustNoError(t, err)
		case differentialBackfill:
			now := env.timeSource.Now()
			_, err := env.handler.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{
				NamespaceId: namespaceID,
				FrontendRequest: &workflowservice.PatchScheduleRequest{
					Namespace: namespace, ScheduleId: scheduleID,
					Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
						StartTime: timestamppb.New(now.Add(-10 * time.Minute)),
						EndTime:   timestamppb.New(now), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
					}}},
				},
			})
			mustNoError(t, err)
		case differentialComplete:
			completeOldestCHASMRun(t, env)
		case differentialListMatching:
			var err error
			matching, err = env.listMatching(t, env.timeSource.Now(), env.timeSource.Now().Add(15*time.Minute))
			mustNoError(t, err)
		default:
			t.Fatalf("unknown differential action %d", action.kind)
		}

		env.drain(t)
		env.timeSource.Update(env.timeSource.Now().Add(time.Second))
		env.drain(t)
		snapshot, err := snapshotCHASMScheduler(t, env, matching)
		mustNoError(t, err)
		result = append(result, snapshot)
	}
	return result
}

func completeOldestCHASMRun(t *rapid.T, env *schedulerModelEnv) {
	t.Helper()
	running := env.runningRequestIDs(t)
	if len(running) == 0 {
		return
	}
	runningSet := make(map[string]struct{}, len(running))
	for _, requestID := range running {
		runningSet[requestID] = struct{}{}
	}
	candidates := make([]modelBufferedStart, 0, len(running))
	for _, start := range env.internal(t).buffered {
		if _, ok := runningSet[start.requestID]; ok {
			candidates = append(candidates, start)
		}
	}
	slices.SortFunc(candidates, func(a, b modelBufferedStart) int {
		if order := a.nominalTime.Compare(b.nominalTime); order != 0 {
			return order
		}
		return compareString(a.requestID, b.requestID)
	})
	env.complete(t, candidates[0].requestID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, []byte("completed"))
}

func snapshotCHASMScheduler(
	t *rapid.T,
	env *schedulerModelEnv,
	matching []time.Time,
) (differentialSnapshot, error) {
	description := env.describe(t)
	workflowSnapshot := env.workflows.snapshot()
	records := make([]differentialStartRecord, 0, len(workflowSnapshot.startCalls))
	for i, request := range workflowSnapshot.startCalls {
		if request.GetWorkflowIdConflictPolicy() == enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING {
			continue
		}
		nominal, err := differentialNominalTime(request)
		if err != nil {
			return differentialSnapshot{}, err
		}
		records = append(records, differentialStartRecord{
			request: request, runID: modelRunID(request.GetRequestId()), nominal: nominal, index: i,
		})
	}
	history := env.history.snapshot()
	cancels := make([]string, len(history.cancels))
	for i, request := range history.cancels {
		cancels[i] = request.GetCancelRequest().GetFirstExecutionRunId()
	}
	terminates := make([]string, len(history.terminates))
	for i, request := range history.terminates {
		terminates[i] = request.GetTerminateRequest().GetFirstExecutionRunId()
	}
	return normalizeDifferentialSnapshot(
		description.GetSchedule(), description.GetInfo(), records, cancels, terminates, matching,
	)
}

func runV1DifferentialTrace(trace []differentialAction) ([]differentialSnapshot, error) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(modelStartTime)
	services := &differentialV1Services{
		env: env, runs: make(map[string]*differentialV1Run),
	}
	env.RegisterActivityWithOptions(services.startWorkflow, activity.RegisterOptions{Name: "StartWorkflow"})
	env.RegisterActivityWithOptions(services.watchWorkflow, activity.RegisterOptions{Name: "WatchWorkflow"})
	env.RegisterActivityWithOptions(services.cancelWorkflow, activity.RegisterOptions{Name: "CancelWorkflow"})
	env.RegisterActivityWithOptions(services.terminateWorkflow, activity.RegisterOptions{Name: "TerminateWorkflow"})

	previousTweakables := legacyscheduler.CurrentTweakablePolicies
	defer func() { legacyscheduler.CurrentTweakablePolicies = previousTweakables }()
	legacyscheduler.CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 100000
	legacyscheduler.CurrentTweakablePolicies.EnableCHASMMigration = false

	var callbackErr error
	recordError := func(err error) {
		if err != nil && callbackErr == nil {
			callbackErr = err
		}
	}
	workflowBase := "scheduled-wf"
	var matching []time.Time
	result := make([]differentialSnapshot, 0, len(trace))
	var elapsed time.Duration
	for i, action := range trace {
		elapsed += action.elapsed
		env.RegisterDelayedCallback(func() {
			switch action.kind {
			case differentialAdvance:
			case differentialPause, differentialUnpause:
				env.SignalWorkflow(legacyscheduler.SignalNamePatch, differentialPausePatch(action))
			case differentialTrigger:
				env.SignalWorkflow(legacyscheduler.SignalNamePatch, &schedulepb.SchedulePatch{
					TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
						ScheduledTime: timestamppb.New(env.Now().UTC()),
					},
				})
			case differentialUpdate:
				description, err := queryV1Description(env)
				if err != nil {
					recordError(err)
					return
				}
				workflowBase = fmt.Sprintf("updated-wf-%d", action.ordinal)
				env.SignalWorkflow(legacyscheduler.SignalNameUpdate, &schedulespb.FullUpdateRequest{
					ConflictToken: description.GetConflictToken(),
					Schedule: differentialSchedule(
						workflowBase,
						proto.CloneOf(description.GetSchedule().GetState()),
					),
				})
			case differentialBackfill:
				now := env.Now().UTC()
				env.SignalWorkflow(legacyscheduler.SignalNamePatch, &schedulepb.SchedulePatch{
					BackfillRequest: []*schedulepb.BackfillRequest{{
						StartTime: timestamppb.New(now.Add(-10 * time.Minute)),
						EndTime:   timestamppb.New(now), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
					}},
				})
			case differentialComplete:
				services.completeOldest(env.Now().UTC())
				env.SignalWorkflow(legacyscheduler.SignalNameRefresh, nil)
			case differentialListMatching:
				var err error
				matching, err = queryV1MatchingTimes(env, env.Now().UTC(), env.Now().UTC().Add(15*time.Minute))
				recordError(err)
			default:
				recordError(fmt.Errorf("unknown differential action %d", action.kind))
			}
		}, elapsed)

		elapsed += time.Second
		env.RegisterDelayedCallback(func() {
			description, err := queryV1Description(env)
			if err != nil {
				recordError(err)
				return
			}
			snapshot, err := services.snapshot(description, matching)
			if err != nil {
				recordError(err)
				return
			}
			result = append(result, snapshot)
			if i == len(trace)-1 {
				env.SignalWorkflow(legacyscheduler.SignalNameForceCAN, nil)
			}
		}, elapsed)
	}

	env.ExecuteWorkflow(legacyscheduler.SchedulerWorkflow, &schedulespb.StartScheduleArgs{
		Schedule: differentialSchedule("scheduled-wf", &schedulepb.ScheduleState{}),
		State: &schedulespb.InternalState{
			Namespace: namespace, NamespaceId: namespaceID, ScheduleId: scheduleID,
			ConflictToken: legacyscheduler.InitialConflictToken,
		},
	})
	if callbackErr != nil {
		return nil, callbackErr
	}
	services.mu.Lock()
	serviceErr := services.err
	services.mu.Unlock()
	if serviceErr != nil {
		return nil, serviceErr
	}
	if err := env.GetWorkflowError(); err != nil && !workflow.IsContinueAsNewError(err) {
		return nil, err
	}
	return result, nil
}

func differentialPausePatch(action differentialAction) *schedulepb.SchedulePatch {
	note := fmt.Sprintf("unpaused by differential action %d", action.ordinal)
	patch := &schedulepb.SchedulePatch{Unpause: note}
	if action.kind == differentialPause {
		note = fmt.Sprintf("paused by differential action %d", action.ordinal)
		patch = &schedulepb.SchedulePatch{Pause: note}
	}
	return patch
}

func differentialSchedule(workflowID string, state *schedulepb.ScheduleState) *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{
			Interval: durationpb.New(5 * time.Minute),
		}}},
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId: workflowID, WorkflowType: &commonpb.WorkflowType{Name: "scheduled-wf-type"},
				TaskQueue: &taskqueuepb.TaskQueue{Name: "scheduler-model-queue"},
			},
		}},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			CatchupWindow: durationpb.New(24 * time.Hour),
		},
		State: state,
	}
}

func (s *differentialV1Services) startWorkflow(
	_ context.Context,
	request *schedulespb.StartWorkflowRequest,
) (*schedulespb.StartWorkflowResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nominal, err := differentialNominalTime(request.GetRequest())
	if err != nil {
		s.err = err
		return nil, err
	}
	s.nextRun++
	runID := fmt.Sprintf("v1-run-%d", s.nextRun)
	s.starts = append(s.starts, differentialStartRecord{
		request: proto.CloneOf(request.GetRequest()), runID: runID, nominal: nominal, index: len(s.starts),
	})
	s.runs[runID] = &differentialV1Run{status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}
	return &schedulespb.StartWorkflowResponse{
		RunId: runID, RealStartTime: timestamppb.New(s.env.Now().UTC()),
	}, nil
}

func (s *differentialV1Services) watchWorkflow(
	_ context.Context,
	request *schedulespb.WatchWorkflowRequest,
) (*schedulespb.WatchWorkflowResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run := s.runs[request.GetFirstExecutionRunId()]
	if run == nil {
		return &schedulespb.WatchWorkflowResponse{}, nil
	}
	return differentialWatchResponse(run), nil
}

func differentialWatchResponse(run *differentialV1Run) *schedulespb.WatchWorkflowResponse {
	response := &schedulespb.WatchWorkflowResponse{Status: run.status}
	if run.status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		response.CloseTime = timestamppb.New(run.closeTime)
		response.ResultFailure = &schedulespb.WatchWorkflowResponse_Result{
			Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("completed")}}},
		}
	}
	return response
}

func (s *differentialV1Services) cancelWorkflow(
	_ context.Context,
	request *schedulespb.CancelWorkflowRequest,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancels = append(s.cancels, request.GetExecution().GetRunId())
	return nil
}

func (s *differentialV1Services) terminateWorkflow(
	_ context.Context,
	request *schedulespb.TerminateWorkflowRequest,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminates = append(s.terminates, request.GetExecution().GetRunId())
	return nil
}

func (s *differentialV1Services) completeOldest(closeTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidates := make([]differentialStartRecord, 0, len(s.starts))
	for _, start := range s.starts {
		if s.runs[start.runID].status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			candidates = append(candidates, start)
		}
	}
	if len(candidates) == 0 {
		return
	}
	slices.SortFunc(candidates, compareDifferentialStartRecords)
	run := s.runs[candidates[0].runID]
	run.status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	run.closeTime = closeTime
}

func (s *differentialV1Services) snapshot(
	description *schedulespb.DescribeResponse,
	matching []time.Time,
) (differentialSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := slices.Clone(s.starts)
	cancels := slices.Clone(s.cancels)
	terminates := slices.Clone(s.terminates)
	return normalizeDifferentialSnapshot(
		description.GetSchedule(), description.GetInfo(), records, cancels, terminates, matching,
	)
}

func queryV1Description(env *testsuite.TestWorkflowEnvironment) (*schedulespb.DescribeResponse, error) {
	encoded, err := env.QueryWorkflow(legacyscheduler.QueryNameDescribe)
	if err != nil {
		return nil, err
	}
	var result schedulespb.DescribeResponse
	if err := encoded.Get(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func queryV1MatchingTimes(
	env *testsuite.TestWorkflowEnvironment,
	start time.Time,
	end time.Time,
) ([]time.Time, error) {
	encoded, err := env.QueryWorkflow(
		legacyscheduler.QueryNameListMatchingTimes,
		&workflowservice.ListScheduleMatchingTimesRequest{
			StartTime: timestamppb.New(start), EndTime: timestamppb.New(end),
		},
	)
	if err != nil {
		return nil, err
	}
	var response workflowservice.ListScheduleMatchingTimesResponse
	if err := encoded.Get(&response); err != nil {
		return nil, err
	}
	result := make([]time.Time, len(response.GetStartTime()))
	for i, value := range response.GetStartTime() {
		result[i] = value.AsTime()
	}
	return result, nil
}

func normalizeDifferentialSnapshot(
	schedule *schedulepb.Schedule,
	info *schedulepb.ScheduleInfo,
	records []differentialStartRecord,
	cancelRunIDs []string,
	terminateRunIDs []string,
	matching []time.Time,
) (differentialSnapshot, error) {
	// Ordinals retain customer-visible ordering without comparing backend-specific request and run IDs.
	slices.SortFunc(records, compareDifferentialStartRecords)
	ordinalByRunID := make(map[string]int, len(records))
	starts := make([]differentialStart, len(records))
	for i, record := range records {
		ordinalByRunID[record.runID] = i
		starts[i] = differentialStart{
			WorkflowID: record.request.GetWorkflowId(), ScheduleTime: record.nominal,
		}
	}

	running, err := differentialExecutionOrdinals(info.GetRunningWorkflows(), ordinalByRunID)
	if err != nil {
		return differentialSnapshot{}, err
	}
	recent := make([]differentialRecentAction, len(info.GetRecentActions()))
	for i, action := range info.GetRecentActions() {
		ordinal, ok := ordinalByRunID[action.GetStartWorkflowResult().GetRunId()]
		if !ok {
			return differentialSnapshot{}, fmt.Errorf(
				"recent action references unknown run %q", action.GetStartWorkflowResult().GetRunId())
		}
		recent[i] = differentialRecentAction{
			StartOrdinal: ordinal, ScheduleTime: action.GetScheduleTime().AsTime(),
			Status: action.GetStartWorkflowStatus(),
		}
	}
	cancels, err := differentialRunOrdinals(cancelRunIDs, ordinalByRunID)
	if err != nil {
		return differentialSnapshot{}, err
	}
	terminates, err := differentialRunOrdinals(terminateRunIDs, ordinalByRunID)
	if err != nil {
		return differentialSnapshot{}, err
	}

	return differentialSnapshot{
		Starts: starts, Cancels: cancels, Terminates: terminates, Running: running,
		RecentActions: recent, ActionCount: info.GetActionCount(),
		MissedCatchupWindow: info.GetMissedCatchupWindow(), OverlapSkipped: info.GetOverlapSkipped(),
		BufferDropped: info.GetBufferDropped(), BufferSize: info.GetBufferSize(),
		LimitedActions:   schedule.GetState().GetLimitedActions(),
		RemainingActions: schedule.GetState().GetRemainingActions(),
		Paused:           schedule.GetState().GetPaused(), Notes: schedule.GetState().GetNotes(),
		MatchingTimes: slices.Clone(matching),
	}, nil
}

func differentialExecutionOrdinals(
	executions []*commonpb.WorkflowExecution,
	ordinalByRunID map[string]int,
) ([]int, error) {
	runIDs := make([]string, len(executions))
	for i, execution := range executions {
		runIDs[i] = execution.GetRunId()
	}
	return differentialRunOrdinals(runIDs, ordinalByRunID)
}

func differentialRunOrdinals(runIDs []string, ordinalByRunID map[string]int) ([]int, error) {
	result := make([]int, len(runIDs))
	for i, runID := range runIDs {
		ordinal, ok := ordinalByRunID[runID]
		if !ok {
			return nil, fmt.Errorf("service call references unknown run %q", runID)
		}
		result[i] = ordinal
	}
	slices.Sort(result)
	return result, nil
}

func compareDifferentialStartRecords(a, b differentialStartRecord) int {
	if order := a.nominal.Compare(b.nominal); order != 0 {
		return order
	}
	if a.request.GetWorkflowId() != b.request.GetWorkflowId() {
		return compareString(a.request.GetWorkflowId(), b.request.GetWorkflowId())
	}
	return a.index - b.index
}

func differentialNominalTime(request *workflowservice.StartWorkflowExecutionRequest) (time.Time, error) {
	value := request.GetSearchAttributes().GetIndexedFields()[sadefs.TemporalScheduledStartTime]
	if value == nil {
		return time.Time{}, fmt.Errorf("start %q has no scheduled start time", request.GetWorkflowId())
	}
	var result time.Time
	if err := payload.Decode(value, &result); err != nil {
		return time.Time{}, err
	}
	return result, nil
}
