package migration

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var migrationPropertyTime = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

func requireMigrationCounterexamples(t *testing.T) {
	t.Helper()
	if os.Getenv("TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES") == "" {
		t.Skip("set TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1 to run known failing migration repros")
	}
}

func legacyRoundTrip(args *schedulespb.StartScheduleArgs) *schedulespb.StartScheduleArgs {
	state := LegacyToCreateFromMigrationStateRequest(args.Schedule, args.Info, args.State, nil, nil, migrationPropertyTime).State
	return CHASMToLegacyStartScheduleArgs(state.SchedulerState, state.GeneratorState, state.InvokerState,
		state.Backfillers, state.LastCompletionResult, state.SearchAttributes, state.Memo, migrationPropertyTime)
}

func TestMigrationGeneratedRoundTrip(t *testing.T) {
	for seed := range int64(64) {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			checkMigrationRoundTrip(t, seed)
		})
	}
}

func FuzzMigrationRoundTrip(f *testing.F) {
	for _, seed := range []int64{0, 1, 7, 42, 63} {
		f.Add(seed)
	}
	f.Fuzz(checkMigrationRoundTrip)
}

func checkMigrationRoundTrip(t *testing.T, seed int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	schedule := newTestSchedule()
	schedule.Policies.OverlapPolicy = enumspb.ScheduleOverlapPolicy(1 + rng.Intn(6))
	schedule.State = &schedulepb.ScheduleState{
		Paused: rng.Intn(2) == 0, Notes: fmt.Sprintf("seed %d", seed),
		LimitedActions: rng.Intn(2) == 0, RemainingActions: int64(rng.Intn(10)),
	}
	state := &schedulespb.InternalState{
		Namespace: "ns", NamespaceId: "ns-id", ScheduleId: "schedule",
		ConflictToken: 17, LastProcessedTime: timestamppb.New(migrationPropertyTime.Add(-time.Minute)),
		LastCompletionResult: &commonpb.Payloads{Payloads: []*commonpb.Payload{payload.EncodeString("success")}},
		ContinuedFailure:     &failurepb.Failure{Message: "later failure"},
	}
	for i := range rng.Intn(8) {
		when := timestamppb.New(migrationPropertyTime.Add(-time.Duration(i) * time.Second))
		state.BufferedStarts = append(state.BufferedStarts, &schedulespb.BufferedStart{
			NominalTime: when, ActualTime: when, DesiredTime: when,
			Manual: rng.Intn(2) == 0, OverlapPolicy: enumspb.ScheduleOverlapPolicy(rng.Intn(7)),
			RequestId: fmt.Sprintf("request-%d", i), WorkflowId: fmt.Sprintf("workflow-%d", i),
		})
	}
	for i := range rng.Intn(4) {
		state.OngoingBackfills = append(state.OngoingBackfills, &schedulepb.BackfillRequest{
			StartTime:     timestamppb.New(migrationPropertyTime.Add(-time.Duration(i+2) * time.Hour)),
			EndTime:       timestamppb.New(migrationPropertyTime.Add(-time.Hour)),
			OverlapPolicy: enumspb.ScheduleOverlapPolicy(1 + rng.Intn(6)),
		})
	}
	info := &schedulepb.ScheduleInfo{
		ActionCount: 20, OverlapSkipped: 3, MissedCatchupWindow: 2,
		CreateTime: timestamppb.New(migrationPropertyTime.Add(-24 * time.Hour)),
		UpdateTime: timestamppb.New(migrationPropertyTime.Add(-time.Hour)),
	}
	for i := range rng.Intn(4) {
		when := timestamppb.New(migrationPropertyTime.Add(-time.Duration(i+2) * time.Hour))
		info.RecentActions = append(info.RecentActions, &schedulepb.ScheduleActionResult{
			ScheduleTime: when,
			ActualTime:   when,
			StartWorkflowResult: &commonpb.WorkflowExecution{
				WorkflowId: fmt.Sprintf("completed-workflow-%d", i),
				RunId:      fmt.Sprintf("completed-run-%d", i),
			},
			StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		})
	}
	if schedule.Policies.OverlapPolicy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
		running := &commonpb.WorkflowExecution{WorkflowId: "running", RunId: "run"}
		info.RunningWorkflows = []*commonpb.WorkflowExecution{running}
		info.RecentActions = append(info.RecentActions, &schedulepb.ScheduleActionResult{
			ScheduleTime: migrationPropertyTimePB(), ActualTime: migrationPropertyTimePB(),
			StartWorkflowResult: running, StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		})
	}
	custom := map[string]*commonpb.Payload{"CustomKeywordField": payload.EncodeString("custom")}
	attrs := &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
		"CustomKeywordField": custom["CustomKeywordField"], sadefs.TemporalNamespaceDivision: payload.EncodeString("TemporalScheduler"),
	}}
	memo := &commonpb.Memo{Fields: map[string]*commonpb.Payload{"memo": payload.EncodeString("memo")}}
	original := &schedulespb.StartScheduleArgs{Schedule: schedule, Info: info, State: state}
	snapshot := common.CloneProto(original)
	current := original
	for cycle := range 3 {
		imported := LegacyToCreateFromMigrationStateRequest(current.Schedule, current.Info, current.State, attrs, memo, migrationPropertyTime).State
		require.Equal(t, custom, imported.SearchAttributes, "seed=%d cycle=%d", seed, cycle)
		require.Equal(t, memo.Fields, imported.Memo)
		next := CHASMToLegacyStartScheduleArgs(imported.SchedulerState, imported.GeneratorState, imported.InvokerState,
			imported.Backfillers, imported.LastCompletionResult, imported.SearchAttributes, imported.Memo, migrationPropertyTime)
		protorequire.ProtoEqual(t, original.Schedule, next.Schedule)
		protorequire.ProtoEqual(t, state.LastProcessedTime, next.State.LastProcessedTime)
		protorequire.ProtoEqual(t, state.LastCompletionResult, next.State.LastCompletionResult)
		protorequire.ProtoEqual(t, state.ContinuedFailure, next.State.ContinuedFailure)
		require.Equal(t, state.ConflictToken, next.State.ConflictToken)
		require.Equal(t, state.NamespaceId, next.State.NamespaceId)
		require.Equal(t, state.ScheduleId, next.State.ScheduleId)
		require.Equal(t, info.ActionCount, next.Info.ActionCount)
		require.Equal(t, info.OverlapSkipped, next.Info.OverlapSkipped)
		require.Equal(t, info.MissedCatchupWindow, next.Info.MissedCatchupWindow)
		protorequire.ProtoEqual(t, info.CreateTime, next.Info.CreateTime)
		protorequire.ProtoEqual(t, info.UpdateTime, next.Info.UpdateTime)
		require.Equal(t, workflowExecutionKeys(info.RunningWorkflows), workflowExecutionKeys(next.Info.RunningWorkflows))
		require.Equal(t, recentActionKeys(info.RecentActions), recentActionKeys(next.Info.RecentActions))
		require.ElementsMatch(t, state.OngoingBackfills, next.State.OngoingBackfills)
		require.Len(t, next.State.BufferedStarts, len(state.BufferedStarts))
		for i, expected := range state.BufferedStarts {
			normalized := common.CloneProto(expected)
			if normalized.OverlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
				normalized.OverlapPolicy = schedule.Policies.OverlapPolicy
			}
			protorequire.ProtoEqual(t, normalized, next.State.BufferedStarts[i])
		}
		current = next
	}
	protorequire.ProtoEqual(t, snapshot, original)
}

func workflowExecutionKeys(executions []*commonpb.WorkflowExecution) []string {
	keys := make([]string, len(executions))
	for i, execution := range executions {
		keys[i] = execution.GetWorkflowId() + "/" + execution.GetRunId()
	}
	slices.Sort(keys)
	return keys
}

func recentActionKeys(actions []*schedulepb.ScheduleActionResult) []string {
	keys := make([]string, len(actions))
	for i, action := range actions {
		keys[i] = fmt.Sprintf("%s|%s|%s|%d",
			timestampKey(action.GetScheduleTime()),
			timestampKey(action.GetActualTime()),
			workflowExecutionKeys([]*commonpb.WorkflowExecution{action.GetStartWorkflowResult()})[0],
			action.GetStartWorkflowStatus(),
		)
	}
	slices.Sort(keys)
	return keys
}

func timestampKey(timestamp *timestamppb.Timestamp) string {
	if timestamp == nil {
		return ""
	}
	return fmt.Sprintf("%d/%d", timestamp.Seconds, timestamp.Nanos)
}

func migrationPropertyTimePB() *timestamppb.Timestamp {
	return timestamppb.New(migrationPropertyTime)
}

func TestMigrationCounterexample_RecentActionTimes(t *testing.T) {
	requireMigrationCounterexamples(t)
	for _, running := range []bool{false, true} {
		t.Run(fmt.Sprintf("running_%t", running), func(t *testing.T) {
			execution := &commonpb.WorkflowExecution{WorkflowId: "action", RunId: "run"}
			action := &schedulepb.ScheduleActionResult{
				ScheduleTime:        timestamppb.New(migrationPropertyTime.Add(-time.Hour)),
				ActualTime:          timestamppb.New(migrationPropertyTime.Add(-time.Hour + time.Second)),
				StartWorkflowResult: execution, StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			}
			info := &schedulepb.ScheduleInfo{RecentActions: []*schedulepb.ScheduleActionResult{action}}
			if running {
				info.RunningWorkflows = []*commonpb.WorkflowExecution{execution}
				action.StartWorkflowStatus = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			}
			result := legacyRoundTrip(&schedulespb.StartScheduleArgs{
				Schedule: newTestSchedule(), Info: info, State: &schedulespb.InternalState{},
			})
			require.Len(t, result.Info.RecentActions, 1)
			protorequire.ProtoEqual(t, action, result.Info.RecentActions[0])
		})
	}
}

func TestMigrationCounterexample_MultipleCompletionPayloads(t *testing.T) {
	requireMigrationCounterexamples(t)
	result := &commonpb.Payloads{Payloads: []*commonpb.Payload{payload.EncodeString("first"), payload.EncodeString("second")}}
	args := legacyRoundTrip(&schedulespb.StartScheduleArgs{
		Schedule: newTestSchedule(), Info: &schedulepb.ScheduleInfo{},
		State: &schedulespb.InternalState{LastCompletionResult: result},
	})
	protorequire.ProtoEqual(t, result, args.State.LastCompletionResult)
}

func TestMigrationGeneratedRollbackProgress(t *testing.T) {
	for seed := range int64(32) {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			watermark := timestamppb.New(migrationPropertyTime.Add(-time.Duration(1+rng.Intn(59)) * time.Minute))
			backfill := &schedulepb.BackfillRequest{
				StartTime: timestamppb.New(migrationPropertyTime.Add(-time.Hour)), EndTime: timestamppb.New(migrationPropertyTime),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			}
			backfillers := map[string]*schedulerpb.BackfillerState{
				"backfill": {Request: &schedulerpb.BackfillerState_BackfillRequest{BackfillRequest: backfill}, LastProcessedTime: watermark, Attempt: 1},
				"trigger":  {Request: &schedulerpb.BackfillerState_TriggerRequest{TriggerRequest: &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}}, LastProcessedTime: watermark},
			}
			pending := &schedulespb.BufferedStart{
				NominalTime: watermark, ActualTime: watermark, RequestId: "in-flight-request", WorkflowId: "in-flight-workflow",
				Attempt: 2, BackoffTime: timestamppb.New(migrationPropertyTime.Add(time.Second)),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			}
			args := CHASMToLegacyStartScheduleArgs(&schedulerpb.SchedulerState{Schedule: newTestSchedule()},
				&schedulerpb.GeneratorState{LastProcessedTime: watermark}, &schedulerpb.InvokerState{BufferedStarts: []*schedulespb.BufferedStart{pending}},
				backfillers, nil, nil, nil, migrationPropertyTime)
			require.Len(t, args.State.OngoingBackfills, 1)
			protorequire.ProtoEqual(t, watermark, args.State.OngoingBackfills[0].StartTime)
			require.Len(t, args.State.BufferedStarts, 2)
			protorequire.ProtoEqual(t, pending, args.State.BufferedStarts[0])
			require.True(t, args.State.BufferedStarts[1].Manual)
			imported := LegacyToCreateFromMigrationStateRequest(args.Schedule, args.Info, args.State, nil, nil, migrationPropertyTime).State
			require.Len(t, imported.InvokerState.BufferedStarts, 2)
			require.Equal(t, pending.RequestId, imported.InvokerState.BufferedStarts[0].RequestId)
			require.Equal(t, pending.WorkflowId, imported.InvokerState.BufferedStarts[0].WorkflowId)
			for _, b := range imported.Backfillers {
				protorequire.ProtoEqual(t, watermark, b.GetBackfillRequest().StartTime)
			}
		})
	}
}
