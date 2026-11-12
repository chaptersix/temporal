package tests

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protodiff"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSchedule2DescribeCatchupWindowAfterCreateAndUpdate(t *testing.T) {
	t.Parallel()
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend})

	ctx := schedule2Backend.context(testcontext.For(t))
	sid := testcore.RandomizeStr("sched-catchup-window-desc")
	schedule := &schedulepb.Schedule{
		Spec:     intervalSpec(noOpInterval),
		Action:   startWorkflowAction(s, "catchup-window-wf", "catchup-window-wt"),
		Policies: &schedulepb.SchedulePolicies{},
		State:    &schedulepb.ScheduleState{Paused: true},
	}
	createSchedule(ctx, t, s, sid, schedule)

	describe := func(t *testing.T) time.Duration {
		t.Helper()
		resp, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		require.NoError(t, err)
		return resp.GetSchedule().GetPolicies().GetCatchupWindow().AsDuration()
	}
	require.Equal(t, chasmscheduler.DefaultTweakables.DefaultCatchupWindow, describe(t))

	updates := []struct {
		name     string
		window   *durationpb.Duration
		expected time.Duration
	}{
		{name: "unset", expected: chasmscheduler.DefaultTweakables.DefaultCatchupWindow},
		{name: "zero", window: durationpb.New(0), expected: chasmscheduler.DefaultTweakables.DefaultCatchupWindow},
		{name: "negative", window: durationpb.New(-time.Second), expected: chasmscheduler.DefaultTweakables.DefaultCatchupWindow},
		{name: "below minimum", window: durationpb.New(time.Second), expected: chasmscheduler.DefaultTweakables.MinCatchupWindow},
		{name: "above minimum", window: durationpb.New(time.Hour), expected: time.Hour},
	}
	for _, tc := range updates {
		t.Run(tc.name, func(t *testing.T) {
			schedule.Policies.CatchupWindow = tc.window
			_, err := s.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
				Namespace:  s.Namespace().String(),
				ScheduleId: sid,
				Schedule:   schedule,
				Identity:   "test",
				RequestId:  uuid.NewString(),
			})
			require.NoError(t, err)
			require.Equal(t, tc.expected, describe(t))
		})
	}
}

func TestSchedule2BufferOverrunDropsActions(t *testing.T) {
	t.Parallel()
	newContext := schedule2Backend.context
	// A small buffer plus a gated workflow under BUFFER_ALL fills within a few
	// fast-interval ticks. Only the CHASM scheduler reads these tweakables.
	tweakables := chasmscheduler.DefaultTweakables
	tweakables.MaxBufferSize = 2
	opts := []testcore.TestOption{testcore.WithDynamicConfig(chasmscheduler.CurrentTweakables, tweakables)}
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend}, opts...)

	sid := testcore.RandomizeStr("sched-buffer-overrun")
	wid := testcore.RandomizeStr("sched-buffer-overrun-wf")
	wt := testcore.RandomizeStr("sched-buffer-overrun-wt")

	var runs atomic.Int32
	registerGatedWorkflow(s, wt, &runs)

	ctx := newContext(t.Context()) //nolint:staticcheck // SA1019
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(s, wid, wt),
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		},
	})

	// RunningWorkflows lags the drop (it needs the async StartWorkflow to land), so
	// assert both together rather than after the wait.
	await.Requiref(ctx, t, func(at *await.T) {
		desc, err := s.FrontendClient().DescribeSchedule(at.Context(), &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		require.NoError(at, err)
		require.Positive(at, desc.GetInfo().GetBufferDropped())
		require.GreaterOrEqual(at, len(desc.GetInfo().GetRunningWorkflows()), 1)
	}, awaitTimeout, pollInterval, "expected BufferDropped > 0 with a gated workflow holding the buffer full")

	require.Positive(t, runs.Load(), "the gated workflow should have started")

	completeRunningWorkflows(ctx, t, s, sid)
}

func TestSchedule2ScheduleLastCompletionResultMatchesV1(t *testing.T) {
	// Must precede env creation, which otherwise fixes the default timeout.
	ctx := testcontext.For(t, testcontext.WithTimeout(3*time.Minute))
	s := newScheduleEnv(t, []scheduleBackend{scheduleV1Backend, schedule2Backend})

	type sched struct {
		name, scheduleID, workflowID string
		newContext                   contextFactory
	}
	scheds := []sched{
		{"v1", testcore.RandomizeStr("lcr-v1"), testcore.RandomizeStr("lcr-v1-wf"), v1ContextFactory},
		{"v2", testcore.RandomizeStr("lcr-v2"), testcore.RandomizeStr("lcr-v2-wf"), schedule2Backend.context},
	}
	wt := testcore.RandomizeStr("lcr-wt")

	const (
		value    = "value"
		noResult = "no-result"
		fail     = "fail"
		null     = "null"
	)
	// Cancel/terminate/timeout are excluded: V2 diverges on ContinuedFailure there (separate fix).
	steps := []string{value, noResult, fail, null, fail, noResult, value, null, noResult, noResult}

	type step struct {
		Index   int
		Outcome string
	}
	var current sync.Map // workflow ID prefix -> step
	s.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) (any, error) {
		var st step
		_ = workflow.SideEffect(ctx, func(workflow.Context) any {
			id := workflow.GetInfo(ctx).WorkflowExecution.ID
			for _, sc := range scheds {
				if strings.HasPrefix(id, sc.workflowID) {
					v, _ := current.Load(sc.workflowID)
					return v
				}
			}
			return step{Outcome: value}
		}).Get(&st)
		switch st.Outcome {
		case noResult:
			return nil, nil
		case null:
			// Typed nil through `any` is encoded as binary/null.
			return (*string)(nil), nil
		case fail:
			return nil, temporal.NewNonRetryableApplicationError("scripted failure", "lcr", nil)
		default:
			// Deterministic so V1 and V2 results compare equal.
			result := fmt.Sprintf("result-%d", st.Index)
			return &result, nil
		}
	}, workflow.RegisterOptions{Name: wt})

	for _, sc := range scheds {
		_, err := s.FrontendClient().CreateSchedule(sc.newContext(ctx), &workflowservice.CreateScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sc.scheduleID,
			Schedule: &schedulepb.Schedule{
				Spec:     &schedulepb.ScheduleSpec{},
				Action:   startWorkflowAction(s, sc.workflowID, wt),
				Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP},
				State:    &schedulepb.ScheduleState{Paused: true},
			},
			Identity:  "test",
			RequestId: uuid.NewString(),
		})
		require.NoError(t, err, sc.name)
	}

	runEvents := func(sc sched, i int) (*historypb.WorkflowExecutionStartedEventAttributes, enumspb.EventType) {
		// Generated workflow IDs have second resolution; nothing to await, so sleep to the next boundary.
		time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second))) //nolint:forbidigo
		_, err := s.FrontendClient().PatchSchedule(sc.newContext(ctx), &workflowservice.PatchScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sc.scheduleID,
			Patch:      triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP),
			Identity:   "test",
			RequestId:  uuid.NewString(),
		})
		require.NoError(t, err, sc.name)

		var execution *commonpb.WorkflowExecution
		await.Requiref(ctx, t, func(at *await.T) {
			desc, err := s.FrontendClient().DescribeSchedule(sc.newContext(ctx), &workflowservice.DescribeScheduleRequest{
				Namespace:  s.Namespace().String(),
				ScheduleId: sc.scheduleID,
			})
			require.NoError(at, err)
			actions := desc.GetInfo().GetRecentActions()
			require.Len(at, actions, i+1)
			execution = actions[i].GetStartWorkflowResult()
			wf, err := s.SdkClient().DescribeWorkflowExecution(at.Context(), execution.GetWorkflowId(), execution.GetRunId())
			require.NoError(at, err)
			require.NotEqual(at, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, wf.GetWorkflowExecutionInfo().GetStatus())
		}, 20*time.Second, 100*time.Millisecond, "%s: action %d never started and closed", sc.name, i)

		var events []*historypb.HistoryEvent
		iter := s.SdkClient().GetWorkflowHistory(ctx, execution.GetWorkflowId(), execution.GetRunId(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			event, err := iter.Next()
			require.NoError(t, err)
			events = append(events, event)
		}
		require.NotEmpty(t, events)

		started := proto.CloneOf(events[0].GetWorkflowExecutionStartedEventAttributes())
		// Per-execution identity and wall-clock fields.
		started.WorkflowId = ""
		started.OriginalExecutionRunId = ""
		started.FirstExecutionRunId = ""
		started.WorkflowExecutionExpirationTime = nil
		started.RootWorkflowExecution = nil
		// V1 starts from the scheduler workflow's worker, V2 from history with a completion callback.
		started.Identity = ""
		started.CompletionCallbacks = nil
		for _, name := range []string{sadefs.TemporalScheduledById, sadefs.TemporalScheduledStartTime} {
			if _, ok := started.GetSearchAttributes().GetIndexedFields()[name]; ok {
				started.SearchAttributes.IndexedFields[name] = &commonpb.Payload{}
			}
		}
		return started, events[len(events)-1].GetEventType()
	}

	v2ResultCleared, v2FailureCleared := false, false
	for i, outcome := range steps {
		started := make([]*historypb.WorkflowExecutionStartedEventAttributes, len(scheds))
		closed := make([]enumspb.EventType, len(scheds))
		for j, sc := range scheds {
			current.Store(sc.workflowID, step{Index: i, Outcome: outcome})
			started[j], closed[j] = runEvents(sc, i)
		}
		if v2ResultCleared {
			started[0].LastCompletionResult = nil
		}
		if v2FailureCleared {
			started[0].ContinuedFailure = nil
		}
		switch outcome {
		case value, null:
			v2ResultCleared, v2FailureCleared = false, false
		case noResult:
			v2ResultCleared, v2FailureCleared = true, true
		default: // fail
			v2FailureCleared = false
		}
		require.Empty(t, protodiff.Diff(started[0], started[1]), "step %d (%s): started event differs (-v1 +v2)", i, outcome)
		require.Equal(t, closed[0], closed[1], "step %d (%s): close event differs (v1, v2)", i, outcome)
	}
}
