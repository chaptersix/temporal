package tests

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSchedule2UpdateScheduleMemo(t *testing.T) {
	t.Parallel()
	newContext := schedule2Backend.context
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend})

	sid := "sched-test-update-memo"
	wid := "sched-test-update-memo-wf"
	wt := "sched-test-update-memo-wt"

	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	memo1 := payload.EncodeString("val1")
	memo2 := payload.EncodeString("val2")

	// Create schedule with initial memo.
	ctx := newContext(t.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{
				"key1": memo1,
				"key2": memo2,
			},
		},
	})
	require.NoError(t, err)

	// Verify initial memo.
	describeResp, err := s.FrontendClient().DescribeSchedule(newContext(t.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Equal(t, memo1.Data, describeResp.Memo.Fields["key1"].Data)
	require.Equal(t, memo2.Data, describeResp.Memo.Fields["key2"].Data)

	// Update: replace memo with only key3 (key1 and key2 should be gone).
	memo3 := payload.EncodeString("new")
	_, err = s.FrontendClient().UpdateSchedule(newContext(t.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{
				"key3": memo3,
			},
		},
	})
	require.NoError(t, err)

	// Verify replaced memo.
	describeResp, err = s.FrontendClient().DescribeSchedule(newContext(t.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Nil(t, describeResp.Memo.Fields["key1"], "key1 should be gone after replace")
	require.Nil(t, describeResp.Memo.Fields["key2"], "key2 should be gone after replace")
	require.Equal(t, memo3.Data, describeResp.Memo.Fields["key3"].Data, "key3 should be set")

	// Update with nil memo (no change).
	_, err = s.FrontendClient().UpdateSchedule(newContext(t.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Verify memo unchanged.
	describeResp, err = s.FrontendClient().DescribeSchedule(newContext(t.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Equal(t, memo3.Data, describeResp.Memo.Fields["key3"].Data, "key3 should be unchanged")

	// Update with empty memo (clear all).
	_, err = s.FrontendClient().UpdateSchedule(newContext(t.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{},
		},
	})
	require.NoError(t, err)

	// Verify memo cleared.
	describeResp, err = s.FrontendClient().DescribeSchedule(newContext(t.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Empty(t, describeResp.Memo.GetFields(), "memo should be empty after replace with empty map")
}

func TestSchedule2LargeScheduleID(t *testing.T) {
	t.Parallel()
	newContext := schedule2Backend.context
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend})
	ctx := newContext(testcore.NewContext())

	// The V1 sentinel shares the SQL workflow ID limit with the schedule ID
	// prefix, so this is the largest schedule ID supported by every SQL backend.
	const workflowIDColumnLimit = 255
	scheduleIDLength := workflowIDColumnLimit - len(scheduler.WorkflowIDPrefix)
	sid := strings.Repeat("a", scheduleIDLength)
	wid := testcore.RandomizeStr("sched-large-id-wf")
	wt := testcore.RandomizeStr("sched-large-id-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(s, wid, wt),
	})

	await.RequireTruef(t, func() bool { return runs.Load() > 0 }, awaitTimeout, pollInterval,
		"schedule ID of length %d should start a workflow", scheduleIDLength)
}

func TestSchedule2ListSchedulesPreservesV2ScheduleIDWithV1Prefix(t *testing.T) {
	t.Parallel()
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend})
	ctx := schedule2Backend.context(testcontext.For(t))

	suffix := testcore.RandomizeStr("sched-list-v2-prefix")
	plainID := "foo-" + suffix
	prefixedID := scheduler.WorkflowIDPrefix + plainID
	for _, scheduleID := range []string{plainID, prefixedID} {
		createSchedule(ctx, t, s, scheduleID, &schedulepb.Schedule{
			Spec:   intervalSpec(noOpInterval),
			State:  &schedulepb.ScheduleState{Paused: true},
			Action: startWorkflowAction(s, "wf-"+scheduleID, "wt-"+scheduleID),
		})
	}

	await.Requiref(ctx, t, func(at *await.T) {
		response, err := s.FrontendClient().ListSchedules(at.Context(), &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace().String(),
			MaximumPageSize: 10,
		})
		require.NoError(at, err)
		ids := make(map[string]struct{}, len(response.Schedules))
		for _, schedule := range response.Schedules {
			ids[schedule.GetScheduleId()] = struct{}{}
		}
		_, foundPlainID := ids[plainID]
		_, foundPrefixedID := ids[prefixedID]
		require.True(at, foundPlainID)
		require.True(at, foundPrefixedID)
	}, awaitTimeout, pollInterval,
		"ListSchedules must preserve both V2 IDs %q and %q", plainID, prefixedID)
}

func TestSchedule2ScheduleNextActionTimeVisibility(t *testing.T) {
	t.Parallel()
	var opts []testcore.TestOption
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend}, opts...)

	v2Sid := testcore.RandomizeStr("sched-next-action-v2")
	wid := testcore.RandomizeStr("sched-next-action-wf")
	wt := testcore.RandomizeStr("sched-next-action-wt")

	// Register a no-op workflow type so a stray fire doesn't generate worker
	// noise. The schedule is never paused: we want its next action time to stay
	// populated so it remains queryable.
	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	mkSchedule := func() *schedulepb.Schedule {
		return &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Hour), Phase: durationpb.New(23 * time.Minute)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   wid,
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
	}

	newContext := schedule2Backend.context
	v2Ctx := newContext(t.Context())
	createTime := time.Now()

	_, err := s.FrontendClient().CreateSchedule(v2Ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: v2Sid,
		Schedule:   mkSchedule(),
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// The CHASM scheduler publishes its next action time to visibility as the
	// ScheduleNextActionTime search attribute. The schedule fires on an hourly
	// interval, so its next action time is always in the future; a query for
	// ScheduleNextActionTime > createTime must therefore eventually return this
	// schedule. (The SA is indexed/queryable but not surfaced on the list entry,
	// so we assert via the query rather than by reading the entry's SAs.)
	query := fmt.Sprintf(`%s > "%s"`,
		chasmscheduler.ScheduleNextActionTimeName,
		createTime.UTC().Format(time.RFC3339Nano),
	)

	await.Requiref(v2Ctx, t, func(at *await.T) {
		listResp, err := s.FrontendClient().ListSchedules(at.Context(), &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace().String(),
			MaximumPageSize: 100,
			Query:           query,
		})
		require.NoError(at, err)
		for _, ent := range listResp.Schedules {
			if ent.ScheduleId == v2Sid {
				return
			}
		}
		require.FailNow(at, "schedule has not appeared in visibility")
	}, 15*time.Second, 1*time.Second,
		"schedule %q must be returned by query %q (next action time published to visibility and in the future)",
		v2Sid, query)
}

func TestSchedule2ScheduleCountsVisibility(t *testing.T) {
	t.Parallel()
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend})
	newContext := schedule2Backend.context

	sid := testcore.RandomizeStr("sched-counts-v2")
	wid := testcore.RandomizeStr("sched-counts-wf")
	wt := testcore.RandomizeStr("sched-counts-wt")

	// A workflow that holds open so a fire stays running while later fires buffer.
	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			return workflow.Sleep(ctx, time.Hour)
		},
		workflow.RegisterOptions{Name: wt},
	)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	}

	ctx := newContext(t.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// matchesQuery reports whether the schedule is returned when filtering on the
	// given search-attribute query.
	matchesQuery := func(ctx context.Context, query string) bool {
		listResp, listErr := s.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace().String(),
			MaximumPageSize: 5,
			Query:           query,
		})
		if listErr != nil {
			return false
		}
		for _, ent := range listResp.Schedules {
			if ent.ScheduleId == sid {
				return true
			}
		}
		return false
	}

	// Starting the workflow and buffering the next fire takes a few seconds on top
	// of visibility propagation, so allow 30s for each count to become queryable.
	await.Requiref(ctx, t, func(at *await.T) {
		require.True(at, matchesQuery(at.Context(), fmt.Sprintf("%s >= 1", chasmscheduler.ScheduleRunningWorkflowCountName)))
	}, 30*time.Second, 500*time.Millisecond,
		"schedule must be queryable by ScheduleRunningWorkflowCount >= 1")

	await.Requiref(ctx, t, func(at *await.T) {
		require.True(at, matchesQuery(at.Context(), fmt.Sprintf("%s >= 1", chasmscheduler.ScheduleBufferedStartsCountName)))
	}, 30*time.Second, 500*time.Millisecond,
		"schedule must be queryable by ScheduleBufferedStartsCount >= 1")
}

func TestSchedule2IdleCloseTimeVisibility(t *testing.T) {
	t.Parallel()
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend})
	ctx := schedule2Backend.context(t.Context())
	id := testcore.RandomizeStr("sched-idle-index")
	created := time.Now()
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace: s.Namespace().String(), ScheduleId: id, RequestId: uuid.NewString(),
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{},
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId: id + "-workflow", WorkflowType: &commonpb.WorkflowType{Name: "unused"},
					TaskQueue: &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue()},
				},
			}},
		},
	})
	require.NoError(t, err)
	query := fmt.Sprintf(`%s > "%s"`, chasmscheduler.ScheduleIdleCloseTimeName, created.UTC().Format(time.RFC3339Nano))
	check := func(present bool) {
		t.Helper()
		await.Require(ctx, t, func(at *await.T) {
			response, err := s.FrontendClient().ListSchedules(at.Context(), &workflowservice.ListSchedulesRequest{
				Namespace: s.Namespace().String(), MaximumPageSize: 100, Query: query,
			})
			require.NoError(at, err)
			found := false
			for _, entry := range response.Schedules {
				found = found || entry.ScheduleId == id
			}
			require.Equal(at, present, found)
		}, 15*time.Second, 100*time.Millisecond)
	}
	check(true)
	_, err = s.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
		Namespace: s.Namespace().String(), ScheduleId: id, RequestId: uuid.NewString(),
		Patch: &schedulepb.SchedulePatch{Pause: "hold idle schedule open"},
	})
	require.NoError(t, err)
	check(false)
}
