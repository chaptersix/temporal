package scheduler_test

import (
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/service/history/tasks"
	"pgregory.net/rapid"
)

func TestSchedulerReloadModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		config := defaultModelEnvConfig()
		config.interval = time.Minute
		config.maxActions = 100
		env := newSchedulerModelEnv(t, config)

		env.timeSource.Update(config.startTime.Add(config.interval))
		if len(env.runnableTasks(t)) == 0 {
			t.Fatalf("expected an active pure generator task")
		}
		env.reload(t)
		env.drain(t)
		if len(env.workflows.snapshot().starts) != 1 {
			t.Fatalf("reloaded generator did not start workflow")
		}

		triggerModelAction(t, env, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
		var sideEffects []tasks.Task
		for _, task := range env.runnableTasks(t) {
			if _, ok := task.(*tasks.ChasmTask); ok {
				sideEffects = append(sideEffects, task)
			}
		}
		if len(sideEffects) == 0 {
			t.Fatalf("expected an active side-effect task")
		}
		env.reload(t)
		env.drain(t)
		if len(env.workflows.snapshot().starts) != 2 {
			t.Fatalf("reloaded invoker did not start workflow")
		}

		env.reload(t)
		for _, task := range sideEffects {
			result := env.redeliver(t, task)
			if !result.Dropped || result.Executed != 0 {
				t.Fatalf("reloaded stale task redelivery: %+v", result)
			}
		}

		_, err := env.handler.DeleteSchedule(env.engineCtx, &schedulerpb.DeleteScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.DeleteScheduleRequest{
				Namespace: namespace, ScheduleId: env.scheduleID,
			},
		})
		mustNoError(t, err)
		env.reload(t)
		if !env.internal(t).closed {
			t.Fatalf("reload reopened a closed execution")
		}
		checkClosedAPIs(t, env)
	})
}
