package rapidtest

import (
	"fmt"
	"slices"
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/service/history/tasks"
	"pgregory.net/rapid"
)

const defaultRetainedDeliveries = 100

// Execution composes deterministic CHASM infrastructure actions into a Rapid
// state-machine test.
type Execution struct {
	Engine     *chasmtest.Engine
	Ref        chasm.ComponentRef
	TimeSource *clock.EventTimeSource

	delivered []tasks.Task
	history   []string
}

// DeliverRunnable draws and delivers one currently runnable task.
func (e *Execution) DeliverRunnable(t *rapid.T) {
	runnable, err := e.Engine.RunnableTasks(e.Ref)
	if err != nil {
		e.fail(t, "inspect runnable tasks", nil, err)
	}
	if len(runnable) == 0 {
		t.Skip("no runnable CHASM tasks")
	}
	task := rapid.SampledFrom(runnable).Draw(t, "runnable task")
	e.history = append(e.history, "deliver "+describeTask(task))
	if _, err := e.Engine.ExecuteTask(t.Context(), e.Ref, task); err != nil {
		e.fail(t, "deliver task", task, err)
	}
	e.delivered = append(e.delivered, task)
	if len(e.delivered) > defaultRetainedDeliveries {
		e.delivered = slices.Delete(e.delivered, 0, len(e.delivered)-defaultRetainedDeliveries)
	}
}

// Redeliver draws and delivers a previously delivered physical task.
func (e *Execution) Redeliver(t *rapid.T) {
	if len(e.delivered) == 0 {
		t.Skip("no previously delivered CHASM tasks")
	}
	task := rapid.SampledFrom(e.delivered).Draw(t, "delivered task")
	e.history = append(e.history, "redeliver "+describeTask(task))
	if _, err := e.Engine.ExecuteTask(t.Context(), e.Ref, task); err != nil {
		e.fail(t, "redeliver task", task, err)
	}
}

// AdvanceToNextTask advances event time to the earliest future CHASM task.
func (e *Execution) AdvanceToNextTask(t *rapid.T) {
	queued, err := e.Engine.Tasks(e.Ref)
	if err != nil {
		e.fail(t, "inspect queued tasks", nil, err)
	}
	now := e.TimeSource.Now()
	var next time.Time
	for category, categoryTasks := range queued {
		if category == tasks.CategoryVisibility {
			continue
		}
		for _, task := range categoryTasks {
			switch task.(type) {
			case *tasks.ChasmTask, *tasks.ChasmTaskPure:
			default:
				continue
			}
			visibilityTime := task.GetVisibilityTime()
			if !visibilityTime.After(now) || !next.IsZero() && !visibilityTime.Before(next) {
				continue
			}
			next = visibilityTime
		}
	}
	if next.IsZero() {
		t.Skip("no future CHASM tasks")
	}
	e.history = append(e.history, "advance time to "+next.Format(time.RFC3339Nano))
	e.TimeSource.Update(next)
}

// Reload round-trips the execution through its persistence snapshot.
func (e *Execution) Reload(t *rapid.T) {
	e.history = append(e.history, "reload execution")
	if err := e.Engine.ReloadExecution(t.Context(), e.Ref); err != nil {
		e.fail(t, "reload execution", nil, err)
	}
}

// History returns the infrastructure operations performed by this execution.
func (e *Execution) History() []string {
	return slices.Clone(e.history)
}

func (e *Execution) fail(t *rapid.T, operation string, task tasks.Task, err error) {
	t.Helper()
	t.Fatalf(
		"%s failed: execution=%+v time=%s task=%s error=%v history=%v",
		operation,
		e.Ref.ExecutionKey,
		e.TimeSource.Now().Format(time.RFC3339Nano),
		describeTask(task),
		err,
		e.history,
	)
}

func describeTask(task tasks.Task) string {
	if task == nil {
		return "<none>"
	}
	return fmt.Sprintf(
		"%T(category=%s, visibility=%s)",
		task,
		task.GetCategory().Name(),
		task.GetVisibilityTime().Format(time.RFC3339Nano),
	)
}
