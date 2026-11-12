package schedulertest

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
)

func (d *Driver) validIdleTask(s *scheduler.Scheduler, ctx chasm.Context) bool {
	id, ok := d.Registry.TaskIDFor(&schedulerpb.SchedulerIdleTask{})
	if !ok {
		d.T.Fatal("idle task is not registered")
	}
	handler := scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
		Config: d.Config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: d.Logger,
	})
	for _, task := range d.runner.PendingPureTasks() {
		if task.GetTypeId() != id {
			continue
		}
		value := &schedulerpb.SchedulerIdleTask{}
		if err := proto.Unmarshal(task.GetData().GetData(), value); err != nil {
			d.T.Fatalf("decode idle task: %v", err)
		}
		valid, err := handler.Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: task.GetScheduledTime().AsTime()}}, value)
		if err != nil {
			d.T.Fatalf("validate idle task: %v", err)
		}
		if valid {
			return true
		}
	}
	return false
}
