package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
)

func scheduledWorkflow(ctx workflow.Context) (string, error) {
	if err := workflow.Sleep(ctx, 2*time.Second); err != nil {
		return "", err
	}
	return "workflow completed", nil
}

func scheduledActivity(ctx context.Context, label string) (string, error) {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-timer.C:
		return label + " completed", nil
	}
}

func main() {
	address := flag.String("address", "127.0.0.1:7233", "modified local server address")
	namespace := flag.String("namespace", "scheduler-experiment", "namespace to create if absent")
	flag.Parse()
	if err := run(*address, *namespace); err != nil {
		log.Fatal(err)
	}
}

func run(address, namespace string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := client.Dial(client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		return err
	}
	defer c.Close()
	_, err = c.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{Namespace: namespace, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour)})
	var exists *serviceerror.NamespaceAlreadyExists
	if err != nil && !errors.As(err, &exists) {
		return err
	}
	taskQueue := fmt.Sprintf("scheduler-experiment-%d", time.Now().UnixNano())
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(scheduledWorkflow)
	w.RegisterActivity(scheduledActivity)
	if err = w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	handles := make([]client.ScheduleHandle, 0, 2)
	defer func() { err = errors.Join(err, cleanupSchedules(c, namespace, handles)) }()
	options := []client.ScheduleOptions{
		{ID: taskQueue + "-workflow", Spec: client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: 2 * time.Second}}}, Action: &client.ScheduleWorkflowAction{ID: taskQueue + "-wf", Workflow: scheduledWorkflow, TaskQueue: taskQueue}, Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, RemainingActions: 5},
		{ID: taskQueue + "-activity", Spec: client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: 2 * time.Second}}}, Action: &client.ScheduleActivityAction{ID: taskQueue + "-activity", Activity: scheduledActivity, Args: []any{"scheduled activity"}, TaskQueue: taskQueue, StartToCloseTimeout: time.Minute, StartDelay: time.Second, StaticSummary: "Scheduled standalone activity"}, CustomOverlapPolicy: "temporal.buffer_latest", RemainingActions: 5},
	}
	for _, option := range options {
		handle, createErr := c.ScheduleClient().Create(ctx, option)
		if createErr != nil {
			return createErr
		}
		handles = append(handles, handle)
	}
	return observeSchedules(ctx, handles)
}

func cleanupSchedules(c client.Client, namespace string, handles []client.ScheduleHandle) (err error) {
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cleanupCancel()
	for _, handle := range handles {
		err = errors.Join(err, handle.Pause(cleanupCtx, client.SchedulePauseOptions{}))
		description, describeErr := handle.Describe(cleanupCtx)
		if describeErr != nil {
			err = errors.Join(err, describeErr)
		} else {
			for _, execution := range description.Info.RunningExecutions {
				if execution.Kind == enumspb.EXECUTION_TYPE_ACTIVITY {
					_, terminateErr := c.WorkflowService().TerminateActivityExecution(cleanupCtx, &workflowservice.TerminateActivityExecutionRequest{Namespace: namespace, ActivityId: execution.ID, RunId: execution.RunID, Reason: "example cleanup"})
					err = errors.Join(err, terminateErr)
				} else {
					err = errors.Join(err, c.TerminateWorkflow(cleanupCtx, execution.ID, execution.RunID, "example cleanup"))
				}
			}
		}
		err = errors.Join(err, handle.Delete(cleanupCtx))
	}
	return err
}

func observeSchedules(ctx context.Context, handles []client.ScheduleHandle) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for tick := range 10 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		if tick == 1 {
			if err := handles[1].Trigger(ctx, client.ScheduleTriggerOptions{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}); err != nil {
				return err
			}
		}
		for _, handle := range handles {
			if err := printDescription(ctx, handle); err != nil {
				return err
			}
		}
	}
	return nil
}

func printDescription(ctx context.Context, handle client.ScheduleHandle) error {
	description, err := handle.Describe(ctx)
	if err != nil {
		return err
	}
	log.Printf("%s: kind=%s type=%s starts=%d skipped=%d", handle.GetID(), description.Info.ActionKind, description.Info.ActionType, description.Info.NumActions, description.Info.NumActionsSkippedOverlap)
	for _, result := range description.Info.RecentActions {
		if result.Execution == nil {
			continue
		}
		status := result.WorkflowStatus.String()
		if result.Execution.Kind == enumspb.EXECUTION_TYPE_ACTIVITY {
			status = result.ActivityStatus.String()
		}
		if result.CloseTime.IsZero() {
			log.Printf("  %s %s/%s status=%s", result.Execution.Kind, result.Execution.ID, result.Execution.RunID, status)
			continue
		}
		log.Printf("  %s %s/%s status=%s closed=%s", result.Execution.Kind, result.Execution.ID, result.Execution.RunID, status, result.CloseTime)
	}
	return nil
}
