package chasmtest

import (
	"context"
	"fmt"
	"slices"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

// TaskExecutionResult describes the logical work performed for one physical task.
type TaskExecutionResult struct {
	Executed int
	Dropped  bool
}

type runnableTask struct {
	task      tasks.Task
	category  tasks.Category
	insertion int
}

// ReloadExecution reconstructs an execution's component tree from a cloned
// persistence snapshot while retaining the execution and its physical tasks.
func (e *Engine) ReloadExecution(ctx context.Context, ref chasm.ComponentRef) error {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return err
	}
	node, root, err := e.cloneExecutionTree(ctx, exec)
	if err != nil {
		return err
	}
	exec.node = node
	exec.root = root
	return nil
}

// RunnableTasks returns the undelivered CHASM tasks for ref that are due at the
// engine's current time. Visibility maintenance tasks are excluded.
func (e *Engine) RunnableTasks(ref chasm.ComponentRef) ([]tasks.Task, error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return nil, err
	}

	now := e.timeSource.Now()
	var runnable []runnableTask
	for category, categoryTasks := range exec.backend.TasksByCategory {
		if category == tasks.CategoryVisibility {
			continue
		}
		for insertion, task := range categoryTasks {
			switch task.(type) {
			case *tasks.ChasmTask, *tasks.ChasmTaskPure:
			default:
				continue
			}
			if task.GetVisibilityTime().After(now) {
				continue
			}
			runnable = append(runnable, runnableTask{
				task:      task,
				category:  category,
				insertion: insertion,
			})
		}
	}

	slices.SortStableFunc(runnable, func(a, b runnableTask) int {
		if order := a.task.GetVisibilityTime().Compare(b.task.GetVisibilityTime()); order != 0 {
			return order
		}
		if a.category.ID() < b.category.ID() {
			return -1
		}
		if a.category.ID() > b.category.ID() {
			return 1
		}
		return a.insertion - b.insertion
	})

	result := make([]tasks.Task, len(runnable))
	for i, item := range runnable {
		result[i] = item.task
	}
	return result, nil
}

// ExecuteTask executes one CHASM physical task. Successful and stale tasks are
// acknowledged; errors leave a queued task pending for retry.
func (e *Engine) ExecuteTask(
	ctx context.Context,
	ref chasm.ComponentRef,
	task tasks.Task,
) (TaskExecutionResult, error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return TaskExecutionResult{}, err
	}
	if err := validateTaskExecution(exec, task, e.timeSource.Now()); err != nil {
		return TaskExecutionResult{}, err
	}

	var result TaskExecutionResult
	switch task := task.(type) {
	case *tasks.ChasmTaskPure:
		result, err = e.executePureTask(ctx, exec, task)
	case *tasks.ChasmTask:
		result, err = e.executeSideEffectTask(ctx, exec, task)
	default:
		err = fmt.Errorf("chasmtest: unsupported task type %T", task)
	}
	if err != nil {
		return TaskExecutionResult{}, err
	}

	e.acknowledgeTask(exec, task)
	return result, nil
}

// DrainTasks executes ready tasks until none remain. It returns the number of
// physical tasks acknowledged.
func (e *Engine) DrainTasks(
	ctx context.Context,
	ref chasm.ComponentRef,
	maxTasks int,
) (int, error) {
	if maxTasks < 0 {
		return 0, fmt.Errorf("chasmtest: maxTasks must be non-negative: %d", maxTasks)
	}

	drained := 0
	for drained < maxTasks {
		runnable, err := e.RunnableTasks(ref)
		if err != nil {
			return drained, err
		}
		if len(runnable) == 0 {
			return drained, nil
		}
		if _, err := e.ExecuteTask(ctx, ref, runnable[0]); err != nil {
			return drained, err
		}
		drained++
	}

	runnable, err := e.RunnableTasks(ref)
	if err != nil {
		return drained, err
	}
	if len(runnable) == 0 {
		return drained, nil
	}
	return drained, fmt.Errorf(
		"chasmtest: task drain limit %d reached with %d runnable tasks remaining (next: %T, category: %s, visibility time: %s)",
		maxTasks,
		len(runnable),
		runnable[0],
		runnable[0].GetCategory().Name(),
		runnable[0].GetVisibilityTime().Format(time.RFC3339Nano),
	)
}

func (e *Engine) executePureTask(
	ctx context.Context,
	exec *execution,
	task *tasks.ChasmTaskPure,
) (TaskExecutionResult, error) {
	if task.ArchetypeID != uint32(exec.node.ArchetypeID()) {
		return TaskExecutionResult{Dropped: true}, nil
	}

	workingNode, workingRoot, err := e.cloneExecutionTree(ctx, exec)
	if err != nil {
		return TaskExecutionResult{}, err
	}

	result := TaskExecutionResult{}
	processed := 0
	err = workingNode.EachPureTask(e.timeSource.Now(), func(
		handler chasm.NodePureTask,
		attrs chasm.TaskAttributes,
		logicalTask any,
	) (bool, error) {
		processed++
		executed, err := handler.ExecutePureTask(ctx, attrs, logicalTask)
		if executed {
			result.Executed++
		}
		return executed, err
	})
	if err != nil {
		return TaskExecutionResult{}, err
	}
	if processed == 0 {
		return TaskExecutionResult{Dropped: true}, nil
	}
	if err = e.closeTransaction(exec, workingNode); err != nil {
		return TaskExecutionResult{}, err
	}

	exec.node = workingNode
	exec.root = workingRoot
	return result, nil
}

func (e *Engine) executeSideEffectTask(
	ctx context.Context,
	exec *execution,
	task *tasks.ChasmTask,
) (TaskExecutionResult, error) {
	if task.Info == nil {
		return TaskExecutionResult{}, fmt.Errorf("chasmtest: CHASM side-effect task has no task info")
	}
	if task.Info.ArchetypeId != uint32(exec.node.ArchetypeID()) {
		return TaskExecutionResult{Dropped: true}, nil
	}

	inTree, valid, err := exec.node.ValidateSideEffectTask(ctx, task)
	if err != nil {
		return TaskExecutionResult{}, err
	}
	if !inTree || !valid {
		return TaskExecutionResult{Dropped: true}, nil
	}

	engineCtx := chasm.NewEngineContext(ctx, e)
	err = exec.node.ExecuteSideEffectTask(
		engineCtx,
		exec.key,
		task,
		func(chasm.NodeBackend, chasm.Context, chasm.Component) error { return nil },
	)
	if err != nil {
		return TaskExecutionResult{}, err
	}
	return TaskExecutionResult{Executed: 1}, nil
}

func (e *Engine) cloneExecutionTree(
	ctx context.Context,
	exec *execution,
) (*chasm.Node, chasm.RootComponent, error) {
	snapshot := exec.node.Snapshot(nil)
	nodes := make(map[string]*persistencespb.ChasmNode, len(snapshot.Nodes))
	for path, node := range snapshot.Nodes {
		nodes[path] = proto.CloneOf(node)
	}

	cloned, err := chasm.NewTreeFromDB(
		nodes,
		e.registry,
		e.timeSource,
		exec.backend,
		chasm.DefaultPathEncoder,
		e.logger,
		e.metrics,
	)
	if err != nil {
		return nil, nil, err
	}
	component, err := cloned.Component(chasm.NewContext(ctx, cloned), chasm.ComponentRef{})
	if err != nil {
		return nil, nil, err
	}
	root, ok := component.(chasm.RootComponent)
	if !ok {
		return nil, nil, fmt.Errorf("chasmtest: execution root has unexpected type %T", component)
	}
	return cloned, root, nil
}

func validateTaskExecution(exec *execution, task tasks.Task, now time.Time) error {
	if task == nil {
		return fmt.Errorf("chasmtest: task is nil")
	}
	if task.GetCategory() == tasks.CategoryVisibility {
		return fmt.Errorf("chasmtest: visibility maintenance tasks are not executable")
	}
	if task.GetNamespaceID() != exec.key.NamespaceID ||
		task.GetWorkflowID() != exec.key.BusinessID ||
		task.GetRunID() != exec.key.RunID {
		return fmt.Errorf(
			"chasmtest: task execution key (%q, %q, %q) does not match ref execution key (%q, %q, %q)",
			task.GetNamespaceID(),
			task.GetWorkflowID(),
			task.GetRunID(),
			exec.key.NamespaceID,
			exec.key.BusinessID,
			exec.key.RunID,
		)
	}
	if task.GetVisibilityTime().After(now) {
		return fmt.Errorf(
			"chasmtest: task is not runnable until %s (current time: %s)",
			task.GetVisibilityTime().Format(time.RFC3339Nano),
			now.Format(time.RFC3339Nano),
		)
	}
	return nil
}

func (e *Engine) acknowledgeTask(exec *execution, target tasks.Task) {
	categoryTasks := exec.backend.TasksByCategory[target.GetCategory()]
	for i, task := range categoryTasks {
		if task == target {
			exec.backend.TasksByCategory[target.GetCategory()] = slices.Delete(categoryTasks, i, i+1)
			return
		}
	}
}
