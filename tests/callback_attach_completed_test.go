package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestAttachCallbackToCompletedWorkflow validates that attempting to attach a
// completion callback to a completed workflow via StartWorkflowExecution with
// USE_EXISTING conflict policy starts a new workflow instead of attaching to
// the completed one. This documents the known race condition in the migration
// callback task.
func TestAttachCallbackToCompletedWorkflow(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableNexus, true),
	)
	env.OverrideDynamicConfig(
		callbacks.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	ctx := testcore.NewContext()
	workflowID := testcore.RandomizeStr("callback-completed-wf")
	workflowType := "callback-completed-test"
	taskQueue := testcore.RandomizeStr("callback-completed-tq")

	sdkClient, err := client.Dial(client.Options{
		HostPort:  env.GetTestCluster().Host().FrontendGRPCAddress(),
		Namespace: env.Namespace().String(),
	})
	require.NoError(t, err)
	defer sdkClient.Close()

	// Register a workflow that completes immediately.
	w := worker.New(sdkClient, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: workflowType},
	)
	require.NoError(t, w.Start())
	defer w.Stop()

	// Start and wait for the workflow to complete.
	response1, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         workflowID,
		WorkflowType:       &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout: durationpb.New(10 * time.Second),
		Identity:           t.Name(),
	})
	require.NoError(t, err)

	run := sdkClient.GetWorkflow(ctx, workflowID, response1.RunId)
	require.NoError(t, run.Get(ctx, nil))

	// Attempt to attach a callback using USE_EXISTING conflict policy.
	response2, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.NewString(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:       durationpb.New(10 * time.Second),
		Identity:                 t.Name(),
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		CompletionCallbacks: []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url: "http://localhost:1234/callback",
					},
				},
			},
		},
		OnConflictOptions: &workflowpb.OnConflictOptions{
			AttachRequestId:           true,
			AttachCompletionCallbacks: true,
		},
	})
	require.NoError(t, err)

	// The workflow already completed, so there's no conflict. USE_EXISTING
	// starts a new workflow instead of attaching to the completed one.
	require.True(t, response2.Started)
	require.NotEqual(t, response1.RunId, response2.RunId)
}
