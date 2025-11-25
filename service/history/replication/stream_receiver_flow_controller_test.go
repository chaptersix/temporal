package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/tests"
)

func TestLowPriorityWithinLimit(t *testing.T) {
	lowPrioritySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: 5}
	}

	highPrioritySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: 150}
	}

	signals := map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW:  lowPrioritySignal,
		enumsspb.TASK_PRIORITY_HIGH: highPrioritySignal,
	}

	config := tests.NewDynamicConfig()
	config.ReplicationReceiverMaxOutstandingTaskCount = func() int {
		return 50
	}
	controller := NewReceiverFlowControl(signals, config)

	actual := controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected := enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	require.Equal(t, expected, actual)
}

func TestHighPriorityExceedsLimit(t *testing.T) {
	lowPrioritySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: 5}
	}

	highPrioritySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: 150}
	}

	signals := map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW:  lowPrioritySignal,
		enumsspb.TASK_PRIORITY_HIGH: highPrioritySignal,
	}

	config := tests.NewDynamicConfig()
	config.ReplicationReceiverMaxOutstandingTaskCount = func() int {
		return 50
	}
	controller := NewReceiverFlowControl(signals, config)

	actual := controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_HIGH)
	expected := enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	require.Equal(t, expected, actual)
}

func TestUnknownPriority(t *testing.T) {
	lowPrioritySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: 5}
	}

	highPrioritySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: 150}
	}

	signals := map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW:  lowPrioritySignal,
		enumsspb.TASK_PRIORITY_HIGH: highPrioritySignal,
	}

	config := tests.NewDynamicConfig()
	config.ReplicationReceiverMaxOutstandingTaskCount = func() int {
		return 50
	}
	controller := NewReceiverFlowControl(signals, config)

	unknownPriority := enumsspb.TaskPriority(999) // Assuming 999 is an unknown priority
	actual := controller.GetFlowControlInfo(unknownPriority)
	expected := enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	require.Equal(t, expected, actual)
}

func TestBoundaryCondition(t *testing.T) {
	config := tests.NewDynamicConfig()
	config.ReplicationReceiverMaxOutstandingTaskCount = func() int {
		return 50
	}
	maxOutStandingTasks := config.ReplicationReceiverMaxOutstandingTaskCount()

	boundarySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: maxOutStandingTasks}
	}

	signals := map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: boundarySignal,
	}

	controller := NewReceiverFlowControl(signals, config)

	actual := controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected := enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	require.Equal(t, expected, actual)

	boundarySignal = func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: maxOutStandingTasks + 1}
	}

	signals = map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: boundarySignal,
	}

	controller = NewReceiverFlowControl(signals, config)

	actual = controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected = enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	require.Equal(t, expected, actual)
}
