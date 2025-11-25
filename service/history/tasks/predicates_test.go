package tasks

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/predicates"
	"go.uber.org/mock/gomock"
)


func TestNamespacePredicate_Test(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	namespaceIDs := []string{uuid.New(), uuid.New()}

	p := NewNamespacePredicate(namespaceIDs)
	for _, id := range namespaceIDs {
		mockTask := NewMockTask(controller)
		mockTask.EXPECT().GetNamespaceID().Return(id).Times(1)
		require.True(t, p.Test(mockTask))
	}

	mockTask := NewMockTask(controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).Times(1)
	require.False(t, p.Test(mockTask))
}

func TestNamespacePredicate_Equals(t *testing.T) {
	namespaceIDs := []string{uuid.New(), uuid.New()}

	p := NewNamespacePredicate(namespaceIDs)

	require.True(t, p.Equals(p))
	require.True(t, p.Equals(NewNamespacePredicate(namespaceIDs)))
	rand.Shuffle(
		len(namespaceIDs),
		func(i, j int) {
			namespaceIDs[i], namespaceIDs[j] = namespaceIDs[j], namespaceIDs[i]
		},
	)
	require.True(t, p.Equals(NewNamespacePredicate(namespaceIDs)))

	require.False(t, p.Equals(NewNamespacePredicate([]string{uuid.New(), uuid.New()})))
	require.False(t, p.Equals(NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})))
	require.False(t, p.Equals(predicates.Universal[Task]()))
}

func TestNamespacePredicate_Size(t *testing.T) {
	namespaceIDs := []string{uuid.New(), uuid.New()}

	p := NewNamespacePredicate(namespaceIDs)

	// UUID length is 36 and 4 bytes accounted for proto overhead.
	require.Equal(t, 76, p.Size())
}

func TestTypePredicate_Test(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	types := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
	}

	p := NewTypePredicate(types)
	for _, taskType := range types {
		mockTask := NewMockTask(controller)
		mockTask.EXPECT().GetType().Return(taskType).Times(1)
		require.True(t, p.Test(mockTask))
	}

	for _, taskType := range enumsspb.TaskType_value {
		if slices.Index(types, enumsspb.TaskType(taskType)) != -1 {
			continue
		}

		mockTask := NewMockTask(controller)
		mockTask.EXPECT().GetType().Return(enumsspb.TaskType(taskType)).Times(1)
		require.False(t, p.Test(mockTask))
	}
}

func TestTypePredicate_Equals(t *testing.T) {
	types := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
	}

	p := NewTypePredicate(types)

	require.True(t, p.Equals(p))
	require.True(t, p.Equals(NewTypePredicate(types)))
	rand.Shuffle(
		len(types),
		func(i, j int) {
			types[i], types[j] = types[j], types[i]
		},
	)
	require.True(t, p.Equals(NewTypePredicate(types)))

	require.False(t, p.Equals(NewTypePredicate([]enumsspb.TaskType{
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
		enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
	})))
	require.False(t, p.Equals(NewNamespacePredicate([]string{uuid.New(), uuid.New()})))
	require.False(t, p.Equals(predicates.Universal[Task]()))
}

func TestTypePredicate_Size(t *testing.T) {
	types := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
	}
	p := NewTypePredicate(types)

	// enum size is 4 and 4 bytes accounted for proto overhead.
	require.Equal(t, 12, p.Size())
}

func TestDestinationPredicate_Test(t *testing.T) {
	destinations := []string{uuid.New(), uuid.New()}

	p := NewDestinationPredicate(destinations)
	for _, dest := range destinations {
		mockTask := &StateMachineOutboundTask{Destination: dest}
		require.True(t, p.Test(mockTask))
	}

	mockTask := &StateMachineOutboundTask{Destination: uuid.New()}
	require.False(t, p.Test(mockTask))
}

func TestDestinationPredicate_Equals(t *testing.T) {
	destinations := []string{uuid.New(), uuid.New()}

	p := NewDestinationPredicate(destinations)

	require.True(t, p.Equals(p))
	require.True(t, p.Equals(NewDestinationPredicate(destinations)))
	rand.Shuffle(
		len(destinations),
		func(i, j int) {
			destinations[i], destinations[j] = destinations[j], destinations[i]
		},
	)
	require.True(t, p.Equals(NewDestinationPredicate(destinations)))

	require.False(t, p.Equals(NewDestinationPredicate([]string{uuid.New(), uuid.New()})))
	require.False(t, p.Equals(NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})))
	require.False(t, p.Equals(predicates.Universal[Task]()))
}

func TestDestinationPredicate_Size(t *testing.T) {
	destinations := []string{uuid.New(), uuid.New()}

	p := NewDestinationPredicate(destinations)

	// UUID length is 36 and 4 bytes accounted for proto overhead.
	require.Equal(t, 76, p.Size())
}

func TestOutboundTaskGroupPredicate_Test(t *testing.T) {
	groups := []string{"1", "2"}

	p := NewOutboundTaskGroupPredicate(groups)
	for _, groupType := range groups {
		mockTask := &StateMachineOutboundTask{StateMachineTask: StateMachineTask{Info: &persistencespb.StateMachineTaskInfo{Type: groupType}}}
		require.True(t, p.Test(mockTask))
	}

	mockTask := &StateMachineOutboundTask{StateMachineTask: StateMachineTask{Info: &persistencespb.StateMachineTaskInfo{Type: "3"}}}
	require.False(t, p.Test(mockTask))
}

func TestOutboundTaskGroupPredicate_Equals(t *testing.T) {
	groups := []string{"1", "2"}

	p := NewOutboundTaskGroupPredicate(groups)

	require.True(t, p.Equals(p))
	require.True(t, p.Equals(NewOutboundTaskGroupPredicate(groups)))
	rand.Shuffle(
		len(groups),
		func(i, j int) {
			groups[i], groups[j] = groups[j], groups[i]
		},
	)
	require.True(t, p.Equals(NewOutboundTaskGroupPredicate(groups)))

	require.False(t, p.Equals(NewOutboundTaskGroupPredicate([]string{"3", "4"})))
	require.False(t, p.Equals(NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})))
	require.False(t, p.Equals(predicates.Universal[Task]()))
}

func TestOutboundTaskGroupPredicate_Size(t *testing.T) {
	groups := []string{uuid.New(), uuid.New()}

	p := NewOutboundTaskGroupPredicate(groups)

	// UUID length is 36 and 4 bytes accounted for proto overhead.
	require.Equal(t, 76, p.Size())
}

func TestOutboundTaskPredicate_Test(t *testing.T) {
	groups := []TaskGroupNamespaceIDAndDestination{
		{"g1", "n1", "d1"},
		{"g2", "n2", "d2"},
	}

	p := NewOutboundTaskPredicate(groups)
	for _, g := range groups {
		mockTask := &StateMachineOutboundTask{
			StateMachineTask: StateMachineTask{
				Info:        &persistencespb.StateMachineTaskInfo{Type: g.TaskGroup},
				WorkflowKey: definition.NewWorkflowKey(g.NamespaceID, "", ""),
			},
			Destination: g.Destination,
		}
		require.True(t, p.Test(mockTask))
	}

	// Verify any field mismatch fails Test().
	mockTask := &StateMachineOutboundTask{
		StateMachineTask: StateMachineTask{
			Info:        &persistencespb.StateMachineTaskInfo{Type: "g1"},
			WorkflowKey: definition.NewWorkflowKey("n1", "", ""),
		},
		Destination: "d3",
	}
	require.False(t, p.Test(mockTask))
	mockTask = &StateMachineOutboundTask{
		StateMachineTask: StateMachineTask{
			Info:        &persistencespb.StateMachineTaskInfo{Type: "g3"},
			WorkflowKey: definition.NewWorkflowKey("n1", "", ""),
		},
		Destination: "d1",
	}
	require.False(t, p.Test(mockTask))
	mockTask = &StateMachineOutboundTask{
		StateMachineTask: StateMachineTask{
			Info:        &persistencespb.StateMachineTaskInfo{Type: "g1"},
			WorkflowKey: definition.NewWorkflowKey("n3", "", ""),
		},
		Destination: "d1",
	}
	require.False(t, p.Test(mockTask))
}

func TestOutboundTaskPredicate_Equals(t *testing.T) {
	groups := []TaskGroupNamespaceIDAndDestination{
		{"g1", "n1", "d1"},
		{"g2", "n2", "d2"},
	}

	p := NewOutboundTaskPredicate(groups)

	require.True(t, p.Equals(p))
	require.True(t, p.Equals(NewOutboundTaskPredicate(groups)))
	rand.Shuffle(
		len(groups),
		func(i, j int) {
			groups[i], groups[j] = groups[j], groups[i]
		},
	)
	require.True(t, p.Equals(NewOutboundTaskPredicate(groups)))

	require.False(t, p.Equals(NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
		{"g1", "n1", "d3"},
		{"g2", "n2", "d4"},
	})))
	require.False(t, p.Equals(NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})))
	require.False(t, p.Equals(predicates.Universal[Task]()))
}

func TestOutboundTaskPredicate_Size(t *testing.T) {
	groups := []TaskGroupNamespaceIDAndDestination{
		{"g1", "n1", "d1"},
		{"g2", "n2", "d2"},
	}
	p := NewOutboundTaskPredicate(groups)

	require.Equal(t, 16, p.Size())
}

func TestAndPredicates(t *testing.T) {
	testCases := []struct {
		predicateA     Predicate
		predicateB     Predicate
		expectedResult Predicate
	}{
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     NewNamespacePredicate([]string{"namespace2", "namespace3"}),
			expectedResult: NewNamespacePredicate([]string{"namespace2"}),
		},
		{
			predicateA: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
			}),
			expectedResult: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
		},
		{
			predicateA:     NewDestinationPredicate([]string{"dest1", "dest2"}),
			predicateB:     NewDestinationPredicate([]string{"dest2", "dest3"}),
			expectedResult: NewDestinationPredicate([]string{"dest2"}),
		},
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     NewNamespacePredicate([]string{"namespace3"}),
			expectedResult: predicates.Empty[Task](),
		},
		{
			predicateA:     NewOutboundTaskGroupPredicate([]string{"g1", "g2"}),
			predicateB:     NewOutboundTaskGroupPredicate([]string{"g3"}),
			expectedResult: predicates.Empty[Task](),
		},
		{
			predicateA: NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
				{"g1", "n1", "d1"},
				{"g2", "n2", "d2"},
			}),
			predicateB: NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
				{"g3", "n3", "d3"},
			}),
			expectedResult: predicates.Empty[Task](),
		},
		{
			predicateA: NewNamespacePredicate([]string{"namespace1"}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			expectedResult: predicates.And(
				NewNamespacePredicate([]string{"namespace1"}),
				NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				}),
			),
		},
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			expectedResult: NewNamespacePredicate([]string{"namespace1"}),
		},
		{
			predicateA:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			predicateB:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			expectedResult: NewNamespacePredicate([]string{"namespace1"}),
		},
		{
			predicateA:     predicates.Not(NewNamespacePredicate([]string{"namespace1", "namespace2"})),
			predicateB:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			expectedResult: predicates.Not(NewNamespacePredicate([]string{"namespace1", "namespace2", "namespace3"})),
		},
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     predicates.Not(NewNamespacePredicate([]string{"namespace1", "namespace2", "namespace3"})),
			expectedResult: predicates.Empty[Task](),
		},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.expectedResult, AndPredicates(tc.predicateA, tc.predicateB))
	}
}

func TestOrPredicates(t *testing.T) {
	testCases := []struct {
		predicateA     Predicate
		predicateB     Predicate
		expectedResult Predicate
	}{
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     NewNamespacePredicate([]string{"namespace2", "namespace3"}),
			expectedResult: NewNamespacePredicate([]string{"namespace1", "namespace2", "namespace3"}),
		},
		{
			predicateA: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
			}),
			expectedResult: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
			}),
		},
		{
			predicateA:     NewDestinationPredicate([]string{"dest1", "dest2"}),
			predicateB:     NewDestinationPredicate([]string{"dest2", "dest3"}),
			expectedResult: NewDestinationPredicate([]string{"dest1", "dest2", "dest3"}),
		},
		{
			predicateA:     NewOutboundTaskGroupPredicate([]string{"g1", "g2"}),
			predicateB:     NewOutboundTaskGroupPredicate([]string{"g2", "g3"}),
			expectedResult: NewOutboundTaskGroupPredicate([]string{"g1", "g2", "g3"}),
		},
		{
			predicateA: NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
				{"g1", "n1", "d1"},
				{"g2", "n2", "d2"},
			}),
			predicateB: NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
				{"g3", "n3", "d3"},
			}),
			expectedResult: NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
				{"g1", "n1", "d1"},
				{"g2", "n2", "d2"},
				{"g3", "n3", "d3"},
			}),
		},
		{
			predicateA: NewNamespacePredicate([]string{"namespace1"}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			expectedResult: predicates.Or(
				NewNamespacePredicate([]string{"namespace1"}),
				NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				}),
			),
		},
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			expectedResult: predicates.Not(NewNamespacePredicate([]string{"namespace3"})),
		},
		{
			predicateA:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			predicateB:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			expectedResult: predicates.Not(NewNamespacePredicate([]string{"namespace3"})),
		},
		{
			predicateA:     predicates.Not(NewNamespacePredicate([]string{"namespace1", "namespace2"})),
			predicateB:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			expectedResult: predicates.Not(NewNamespacePredicate([]string{"namespace2"})),
		},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.expectedResult, OrPredicates(tc.predicateA, tc.predicateB))
	}
}
