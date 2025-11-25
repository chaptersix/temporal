package queues

import (
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func setupScopeTest(t *testing.T) *gomock.Controller {
	t.Helper()
	return gomock.NewController(t)
}

func TestScopeContains(t *testing.T) {
	t.Parallel()

	controller := setupScopeTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()

		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).Times(1)
		require.True(t, scope.Contains(mockTask))

		mockTask.EXPECT().GetKey().Return(tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1)).Times(1)
		require.False(t, scope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).MaxTimes(1)
	require.False(t, scope.Contains(mockTask))
}

func TestScopeCanSplitByRange(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()
	scope := NewScope(r, predicate)

	require.True(t, scope.CanSplitByRange(r.InclusiveMin))
	require.True(t, scope.CanSplitByRange(r.ExclusiveMax))
	require.True(t, scope.CanSplitByRange(NewRandomKeyInRange(r)))

	require.False(t, scope.CanSplitByRange(tasks.NewKey(
		r.InclusiveMin.FireTime,
		r.InclusiveMin.TaskID-1,
	)))
	require.False(t, scope.CanSplitByRange(tasks.NewKey(
		r.ExclusiveMax.FireTime.Add(time.Nanosecond),
		r.ExclusiveMax.TaskID,
	)))
}

func TestScopeSplitByRange(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()
	scope := NewScope(r, predicate)

	splitKey := NewRandomKeyInRange(r)

	leftScope, rightScope := scope.SplitByRange(splitKey)
	require.Equal(t, NewRange(r.InclusiveMin, splitKey), leftScope.Range)
	require.Equal(t, NewRange(splitKey, r.ExclusiveMax), rightScope.Range)
	require.Equal(t, predicate, leftScope.Predicate)
	require.Equal(t, predicate, rightScope.Predicate)
}

func TestScopeSplitByPredicate_SamePredicateType(t *testing.T) {
	t.Parallel()

	controller := setupScopeTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	splitNamespaceIDs := append(slices.Clone(namespaceIDs[:rand.Intn(len(namespaceIDs))]), uuid.New(), uuid.New())
	splitPredicate := tasks.NewNamespacePredicate(splitNamespaceIDs)
	passScope, failScope := scope.SplitByPredicate(splitPredicate)
	require.Equal(t, r, passScope.Range)
	require.Equal(t, r, failScope.Range)

	for _, namespaceID := range splitNamespaceIDs {
		mockTask := tasks.NewMockTask(controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		if slices.Contains(namespaceIDs, namespaceID) {
			require.True(t, passScope.Contains(mockTask))
		} else {
			require.False(t, passScope.Contains(mockTask))
		}
		require.False(t, failScope.Contains(mockTask))
	}
	for _, namespaceID := range namespaceIDs {
		if slices.Contains(splitNamespaceIDs, namespaceID) {
			continue
		}

		mockTask := tasks.NewMockTask(controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		require.False(t, passScope.Contains(mockTask))
		require.True(t, failScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	require.False(t, passScope.Contains(mockTask))
	require.False(t, failScope.Contains(mockTask))
}

func TestScopeSplitByPredicate_DifferentPredicateType(t *testing.T) {
	t.Parallel()

	controller := setupScopeTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	splitTaskTypes := []enumsspb.TaskType{
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
	}
	splitPredicate := tasks.NewTypePredicate(splitTaskTypes)
	passScope, failScope := scope.SplitByPredicate(splitPredicate)
	require.Equal(t, r, passScope.Range)
	require.Equal(t, r, failScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		for _, typeType := range splitTaskTypes {
			mockTask.EXPECT().GetType().Return(typeType).Times(2)

			require.True(t, passScope.Contains(mockTask))
			require.False(t, failScope.Contains(mockTask))
		}

		mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).Times(2)
		require.False(t, passScope.Contains(mockTask))
		require.True(t, failScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	for _, typeType := range splitTaskTypes {
		mockTask.EXPECT().GetType().Return(typeType).MaxTimes(2)

		require.False(t, passScope.Contains(mockTask))
		require.False(t, failScope.Contains(mockTask))
	}

	mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).MaxTimes(2)
	require.False(t, passScope.Contains(mockTask))
	require.False(t, failScope.Contains(mockTask))
}

func TestScopeCanMergeByRange(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	testPredicates := []tasks.Predicate{
		predicate,
		tasks.NewNamespacePredicate(namespaceIDs),
		tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}),
		tasks.NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER}),
	}
	require.True(t, predicate.Equals(testPredicates[0]))
	require.True(t, predicate.Equals(testPredicates[1]))
	require.False(t, predicate.Equals(testPredicates[2]))
	require.False(t, predicate.Equals(testPredicates[3]))

	for _, mergePredicate := range testPredicates {
		canMerge := predicate.Equals(mergePredicate)

		incomingScope := NewScope(r, mergePredicate)
		require.Equal(t, canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(tasks.MinimumKey, r.InclusiveMin), mergePredicate)
		require.Equal(t, canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(r.ExclusiveMax, tasks.MaximumKey), mergePredicate)
		require.Equal(t, canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(tasks.MinimumKey, NewRandomKeyInRange(r)), mergePredicate)
		require.Equal(t, canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(NewRandomKeyInRange(r), tasks.MaximumKey), mergePredicate)
		require.Equal(t, canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(tasks.MinimumKey, tasks.MaximumKey), mergePredicate)
		require.Equal(t, canMerge, scope.CanMergeByRange(incomingScope))
	}

	incomingScope := NewScope(NewRange(
		tasks.MinimumKey,
		tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
	), predicate)
	require.False(t, scope.CanMergeByRange(incomingScope))

	incomingScope = NewScope(NewRange(
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.MaximumKey,
	), predicate)
	require.False(t, scope.CanMergeByRange(incomingScope))
}

func TestScopeMergeByRange(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()
	scope := NewScope(r, predicate)

	mergeRange := r
	mergedScope := scope.MergeByRange(NewScope(mergeRange, predicate))
	require.Equal(t, predicate, mergedScope.Predicate)
	require.Equal(t, r, mergedScope.Range)

	mergeRange = NewRange(tasks.MinimumKey, r.InclusiveMin)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	require.Equal(t, predicate, mergedScope.Predicate)
	require.Equal(t, NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedScope.Range)

	mergeRange = NewRange(r.ExclusiveMax, tasks.MaximumKey)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	require.Equal(t, predicate, mergedScope.Predicate)
	require.Equal(t, NewRange(r.InclusiveMin, tasks.MaximumKey), mergedScope.Range)

	mergeRange = NewRange(tasks.MinimumKey, NewRandomKeyInRange(r))
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	require.Equal(t, predicate, mergedScope.Predicate)
	require.Equal(t, NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedScope.Range)

	mergeRange = NewRange(NewRandomKeyInRange(r), tasks.MaximumKey)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	require.Equal(t, predicate, mergedScope.Predicate)
	require.Equal(t, NewRange(r.InclusiveMin, tasks.MaximumKey), mergedScope.Range)

	mergeRange = NewRange(tasks.MinimumKey, tasks.MaximumKey)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	require.Equal(t, predicate, mergedScope.Predicate)
	require.Equal(t, mergeRange, mergedScope.Range)
}

func TestScopeCanMergeByPredicate(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	require.True(t, scope.CanMergeByPredicate(scope))
	require.True(t, scope.CanMergeByPredicate(NewScope(r, predicate)))
	require.True(t, scope.CanMergeByPredicate(NewScope(r, tasks.NewTypePredicate([]enumsspb.TaskType{}))))

	require.False(t, scope.CanMergeByPredicate(NewScope(NewRandomRange(), predicate)))
	require.False(t, scope.CanMergeByPredicate(NewScope(NewRandomRange(), predicates.Universal[tasks.Task]())))
}

func TestScopeMergeByPredicate_SamePredicateType(t *testing.T) {
	t.Parallel()

	controller := setupScopeTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	mergeNamespaceIDs := append(slices.Clone(namespaceIDs[:rand.Intn(len(namespaceIDs))]), uuid.New(), uuid.New())
	mergePredicate := tasks.NewNamespacePredicate(mergeNamespaceIDs)
	mergedScope := scope.MergeByPredicate(NewScope(r, mergePredicate))
	require.Equal(t, r, mergedScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(controller)
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).MaxTimes(2)

		require.True(t, mergedScope.Contains(mockTask))
	}
	for _, namespaceID := range mergeNamespaceIDs {
		mockTask := tasks.NewMockTask(controller)
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).MaxTimes(2)

		require.True(t, mergedScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(controller)
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	require.False(t, mergedScope.Contains(mockTask))
}

func TestScopeMergeByPredicate_DifferentPredicateType(t *testing.T) {
	t.Parallel()

	controller := setupScopeTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	mergeTaskTypes := []enumsspb.TaskType{
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
	}
	mergePredicate := tasks.NewTypePredicate(mergeTaskTypes)
	mergedScope := scope.MergeByPredicate(NewScope(r, mergePredicate))
	require.Equal(t, r, mergedScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		for _, typeType := range mergeTaskTypes {
			mockTask.EXPECT().GetType().Return(typeType).MaxTimes(1)
			require.True(t, mergedScope.Contains(mockTask))
		}

		mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).MaxTimes(1)
		require.True(t, mergedScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	for _, typeType := range mergeTaskTypes {
		mockTask.EXPECT().GetType().Return(typeType).MaxTimes(1)

		require.True(t, mergedScope.Contains(mockTask))
	}

	mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).Times(1)
	require.False(t, mergedScope.Contains(mockTask))
}
