package queues

import (
	"errors"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/predicates"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type sliceTestDeps struct {
	controller            *gomock.Controller
	mockClusterMetadata   *cluster.MockMetadata
	mockNamespaceRegistry *namespace.MockRegistry
	executableFactory     ExecutableFactory
	monitor               *monitorImpl
}

func noPredicateSizeLimit() int {
	return 0
}

func setupSliceTest(t *testing.T) *sliceTestDeps {
	t.Helper()

	controller := gomock.NewController(t)
	mockClusterMetadata := cluster.NewMockMetadata(controller)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockNamespaceRegistry := namespace.NewMockRegistry(controller)
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	executableFactory := ExecutableFactoryFn(func(readerID int64, t tasks.Task) Executable {
		return NewExecutable(
			readerID,
			t,
			nil,
			nil,
			nil,
			NewNoopPriorityAssigner(),
			clock.NewRealTimeSource(),
			mockNamespaceRegistry,
			mockClusterMetadata,
			chasm.NewRegistry(log.NewTestLogger()),
			testTaskTagValueProvider,
			nil,
			metrics.NoopMetricsHandler,
			telemetry.NoopTracer,
		)
	})
	monitor := newMonitor(tasks.CategoryTypeScheduled, clock.NewRealTimeSource(), &MonitorOptions{
		PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
		ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
		SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
	})

	return &sliceTestDeps{
		controller:            controller,
		mockClusterMetadata:   mockClusterMetadata,
		mockNamespaceRegistry: mockNamespaceRegistry,
		executableFactory:     executableFactory,
		monitor:               monitor,
	}
}

func (d *sliceTestDeps) newTestSlice(
	t *testing.T,
	r Range,
	namespaceIDs []string,
	taskTypes []enumsspb.TaskType,
) *SliceImpl {
	t.Helper()

	predicate := predicates.Universal[tasks.Task]()
	if len(namespaceIDs) != 0 {
		predicate = predicates.And[tasks.Task](
			predicate,
			tasks.NewNamespacePredicate(namespaceIDs),
		)
	}
	if len(taskTypes) != 0 {
		predicate = predicates.And[tasks.Task](
			predicate,
			tasks.NewTypePredicate(taskTypes),
		)
	}

	if len(namespaceIDs) == 0 {
		namespaceIDs = []string{uuid.New()}
	}

	if len(taskTypes) == 0 {
		taskTypes = []enumsspb.TaskType{enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION}
	}

	slice := NewSlice(nil, d.executableFactory, d.monitor, NewScope(r, predicate), GrouperNamespaceID{}, noPredicateSizeLimit)
	for _, executable := range d.randomExecutablesInRange(r, rand.Intn(20)) {
		slice.pendingExecutables[executable.GetKey()] = executable

		mockExecutable := executable.(*MockExecutable)
		mockExecutable.EXPECT().GetNamespaceID().Return(namespaceIDs[rand.Intn(len(namespaceIDs))]).AnyTimes()
		mockExecutable.EXPECT().GetType().Return(taskTypes[rand.Intn(len(taskTypes))]).AnyTimes()
	}
	slice.iterators = d.randomIteratorsInRange(r, rand.Intn(10), nil)

	return slice
}

func (d *sliceTestDeps) randomExecutablesInRange(
	r Range,
	numExecutables int,
) []Executable {
	executables := make([]Executable, 0, numExecutables)
	for i := 0; i != numExecutables; i++ {
		mockExecutable := NewMockExecutable(d.controller)
		key := NewRandomKeyInRange(r)
		mockExecutable.EXPECT().GetKey().Return(key).AnyTimes()
		executables = append(executables, mockExecutable)
	}
	return executables
}

func (d *sliceTestDeps) randomIteratorsInRange(
	r Range,
	numIterators int,
	paginationFnProvider PaginationFnProvider,
) []Iterator {
	ranges := NewRandomOrderedRangesInRange(r, numIterators)

	iterators := make([]Iterator, 0, numIterators)
	for _, r := range ranges {
		start := NewRandomKeyInRange(r)
		end := NewRandomKeyInRange(r)
		if start.CompareTo(end) > 0 {
			start, end = end, start
		}
		iterator := NewIterator(paginationFnProvider, NewRange(start, end))
		iterators = append(iterators, iterator)
	}

	return iterators
}

func TestSliceCanSplitByRange(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	scope := NewScope(r, predicates.Universal[tasks.Task]())

	slice := NewSlice(nil, deps.executableFactory, deps.monitor, scope, GrouperNamespaceID{}, noPredicateSizeLimit)
	require.Equal(t, scope, slice.Scope())

	require.True(t, slice.CanSplitByRange(r.InclusiveMin))
	require.True(t, slice.CanSplitByRange(r.ExclusiveMax))
	require.True(t, slice.CanSplitByRange(NewRandomKeyInRange(r)))

	require.False(t, slice.CanSplitByRange(tasks.NewKey(
		r.InclusiveMin.FireTime,
		r.InclusiveMin.TaskID-1,
	)))
	require.False(t, slice.CanSplitByRange(tasks.NewKey(
		r.ExclusiveMax.FireTime.Add(time.Nanosecond),
		r.ExclusiveMax.TaskID,
	)))
}

func TestSliceSplitByRange(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	slice := deps.newTestSlice(t, r, nil, nil)

	splitKey := NewRandomKeyInRange(r)
	leftSlice, rightSlice := slice.SplitByRange(splitKey)
	require.Equal(t, NewScope(
		NewRange(r.InclusiveMin, splitKey),
		slice.scope.Predicate,
	), leftSlice.Scope())
	require.Equal(t, NewScope(
		NewRange(splitKey, r.ExclusiveMax),
		slice.scope.Predicate,
	), rightSlice.Scope())

	validateSliceState(t, leftSlice.(*SliceImpl))
	validateSliceState(t, rightSlice.(*SliceImpl))

	require.Panics(t, func() { slice.stateSanityCheck() })
}

func TestSliceSplitByPredicate(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := deps.newTestSlice(t, r, namespaceIDs, nil)

	splitNamespaceIDs := append(slices.Clone(namespaceIDs[:rand.Intn(len(namespaceIDs))]), uuid.New(), uuid.New())
	splitPredicate := tasks.NewNamespacePredicate(splitNamespaceIDs)
	passSlice, failSlice := slice.SplitByPredicate(splitPredicate)
	require.Equal(t, r, passSlice.Scope().Range)
	require.Equal(t, r, failSlice.Scope().Range)
	require.True(t, tasks.AndPredicates(slice.scope.Predicate, splitPredicate).Equals(passSlice.Scope().Predicate))
	require.True(t, tasks.AndPredicates(slice.scope.Predicate, predicates.Not(splitPredicate)).Equals(failSlice.Scope().Predicate))

	validateSliceState(t, passSlice.(*SliceImpl))
	validateSliceState(t, failSlice.(*SliceImpl))

	require.Panics(t, func() { slice.stateSanityCheck() })
}

func TestSliceCanMergeWithSlice(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	slice := NewSlice(nil, nil, deps.monitor, NewScope(r, predicate), GrouperNamespaceID{}, noPredicateSizeLimit)

	testPredicates := []tasks.Predicate{
		predicate,
		tasks.NewNamespacePredicate(namespaceIDs),
		tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}),
	}
	require.True(t, predicate.Equals(testPredicates[0]))
	require.True(t, predicate.Equals(testPredicates[1]))
	require.False(t, predicate.Equals(testPredicates[2]))

	for _, mergePredicate := range testPredicates {
		testSlice := NewSlice(nil, nil, deps.monitor, NewScope(r, mergePredicate), GrouperNamespaceID{}, noPredicateSizeLimit)
		require.True(t, slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, deps.monitor, NewScope(NewRange(tasks.MinimumKey, r.InclusiveMin), mergePredicate), GrouperNamespaceID{}, noPredicateSizeLimit)
		require.True(t, slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, deps.monitor, NewScope(NewRange(r.ExclusiveMax, tasks.MaximumKey), mergePredicate), GrouperNamespaceID{}, noPredicateSizeLimit)
		require.True(t, slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, deps.monitor, NewScope(NewRange(tasks.MinimumKey, NewRandomKeyInRange(r)), mergePredicate), GrouperNamespaceID{}, noPredicateSizeLimit)
		require.True(t, slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, deps.monitor, NewScope(NewRange(NewRandomKeyInRange(r), tasks.MaximumKey), mergePredicate), GrouperNamespaceID{}, noPredicateSizeLimit)
		require.True(t, slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, deps.monitor, NewScope(NewRange(tasks.MinimumKey, tasks.MaximumKey), mergePredicate), GrouperNamespaceID{}, noPredicateSizeLimit)
		require.True(t, slice.CanMergeWithSlice(testSlice))
	}

	require.False(t, slice.CanMergeWithSlice(slice))

	testSlice := NewSlice(nil, nil, deps.monitor, NewScope(NewRange(
		tasks.MinimumKey,
		tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
	), predicate), GrouperNamespaceID{}, noPredicateSizeLimit)
	require.False(t, slice.CanMergeWithSlice(testSlice))

	testSlice = NewSlice(nil, nil, deps.monitor, NewScope(NewRange(
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.MaximumKey,
	), predicate), GrouperNamespaceID{}, noPredicateSizeLimit)
	require.False(t, slice.CanMergeWithSlice(testSlice))
}

func TestSliceMergeWithSlice_SamePredicate(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	slice := deps.newTestSlice(t, r, nil, nil)
	totalExecutables := len(slice.pendingExecutables)

	incomingRange := NewRange(tasks.MinimumKey, NewRandomKeyInRange(r))
	incomingSlice := deps.newTestSlice(t, incomingRange, nil, nil)
	totalExecutables += len(incomingSlice.pendingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	require.Len(t, mergedSlices, 1)

	validateMergedSlice(t, slice, incomingSlice, mergedSlices, totalExecutables)
}

func TestSliceMergeWithSlice_SameRange(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := deps.newTestSlice(t, r, namespaceIDs, nil)
	totalExecutables := len(slice.pendingExecutables)

	taskTypes := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
	}
	incomingSlice := deps.newTestSlice(t, r, nil, taskTypes)
	totalExecutables += len(incomingSlice.pendingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	require.Len(t, mergedSlices, 1)

	validateMergedSlice(t, slice, incomingSlice, mergedSlices, totalExecutables)
}

func TestSliceMergeWithSlice_MaxPredicateSizeApplied(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := deps.newTestSlice(t, r, namespaceIDs, nil)
	slice.maxPredicateSizeFn = func() int { return 4 }
	totalExecutables := len(slice.pendingExecutables)

	taskTypes := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
	}
	incomingSlice := deps.newTestSlice(t, r, nil, taskTypes)
	totalExecutables += len(incomingSlice.pendingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	require.Len(t, mergedSlices, 1)

	require.True(t, mergedSlices[0].Scope().Predicate.Equals(predicates.Universal[tasks.Task]()))
}

func TestSliceMergeWithSlice_SameMinKey(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := deps.newTestSlice(t, r, namespaceIDs, nil)
	totalExecutables := len(slice.pendingExecutables)

	incomingRange := NewRange(
		r.InclusiveMin,
		NewRandomKeyInRange(NewRange(r.InclusiveMin, tasks.MaximumKey)),
	)
	incomingNamespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	incomingSlice := deps.newTestSlice(t, incomingRange, incomingNamespaceIDs, nil)
	totalExecutables += len(incomingSlice.pendingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	require.Len(t, mergedSlices, 2)

	validateMergedSlice(t, slice, incomingSlice, mergedSlices, totalExecutables)
}

func TestSliceMergeWithSlice_SameMaxKey(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := deps.newTestSlice(t, r, namespaceIDs, nil)
	totalExecutables := len(slice.pendingExecutables)

	incomingRange := NewRange(
		NewRandomKeyInRange(NewRange(tasks.MinimumKey, r.ExclusiveMax)),
		r.ExclusiveMax,
	)
	incomingNamespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	incomingSlice := deps.newTestSlice(t, incomingRange, incomingNamespaceIDs, nil)
	totalExecutables += len(incomingSlice.pendingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	require.Len(t, mergedSlices, 2)

	validateMergedSlice(t, slice, incomingSlice, mergedSlices, totalExecutables)
}

func TestSliceMergeWithSlice_DifferentMinMaxKey(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := deps.newTestSlice(t, r, namespaceIDs, nil)
	totalExecutables := len(slice.pendingExecutables)

	incomingMinKey := NewRandomKeyInRange(NewRange(r.InclusiveMin, r.ExclusiveMax))
	incomingRange := NewRange(
		incomingMinKey,
		NewRandomKeyInRange(NewRange(incomingMinKey, tasks.MaximumKey)),
	)
	incomingNamespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	incomingSlice := deps.newTestSlice(t, incomingRange, incomingNamespaceIDs, nil)
	totalExecutables += len(incomingSlice.pendingExecutables)

	validateSliceState(t, slice)
	validateSliceState(t, incomingSlice)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	require.Len(t, mergedSlices, 3)

	validateMergedSlice(t, slice, incomingSlice, mergedSlices, totalExecutables)
}

func TestSliceCompactWithSlice(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r1 := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice1 := deps.newTestSlice(t, r1, namespaceIDs, nil)
	totalExecutables := len(slice1.pendingExecutables)

	r2 := NewRandomRange()
	taskTypes := []enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT, enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT}
	slice2 := deps.newTestSlice(t, r2, nil, taskTypes)
	totalExecutables += len(slice2.pendingExecutables)

	validateSliceState(t, slice1)
	validateSliceState(t, slice2)

	compactedSlice := slice1.CompactWithSlice(slice2).(*SliceImpl)
	compactedRange := compactedSlice.Scope().Range
	require.True(t, compactedRange.ContainsRange(r1))
	require.True(t, compactedRange.ContainsRange(r2))
	require.Equal(t,
		tasks.MinKey(r1.InclusiveMin, r2.InclusiveMin),
		compactedRange.InclusiveMin,
	)
	require.Equal(t,
		tasks.MaxKey(r1.ExclusiveMax, r2.ExclusiveMax),
		compactedRange.ExclusiveMax,
	)
	require.True(t,
		tasks.OrPredicates(slice1.scope.Predicate, slice2.scope.Predicate).
			Equals(compactedSlice.scope.Predicate),
	)

	validateSliceState(t, compactedSlice)
	require.Len(t, compactedSlice.pendingExecutables, totalExecutables)

	require.Panics(t, func() { slice1.stateSanityCheck() })
	require.Panics(t, func() { slice2.stateSanityCheck() })
}

func TestSliceShrinkScope_ShrinkRange(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()

	slice := NewSlice(nil, deps.executableFactory, deps.monitor, NewScope(r, predicate), GrouperNamespaceID{}, noPredicateSizeLimit)
	slice.iterators = deps.randomIteratorsInRange(r, rand.Intn(2), nil)

	executables := deps.randomExecutablesInRange(r, 5)
	slices.SortFunc(executables, func(a, b Executable) int {
		return a.GetKey().CompareTo(b.GetKey())
	})

	firstPendingIdx := len(executables)
	numAcked := 0
	for idx, executable := range executables {
		mockExecutable := executable.(*MockExecutable)
		mockExecutable.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
		mockExecutable.EXPECT().GetTask().Return(mockExecutable).AnyTimes()

		acked := rand.Intn(10) < 8
		if acked {
			mockExecutable.EXPECT().State().Return(ctasks.TaskStateAcked).MaxTimes(1)
			numAcked++
		} else {
			mockExecutable.EXPECT().State().Return(ctasks.TaskStatePending).MaxTimes(1)
			if firstPendingIdx == len(executables) {
				firstPendingIdx = idx
			}
		}

		slice.pendingExecutables[executable.GetKey()] = executable
	}

	require.Equal(t, numAcked, slice.ShrinkScope())
	require.Len(t, slice.pendingExecutables, len(executables)-numAcked)
	validateSliceState(t, slice)

	newInclusiveMin := r.ExclusiveMax
	if len(slice.iterators) != 0 {
		newInclusiveMin = slice.iterators[0].Range().InclusiveMin
	}

	if numAcked != len(executables) {
		newInclusiveMin = tasks.MinKey(newInclusiveMin, executables[firstPendingIdx].GetKey())
	}

	require.Equal(t, NewRange(newInclusiveMin, r.ExclusiveMax), slice.Scope().Range)
}

func TestSliceShrinkScope_ShrinkPredicate(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()

	slice := NewSlice(nil, deps.executableFactory, deps.monitor, NewScope(r, predicate), GrouperNamespaceID{}, noPredicateSizeLimit)
	slice.iterators = []Iterator{} // manually set iterators to be empty to trigger predicate update

	executables := deps.randomExecutablesInRange(r, 100)
	slices.SortFunc(executables, func(a, b Executable) int {
		return a.GetKey().CompareTo(b.GetKey())
	})

	pendingNamespaceID := []string{uuid.New(), uuid.New()}
	require.True(t, len(pendingNamespaceID) <= shrinkPredicateMaxPendingKeys)
	for _, executable := range executables {
		mockExecutable := executable.(*MockExecutable)

		mockExecutable.EXPECT().GetTask().Return(mockExecutable).AnyTimes()

		acked := rand.Intn(10) < 8
		if acked {
			mockExecutable.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
			mockExecutable.EXPECT().State().Return(ctasks.TaskStateAcked).MaxTimes(1)
		} else {
			mockExecutable.EXPECT().GetNamespaceID().Return(pendingNamespaceID[rand.Intn(len(pendingNamespaceID))]).AnyTimes()
			mockExecutable.EXPECT().State().Return(ctasks.TaskStatePending).MaxTimes(1)
		}

		slice.executableTracker.add(executable)
	}

	slice.ShrinkScope()
	validateSliceState(t, slice)

	namespacePredicate, ok := slice.Scope().Predicate.(*tasks.NamespacePredicate)
	require.True(t, ok)
	for namespaceID := range namespacePredicate.NamespaceIDs {
		require.True(t, slices.Index(pendingNamespaceID, namespaceID) != -1)
	}
}

func TestSliceSelectTasks_NoError(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)

	numTasks := 20
	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {

			mockTasks := make([]tasks.Task, 0, numTasks)
			for i := 0; i != numTasks; i++ {
				mockTask := tasks.NewMockTask(deps.controller)
				key := NewRandomKeyInRange(paginationRange)
				mockTask.EXPECT().GetKey().Return(key).AnyTimes()
				mockTask.EXPECT().GetVisibilityTime().Return(time.Now()).AnyTimes()

				namespaceID := namespaceIDs[rand.Intn(len(namespaceIDs))]
				if i >= numTasks/2 {
					namespaceID = uuid.New() // should be filtered out
				}
				mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
				mockTasks = append(mockTasks, mockTask)
			}

			slices.SortFunc(mockTasks, func(a, b tasks.Task) int {
				return a.GetKey().CompareTo(b.GetKey())
			})

			return mockTasks, nil, nil
		}
	}

	for _, batchSize := range []int{1, 2, 5, 10, 20, 100} {
		slice := NewSlice(paginationFnProvider, deps.executableFactory, deps.monitor, NewScope(r, predicate), GrouperNamespaceID{}, noPredicateSizeLimit)

		executables := make([]Executable, 0, numTasks)
		for {
			selectedExecutables, err := slice.SelectTasks(DefaultReaderId, batchSize)
			require.NoError(t, err)
			if len(selectedExecutables) == 0 {
				break
			}

			executables = append(executables, selectedExecutables...)
		}

		require.Len(t, executables, numTasks/2) // half of tasks should be filtered out based on its namespaceID
		require.Empty(t, slice.iterators)
	}
}

func TestSliceSelectTasks_Error_NoLoadedTasks(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()

	numTasks := 20
	loadErr := true
	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			if loadErr {
				loadErr = false
				return nil, nil, errors.New("some random load task error")
			}

			mockTasks := make([]tasks.Task, 0, numTasks)
			for i := 0; i != numTasks; i++ {
				mockTask := tasks.NewMockTask(deps.controller)
				key := NewRandomKeyInRange(paginationRange)
				mockTask.EXPECT().GetKey().Return(key).AnyTimes()
				mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
				mockTask.EXPECT().GetVisibilityTime().Return(time.Now()).AnyTimes()
				mockTasks = append(mockTasks, mockTask)
			}

			slices.SortFunc(mockTasks, func(a, b tasks.Task) int {
				return a.GetKey().CompareTo(b.GetKey())
			})

			return mockTasks, nil, nil
		}
	}

	slice := NewSlice(paginationFnProvider, deps.executableFactory, deps.monitor, NewScope(r, predicate), GrouperNamespaceID{}, noPredicateSizeLimit)
	_, err := slice.SelectTasks(DefaultReaderId, 100)
	require.Error(t, err)

	executables, err := slice.SelectTasks(DefaultReaderId, 100)
	require.NoError(t, err)
	require.Len(t, executables, numTasks)
	require.Empty(t, slice.iterators)
}

func TestSliceSelectTasks_Error_WithLoadedTasks(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()

	numTasks := 20
	loadErr := false
	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			defer func() {
				loadErr = !loadErr
			}()

			if loadErr {
				return nil, nil, errors.New("some random load task error")
			}

			mockTasks := make([]tasks.Task, 0, numTasks)
			for i := 0; i != numTasks; i++ {
				mockTask := tasks.NewMockTask(deps.controller)
				key := NewRandomKeyInRange(paginationRange)
				mockTask.EXPECT().GetKey().Return(key).AnyTimes()
				mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
				mockTask.EXPECT().GetVisibilityTime().Return(time.Now()).AnyTimes()
				mockTasks = append(mockTasks, mockTask)
			}

			slices.SortFunc(mockTasks, func(a, b tasks.Task) int {
				return a.GetKey().CompareTo(b.GetKey())
			})

			return mockTasks, []byte{1, 2, 3}, nil
		}
	}

	slice := NewSlice(paginationFnProvider, deps.executableFactory, deps.monitor, NewScope(r, predicate), GrouperNamespaceID{}, noPredicateSizeLimit)
	executables, err := slice.SelectTasks(DefaultReaderId, 100)
	require.NoError(t, err)
	require.Len(t, executables, numTasks)
	require.True(t, slice.MoreTasks())
}

func TestSliceMoreTasks(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	slice := deps.newTestSlice(t, NewRandomRange(), nil, nil)

	require.True(t, slice.MoreTasks())

	slice.pendingExecutables = nil
	require.True(t, slice.MoreTasks())

	slice.iterators = nil
	require.False(t, slice.MoreTasks())
}

func TestSliceClear(t *testing.T) {
	t.Parallel()

	deps := setupSliceTest(t)
	slice := deps.newTestSlice(t, NewRandomRange(), nil, nil)
	for _, executable := range slice.pendingExecutables {
		executable.(*MockExecutable).EXPECT().State().Return(ctasks.TaskStatePending).Times(1)
		executable.(*MockExecutable).EXPECT().Cancel().Times(1)
	}

	slice.Clear()
	require.Empty(t, slice.pendingExecutables)
	require.Len(t, slice.iterators, 1)
	require.Equal(t, slice.scope.Range, slice.iterators[0].Range())
}

func validateMergedSlice(
	t *testing.T,
	currentSlice *SliceImpl,
	incomingSlice *SliceImpl,
	mergedSlices []Slice,
	expectedTotalExecutables int,
) {
	t.Helper()

	expectedMergedRange := currentSlice.scope.Range.Merge(incomingSlice.scope.Range)
	actualMergedRange := mergedSlices[0].Scope().Range
	for _, mergedSlice := range mergedSlices {
		actualMergedRange = actualMergedRange.Merge(mergedSlice.Scope().Range)

		containedByCurrent := currentSlice.scope.Range.ContainsRange(mergedSlice.Scope().Range)
		containedByIncoming := incomingSlice.scope.Range.ContainsRange(mergedSlice.Scope().Range)
		if containedByCurrent && containedByIncoming {
			require.True(t, tasks.OrPredicates(
				currentSlice.scope.Predicate,
				incomingSlice.scope.Predicate,
			).Equals(mergedSlice.Scope().Predicate))
		} else if containedByCurrent {
			require.True(t, currentSlice.scope.Predicate.Equals(mergedSlice.Scope().Predicate))
		} else if containedByIncoming {
			require.True(t, incomingSlice.scope.Predicate.Equals(mergedSlice.Scope().Predicate))
		} else if currentSlice.scope.Predicate.Equals(incomingSlice.scope.Predicate) {
			require.True(t, currentSlice.scope.Predicate.Equals(mergedSlice.Scope().Predicate))
		} else {
			require.Fail(t, "Merged slice range not contained by the merging slices")
		}

	}
	require.True(t, expectedMergedRange.Equals(actualMergedRange))

	actualTotalExecutables := 0
	for _, mergedSlice := range mergedSlices {
		mergedSliceImpl := mergedSlice.(*SliceImpl)
		validateSliceState(t, mergedSliceImpl)
		actualTotalExecutables += len(mergedSliceImpl.pendingExecutables)
	}
	require.Equal(t, expectedTotalExecutables, actualTotalExecutables)

	require.Panics(t, func() { currentSlice.stateSanityCheck() })
	require.Panics(t, func() { incomingSlice.stateSanityCheck() })
}

func validateSliceState(
	t *testing.T,
	slice *SliceImpl,
) {
	t.Helper()

	require.NotNil(t, slice.executableFactory)

	for _, executable := range slice.pendingExecutables {
		require.True(t, slice.scope.Contains(executable))
	}

	r := slice.Scope().Range
	for idx, iterator := range slice.iterators {
		require.True(t, r.ContainsRange(iterator.Range()))
		if idx != 0 {
			currentRange := iterator.Range()
			previousRange := slice.iterators[idx-1].Range()
			require.False(t, currentRange.CanMerge(previousRange))
			require.True(t, previousRange.ExclusiveMax.CompareTo(currentRange.InclusiveMin) < 0)
		}
	}

	require.False(t, slice.destroyed)
}
