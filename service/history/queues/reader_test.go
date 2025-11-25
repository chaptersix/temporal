package queues

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type readerTestDeps struct {
	controller            *gomock.Controller
	mockScheduler         *MockScheduler
	mockRescheduler       *MockRescheduler
	mockClusterMetadata   *cluster.MockMetadata
	mockNamespaceRegistry *namespace.MockRegistry
	logger                log.Logger
	metricsHandler        metrics.Handler
	executableFactory     ExecutableFactory
	monitor               *monitorImpl
}

func setupReaderTest(t *testing.T) *readerTestDeps {
	t.Helper()

	controller := gomock.NewController(t)
	mockScheduler := NewMockScheduler(controller)
	mockRescheduler := NewMockRescheduler(controller)
	mockClusterMetadata := cluster.NewMockMetadata(controller)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockNamespaceRegistry := namespace.NewMockRegistry(controller)
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	logger := log.NewTestLogger()
	metricsHandler := metrics.NoopMetricsHandler

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

	return &readerTestDeps{
		controller:            controller,
		mockScheduler:         mockScheduler,
		mockRescheduler:       mockRescheduler,
		mockClusterMetadata:   mockClusterMetadata,
		mockNamespaceRegistry: mockNamespaceRegistry,
		logger:                logger,
		metricsHandler:        metricsHandler,
		executableFactory:     executableFactory,
		monitor:               monitor,
	}
}

func (d *readerTestDeps) newTestReader(
	scopes []Scope,
	paginationFnProvider PaginationFnProvider,
	completionFn ReaderCompletionFn,
) *ReaderImpl {
	slices := make([]Slice, 0, len(scopes))
	for _, scope := range scopes {
		slice := NewSlice(paginationFnProvider, d.executableFactory, d.monitor, scope, GrouperNamespaceID{}, noPredicateSizeLimit)
		slices = append(slices, slice)
	}

	return NewReader(
		DefaultReaderId,
		slices,
		&ReaderOptions{
			BatchSize:            dynamicconfig.GetIntPropertyFn(10),
			MaxPendingTasksCount: dynamicconfig.GetIntPropertyFn(100),
			PollBackoffInterval:  dynamicconfig.GetDurationPropertyFn(200 * time.Millisecond),
			MaxPredicateSize:     dynamicconfig.GetIntPropertyFn(10),
		},
		d.mockScheduler,
		d.mockRescheduler,
		clock.NewRealTimeSource(),
		NewReaderPriorityRateLimiter(func() float64 { return 20 }, 1),
		d.monitor,
		completionFn,
		d.logger,
		d.metricsHandler,
	)
}

func TestReaderStartLoadStop(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)

	r := NewRandomRange()
	scopes := []Scope{NewScope(r, predicates.Universal[tasks.Task]())}

	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		require.Equal(t, r, paginationRange)
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			mockTask := tasks.NewMockTask(deps.controller)
			mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
			mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
			mockTask.EXPECT().GetVisibilityTime().Return(time.Now()).AnyTimes()
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	doneCh := make(chan struct{})
	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		close(doneCh)
		return true
	}).Times(1)
	deps.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	reader := deps.newTestReader(scopes, paginationFnProvider, NoopReaderCompletionFn)
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(scopes[0].Range.ExclusiveMax.FireTime)
	reader.timeSource = mockTimeSource

	reader.Start()
	<-doneCh
	reader.Stop()
}

func TestReaderScopes(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	scopes := NewRandomScopes(10)

	reader := deps.newTestReader(scopes, nil, NoopReaderCompletionFn)
	actualScopes := reader.Scopes()
	for idx, expectedScope := range scopes {
		require.True(t, expectedScope.Equals(actualScopes[idx]))
	}
}

func TestReaderSplitSlices(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	scopes := NewRandomScopes(3)
	reader := deps.newTestReader(scopes, nil, NoopReaderCompletionFn)

	splitter := func(s Slice) ([]Slice, bool) {
		// split head
		if scope := s.Scope(); !scope.Equals(scopes[0]) {
			return nil, false
		}

		// test remove slice
		return nil, true
	}
	reader.SplitSlices(splitter)
	require.Len(t, reader.Scopes(), 2)
	validateSlicesOrdered(t, reader)

	splitter = func(s Slice) ([]Slice, bool) {
		// split tail
		if scope := s.Scope(); !scope.Equals(scopes[2]) {
			return nil, false
		}

		left, right := s.SplitByRange(NewRandomKeyInRange(s.Scope().Range))
		_, right = right.SplitByRange(NewRandomKeyInRange(right.Scope().Range))

		return []Slice{left, right}, true
	}
	reader.SplitSlices(splitter)
	require.Len(t, reader.Scopes(), 3)
	validateSlicesOrdered(t, reader)

	splitter = func(s Slice) ([]Slice, bool) {
		left, right := s.SplitByRange(NewRandomKeyInRange(s.Scope().Range))

		// empty slices should be ignored
		left, empty := left.SplitByRange(left.Scope().Range.ExclusiveMax)
		return []Slice{left, empty, right}, true
	}
	reader.SplitSlices(splitter)
	require.Len(t, reader.Scopes(), 6)
	validateSlicesOrdered(t, reader)
}

func TestReaderMergeSlices(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	scopes := NewRandomScopes(rand.Intn(10))
	reader := deps.newTestReader(scopes, nil, NoopReaderCompletionFn)

	incomingScopes := NewRandomScopes(10)
	// manually set some scopes to be empty and verify they are ignored during merge
	incomingScopes[2].Predicate = predicates.Empty[tasks.Task]()
	incomingScopes[7].Predicate = predicates.Empty[tasks.Task]()

	incomingSlices := make([]Slice, 0, len(incomingScopes))
	for _, incomingScope := range incomingScopes {
		incomingSlices = append(incomingSlices, NewSlice(nil, deps.executableFactory, deps.monitor, incomingScope, GrouperNamespaceID{}, noPredicateSizeLimit))
	}

	reader.MergeSlices(incomingSlices...)

	mergedScopes := reader.Scopes()
	for idx, scope := range mergedScopes[:len(mergedScopes)-1] {
		require.False(t, scope.IsEmpty())
		nextScope := mergedScopes[idx+1]
		if scope.Range.ExclusiveMax.CompareTo(nextScope.Range.InclusiveMin) > 0 {
			panic(fmt.Sprintf(
				"Found overlapping scope in merged slices, left: %v, right: %v",
				scope,
				nextScope,
			))
		}
	}
}

func TestReaderAppendSlices(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	totalScopes := 10
	scopes := NewRandomScopes(totalScopes)
	currentScopes := scopes[:totalScopes/2]
	reader := deps.newTestReader(currentScopes, nil, NoopReaderCompletionFn)

	incomingScopes := scopes[totalScopes/2:]
	incomingScopes[2].Predicate = predicates.Empty[tasks.Task]()
	incomingSlices := make([]Slice, 0, len(incomingScopes))
	for _, incomingScope := range incomingScopes {
		incomingSlices = append(incomingSlices, NewSlice(nil, deps.executableFactory, deps.monitor, incomingScope, GrouperNamespaceID{}, noPredicateSizeLimit))
	}

	reader.AppendSlices(incomingSlices...)

	scopesAfterAppend := reader.Scopes()
	require.Len(t, scopesAfterAppend, totalScopes-1) // one empty scope should be ignored
	for idx, scope := range scopesAfterAppend[:len(scopesAfterAppend)-1] {
		require.False(t, scope.IsEmpty())
		nextScope := scopesAfterAppend[idx+1]
		if scope.Range.ExclusiveMax.CompareTo(nextScope.Range.InclusiveMin) > 0 {
			panic(fmt.Sprintf(
				"Found overlapping scope in appended slices, left: %v, right: %v",
				scope,
				nextScope,
			))
		}
	}
}

func TestReaderShrinkSlices(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	numScopes := 10
	scopes := NewRandomScopes(numScopes)

	// manually set some scopes to be empty
	emptyIdx := map[int]struct{}{0: {}, 2: {}, 5: {}, 9: {}}
	for idx := range emptyIdx {
		scopes[idx].Range.InclusiveMin = scopes[idx].Range.ExclusiveMax
	}

	reader := deps.newTestReader(scopes, nil, NoopReaderCompletionFn)
	completed := reader.ShrinkSlices()
	require.Equal(t, 0, completed)

	actualScopes := reader.Scopes()
	require.Len(t, actualScopes, numScopes-len(emptyIdx))

	expectedScopes := make([]Scope, 0, numScopes-len(emptyIdx))
	for idx, scope := range scopes {
		if _, ok := emptyIdx[idx]; !ok {
			expectedScopes = append(expectedScopes, scope)
		}
	}

	for idx, expectedScope := range expectedScopes {
		require.True(t, expectedScope.Equals(actualScopes[idx]))
	}
}

func TestReaderNotify(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	reader := deps.newTestReader([]Scope{}, nil, NoopReaderCompletionFn)

	// pause will set the throttle timer, which notify is supposed to stop
	reader.Pause(time.Hour)

	reader.Lock()
	require.NotNil(t, reader.throttleTimer)
	reader.Unlock()

	reader.Notify()
	<-reader.notifyCh

	reader.Lock()
	require.Nil(t, reader.throttleTimer)
	reader.Unlock()
}

func TestReaderPause(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	scopes := NewRandomScopes(1)

	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			mockTask := tasks.NewMockTask(deps.controller)
			mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(scopes[0].Range)).AnyTimes()
			mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
			mockTask.EXPECT().GetVisibilityTime().Return(time.Now()).AnyTimes()
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	reader := deps.newTestReader(scopes, paginationFnProvider, NoopReaderCompletionFn)
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(scopes[0].Range.ExclusiveMax.FireTime)
	reader.timeSource = mockTimeSource

	now := time.Now()
	delay := 100 * time.Millisecond
	reader.Pause(delay / 2)

	// check if existing throttle timer will be overwritten
	reader.Pause(delay)

	doneCh := make(chan struct{})
	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		require.True(t, time.Now().After(now.Add(delay)))
		close(doneCh)
		return true
	}).Times(1)
	deps.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	reader.Start()
	<-doneCh
	reader.Stop()
}

func TestReaderLoadAndSubmitTasks_Throttled(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	scopes := NewRandomScopes(1)

	completionFnCalled := false
	reader := deps.newTestReader(scopes, nil, func(_ int64) { completionFnCalled = true })
	reader.Pause(100 * time.Millisecond)

	deps.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	// should be no-op
	reader.loadAndSubmitTasks()
	require.False(t, completionFnCalled)
}

func TestReaderLoadAndSubmitTasks_TooManyPendingTasks(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	scopes := NewRandomScopes(1)

	completionFnCalled := false
	reader := deps.newTestReader(scopes, nil, func(_ int64) { completionFnCalled = true })

	deps.monitor.SetSlicePendingTaskCount(
		reader.slices.Front().Value.(Slice),
		reader.options.MaxPendingTasksCount(),
	)

	// should be no-op
	reader.loadAndSubmitTasks()
	require.False(t, completionFnCalled)
}

func TestReaderLoadAndSubmitTasks_MoreTasks(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	scopes := NewRandomScopes(1)

	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			result := make([]tasks.Task, 0, 100)
			for i := 0; i != 100; i++ {
				mockTask := tasks.NewMockTask(deps.controller)
				mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(scopes[0].Range)).AnyTimes()
				mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
				mockTask.EXPECT().GetVisibilityTime().Return(time.Now()).AnyTimes()
				result = append(result, mockTask)
			}

			return result, nil, nil
		}
	}

	completionFnCalled := false
	reader := deps.newTestReader(scopes, paginationFnProvider, func(_ int64) { completionFnCalled = true })
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(scopes[0].Range.ExclusiveMax.FireTime)
	reader.timeSource = mockTimeSource

	taskSubmitted := 0
	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskSubmitted++
		return true
	}).AnyTimes()
	deps.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	reader.loadAndSubmitTasks()
	<-reader.notifyCh // should trigger next round of load
	require.Equal(t, reader.options.BatchSize(), taskSubmitted)
	require.True(t, scopes[0].Equals(reader.nextReadSlice.Value.(Slice).Scope()))
	require.False(t, completionFnCalled)
}

func TestReaderLoadAndSubmitTasks_NoMoreTasks_HasNextSlice(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	scopes := NewRandomScopes(2)

	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			mockTask := tasks.NewMockTask(deps.controller)
			mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(scopes[0].Range)).AnyTimes()
			mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
			mockTask.EXPECT().GetVisibilityTime().Return(time.Now()).AnyTimes()
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	completionFnCalled := false
	reader := deps.newTestReader(scopes, paginationFnProvider, func(_ int64) { completionFnCalled = true })
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(scopes[0].Range.ExclusiveMax.FireTime)
	reader.timeSource = mockTimeSource

	taskSubmitted := 0
	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskSubmitted++
		return true
	}).AnyTimes()
	deps.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	reader.loadAndSubmitTasks()
	<-reader.notifyCh // should trigger next round of load
	require.Equal(t, 1, taskSubmitted)
	require.True(t, scopes[1].Equals(reader.nextReadSlice.Value.(Slice).Scope()))
	require.False(t, completionFnCalled)
}

func TestReaderLoadAndSubmitTasks_NoMoreTasks_NoNextSlice(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	scopes := NewRandomScopes(1)

	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			mockTask := tasks.NewMockTask(deps.controller)
			mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(scopes[0].Range)).AnyTimes()
			mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
			mockTask.EXPECT().GetVisibilityTime().Return(time.Now()).AnyTimes()
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	completionFnCalled := false
	reader := deps.newTestReader(scopes, paginationFnProvider, func(_ int64) { completionFnCalled = true })
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(scopes[0].Range.ExclusiveMax.FireTime)
	reader.timeSource = mockTimeSource

	taskSubmitted := 0
	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskSubmitted++
		return true
	}).AnyTimes()
	deps.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	reader.loadAndSubmitTasks()
	select {
	case <-reader.notifyCh:
		require.Fail(t, "should not signal notify ch as there's no more task or slice")
	default:
		// should not trigger next round of load
	}
	require.Equal(t, 1, taskSubmitted)
	require.Nil(t, reader.nextReadSlice)
	require.True(t, completionFnCalled)
}

func TestReaderSubmitTask(t *testing.T) {
	t.Parallel()

	deps := setupReaderTest(t)
	r := NewRandomRange()
	scopes := []Scope{NewScope(r, predicates.Universal[tasks.Task]())}
	reader := deps.newTestReader(scopes, nil, NoopReaderCompletionFn)

	mockExecutable := NewMockExecutable(deps.controller)

	pastFireTime := reader.timeSource.Now().Add(-time.Minute)
	mockExecutable.EXPECT().GetKey().Return(tasks.NewKey(pastFireTime, rand.Int63())).Times(1)
	mockExecutable.EXPECT().SetScheduledTime(gomock.Any()).Times(1)
	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(1)
	reader.submit(mockExecutable)

	mockExecutable.EXPECT().GetKey().Return(tasks.NewKey(pastFireTime, rand.Int63())).Times(1)
	mockExecutable.EXPECT().SetScheduledTime(gomock.Any()).Times(1)
	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(false).Times(1)
	mockExecutable.EXPECT().Reschedule().Times(1)
	reader.submit(mockExecutable)

	futureFireTime := reader.timeSource.Now().Add(time.Minute)
	mockExecutable.EXPECT().GetKey().Return(tasks.NewKey(futureFireTime, rand.Int63())).Times(1)
	deps.mockRescheduler.EXPECT().Add(mockExecutable, futureFireTime.Add(common.ScheduledTaskMinPrecision)).Times(1)
	reader.submit(mockExecutable)
}

func validateSlicesOrdered(t *testing.T, reader Reader) {
	t.Helper()
	scopes := reader.Scopes()
	if len(scopes) <= 1 {
		return
	}

	for idx := range scopes[:len(scopes)-1] {
		require.True(t, scopes[idx].Range.ExclusiveMax.CompareTo(scopes[idx+1].Range.InclusiveMin) <= 0)
	}
}

func testTaskTagValueProvider(_ tasks.Task, _ bool, _ *chasm.Registry) string {
	return "testTaskType"
}
