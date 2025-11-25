package queues

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/temporalproto"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

func TestConvertPredicate_All(t *testing.T) {
	t.Parallel()
	predicate := predicates.Universal[tasks.Task]()
	require.Equal(t, predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
}

func TestConvertPredicate_Empty(t *testing.T) {
	t.Parallel()
	predicate := predicates.Empty[tasks.Task]()
	require.Equal(t, predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
}

func TestConvertPredicate_And(t *testing.T) {
	t.Parallel()
	testCases := []tasks.Predicate{
		predicates.And(
			predicates.Universal[tasks.Task](),
			predicates.Empty[tasks.Task](),
		),
		predicates.And(
			predicates.Or[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewNamespacePredicate([]string{uuid.New()}),
			),
			predicates.Or[tasks.Task](
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
				}),
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
				}),
			),
		),
		predicates.And(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewNamespacePredicate([]string{uuid.New()}),
			),
		),
		predicates.And(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
				}),
			),
		),
	}

	for _, predicate := range testCases {
		require.Equal(t, predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
	}
}

func TestConvertPredicate_Or(t *testing.T) {
	t.Parallel()
	testCases := []tasks.Predicate{
		predicates.Or(
			predicates.Universal[tasks.Task](),
			predicates.Empty[tasks.Task](),
		),
		predicates.Or(
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewNamespacePredicate([]string{uuid.New()}),
			),
			predicates.And[tasks.Task](
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
				}),
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
				}),
			),
		),
		predicates.Or(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewNamespacePredicate([]string{uuid.New()}),
			),
		),
		predicates.Or(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
				}),
			),
		),
	}

	for _, predicate := range testCases {
		require.Equal(t, predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
	}
}

func TestConvertPredicate_Not(t *testing.T) {
	t.Parallel()
	testCases := []tasks.Predicate{
		predicates.Not(predicates.Universal[tasks.Task]()),
		predicates.Not(predicates.Empty[tasks.Task]()),
		predicates.Not(predicates.And[tasks.Task](
			tasks.NewNamespacePredicate([]string{uuid.New()}),
			tasks.NewTypePredicate([]enumsspb.TaskType{}),
		)),
		predicates.Not(predicates.Or[tasks.Task](
			tasks.NewNamespacePredicate([]string{uuid.New()}),
			tasks.NewTypePredicate([]enumsspb.TaskType{}),
		)),
		predicates.Not(predicates.Not(predicates.Empty[tasks.Task]())),
		predicates.Not[tasks.Task](tasks.NewNamespacePredicate([]string{uuid.New()})),
		predicates.Not[tasks.Task](tasks.NewTypePredicate([]enumsspb.TaskType{
			enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		})),
	}

	for _, predicate := range testCases {
		require.Equal(t, predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
	}
}

func TestConvertPredicate_NamespaceID(t *testing.T) {
	t.Parallel()
	testCases := []tasks.Predicate{
		tasks.NewNamespacePredicate(nil),
		tasks.NewNamespacePredicate([]string{}),
		tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New(), uuid.New()}),
	}

	for _, predicate := range testCases {
		require.Equal(t, predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
	}
}

func TestConvertPredicate_TaskType(t *testing.T) {
	t.Parallel()
	testCases := []tasks.Predicate{
		tasks.NewTypePredicate(nil),
		tasks.NewTypePredicate([]enumsspb.TaskType{}),
		tasks.NewTypePredicate([]enumsspb.TaskType{
			enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
			enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		}),
	}

	for _, predicate := range testCases {
		require.Equal(t, predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
	}
}

func TestConvertTaskKey(t *testing.T) {
	t.Parallel()
	key := NewRandomKey()
	require.Equal(t, key, FromPersistenceTaskKey(
		ToPersistenceTaskKey(key),
	))
}

func TestConvertTaskRange(t *testing.T) {
	t.Parallel()
	r := NewRandomRange()
	require.Equal(t, r, FromPersistenceRange(
		ToPersistenceRange(r),
	))
}

func TestConvertScope(t *testing.T) {
	t.Parallel()
	scope := NewScope(
		NewRandomRange(),
		tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New()}),
	)

	require.True(t, temporalproto.DeepEqual(scope, FromPersistenceScope(
		ToPersistenceScope(scope),
	)))
}

func TestConvertQueueState(t *testing.T) {
	t.Parallel()
	readerScopes := map[int64][]Scope{
		0: {},
		1: {
			NewScope(
				NewRandomRange(),
				tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New()}),
			),
		},
		123: {
			NewScope(
				NewRandomRange(),
				tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New()}),
			),
			NewScope(
				NewRandomRange(),
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
					enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
				}),
			),
		},
	}

	queueState := &queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: tasks.NewKey(time.Unix(0, rand.Int63()).UTC(), 0),
	}

	require.True(t, temporalproto.DeepEqual(queueState, FromPersistenceQueueState(
		ToPersistenceQueueState(queueState),
	)))
}
