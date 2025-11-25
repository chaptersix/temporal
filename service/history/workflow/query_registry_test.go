package workflow

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/server/common/payloads"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func TestQueryRegistry(t *testing.T) {
	qr := NewQueryRegistry()
	ids := make([]string, 100)
	completionChs := make([]<-chan struct{}, 100)
	for i := 0; i < 100; i++ {
		ids[i], completionChs[i] = qr.BufferQuery(&querypb.WorkflowQuery{})
	}
	assertBufferedState(t, qr, ids...)
	assertHasQueries(t, qr, true, false, false, false)
	assertQuerySizes(t, qr, 100, 0, 0, 0)
	assertChanState(t, false, completionChs...)

	for i := 0; i < 25; i++ {
		err := qr.SetCompletionState(ids[i], &historyi.QueryCompletionState{
			Type: QueryCompletionTypeSucceeded,
			Result: &querypb.WorkflowQueryResult{
				ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
				Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
			},
		})
		require.NoError(t, err)
	}
	assertCompletedState(t, qr, ids[0:25]...)
	assertBufferedState(t, qr, ids[25:]...)
	assertHasQueries(t, qr, true, true, false, false)
	assertQuerySizes(t, qr, 75, 25, 0, 0)
	assertChanState(t, true, completionChs[0:25]...)
	assertChanState(t, false, completionChs[25:]...)

	for i := 25; i < 50; i++ {
		err := qr.SetCompletionState(ids[i], &historyi.QueryCompletionState{
			Type: QueryCompletionTypeUnblocked,
		})
		require.NoError(t, err)
	}
	assertCompletedState(t, qr, ids[0:25]...)
	assertUnblockedState(t, qr, ids[25:50]...)
	assertBufferedState(t, qr, ids[50:]...)
	assertHasQueries(t, qr, true, true, true, false)
	assertQuerySizes(t, qr, 50, 25, 25, 0)
	assertChanState(t, true, completionChs[0:50]...)
	assertChanState(t, false, completionChs[50:]...)

	for i := 50; i < 75; i++ {
		err := qr.SetCompletionState(ids[i], &historyi.QueryCompletionState{
			Type: QueryCompletionTypeFailed,
			Err:  errors.New("err"),
		})
		require.NoError(t, err)
	}
	assertCompletedState(t, qr, ids[0:25]...)
	assertUnblockedState(t, qr, ids[25:50]...)
	assertFailedState(t, qr, ids[50:75]...)
	assertBufferedState(t, qr, ids[75:]...)
	assertHasQueries(t, qr, true, true, true, true)
	assertQuerySizes(t, qr, 25, 25, 25, 25)
	assertChanState(t, true, completionChs[0:75]...)
	assertChanState(t, false, completionChs[75:]...)

	for i := 0; i < 75; i++ {
		switch i % 3 {
		case 0:
			require.Equal(t, errQueryNotExists, qr.SetCompletionState(ids[i], &historyi.QueryCompletionState{
				Type:   QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{},
			}))
		case 1:
			require.Equal(t, errQueryNotExists, qr.SetCompletionState(ids[i], &historyi.QueryCompletionState{
				Type: QueryCompletionTypeUnblocked,
			}))
		case 2:
			require.Equal(t, errQueryNotExists, qr.SetCompletionState(ids[i], &historyi.QueryCompletionState{
				Type: QueryCompletionTypeFailed,
				Err:  errors.New("err"),
			}))
		}
	}
	assertCompletedState(t, qr, ids[0:25]...)
	assertUnblockedState(t, qr, ids[25:50]...)
	assertFailedState(t, qr, ids[50:75]...)
	assertBufferedState(t, qr, ids[75:]...)
	assertHasQueries(t, qr, true, true, true, true)
	assertQuerySizes(t, qr, 25, 25, 25, 25)
	assertChanState(t, true, completionChs[0:75]...)
	assertChanState(t, false, completionChs[75:]...)

	for i := 0; i < 25; i++ {
		qr.RemoveQuery(ids[i])
		assertHasQueries(t, qr, true, i < 24, true, true)
		assertQuerySizes(t, qr, 25, 25-i-1, 25, 25)
	}
	for i := 25; i < 50; i++ {
		qr.RemoveQuery(ids[i])
		assertHasQueries(t, qr, true, false, i < 49, true)
		assertQuerySizes(t, qr, 25, 0, 50-i-1, 25)
	}
	for i := 50; i < 75; i++ {
		qr.RemoveQuery(ids[i])
		assertHasQueries(t, qr, true, false, false, i < 74)
		assertQuerySizes(t, qr, 25, 0, 0, 75-i-1)
	}
	for i := 75; i < 100; i++ {
		qr.RemoveQuery(ids[i])
		assertHasQueries(t, qr, i < 99, false, false, false)
		assertQuerySizes(t, qr, 100-i-1, 0, 0, 0)
	}
	assertChanState(t, true, completionChs[0:75]...)
	assertChanState(t, false, completionChs[75:]...)
}

func assertBufferedState(t *testing.T, qr historyi.QueryRegistry, ids ...string) {
	for _, id := range ids {
		completionCh, err := qr.GetQueryCompletionCh(id)
		require.NoError(t, err)
		require.False(t, closed(completionCh))
		input, err := qr.GetQueryInput(id)
		require.NoError(t, err)
		require.NotNil(t, input)
		completionState, err := qr.GetCompletionState(id)
		require.Equal(t, errQueryNotInCompletionState, err)
		require.Nil(t, completionState)
	}
}

func assertCompletedState(t *testing.T, qr historyi.QueryRegistry, ids ...string) {
	for _, id := range ids {
		completionCh, err := qr.GetQueryCompletionCh(id)
		require.NoError(t, err)
		require.True(t, closed(completionCh))
		input, err := qr.GetQueryInput(id)
		require.NoError(t, err)
		require.NotNil(t, input)
		completionState, err := qr.GetCompletionState(id)
		require.NoError(t, err)
		require.NotNil(t, completionState)
		require.Equal(t, QueryCompletionTypeSucceeded, completionState.Type)
		require.NotNil(t, completionState.Result)
		require.Nil(t, completionState.Err)
	}
}

func assertUnblockedState(t *testing.T, qr historyi.QueryRegistry, ids ...string) {
	for _, id := range ids {
		completionCh, err := qr.GetQueryCompletionCh(id)
		require.NoError(t, err)
		require.True(t, closed(completionCh))
		input, err := qr.GetQueryInput(id)
		require.NoError(t, err)
		require.NotNil(t, input)
		completionState, err := qr.GetCompletionState(id)
		require.NoError(t, err)
		require.NotNil(t, completionState)
		require.Equal(t, QueryCompletionTypeUnblocked, completionState.Type)
		require.Nil(t, completionState.Result)
		require.Nil(t, completionState.Err)
	}
}

func assertFailedState(t *testing.T, qr historyi.QueryRegistry, ids ...string) {
	for _, id := range ids {
		completionCh, err := qr.GetQueryCompletionCh(id)
		require.NoError(t, err)
		require.True(t, closed(completionCh))
		input, err := qr.GetQueryInput(id)
		require.NoError(t, err)
		require.NotNil(t, input)
		completionState, err := qr.GetCompletionState(id)
		require.NoError(t, err)
		require.NotNil(t, completionState)
		require.Equal(t, QueryCompletionTypeFailed, completionState.Type)
		require.Nil(t, completionState.Result)
		require.NotNil(t, completionState.Err)
	}
}

func assertHasQueries(t *testing.T, qr historyi.QueryRegistry, buffered, completed, unblocked, failed bool) {
	require.Equal(t, buffered, qr.HasBufferedQuery())
	require.Equal(t, completed, qr.HasCompletedQuery())
	require.Equal(t, unblocked, qr.HasUnblockedQuery())
	require.Equal(t, failed, qr.HasFailedQuery())
}

func assertQuerySizes(t *testing.T, qr historyi.QueryRegistry, buffered, completed, unblocked, failed int) {
	require.Len(t, qr.GetBufferedIDs(), buffered)
	require.Len(t, qr.GetCompletedIDs(), completed)
	require.Len(t, qr.GetUnblockedIDs(), unblocked)
	require.Len(t, qr.GetFailedIDs(), failed)
}

func assertChanState(t *testing.T, expectedClosed bool, chans ...<-chan struct{}) {
	for _, ch := range chans {
		require.Equal(t, expectedClosed, closed(ch))
	}
}
