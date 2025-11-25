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

func TestValidateCompletionState(t *testing.T) {
	testCases := []struct {
		ts        *historyi.QueryCompletionState
		expectErr bool
	}{
		{
			ts:        nil,
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type:   QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{},
				Err:    errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
				},
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType:   enumspb.QUERY_RESULT_TYPE_ANSWERED,
					Answer:       payloads.EncodeBytes([]byte{1, 2, 3}),
					ErrorMessage: "err",
				},
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_FAILED,
					Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
				},
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType:   enumspb.QUERY_RESULT_TYPE_FAILED,
					ErrorMessage: "err",
				},
			},
			expectErr: false,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
					Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
				},
			},
			expectErr: false,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type:   QueryCompletionTypeUnblocked,
				Result: &querypb.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeUnblocked,
				Err:  errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeUnblocked,
			},
			expectErr: false,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeFailed,
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type:   QueryCompletionTypeFailed,
				Result: &querypb.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeFailed,
				Err:  errors.New("err"),
			},
			expectErr: false,
		},
	}

	queryImpl := &queryImpl{}
	for _, tc := range testCases {
		if tc.expectErr {
			require.Error(t, queryImpl.validateCompletionState(tc.ts))
		} else {
			require.NoError(t, queryImpl.validateCompletionState(tc.ts))
		}
	}
}

func TestCompletionState_Failed(t *testing.T) {
	completionStateFailed := &historyi.QueryCompletionState{
		Type: QueryCompletionTypeFailed,
		Err:  errors.New("err"),
	}
	testSetCompletionState(t, completionStateFailed)
}

func TestCompletionState_Completed(t *testing.T) {
	answeredCompletionState := &historyi.QueryCompletionState{
		Type: QueryCompletionTypeSucceeded,
		Result: &querypb.WorkflowQueryResult{
			ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
			Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
		},
	}
	testSetCompletionState(t, answeredCompletionState)
}

func TestCompletionState_Unblocked(t *testing.T) {
	unblockedCompletionState := &historyi.QueryCompletionState{
		Type: QueryCompletionTypeUnblocked,
	}
	testSetCompletionState(t, unblockedCompletionState)
}

func testSetCompletionState(t *testing.T, completionState *historyi.QueryCompletionState) {
	query := newQuery(nil)
	ts, err := query.GetCompletionState()
	require.Equal(t, errQueryNotInCompletionState, err)
	require.Nil(t, ts)
	require.False(t, closed(query.getCompletionCh()))
	require.Equal(t, errCompletionStateInvalid, query.setCompletionState(nil))
	require.NoError(t, query.setCompletionState(completionState))
	require.True(t, closed(query.getCompletionCh()))
	actualCompletionState, err := query.GetCompletionState()
	require.NoError(t, err)
	assertCompletionStateEqual(t, completionState, actualCompletionState)
}

func assertCompletionStateEqual(t *testing.T, expected *historyi.QueryCompletionState, actual *historyi.QueryCompletionState) {
	require.Equal(t, expected.Type, actual.Type)
	if expected.Err != nil {
		require.Equal(t, expected.Err.Error(), actual.Err.Error())
	}
	if expected.Result != nil {
		require.EqualValues(t, actual.Result, expected.Result)
	}
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
