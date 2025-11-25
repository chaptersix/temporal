package configs

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
)

func TestCallerTypeToPriorityMapping(t *testing.T) {
	for _, priority := range CallerTypeToPriority {
		index := slices.Index(APIPrioritiesOrdered, priority)
		require.NotEqual(t, -1, index)
	}
}

func TestAPIPrioritiesOrdered(t *testing.T) {
	for idx := range APIPrioritiesOrdered[1:] {
		require.True(t, APIPrioritiesOrdered[idx] < APIPrioritiesOrdered[idx+1])
	}
}

func TestOperatorPrioritized(t *testing.T) {
	rateFn := func() float64 { return 5 }
	operatorRPSRatioFn := func() float64 { return 0.2 }
	limiter := NewPriorityRateLimiter(rateFn, operatorRPSRatioFn)

	operatorRequest := quotas.NewRequest(
		"/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution",
		1,
		"",
		headers.CallerTypeOperator,
		-1,
		"")

	apiRequest := quotas.NewRequest(
		"/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution",
		1,
		"",
		headers.CallerTypeAPI,
		-1,
		"")

	requestTime := time.Now()
	limitCount := 0

	for i := 0; i < 12; i++ {
		if !limiter.Allow(requestTime, apiRequest) {
			limitCount++
			require.True(t, limiter.Allow(requestTime, operatorRequest))
		}
	}
	require.Equal(t, 2, limitCount)
}
