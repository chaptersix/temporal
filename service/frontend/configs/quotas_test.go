package configs

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/temporalapi"
)

var (
	testRateBurstFn        = quotas.NewDefaultIncomingRateBurst(func() float64 { return 5 })
	testOperatorRPSRatioFn = func() float64 { return 0.2 }
)

func TestExecutionAPIToPriorityMapping(t *testing.T) {
	for _, priority := range APIToPriority {
		index := slices.Index(ExecutionAPIPrioritiesOrdered, priority)
		require.NotEqual(t, -1, index)
	}
}

func TestVisibilityAPIToPriorityMapping(t *testing.T) {
	for _, priority := range VisibilityAPIToPriority {
		index := slices.Index(VisibilityAPIPrioritiesOrdered, priority)
		require.NotEqual(t, -1, index)
	}
}

func TestNamespaceReplicationInducingAPIToPriorityMapping(t *testing.T) {
	for _, priority := range NamespaceReplicationInducingAPIToPriority {
		index := slices.Index(NamespaceReplicationInducingAPIPrioritiesOrdered, priority)
		require.NotEqual(t, -1, index)
	}
}

func TestExecutionAPIPrioritiesOrdered(t *testing.T) {
	for idx := range ExecutionAPIPrioritiesOrdered[1:] {
		require.True(t, ExecutionAPIPrioritiesOrdered[idx] < ExecutionAPIPrioritiesOrdered[idx+1])
	}
}

func TestVisibilityAPIPrioritiesOrdered(t *testing.T) {
	for idx := range VisibilityAPIPrioritiesOrdered[1:] {
		require.True(t, VisibilityAPIPrioritiesOrdered[idx] < VisibilityAPIPrioritiesOrdered[idx+1])
	}
}

func TestNamespaceReplicationInducingAPIPrioritiesOrdered(t *testing.T) {
	for idx := range NamespaceReplicationInducingAPIPrioritiesOrdered[1:] {
		require.True(t, NamespaceReplicationInducingAPIPrioritiesOrdered[idx] < NamespaceReplicationInducingAPIPrioritiesOrdered[idx+1])
	}
}

func TestVisibilityAPIs(t *testing.T) {
	apis := map[string]struct{}{
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecution":           {},
		"/temporal.api.workflowservice.v1.WorkflowService/CountWorkflowExecutions":        {},
		"/temporal.api.workflowservice.v1.WorkflowService/ScanWorkflowExecutions":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListOpenWorkflowExecutions":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListClosedWorkflowExecutions":   {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListArchivedWorkflowExecutions": {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkers":                    {},
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeWorker":                 {},

		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkerTaskReachability":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListSchedules":                     {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListBatchOperations":               {},
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeTaskQueueWithReachability": {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListDeployments":                   {},
		"/temporal.api.workflowservice.v1.WorkflowService/GetDeploymentReachability":         {},
	}

	var service workflowservice.WorkflowServiceServer
	ty := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, ty.NumMethod())
	for i := 0; i < ty.NumMethod(); i++ {
		apiName := "/temporal.api.workflowservice.v1.WorkflowService/" + ty.Method(i).Name
		if ty.Method(i).Name == "DescribeTaskQueue" {
			apiName += "WithReachability"
		}
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = VisibilityAPIToPriority[apiName]
		}
	}
	require.Equal(t, apiToPriority, VisibilityAPIToPriority)
}

func TestNamespaceReplicationInducingAPIs(t *testing.T) {
	apis := map[string]struct{}{
		"/temporal.api.workflowservice.v1.WorkflowService/RegisterNamespace":                {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateNamespace":                  {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkerBuildIdCompatibility": {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkerVersioningRules":      {},
	}

	var service workflowservice.WorkflowServiceServer
	ty := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, ty.NumMethod())
	for i := 0; i < ty.NumMethod(); i++ {
		apiName := "/temporal.api.workflowservice.v1.WorkflowService/" + ty.Method(i).Name
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = NamespaceReplicationInducingAPIToPriority[apiName]
		}
	}
	require.Equal(t, apiToPriority, NamespaceReplicationInducingAPIToPriority)
}

func TestAllAPIs(t *testing.T) {
	apisWithPriority := make(map[string]struct{})
	for api := range APIToPriority {
		apisWithPriority[api] = struct{}{}
	}
	for api := range VisibilityAPIToPriority {
		apisWithPriority[api] = struct{}{}
	}
	for api := range NamespaceReplicationInducingAPIToPriority {
		apisWithPriority[api] = struct{}{}
	}
	var service workflowservice.WorkflowServiceServer
	temporalapi.WalkExportedMethods(&service, func(m reflect.Method) {
		_, ok := apisWithPriority["/temporal.api.workflowservice.v1.WorkflowService/"+m.Name]
		require.True(t, ok, "missing priority for API: %v", m.Name)
	})
	_, ok := apisWithPriority[DispatchNexusTaskByNamespaceAndTaskQueueAPIName]
	require.Truef(t, ok, "missing priority for API: %q", DispatchNexusTaskByNamespaceAndTaskQueueAPIName)
	_, ok = apisWithPriority[DispatchNexusTaskByEndpointAPIName]
	require.Truef(t, ok, "missing priority for API: %q", DispatchNexusTaskByEndpointAPIName)
	_, ok = apisWithPriority[CompleteNexusOperation]
	require.Truef(t, ok, "missing priority for API: %q", CompleteNexusOperation)
}

func TestOperatorPriority_Execution(t *testing.T) {
	limiter := NewExecutionPriorityRateLimiter(testRateBurstFn, testOperatorRPSRatioFn)
	testOperatorPrioritized(t, limiter, "DescribeWorkflowExecution")
}

func TestOperatorPriority_Visibility(t *testing.T) {
	limiter := NewVisibilityPriorityRateLimiter(testRateBurstFn, testOperatorRPSRatioFn)
	testOperatorPrioritized(t, limiter, "ListOpenWorkflowExecutions")
}

func TestOperatorPriority_NamespaceReplicationInducing(t *testing.T) {
	limiter := NewNamespaceReplicationInducingAPIPriorityRateLimiter(testRateBurstFn, testOperatorRPSRatioFn)
	testOperatorPrioritized(t, limiter, "RegisterNamespace")
}

func testOperatorPrioritized(t *testing.T, limiter quotas.RequestRateLimiter, api string) {
	operatorRequest := quotas.NewRequest(
		api,
		1,
		"test-namespace",
		headers.CallerTypeOperator,
		-1,
		"")

	apiRequest := quotas.NewRequest(
		api,
		1,
		"test-namespace",
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
