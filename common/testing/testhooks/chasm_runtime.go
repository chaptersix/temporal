package testhooks

import (
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
)

// HistoryChasmRuntime exposes actual service dependencies during local test-cluster
// construction. Clients use normal service transport; Engine and Visibility are
// direct in-process handles. Libraries must be registered before services start.
type HistoryChasmRuntime struct {
	Engine            chasm.Engine
	Visibility        chasm.VisibilityManager
	Registry          *chasm.Registry
	FrontendClient    workflowservice.WorkflowServiceClient
	HistoryClient     historyservice.HistoryServiceClient
	NamespaceRegistry namespace.Registry
	DynamicConfig     *dynamicconfig.Collection
	CallbackValidator callbacks.Validator
}
