package testhooks

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
)

// ScheduleFrontend selects an isolated schedule implementation in a dedicated test cluster.
// List and Count receive a validated query including the native running-status predicate.
type ScheduleFrontend interface {
	CreateSchedule(context.Context, namespace.ID, *workflowservice.CreateScheduleRequest) (*workflowservice.CreateScheduleResponse, error)
	DescribeSchedule(context.Context, namespace.ID, *workflowservice.DescribeScheduleRequest) (*workflowservice.DescribeScheduleResponse, error)
	DeleteSchedule(context.Context, namespace.ID, *workflowservice.DeleteScheduleRequest) (*workflowservice.DeleteScheduleResponse, error)
	ListSchedules(context.Context, namespace.ID, *workflowservice.ListSchedulesRequest, string) (*workflowservice.ListSchedulesResponse, error)
	CountSchedules(context.Context, namespace.ID, *workflowservice.CountSchedulesRequest, string) (*workflowservice.CountSchedulesResponse, error)
	SearchAttributesMapper() *chasm.VisibilitySearchAttributesMapper
}

var FrontendSchedule = newKey[ScheduleFrontend, global]()

// FrontendChasmRegistryProvider registers offline test libraries before frontend services start.
var FrontendChasmRegistryProvider = newKey[func(*chasm.Registry) error, global]()
