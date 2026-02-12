package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Validates that schedules work across search attribute mutations:
// 1) Create a custom search attribute and a schedule that uses it
// 2) Remove the attribute via operator API
// 3) Re-add the same attribute name with a different type
// 4) Describe the schedule to confirm attributes persist and types are annotated accordingly
// 5) Add another search attribute and update schedule; describe again
func TestSchedule_SearchAttribute_TypeMutation(t *testing.T) {
	s := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, true),
	)

	newContext := func() context.Context {
		return metadata.NewOutgoingContext(testcore.NewContext(), metadata.Pairs(
			headers.ExperimentHeaderName, "chasm-scheduler",
		))
	}

	ctx, cancel := context.WithTimeout(newContext(), 30*time.Second)
	defer cancel()

	dataConverter := testcore.NewTestDataConverter()
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:      s.FrontendGRPCAddress(),
		Namespace:     s.Namespace().String(),
		DataConverter: dataConverter,
	})
	s.NoError(err)
	defer sdkClient.Close()

	wt := "sched-sa-mutation-wt"

	sid := "sched-sa-mutation"
	wid := "sched-sa-mutation-wf"

	// Define custom search attributes
	attrName := "CustomAttrForSchedule"
	anotherAttr := "AnotherAttrForSchedule"

	// Add initial custom attributes via operator API
	_, err = sdkClient.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: s.Namespace().String(),
		SearchAttributes: map[string]enumspb.IndexedValueType{
			attrName:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			anotherAttr: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	})
	s.NoError(err)

	// Prepare schedule with both schedule-level and scheduled-workflow SA
	attrVal := payload.EncodeString("value1")
	anotherAttrVal, err := payload.Encode(true)
	s.NoError(err)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(5 * time.Second)}},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "unused", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					// Also set the attribute on the scheduled workflow so Describe can annotate types
					SearchAttributes: &commonpb.SearchAttributes{
						IndexedFields: map[string]*commonpb.Payload{
							attrName: attrVal,
						},
					},
				},
			},
		},
	}

	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		// Schedule-level search attributes
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				attrName:    attrVal,
				anotherAttr: anotherAttrVal,
			},
		},
	}

	_, err = s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)
	t.Cleanup(func() {
		_, _ = s.FrontendClient().DeleteSchedule(newContext(), &workflowservice.DeleteScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			Identity:   "test",
		})
	})

	// Describe should show attributes and annotate scheduled workflow SA types
	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	s.NoError(err)
	s.Equal(attrVal.Data, desc.GetSearchAttributes().GetIndexedFields()[attrName].GetData())
	s.Equal(anotherAttrVal.Data, desc.GetSearchAttributes().GetIndexedFields()[anotherAttr].GetData())
	// Verify type annotation on scheduled workflow SA
	ann := desc.GetSchedule().GetAction().GetStartWorkflow().GetSearchAttributes().GetIndexedFields()[attrName].GetMetadata()
	s.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD.String(), string(ann[searchattribute.MetadataType]))

	// Remove the attribute, then re-add it with a different type
	_, err = sdkClient.OperatorService().RemoveSearchAttributes(ctx, &operatorservice.RemoveSearchAttributesRequest{
		Namespace:        s.Namespace().String(),
		SearchAttributes: []string{attrName},
	})
	s.NoError(err)

	_, err = sdkClient.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: s.Namespace().String(),
		SearchAttributes: map[string]enumspb.IndexedValueType{
			attrName: enumspb.INDEXED_VALUE_TYPE_TEXT,
		},
	})
	s.NoError(err)

	// Describe again: payloads persist; type annotation should reflect new type
	desc, err = s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	s.NoError(err)
	s.Equal(attrVal.Data, desc.GetSearchAttributes().GetIndexedFields()[attrName].GetData())
	ann = desc.GetSchedule().GetAction().GetStartWorkflow().GetSearchAttributes().GetIndexedFields()[attrName].GetMetadata()
	s.Equal(enumspb.INDEXED_VALUE_TYPE_TEXT.String(), string(ann[searchattribute.MetadataType]))

	// Add another custom attribute type and update schedule-level attributes to include it
	thirdAttr := "ThirdAttrForSchedule"
	thirdVal, err := payload.Encode(int64(42))
	s.NoError(err)
	_, err = sdkClient.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: s.Namespace().String(),
		SearchAttributes: map[string]enumspb.IndexedValueType{
			thirdAttr: enumspb.INDEXED_VALUE_TYPE_INT,
		},
	})
	s.NoError(err)

	_, err = s.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				attrName:    attrVal,
				anotherAttr: anotherAttrVal,
				thirdAttr:   thirdVal,
			},
		},
	})
	s.NoError(err)

	// Final describe should include the added attribute at schedule-level
	desc, err = s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	s.NoError(err)
	s.Equal(thirdVal.Data, desc.GetSearchAttributes().GetIndexedFields()[thirdAttr].GetData())
}
