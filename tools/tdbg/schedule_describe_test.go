package tdbg

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBuildScheduleDescribeReport(t *testing.T) {
	response := newScheduleDescribeTestResponse(t)
	report, err := buildScheduleDescribeReport("default", "schedule-id", response)
	require.NoError(t, err)

	require.Equal(t, "schedule-id", report.Summary.ScheduleID)
	require.Equal(t, "paused", report.Summary.Status)
	require.Equal(t, "BufferAll", report.Summary.OverlapPolicy)
	require.NotNil(t, report.Components.Scheduler.EventLog)
	require.NotNil(t, report.Components.Invoker.EventLog)
	require.Len(t, report.Components.Backfillers, 1)
	require.NotNil(t, report.Components.Backfillers[0].EventLog)
	require.Equal(t, "2026-08-02T03:48:20Z", report.Components.Generator.LastProcessedTime)
	require.Equal(t, "2026-08-02T03:48:20Z", report.Components.Invoker.LastProcessedTime)
	require.Equal(t, "2026-08-02T03:48:20Z", report.Components.Backfillers[0].LastProcessedTime)

	require.Len(t, report.Components.Invoker.Buffer, 3)
	require.Equal(t, "deferred", report.Components.Invoker.Buffer[0].State)
	require.Equal(t, "running", report.Components.Invoker.Buffer[1].State)
	require.Equal(t, "completed", report.Components.Invoker.Buffer[2].State)
	require.Equal(t, "2026-08-02T03:48:20Z", report.Components.Invoker.Buffer[0].NominalTime)
	require.Equal(t, "2026-08-02T03:48:20.5Z", report.Components.Invoker.Buffer[0].ActualTime)
	require.Equal(t, "2026-08-02T03:48:21.5Z", report.Components.Invoker.Buffer[1].StartTime)

	require.Len(t, report.EventTimeline, 5)
	require.Equal(t, "Backfiller backfill-id", report.EventTimeline[0].Source)
	require.Equal(t, "Scheduler", report.EventTimeline[1].Source)
	require.Equal(t, "Generator", report.EventTimeline[2].Source)
	require.Equal(t, "Invoker", report.EventTimeline[3].Source)
	require.Equal(t, "1s", report.EventTimeline[4].Delta)
	require.Len(t, report.LogicalTasks, 3)
	require.Equal(t, "scheduler.execute", report.LogicalTasks[0].TaskFQN)
	require.Equal(t, "2026-08-02T03:48:20.5Z", report.LogicalTasks[0].ScheduledTime)
	require.Equal(t, "scheduler.generate", report.LogicalTasks[1].TaskFQN)
	require.Equal(t, "2026-08-02T03:48:21Z", report.LogicalTasks[1].ScheduledTime)
	require.Equal(t, "scheduler.backfill", report.LogicalTasks[2].TaskFQN)
	require.Equal(t, "2026-08-02T03:48:22Z", report.LogicalTasks[2].ScheduledTime)
	require.Contains(t, report.LogicalTasks[2].Pretty, `"ownerPath": "Backfillers#backfill-id"`)
	require.Len(t, report.LogicalTaskGroups, 2)
	require.Equal(t, "pure", report.LogicalTaskGroups[0].Kind)
	require.Equal(t, []string{"scheduler.generate", "scheduler.backfill"}, []string{
		report.LogicalTaskGroups[0].Tasks[0].TaskFQN,
		report.LogicalTaskGroups[0].Tasks[1].TaskFQN,
	})
	require.Equal(t, "side-effect", report.LogicalTaskGroups[1].Kind)
	require.Equal(t, "scheduler.execute", report.LogicalTaskGroups[1].Tasks[0].TaskFQN)
	require.Contains(t, string(report.Components.Scheduler.Node.Node.DecodedData), `"fixture":"\u003cdecoded\u003e"`)
	require.Contains(t, string(report.Components.Scheduler.Node.Node.DecodedData), `"encoding":"json/plain"`)
	require.Contains(t, string(report.Components.Scheduler.Node.Node.DecodedData), `"closed":false`)
	require.Contains(t, string(report.Components.Scheduler.Node.Node.DecodedData), `"sentinel":false`)
	require.Contains(t, report.Components.Invoker.Buffer[0].RawPretty, `"hasCallback": false`)
	require.Contains(t, report.Components.Invoker.Buffer[0].RawPretty, `"startTime": null`)
	require.Contains(t, report.SearchAttributes.Pretty, `"alias": "ExecutionStatus"`)
	require.Contains(t, report.SearchAttributes.Pretty, `"value": "Running"`)
	require.Contains(t, report.SearchAttributes.Pretty, `"alias": "ScheduleBufferedStartsCount"`)
	require.Contains(t, report.SearchAttributes.Pretty, `"alias": "TemporalSchedulePaused"`)
	require.Contains(t, report.SearchAttributes.Pretty, `"CustomKeywordField"`)
	require.Contains(t, report.SearchAttributes.Pretty, `"present": false`)
	require.Contains(t, report.SearchAttributes.Pretty, `"value": null`)

	var memoNode *scheduleDescribeNode
	for _, node := range report.SupportingNodes {
		if node.Path == "Visibility$Memo" {
			memoNode = node
			break
		}
	}
	require.NotNil(t, memoNode)
	require.Contains(t, memoNode.Pretty, `"purpose"`)
	require.Contains(t, memoNode.Pretty, `"metadata"`)
	require.Contains(t, memoNode.Pretty, `"rawData"`)
}

func TestRenderScheduleDescribeJSONPreservesCompleteBuffer(t *testing.T) {
	report, err := buildScheduleDescribeReport("default", "schedule-id", newScheduleDescribeTestResponse(t))
	require.NoError(t, err)

	var output bytes.Buffer
	require.NoError(t, renderScheduleDescribeJSON(&output, report))

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
	require.Contains(t, output.String(), `"bufferedStarts"`)
	require.Contains(t, output.String(), `"request-deferred"`)
	require.Contains(t, output.String(), `"request-running"`)
	require.Contains(t, output.String(), `"request-completed"`)
	require.NotContains(t, decoded, "execution")
	require.NotContains(t, decoded, "logicalTasks")
	require.NotContains(t, decoded, "eventTimeline")
	require.Equal(t, []any{}, decoded["decodeErrors"])
	summary, ok := decoded["summary"].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, summary, "spec")
	require.NotContains(t, summary, "action")
	require.NotContains(t, summary, "state")
}

func TestRenderScheduleDescribeHTMLEscapesDataAndShowsInternals(t *testing.T) {
	report, err := buildScheduleDescribeReport("default", `<script>alert("namespace")</script>`, newScheduleDescribeTestResponse(t))
	require.NoError(t, err)
	report.Summary.ScheduleID = `<script>alert("schedule")</script>`

	var output bytes.Buffer
	require.NoError(t, renderScheduleDescribeHTML(&output, report))
	html := output.String()

	require.Contains(t, html, "Invoker buffer")
	require.Contains(t, html, "Event timeline")
	require.Contains(t, html, "Search attributes")
	require.Contains(t, html, "ExecutionStatus")
	require.Contains(t, html, "ScheduleBufferedStartsCount")
	require.Contains(t, html, "request-deferred")
	require.Contains(t, html, "Backfiller backfill-id")
	require.Contains(t, html, `&#34;manual&#34;: true`)
	require.Contains(t, html, ">Nominal</span>")
	require.Contains(t, html, ">Actual</span>")
	require.Contains(t, html, ">Start</span>")
	require.Contains(t, html, `&#34;message&#34;: &#34;processed buffer&#34;`)
	require.Contains(t, html, `<select id="time-display"`)
	require.Contains(t, html, `<option value="relative">Relative</option>`)
	require.Contains(t, html, `<option value="timestamp">Timestamp</option>`)
	require.Contains(t, html, `https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4`)
	require.Contains(t, html, `max-h-[70vh]`)
	require.Contains(t, html, `id="logical-task-list"`)
	require.Less(t, strings.Index(html, `data-task-kind="pure"`), strings.Index(html, `data-task-kind="side-effect"`))
	require.Contains(t, html, `max-h-[40rem]`)
	require.Contains(t, html, `bg-[#edf9f8]`)
	require.Contains(t, html, `bg-[#eef8ff]`)
	require.Contains(t, html, `bg-[#faf1ff]`)
	require.Contains(t, html, `bg-[#fff8df]`)
	require.Equal(t, 3, strings.Count(html, "Last processed"))
	require.Contains(t, html, `class="timestamp mt-1 font-semibold" data-time="2026-08-02T03:48:20Z"`)
	require.Contains(t, html, `const formatGoDuration = (totalSeconds) =>`)
	require.Contains(t, html, "`${minutes}m${remainder}s`")
	require.NotContains(t, html, `id="buffer-search"`)
	require.NotContains(t, html, "<details")
	require.NotContains(t, html, "<summary")
	require.NotContains(t, html, "cache state available")
	require.NotContains(t, html, `<div class="counts">`)
	require.Less(t, strings.Index(html, "Invoker buffer"), strings.Index(html, "Event timeline"))
	require.Less(t, strings.Index(html, "Event timeline"), strings.Index(html, "Component internals"))
	require.NotContains(t, html, `<script>alert("schedule")</script>`)
	require.Contains(t, html, `&lt;script&gt;alert(&#34;schedule&#34;)&lt;/script&gt;`)
	require.True(t, strings.HasPrefix(html, "<!doctype html>"))
}

func TestBuildScheduleDescribeReportRejectsMissingState(t *testing.T) {
	_, err := buildScheduleDescribeReport("default", "schedule-id", &adminservice.DescribeMutableStateResponse{})
	require.EqualError(t, err, "no database mutable state returned")

	_, err = buildScheduleDescribeReport("default", "schedule-id", &adminservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencespb.WorkflowMutableState{},
	})
	require.EqualError(t, err, "execution does not contain CHASM schedule nodes")
}

func newScheduleDescribeTestResponse(t *testing.T) *adminservice.DescribeMutableStateResponse {
	t.Helper()
	baseTime := time.Date(2026, time.August, 2, 3, 48, 20, 0, time.UTC)
	root := &schedulerpb.SchedulerState{
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId: "workflow-id",
						Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
							Metadata: map[string][]byte{"encoding": []byte("json/plain")},
							Data:     []byte(`{"fixture":"<decoded>"}`),
						}}},
					},
				},
			},
			Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL},
			State:    &schedulepb.ScheduleState{Paused: true, Notes: `<script>alert("notes")</script>`},
		},
		Info: &schedulepb.ScheduleInfo{
			CreateTime: timestamppb.New(baseTime.Add(-time.Minute)),
			UpdateTime: timestamppb.New(baseTime),
		},
		Namespace:     "default",
		ScheduleId:    "schedule-id",
		ConflictToken: 2,
	}

	invoker := &schedulerpb.InvokerState{
		LastProcessedTime: timestamppb.New(baseTime),
		BufferedStarts: []*schedulespb.BufferedStart{
			{NominalTime: timestamppb.New(baseTime), ActualTime: timestamppb.New(baseTime.Add(500 * time.Millisecond)), Manual: true, Attempt: -1, RequestId: "request-deferred", WorkflowId: "workflow-deferred"},
			{NominalTime: timestamppb.New(baseTime.Add(time.Second)), ActualTime: timestamppb.New(baseTime.Add(time.Second)), StartTime: timestamppb.New(baseTime.Add(1500 * time.Millisecond)), Attempt: 1, RequestId: "request-running", WorkflowId: "workflow-running", RunId: "run-id"},
			{NominalTime: timestamppb.New(baseTime.Add(2 * time.Second)), Attempt: 1, RequestId: "request-completed", WorkflowId: "workflow-completed", Completed: &schedulespb.CompletedResult{}},
		},
	}

	nodes := map[string]*persistencespb.ChasmNode{
		"": componentNode(t, chasm.SchedulerArchetypeID, root),
		"Generator": componentNodeWithTasks(t, chasm.GenerateTypeID("scheduler.generator"), &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(baseTime),
		}, []*persistencespb.ChasmComponentAttributes_Task{{
			TypeId:        chasm.GenerateTypeID("scheduler.generate"),
			ScheduledTime: timestamppb.New(baseTime.Add(time.Second)),
			Data:          encodeScheduleTestBlob(t, &schedulerpb.GeneratorTask{}),
		}}),
		"Invoker": componentNodeWithAllTasks(t, chasm.GenerateTypeID("scheduler.invoker"), invoker, nil, []*persistencespb.ChasmComponentAttributes_Task{{
			TypeId:        chasm.GenerateTypeID("scheduler.execute"),
			ScheduledTime: timestamppb.New(baseTime.Add(500 * time.Millisecond)),
			Data:          encodeScheduleTestBlob(t, &schedulerpb.InvokerExecuteTask{}),
		}}),
		"Backfillers": {
			Metadata: &persistencespb.ChasmNodeMetadata{Attributes: &persistencespb.ChasmNodeMetadata_CollectionAttributes{
				CollectionAttributes: &persistencespb.ChasmCollectionAttributes{},
			}},
		},
		"Backfillers#backfill-id": componentNodeWithTasks(t, chasm.GenerateTypeID("scheduler.backfiller"), &schedulerpb.BackfillerState{
			BackfillId:        "backfill-id",
			LastProcessedTime: timestamppb.New(baseTime),
			Attempt:           2,
		}, []*persistencespb.ChasmComponentAttributes_Task{{
			TypeId:        chasm.GenerateTypeID("scheduler.backfill"),
			ScheduledTime: timestamppb.New(baseTime.Add(2 * time.Second)),
			Data:          encodeScheduleTestBlob(t, &schedulerpb.BackfillerTask{}),
		}}),
		"EventLog": componentNode(t, chasm.GenerateTypeID("scheduler.eventlog"), &schedulerpb.EventLog{Events: []*schedulerpb.Event{
			{Time: timestamppb.New(baseTime), Message: "added backfiller"},
		}}),
		"Backfillers$backfill-id$EventLog": componentNode(t, chasm.GenerateTypeID("scheduler.eventlog"), &schedulerpb.EventLog{Events: []*schedulerpb.Event{
			{Time: timestamppb.New(baseTime), Message: "backfiller executed"},
		}}),
		"Invoker$EventLog": componentNode(t, chasm.GenerateTypeID("scheduler.eventlog"), &schedulerpb.EventLog{Events: []*schedulerpb.Event{
			{Time: timestamppb.New(baseTime), Message: "enqueued starts"},
			{Time: timestamppb.New(baseTime.Add(time.Second)), Message: "processed buffer"},
		}}),
		"Generator$EventLog": componentNode(t, chasm.GenerateTypeID("scheduler.eventlog"), &schedulerpb.EventLog{Events: []*schedulerpb.Event{
			{Time: timestamppb.New(baseTime), Message: "generated starts"},
		}}),
		"Visibility$Memo": {
			Metadata: &persistencespb.ChasmNodeMetadata{Attributes: &persistencespb.ChasmNodeMetadata_DataAttributes{
				DataAttributes: &persistencespb.ChasmDataAttributes{},
			}},
			Data: encodeScheduleTestBlob(t, &commonpb.Memo{Fields: map[string]*commonpb.Payload{
				"purpose": {
					Metadata: map[string][]byte{"encoding": []byte("json/plain")},
					Data:     []byte(`{"owner":"scheduler-team","enabled":true}`),
				},
			}}),
		},
		"Visibility$SA": {
			Metadata: &persistencespb.ChasmNodeMetadata{Attributes: &persistencespb.ChasmNodeMetadata_DataAttributes{
				DataAttributes: &persistencespb.ChasmDataAttributes{},
			}},
			Data: encodeScheduleTestBlob(t, &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": {
					Metadata: map[string][]byte{"encoding": []byte("json/plain")},
					Data:     []byte(`"custom-value"`),
				},
			}}),
		},
	}

	return &adminservice.DescribeMutableStateResponse{
		ShardId:              "1",
		HistoryAddr:          "127.0.0.1:7234",
		CacheMutableState:    &persistencespb.WorkflowMutableState{},
		DatabaseMutableState: &persistencespb.WorkflowMutableState{ChasmNodes: nodes},
	}
}

func componentNode(t *testing.T, typeID uint32, message proto.Message) *persistencespb.ChasmNode {
	t.Helper()
	return componentNodeWithTasks(t, typeID, message, nil)
}

func componentNodeWithTasks(
	t *testing.T,
	typeID uint32,
	message proto.Message,
	pureTasks []*persistencespb.ChasmComponentAttributes_Task,
) *persistencespb.ChasmNode {
	t.Helper()
	return componentNodeWithAllTasks(t, typeID, message, pureTasks, nil)
}

func componentNodeWithAllTasks(
	t *testing.T,
	typeID uint32,
	message proto.Message,
	pureTasks []*persistencespb.ChasmComponentAttributes_Task,
	sideEffectTasks []*persistencespb.ChasmComponentAttributes_Task,
) *persistencespb.ChasmNode {
	t.Helper()
	return &persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
			ComponentAttributes: &persistencespb.ChasmComponentAttributes{
				TypeId:          typeID,
				PureTasks:       pureTasks,
				SideEffectTasks: sideEffectTasks,
			},
		}},
		Data: encodeScheduleTestBlob(t, message),
	}
}

func encodeScheduleTestBlob(t *testing.T, message proto.Message) *commonpb.DataBlob {
	t.Helper()
	blob, err := serialization.Encode(message)
	require.NoError(t, err)
	return blob
}
