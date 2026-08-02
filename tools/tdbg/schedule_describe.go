package tdbg

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	scheduler "go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type scheduleDescribeReport struct {
	Summary           scheduleDescribeSummary       `json:"summary"`
	Components        scheduleDescribeComponents    `json:"components"`
	SearchAttributes  scheduleSearchAttributes      `json:"searchAttributes"`
	SupportingNodes   []*scheduleDescribeNode       `json:"supportingNodes"`
	LogicalTasks      []*scheduleDescribeTask       `json:"-"`
	LogicalTaskGroups []scheduleDescribeTaskGroup   `json:"-"`
	EventTimeline     []scheduleDescribeEvent       `json:"-"`
	DecodeErrors      []scheduleDescribeDecodeError `json:"decodeErrors"`
}

type scheduleDescribeSummary struct {
	Namespace  string `json:"namespace"`
	ScheduleID string `json:"scheduleId"`

	CreatedAt     string `json:"-"`
	UpdatedAt     string `json:"-"`
	Status        string `json:"-"`
	OverlapPolicy string `json:"-"`
}

type scheduleDescribeComponents struct {
	Scheduler   *scheduleDescribeComponent   `json:"scheduler"`
	Generator   *scheduleDescribeComponent   `json:"generator"`
	Invoker     *scheduleDescribeComponent   `json:"invoker"`
	Backfillers []*scheduleDescribeComponent `json:"backfillers"`
}

type scheduleDescribeComponent struct {
	Node              *scheduleDescribeNode `json:"node"`
	EventLog          *scheduleDescribeNode `json:"eventLog"`
	LastProcessedTime string                `json:"-"`
	Buffer            []scheduleBufferEntry `json:"-"`
}

type scheduleDescribeNode struct {
	Path        string            `json:"path"`
	Node        *decodedChasmNode `json:"node"`
	DecodeError string            `json:"decodeError"`

	MetadataPretty string `json:"-"`
	DecodedPretty  string `json:"-"`
	Pretty         string `json:"-"`
}

type scheduleDescribeTask struct {
	OwnerPath string          `json:"ownerPath"`
	Kind      string          `json:"kind"`
	Task      json.RawMessage `json:"task"`

	TaskFQN       string `json:"-"`
	ScheduledTime string `json:"-"`
	TypeID        uint32 `json:"-"`
	Pretty        string `json:"-"`
}

type scheduleDescribeTaskGroup struct {
	Kind  string
	Label string
	Tasks []*scheduleDescribeTask
}

type scheduleBufferEntry struct {
	Index       int    `json:"-"`
	State       string `json:"-"`
	NominalTime string `json:"-"`
	ActualTime  string `json:"-"`
	StartTime   string `json:"-"`
	RawPretty   string `json:"-"`
}

type scheduleDescribeEvent struct {
	Time        string          `json:"-"`
	Delta       string          `json:"deltaFromPrevious"`
	Source      string          `json:"source"`
	SourcePath  string          `json:"sourcePath"`
	SourceIndex int             `json:"sourceIndex"`
	Event       json.RawMessage `json:"event"`
	RawPretty   string          `json:"-"`

	time time.Time
}

type scheduleDescribeDecodeError struct {
	Path  string `json:"path"`
	Error string `json:"error"`
}

type scheduleSearchAttributes struct {
	Framework []scheduleSearchAttribute `json:"framework"`
	Custom    json.RawMessage           `json:"custom"`

	Pretty string `json:"-"`
}

type scheduleSearchAttribute struct {
	Alias     string `json:"alias"`
	Field     string `json:"field"`
	ValueType string `json:"valueType"`
	Present   bool   `json:"present"`
	Value     any    `json:"value"`
}

type scheduleDecodedNodeJSON struct {
	Metadata        json.RawMessage           `json:"metadata"`
	DecodedData     json.RawMessage           `json:"decodedData"`
	RawData         json.RawMessage           `json:"rawData"`
	NodeType        string                    `json:"nodeType"`
	ComponentFQN    string                    `json:"componentFQN"`
	SideEffectTasks []scheduleDecodedTaskJSON `json:"sideEffectTasks"`
	PureTasks       []scheduleDecodedTaskJSON `json:"pureTasks"`
}

type scheduleDecodedTaskJSON struct {
	TypeID              uint32          `json:"typeId"`
	TaskFQN             string          `json:"taskFQN"`
	Destination         string          `json:"destination"`
	ScheduledTime       string          `json:"scheduledTime"`
	DecodedData         json.RawMessage `json:"decodedData"`
	RawData             json.RawMessage `json:"rawData"`
	VersionedTransition json.RawMessage `json:"versionedTransition"`
	PhysicalTaskStatus  int32           `json:"physicalTaskStatus"`
}

func (n *scheduleDescribeNode) MarshalJSON() ([]byte, error) {
	node := scheduleDecodedNodeJSON{
		SideEffectTasks: []scheduleDecodedTaskJSON{},
		PureTasks:       []scheduleDecodedTaskJSON{},
	}
	if n.Node != nil {
		node.Metadata = encodeProtoJSON(n.Node.Metadata)
		node.DecodedData = n.Node.DecodedData
		node.RawData = encodeProtoJSON(n.Node.RawData)
		node.NodeType = n.Node.NodeType
		node.ComponentFQN = n.Node.ComponentFQN
		node.SideEffectTasks = makeScheduleDecodedTasksJSON(n.Node.SideEffectTasks)
		node.PureTasks = makeScheduleDecodedTasksJSON(n.Node.PureTasks)
	}
	return json.Marshal(struct {
		Path        string                  `json:"path"`
		Node        scheduleDecodedNodeJSON `json:"node"`
		DecodeError string                  `json:"decodeError"`
	}{
		Path:        n.Path,
		Node:        node,
		DecodeError: n.DecodeError,
	})
}

func makeScheduleDecodedTasksJSON(tasks []*decodedTask) []scheduleDecodedTaskJSON {
	result := make([]scheduleDecodedTaskJSON, 0, len(tasks))
	for _, task := range tasks {
		result = append(result, scheduleDecodedTaskJSON{
			TypeID:              task.TypeID,
			TaskFQN:             task.TaskFQN,
			Destination:         task.Destination,
			ScheduledTime:       task.ScheduledTime,
			DecodedData:         task.DecodedData,
			RawData:             encodeProtoJSON(task.RawData),
			VersionedTransition: encodeProtoJSON(task.VersionedTransition),
			PhysicalTaskStatus:  task.PhysicalTaskStatus,
		})
	}
	return result
}

func AdminDescribeSchedule(c *cli.Context, clientFactory ClientFactory) error {
	format := strings.ToLower(c.String(FlagFormat))
	if format != "json" && format != "html" {
		return fmt.Errorf("unsupported --%s value %q: expected json or html", FlagFormat, format)
	}

	namespace, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}
	scheduleID, err := getRequiredOption(c, FlagScheduleID)
	if err != nil {
		return err
	}

	ctx, cancel := newContext(c)
	defer cancel()
	resp, err := clientFactory.AdminClient(c).DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace:   namespace,
		Execution:   &commonpb.WorkflowExecution{WorkflowId: scheduleID},
		Archetype:   string(chasm.SchedulerArchetype),
		ArchetypeId: chasm.SchedulerArchetypeID,
	})
	if err != nil {
		return fmt.Errorf("unable to describe CHASM schedule mutable state: %w", err)
	}

	report, err := buildScheduleDescribeReport(namespace, scheduleID, resp)
	if err != nil {
		return err
	}

	output, err := getOutputFile(c.String(FlagOutputFilename), c.App.Writer)
	if err != nil {
		return err
	}
	var renderErr error
	if format == "html" {
		renderErr = renderScheduleDescribeHTML(output, report)
	} else {
		renderErr = renderScheduleDescribeJSON(output, report)
	}
	closeErr := output.Close()
	if renderErr != nil {
		return renderErr
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close output: %w", closeErr)
	}
	return nil
}

func buildScheduleDescribeReport(
	namespace string,
	scheduleID string,
	resp *adminservice.DescribeMutableStateResponse,
) (*scheduleDescribeReport, error) {
	if resp == nil || resp.GetDatabaseMutableState() == nil {
		return nil, errors.New("no database mutable state returned")
	}
	nodes := resp.GetDatabaseMutableState().GetChasmNodes()
	if len(nodes) == 0 {
		return nil, errors.New("execution does not contain CHASM schedule nodes")
	}

	registry, err := newChasmRegistry(log.NewNoopLogger())
	if err != nil {
		return nil, fmt.Errorf("failed to create CHASM registry: %w", err)
	}
	decodedNodes, err := decodeChasmNodesWithEncoder(nodes, registry, marshalScheduleProtoJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to decode CHASM nodes: %w", err)
	}

	report := &scheduleDescribeReport{
		Summary:         scheduleDescribeSummary{Namespace: namespace, ScheduleID: scheduleID},
		Components:      scheduleDescribeComponents{Backfillers: []*scheduleDescribeComponent{}},
		SupportingNodes: []*scheduleDescribeNode{},
		DecodeErrors:    []scheduleDescribeDecodeError{},
	}
	reportNodes := make(map[string]*scheduleDescribeNode, len(decodedNodes))
	paths := make([]string, 0, len(decodedNodes))
	for path := range decodedNodes {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, path := range paths {
		reportNode := makeScheduleDescribeNode(path, decodedNodes[path])
		if knownData, decodeErr := decodeKnownScheduleDataNode(path, nodes[path]); knownData != nil || decodeErr != nil {
			if decodeErr != nil {
				reportNode.DecodeError = decodeErr.Error()
				report.DecodeErrors = append(report.DecodeErrors, scheduleDescribeDecodeError{Path: path, Error: decodeErr.Error()})
			} else {
				reportNode.Node.DecodedData = knownData
				reportNode.DecodedPretty = prettyJSON(knownData)
				reportNode.Pretty = prettyValue(reportNode)
			}
		}
		reportNodes[path] = reportNode
		componentAttributes := nodes[path].GetMetadata().GetComponentAttributes()
		report.LogicalTasks = append(report.LogicalTasks, makeScheduleTasks(
			path,
			"pure",
			componentAttributes.GetPureTasks(),
			decodedNodes[path].PureTasks,
		)...)
		report.LogicalTasks = append(report.LogicalTasks, makeScheduleTasks(
			path,
			"side-effect",
			componentAttributes.GetSideEffectTasks(),
			decodedNodes[path].SideEffectTasks,
		)...)
	}

	rootState := &schedulerpb.SchedulerState{}
	if err := decodeScheduleBlob(nodes[""].GetData(), rootState); err != nil {
		return nil, fmt.Errorf("failed to decode scheduler root state: %w", err)
	}
	report.Summary.CreatedAt = protoTime(rootState.GetInfo().GetCreateTime())
	report.Summary.UpdatedAt = protoTime(rootState.GetInfo().GetUpdateTime())
	report.Summary.OverlapPolicy = rootState.GetSchedule().GetPolicies().GetOverlapPolicy().String()
	switch {
	case rootState.GetClosed():
		report.Summary.Status = "closed"
	case rootState.GetSchedule().GetState().GetPaused():
		report.Summary.Status = "paused"
	default:
		report.Summary.Status = "running"
	}
	report.SearchAttributes, err = buildScheduleSearchAttributes(registry, rootState, nodes, reportNodes)
	if err != nil {
		return nil, err
	}

	report.Components.Scheduler = makeScheduleComponent(reportNodes[""])
	report.Components.Generator = componentForPath(reportNodes, "Generator")
	report.Components.Invoker = componentForPath(reportNodes, "Invoker")

	for _, path := range paths {
		node := reportNodes[path]
		switch {
		case node.ComponentFQN() == "scheduler.backfiller":
			report.Components.Backfillers = append(report.Components.Backfillers, makeScheduleComponent(node))
		case node.ComponentFQN() == "scheduler.eventlog":
			continue
		case path == "", path == "Generator", path == "Invoker":
			continue
		default:
			report.SupportingNodes = append(report.SupportingNodes, node)
		}
	}

	attachScheduleEventLogs(report, reportNodes)
	if err := populateScheduleBuffer(report.Components.Invoker, nodes["Invoker"]); err != nil {
		return nil, fmt.Errorf("failed to decode Invoker buffer: %w", err)
	}
	report.EventTimeline, err = buildScheduleEventTimeline(nodes, report)
	if err != nil {
		return nil, err
	}
	sort.Slice(report.LogicalTasks, func(i, j int) bool {
		left, right := report.LogicalTasks[i], report.LogicalTasks[j]
		if left.ScheduledTime != right.ScheduledTime {
			return left.ScheduledTime < right.ScheduledTime
		}
		if left.OwnerPath != right.OwnerPath {
			return left.OwnerPath < right.OwnerPath
		}
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		return left.TypeID < right.TypeID
	})
	for _, group := range []scheduleDescribeTaskGroup{
		{Kind: "pure", Label: "Pure"},
		{Kind: "side-effect", Label: "Side effect"},
	} {
		for _, task := range report.LogicalTasks {
			if task.Kind == group.Kind {
				group.Tasks = append(group.Tasks, task)
			}
		}
		if len(group.Tasks) > 0 {
			report.LogicalTaskGroups = append(report.LogicalTaskGroups, group)
		}
	}
	return report, nil
}

func componentForPath(nodes map[string]*scheduleDescribeNode, path string) *scheduleDescribeComponent {
	if node := nodes[path]; node != nil {
		return makeScheduleComponent(node)
	}
	return nil
}

func buildScheduleSearchAttributes(
	registry *chasm.Registry,
	root *schedulerpb.SchedulerState,
	nodes map[string]*persistencespb.ChasmNode,
	reportNodes map[string]*scheduleDescribeNode,
) (scheduleSearchAttributes, error) {
	type currentValue struct {
		present bool
		value   any
	}
	values := map[string]currentValue{
		"ScheduleId":      {present: root.GetScheduleId() != "", value: root.GetScheduleId()},
		"ExecutionStatus": {present: true, value: map[bool]string{true: "Completed", false: "Running"}[root.GetClosed()]},
	}
	if !root.GetSentinel() {
		values[sadefs.TemporalSchedulePaused] = currentValue{
			present: true,
			value:   root.GetSchedule().GetState().GetPaused(),
		}
		if !root.GetClosed() {
			generatorState := &schedulerpb.GeneratorState{}
			if err := decodeScheduleBlob(nodes["Generator"].GetData(), generatorState); err != nil {
				return scheduleSearchAttributes{}, fmt.Errorf("failed to decode Generator search attributes: %w", err)
			}
			if futureActionTimes := generatorState.GetFutureActionTimes(); len(futureActionTimes) > 0 {
				values[scheduler.ScheduleNextActionTimeName] = currentValue{present: true, value: protoTime(futureActionTimes[0])}
			}
			if root.GetIdleCloseTime() != nil {
				values[scheduler.ScheduleIdleCloseTimeName] = currentValue{present: true, value: protoTime(root.GetIdleCloseTime())}
			}

			invokerState := &schedulerpb.InvokerState{}
			if err := decodeScheduleBlob(nodes["Invoker"].GetData(), invokerState); err != nil {
				return scheduleSearchAttributes{}, fmt.Errorf("failed to decode Invoker search attributes: %w", err)
			}
			var runningWorkflowCount, recentActionCount int64
			for _, start := range invokerState.GetBufferedStarts() {
				if start.GetRunId() != "" {
					recentActionCount++
					if start.GetCompleted() == nil {
						runningWorkflowCount++
					}
				}
			}
			values[scheduler.ScheduleRunningWorkflowCountName] = currentValue{present: true, value: runningWorkflowCount}
			values[scheduler.ScheduleBufferedStartsCountName] = currentValue{
				present: true,
				value:   int64(len(invokerState.GetBufferedStarts())) - recentActionCount,
			}
		}
	}

	result := scheduleSearchAttributes{
		Framework: []scheduleSearchAttribute{},
		Custom:    encodeProtoJSON(&commonpb.SearchAttributes{}),
	}
	if component, ok := registry.ComponentByID(chasm.SchedulerArchetypeID); ok {
		mapper := component.SearchAttributesMapper()
		fields := make([]string, 0, len(mapper.SATypeMap()))
		for field := range mapper.SATypeMap() {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		for _, field := range fields {
			alias, aliasErr := mapper.Alias(field)
			if aliasErr != nil {
				return scheduleSearchAttributes{}, fmt.Errorf("failed to resolve search attribute field %q: %w", field, aliasErr)
			}
			current := values[alias]
			result.Framework = append(result.Framework, scheduleSearchAttribute{
				Alias:     alias,
				Field:     field,
				ValueType: mapper.SATypeMap()[field].String(),
				Present:   current.present,
				Value:     current.value,
			})
		}
	}
	paused := values[sadefs.TemporalSchedulePaused]
	result.Framework = append(result.Framework, scheduleSearchAttribute{
		Alias:     sadefs.TemporalSchedulePaused,
		Field:     sadefs.TemporalSchedulePaused,
		ValueType: enumspb.INDEXED_VALUE_TYPE_BOOL.String(),
		Present:   paused.present,
		Value:     paused.value,
	})
	sort.Slice(result.Framework, func(i, j int) bool { return result.Framework[i].Alias < result.Framework[j].Alias })
	if custom := reportNodes["Visibility$SA"]; custom != nil && custom.Node != nil {
		result.Custom = custom.Node.DecodedData
	}
	result.Pretty = prettyValue(result)
	return result, nil
}

func makeScheduleComponent(node *scheduleDescribeNode) *scheduleDescribeComponent {
	if node == nil {
		return nil
	}
	component := &scheduleDescribeComponent{Node: node}
	var decoded struct {
		LastProcessedTime string `json:"lastProcessedTime"`
	}
	if json.Unmarshal(node.Node.DecodedData, &decoded) == nil {
		component.LastProcessedTime = decoded.LastProcessedTime
	}
	return component
}

func makeScheduleDescribeNode(path string, node *decodedChasmNode) *scheduleDescribeNode {
	normalized := *node
	normalized.DecodedData = decodePayloadsInJSON(node.DecodedData)
	normalized.PureTasks = normalizeScheduleTasks(node.PureTasks)
	normalized.SideEffectTasks = normalizeScheduleTasks(node.SideEffectTasks)
	reportNode := &scheduleDescribeNode{
		Path: path,
		Node: &normalized,
	}
	reportNode.MetadataPretty = prettyJSON(encodeProtoJSON(normalized.Metadata))
	reportNode.DecodedPretty = prettyJSON(normalized.DecodedData)
	reportNode.Pretty = prettyValue(reportNode)
	return reportNode
}

func normalizeScheduleTasks(tasks []*decodedTask) []*decodedTask {
	normalized := make([]*decodedTask, 0, len(tasks))
	for _, task := range tasks {
		copy := *task
		copy.DecodedData = decodePayloadsInJSON(task.DecodedData)
		normalized = append(normalized, &copy)
	}
	return normalized
}

func (n *scheduleDescribeNode) ComponentFQN() string {
	if n == nil || n.Node == nil {
		return ""
	}
	return n.Node.ComponentFQN
}

func (n *scheduleDescribeNode) NodeType() string {
	if n == nil || n.Node == nil {
		return ""
	}
	return n.Node.NodeType
}

func makeScheduleTasks(
	ownerPath string,
	kind string,
	attributes []*persistencespb.ChasmComponentAttributes_Task,
	decodedTasks []*decodedTask,
) []*scheduleDescribeTask {
	result := make([]*scheduleDescribeTask, 0, len(attributes))
	for index, attribute := range attributes {
		var decoded *decodedTask
		if index < len(decodedTasks) {
			decoded = decodedTasks[index]
		}
		taskFQN := ""
		var decodedData json.RawMessage
		if decoded != nil {
			taskFQN = decoded.TaskFQN
			decodedData = decodePayloadsInJSON(decoded.DecodedData)
		}
		taskJSON, err := json.Marshal(struct {
			Attributes  json.RawMessage `json:"attributes"`
			TaskFQN     string          `json:"taskFQN"`
			DecodedData json.RawMessage `json:"decodedData"`
		}{
			Attributes:  encodeProtoJSON(attribute),
			TaskFQN:     taskFQN,
			DecodedData: decodedData,
		})
		if err != nil {
			taskJSON = []byte("null")
		}
		reportTask := &scheduleDescribeTask{
			OwnerPath:     ownerPath,
			Kind:          kind,
			Task:          taskJSON,
			TypeID:        attribute.GetTypeId(),
			TaskFQN:       taskFQN,
			ScheduledTime: protoTime(attribute.GetScheduledTime()),
		}
		reportTask.Pretty = prettyValue(reportTask)
		result = append(result, reportTask)
	}
	return result
}

func decodeKnownScheduleDataNode(path string, node *persistencespb.ChasmNode) (json.RawMessage, error) {
	var message proto.Message
	switch {
	case path == "LastCompletionResult":
		message = &schedulerpb.LastCompletionResult{}
	case path == "Visibility":
		message = &persistencespb.ChasmVisibilityData{}
	case strings.HasSuffix(path, "$Memo"):
		message = &commonpb.Memo{}
	case strings.HasSuffix(path, "$SA"):
		message = &commonpb.SearchAttributes{}
	default:
		return nil, nil
	}
	if err := decodeScheduleBlob(node.GetData(), message); err != nil {
		return nil, err
	}
	return decodePayloadsInJSON(encodeProtoJSON(message)), nil
}

func decodeScheduleBlob(blob *commonpb.DataBlob, message proto.Message) error {
	if blob == nil || len(blob.GetData()) == 0 {
		return nil
	}
	return serialization.Decode(blob, message)
}

func encodeProtoJSON(message proto.Message) json.RawMessage {
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil
	}
	data, err := marshalScheduleProtoJSON(message)
	if err != nil {
		return nil
	}
	return json.RawMessage(data)
}

func marshalScheduleProtoJSON(message proto.Message) ([]byte, error) {
	return (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(message)
}

func prettyJSON(data json.RawMessage) string {
	if len(data) == 0 {
		return ""
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return string(data)
	}
	pretty, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return string(data)
	}
	return string(pretty)
}

func prettyValue(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return prettyJSON(data)
}

func protoTime(value *timestamppb.Timestamp) string {
	if value == nil {
		return ""
	}
	return value.AsTime().UTC().Format(time.RFC3339Nano)
}

func attachScheduleEventLogs(report *scheduleDescribeReport, nodes map[string]*scheduleDescribeNode) {
	report.Components.Scheduler.EventLog = nodes["EventLog"]
	if report.Components.Generator != nil {
		report.Components.Generator.EventLog = nodes["Generator$EventLog"]
	}
	if report.Components.Invoker != nil {
		report.Components.Invoker.EventLog = nodes["Invoker$EventLog"]
	}
	for _, backfiller := range report.Components.Backfillers {
		separator := strings.Index(backfiller.Node.Path, "#")
		if separator < 0 {
			continue
		}
		id := backfiller.Node.Path[separator+1:]
		backfiller.EventLog = nodes["Backfillers$"+id+"$EventLog"]
	}
}

func populateScheduleBuffer(component *scheduleDescribeComponent, node *persistencespb.ChasmNode) error {
	if component == nil || node == nil {
		return nil
	}
	state := &schedulerpb.InvokerState{}
	if err := decodeScheduleBlob(node.GetData(), state); err != nil {
		return err
	}
	for index, start := range state.GetBufferedStarts() {
		raw := decodePayloadsInJSON(encodeProtoJSON(start))
		entry := scheduleBufferEntry{
			Index:       index,
			NominalTime: protoTime(start.GetNominalTime()),
			ActualTime:  protoTime(start.GetActualTime()),
			StartTime:   protoTime(start.GetStartTime()),
			RawPretty:   prettyJSON(raw),
		}
		switch {
		case start.GetCompleted() != nil:
			entry.State = "completed"
		case start.GetRunId() != "":
			entry.State = "running"
		case start.GetAttempt() < 0:
			entry.State = "deferred"
		case start.GetAttempt() == 0:
			entry.State = "new"
		case start.GetAttempt() > 1:
			entry.State = "retrying"
		default:
			entry.State = "ready"
		}
		component.Buffer = append(component.Buffer, entry)
	}
	return nil
}

func buildScheduleEventTimeline(
	nodes map[string]*persistencespb.ChasmNode,
	report *scheduleDescribeReport,
) ([]scheduleDescribeEvent, error) {
	var events []scheduleDescribeEvent
	for path, node := range nodes {
		reportNode := findReportNode(report, path)
		if reportNode == nil || reportNode.ComponentFQN() != "scheduler.eventlog" {
			continue
		}
		state := &schedulerpb.EventLog{}
		if err := decodeScheduleBlob(node.GetData(), state); err != nil {
			return nil, fmt.Errorf("failed to decode EventLog at %q: %w", path, err)
		}
		for index, event := range state.GetEvents() {
			eventTime := event.GetTime().AsTime().UTC()
			raw := decodePayloadsInJSON(encodeProtoJSON(event))
			events = append(events, scheduleDescribeEvent{
				Time:        eventTime.Format(time.RFC3339Nano),
				Source:      eventSource(path),
				SourcePath:  path,
				SourceIndex: index,
				Event:       raw,
				RawPretty:   prettyJSON(raw),
				time:        eventTime,
			})
		}
	}
	sort.SliceStable(events, func(i, j int) bool {
		if !events[i].time.Equal(events[j].time) {
			return events[i].time.Before(events[j].time)
		}
		if events[i].SourcePath != events[j].SourcePath {
			return events[i].SourcePath < events[j].SourcePath
		}
		return events[i].SourceIndex < events[j].SourceIndex
	})
	for index := 1; index < len(events); index++ {
		events[index].Delta = events[index].time.Sub(events[index-1].time).String()
	}
	return events, nil
}

func findReportNode(report *scheduleDescribeReport, path string) *scheduleDescribeNode {
	components := []*scheduleDescribeComponent{
		report.Components.Scheduler,
		report.Components.Generator,
		report.Components.Invoker,
	}
	components = append(components, report.Components.Backfillers...)
	for _, component := range components {
		if component == nil {
			continue
		}
		if component.Node != nil && component.Node.Path == path {
			return component.Node
		}
		if component.EventLog != nil && component.EventLog.Path == path {
			return component.EventLog
		}
	}
	for _, node := range report.SupportingNodes {
		if node.Path == path {
			return node
		}
	}
	return nil
}

func eventSource(path string) string {
	switch {
	case path == "EventLog":
		return "Scheduler"
	case path == "Generator$EventLog":
		return "Generator"
	case path == "Invoker$EventLog":
		return "Invoker"
	case strings.HasPrefix(path, "Backfillers$"):
		parts := strings.Split(path, "$")
		if len(parts) > 1 {
			return "Backfiller " + parts[1]
		}
	}
	return path
}

func renderScheduleDescribeJSON(output interface{ Write([]byte) (int, error) }, report *scheduleDescribeReport) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return fmt.Errorf("failed to encode schedule report as JSON: %w", err)
	}
	return nil
}
