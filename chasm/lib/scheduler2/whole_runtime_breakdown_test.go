//go:build test_dep

package scheduler2

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This compares representations, not end-to-end persistence or behavioral equivalence.
// Legacy invocation constructs a request from current schedule state; selected
// invocation retains the admitted immutable request even across definition edits.
func TestSelectedRuntimeStorageBreakdown(t *testing.T) {
	h, wire := wholeRuntimeTree(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	for range 4 {
		wholeRuntimeDue(t, h, wire)
	}
	s, ctx := h.root(false)
	ready, ok := s.ReadyRecords.Get(ctx, readyRecordKey(1))
	require.True(t, ok)
	op, ok := s.OperationRecords.Get(ctx, ready.OperationId)
	require.True(t, ok)
	record, ok := s.RequestRecords.Get(ctx, ready.OperationId)
	require.True(t, ok)
	request := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(record.Data, request))
	logFields := func(label string, value proto.Message) {
		fields := map[string]int{}
		value.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			single := value.ProtoReflect().New()
			single.Set(field, v)
			fields[string(field.Name())] = proto.Size(single.Interface())
			return true
		})
		encoded, err := json.Marshal(fields)
		require.NoError(t, err)
		t.Logf("FIELDS %s total=%d fields=%s", label, proto.Size(value), encoded)
	}
	logFields("request_record", record)
	logFields("request_skeleton", request)
	logFields("operation", op)
	logFields("callback", request.CompletionCallbacks[0])
	require.Nil(t, request.Header)
	require.Nil(t, request.Input)
	legacy := &schedulerpb.InvokerState{}
	for i := range 10000 {
		nominal := generationStart().Add(time.Duration(i + 1))
		legacy.BufferedStarts = append(legacy.BufferedStarts, &schedulespb.BufferedStart{
			NominalTime: timestamppb.New(nominal), ActualTime: timestamppb.New(nominal),
			OverlapPolicy: op.Policy, RequestId: op.Id, WorkflowId: op.WorkflowId, Attempt: 1,
		})
	}
	t.Logf("LEGACY_STRUCTURAL buffered_start_first=%d invoker_10000=%d; same-length identities; excludes shared schedule and CHASM/execution framing", proto.Size(legacy.BufferedStarts[0]), proto.Size(legacy))
}
