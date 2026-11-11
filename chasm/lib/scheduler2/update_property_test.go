package scheduler2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

// At most 19 transitions plus setup: two manual accept/grants, up to four full
// replacement attempts with retries, two ordered results, rejected/accepted drain,
// and one later replacement/retry. Grants explicitly bypass coordinator selection;
// these cases verify update/admission composition, not arbitration or enumeration.
// No automatic source scan executes. Specs are empty or simple interval sources,
// queue occupancy is one, and external completion eventually frees the tracked slot.
// Oracle assertions use rapid.T and shrink. Shared treeHarness setup/publication
// helpers still assert through testing.T; helper failures may not be shrunk.
func TestUpdateComposedProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		h := newTreeHarness(t)
		h.now = generationStart().UnixNano()
		s, ctx := h.root(true)
		s.Diagnostics = chasm.NewDataField(ctx.(chasm.MutableContext), &scheduler2pb.Payload{Data: []byte("small update fixture")})
		initialRequest := &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace", WorkflowId: "initial", WorkflowType: nil}
		require.NoError(rt, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: &schedulepb.ScheduleSpec{}, Request: initialRequest, Revision: 1, After: time.Unix(0, h.now), CatchupWindow: time.Minute, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}))
		commitGeneration(h, true)
		want := UpdateConfig{Generation: GenerationConfig{Spec: &schedulepb.ScheduleSpec{}, Request: initialRequest, Revision: 1, CatchupWindow: time.Minute, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}, Limited: true, Remaining: 1}
		cursorNano := h.now
		frozen := map[string]*workflowservice.StartWorkflowExecutionRequest{}
		phase := map[string]decision.Phase{}
		chain := map[string]string{}
		deadlineNano := map[string]int64{}
		wakeNano := map[string]int64{}
		blocker := ""
		started := int64(0)
		queued := false
		queuePolicy := enumspb.ScheduleOverlapPolicy(rapid.IntRange(2, 3).Draw(rt, "queued_buffer_policy"))
		const firstID = "incarnation/manual/1/action/start"
		const secondID = "incarnation/manual/2/action/start"
		firstWorkflow := "initial-" + time.Unix(0, h.now).UTC().Format(time.RFC3339)
		queueWorkflow := ""
		queueNominalNano := int64(0)
		check := func() {
			s, cc := h.root(false)
			require.Equal(rt, want.Generation.Revision, s.Revision)
			require.Equal(rt, want.Paused, s.Paused)
			require.Equal(rt, want.Limited, s.Limited)
			require.Equal(rt, want.Remaining, s.Remaining)
			require.Equal(rt, want.PauseOnFailure, s.PauseOnFailure)
			require.Equal(rt, want.Generation.Policy, s.Policy)
			require.Equal(rt, cursorNano, s.Cursor)
			require.Equal(rt, started, s.Started)
			require.Empty(rt, s.Manuals)
			require.Len(rt, s.Operations, len(frozen))
			pending, unfinished := int64(0), int64(0)
			expectedPending := map[string]int64{}
			require.Equal(rt, blocker, s.BlockingOperationId)
			for id, request := range frozen {
				actual := &workflowservice.StartWorkflowExecutionRequest{}
				require.NoError(rt, proto.Unmarshal(s.Requests[id].Get(cc).Data, actual))
				protorequire.ProtoEqual(rt, request, actual)
				op := s.Operations[id].Get(cc)
				require.Equal(rt, scheduler2pb.Phase(phase[id]), op.Phase)
				require.Equal(rt, chain[id], op.ChainId)
				require.Equal(rt, deadlineNano[id], op.Deadline)
				if phase[id] == decision.Pending {
					pending++
					expectedPending[id] = wakeNano[id]
				}
				if phase[id] != decision.Completed {
					unfinished++
				}
			}
			require.Len(rt, s.PendingStarts, len(expectedPending))
			for id, at := range expectedPending {
				require.Contains(rt, s.PendingStarts, id)
				require.Equal(rt, at, s.PendingStarts[id])
			}
			startType, ok := h.registry.TaskIDFor(&scheduler2pb.StartTask{})
			require.True(rt, ok)
			actualTasks := map[string]int64{}
			for _, node := range h.durable {
				for _, task := range node.GetMetadata().GetComponentAttributes().GetSideEffectTasks() {
					require.Equal(rt, startType, task.TypeId)
					var start scheduler2pb.StartTask
					require.NoError(rt, proto.Unmarshal(task.Data.Data, &start))
					require.NotContains(rt, actualTasks, start.OperationId, "one logical start obligation per pending operation")
					actualTasks[start.OperationId] = task.ScheduledTime.AsTime().UnixNano()
				}
			}
			require.Equal(rt, expectedPending, actualTasks)
			require.Equal(rt, pending, s.UnconfirmedStarts)
			require.Equal(rt, unfinished, s.Unfinished)
			if queued {
				require.Len(rt, s.Queue, 1)
				require.Equal(rt, queuePolicy, s.Queue[0].Policy)
				require.Equal(rt, queueWorkflow, s.Queue[0].WorkflowId)
				require.Equal(rt, queueNominalNano, s.Queue[0].Nominal)
				require.Equal(rt, uint64(2), s.Queue[0].ManualId)
			} else {
				require.Empty(rt, s.Queue)
			}
			g := s.Generator.Get(cc)
			def := g.Definition.Get(cc)
			encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(want.Generation.Request)
			require.NoError(rt, err)
			require.Equal(rt, encoded, def.Request)
			require.Equal(rt, want.Generation.Seed, def.Seed)
			require.Equal(rt, int64(want.Generation.CatchupWindow), g.CatchupWindow)
			var actualSpec schedulepb.ScheduleSpec
			require.NoError(rt, proto.Unmarshal(def.Spec, &actualSpec))
			protorequire.ProtoEqual(rt, want.Generation.Spec, &actualSpec)
		}
		acceptManual(h, &schedulepb.TriggerImmediatelyRequest{}, true)
		h.apply(decision.Event{Kind: decision.DispatchManual, Incarnation: "incarnation"}, true)
		firstRequest := proto.Clone(initialRequest).(*workflowservice.StartWorkflowExecutionRequest)
		firstRequest.RequestId, firstRequest.WorkflowId = firstID, firstWorkflow
		firstRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
		frozen[firstID], phase[firstID] = firstRequest, decision.Pending
		deadlineNano[firstID], wakeNano[firstID] = h.now+int64(60*time.Second), h.now
		blocker = firstID
		check()
		h.now += int64(time.Second)
		queueNominalNano = h.now
		queueWorkflow = "initial-" + time.Unix(0, h.now).UTC().Format(time.RFC3339)
		acceptManual(h, &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: queuePolicy}, true)
		h.apply(decision.Event{Kind: decision.DispatchManual, Incarnation: "incarnation"}, true)
		queued = true
		check()
		replace := func(label string) {
			h.now += int64(time.Second)
			next := UpdateConfig{Generation: GenerationConfig{Spec: &schedulepb.ScheduleSpec{}, Request: &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace", WorkflowId: fmt.Sprintf("template-%d", want.Generation.Revision+1)}, Seed: fmt.Sprintf("seed-%d", want.Generation.Revision+1), Revision: want.Generation.Revision + 1, CatchupWindow: time.Duration(rapid.IntRange(0, 120).Draw(rt, label+"_catchup")) * time.Second, Policy: enumspb.ScheduleOverlapPolicy(rapid.IntRange(1, 6).Draw(rt, label+"_policy"))}, Paused: rapid.Bool().Draw(rt, label+"_paused"), Limited: rapid.Bool().Draw(rt, label+"_limited"), Remaining: int64(rapid.IntRange(0, 5).Draw(rt, label+"_budget")), PauseOnFailure: rapid.Bool().Draw(rt, label+"_failure_pause")}
			if rapid.Bool().Draw(rt, label+"_interval") {
				next.Generation.Spec = generationSpec(generationStart(), generationStart().Add(time.Hour), time.Minute)
			}
			if rapid.Bool().Draw(rt, label+"_reject") {
				before := cloneNodes(h.durable)
				s, cc := h.root(true)
				require.NoError(rt, s.UpdateConfiguration(cc.(chasm.MutableContext), next))
				commitGeneration(h, false)
				require.Equal(rt, before, h.durable)
				check()
			}
			s, cc := h.root(true)
			require.NoError(rt, s.UpdateConfiguration(cc.(chasm.MutableContext), next))
			commitGeneration(h, true)
			want = next
			cursorNano = h.now
			check()
		}
		replacements := rapid.IntRange(1, 4).Draw(rt, "replacement_count")
		for i := 0; i < replacements; i++ {
			replace(fmt.Sprintf("before_drain_%d", i))
		}
		early := rapid.Bool().Draw(rt, "completion_before_ack")
		firstKind, secondKind := decision.Acknowledged, decision.Complete
		if early {
			firstKind, secondKind = secondKind, firstKind
		}
		h.apply(decision.Event{Kind: firstKind, Incarnation: "incarnation", OperationID: firstID, ChainID: "chain"}, true)
		started = 1
		chain[firstID] = "chain"
		phase[firstID] = decision.Started
		if early {
			blocker = ""
			phase[firstID] = decision.Completed
		}
		check()
		h.apply(decision.Event{Kind: secondKind, Incarnation: "incarnation", OperationID: firstID, ChainID: "chain"}, true)
		phase[firstID] = decision.Completed
		blocker = ""
		check()
		before := cloneNodes(h.durable)
		h.apply(decision.Event{Kind: decision.Drain, Incarnation: "incarnation"}, false)
		require.Equal(rt, before, h.durable)
		check()
		h.apply(decision.Event{Kind: decision.Drain, Incarnation: "incarnation"}, true)
		queued = false
		secondRequest := proto.Clone(want.Generation.Request).(*workflowservice.StartWorkflowExecutionRequest)
		secondRequest.WorkflowId, secondRequest.RequestId = queueWorkflow, secondID
		secondRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
		frozen[secondID], phase[secondID] = secondRequest, decision.Pending
		deadlineNano[secondID], wakeNano[secondID] = h.now+int64(60*time.Second), h.now
		blocker = secondID
		check()
		replace("after_drain")
		h.apply(decision.Event{Kind: decision.RetryableFailure, Incarnation: "incarnation", OperationID: secondID}, true)
		wakeNano[secondID] = min(h.now+int64(time.Second), deadlineNano[secondID])
		check()
	})
}
