package scheduler2

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

// TestCoordinatorMixedPublicationProperties checks finite range identities against
// input-derived expectations while manual ingress remains continuously ready.
// Assumptions are explicit: each failed publication is retried successfully next,
// each admitted workflow completes before the next grant, definitions stay fixed,
// and every due work task is delivered. This is a bounded fairness witness, not a
// claim of liveness when external workflows hold capacity indefinitely.
func TestCoordinatorMixedPublicationProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 3).Draw(rt, "range_size")
		policies := []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}
		policy := rapid.SampledFrom(policies).Draw(rt, "policy")
		base := generationStart()
		end := base.Add(time.Duration(n-1) * time.Second)
		now := end.Add(time.Second)
		h := coordinatorHarness(t, generationSpec(base, end, time.Second), base.Add(-time.Nanosecond), now)
		s, _ := h.root(true)
		s.Policy = policy
		h.commit(true)
		coordinatorBackfill(h, base, end, policy)
		coordinatorBackfill(h, base, end, policy)
		type expectedStart struct {
			nominal          int64
			manual, backfill uint64
		}
		expected := map[string]expectedStart{}
		for i := 0; i < n; i++ {
			nominalNano := base.Add(time.Duration(i) * time.Second).UnixNano()
			expected[fmt.Sprintf("incarnation/automatic/%d/action/start", nominalNano)] = expectedStart{nominal: nominalNano}
			for job := uint64(1); job <= 2; job++ {
				expected[fmt.Sprintf("incarnation/backfill/%d/%d/action/start", job, nominalNano)] = expectedStart{nominal: nominalNano, backfill: job}
			}
		}
		seen := map[string]bool{}
		var acceptedManual uint64
		automatic := int64(0)
		cursorNano := base.Add(-time.Nanosecond).UnixNano()
		historical := 0
		finished := false
		for quantum := 0; quantum < 64; quantum++ {
			s, _ = h.root(false)
			// Keep competing ingress ready until both finite sources have finished.
			finiteDone := historical == 3*n && len(s.Backfills) == 0 && !generationState(h).Armed
			if !finiteDone && len(s.Manuals) < 2 {
				lease := s.Coordination.OwnerKey
				nominal := now.Add(time.Duration(rapid.IntRange(-10, 10).Draw(rt, "manual_time")) * time.Second)
				s, ctx := h.root(true)
				require.NoError(rt, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(nominal), OverlapPolicy: policy}))
				commitGeneration(h, true)
				acceptedManual++
				expected[fmt.Sprintf("incarnation/manual/%d/action/start", acceptedManual)] = expectedStart{nominal: nominal.UnixNano(), manual: acceptedManual}
				s, _ = h.root(false)
				if lease != "" {
					require.Equal(rt, lease, s.Coordination.OwnerKey, "new ingress stole an existing vacancy grant")
				}
			}
			s, _ = h.root(false)
			if !s.Coordination.Armed {
				finished = true
				break
			}
			previous := s.OperationId
			if rapid.Bool().Draw(rt, "failed_publication") {
				snapshot := cloneNodes(h.durable)
				physical := len(h.physical)
				coordinatorStep(h, false)
				require.Equal(rt, snapshot, h.durable)
				require.Len(rt, h.physical, physical)
			}
			coordinatorStep(h, true)
			s, ctx := h.root(false)
			if s.OperationId != previous {
				id := s.OperationId
				want, ok := expected[id]
				require.True(rt, ok, "unexpected identity %s", id)
				require.False(rt, seen[id], "duplicate admitted identity %s", id)
				seen[id] = true
				op := s.Operations[id].Get(ctx)
				require.Equal(rt, want.nominal, op.Nominal)
				require.Equal(rt, want.manual, op.ManualId)
				require.Equal(rt, want.backfill, op.BackfillId)
				require.Equal(rt, policy, op.Policy)
				if want.manual == 0 {
					historical++
				}
				if want.manual == 0 && want.backfill == 0 {
					automatic++
					cursorNano = max(cursorNano, want.nominal)
				}
				template, _ := frozenRequest(t)
				expectedRequest := proto.Clone(template).(*workflowservice.StartWorkflowExecutionRequest)
				expectedRequest.RequestId = id
				expectedRequest.WorkflowId = "workflow-" + time.Unix(0, want.nominal).UTC().Format(time.RFC3339)
				if want.manual != 0 || want.backfill != 0 {
					expectedRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
				}
				if policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
					expectedRequest.LastCompletionResult = nil
					expectedRequest.ContinuedFailure = nil
				}
				require.True(rt, proto.Equal(expectedRequest, allowRequest(h, id)), "frozen request differs for %s", id)
				h.apply(decision.Event{Kind: decision.Complete, Incarnation: "incarnation", OperationID: id, ChainID: id + "/chain"}, true)
			}
			s, _ = h.root(false)
			require.Equal(rt, int64(100)-automatic, s.Remaining)
			require.Equal(rt, cursorNano, s.Cursor)
			require.Equal(rt, int64(len(seen)), s.Started)
			require.Zero(rt, s.Unfinished)
			require.Empty(rt, s.BlockingOperationId)
			require.Empty(rt, s.Queue)
			require.LessOrEqual(rt, len(s.Operations), 4)
			require.LessOrEqual(rt, len(s.Backfills), 2)
			require.LessOrEqual(rt, len(s.Manuals), 2)
			require.Zero(rt, logicalCount(h.durable))
		}
		require.True(rt, finished, "finite sources failed to finish under sustained manual competition within64 grants")
		require.Equal(rt, 3*n, historical)
		require.Len(rt, seen, len(expected))
		s, _ = h.root(false)
		require.Empty(rt, s.Backfills)
		require.Empty(rt, s.Manuals)
		require.Zero(rt, pureObligations(h))
	})
}

// TestCoordinatorDelayedCapacityProperties starts with all four slots occupied,
// then releases one random external workflow at least every third quantum (or
// immediately when no work is runnable). Manual ALLOW_ALL competes with two
// tracked backfills; buffering and cancel/terminate replacement are accounted for
// independently from the accepted identities. Effects need not be delivered: the
// environment supplies matching completion evidence, never an RPC-success proxy.
func TestCoordinatorDelayedCapacityProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		base := generationStart()
		end := base.Add(3 * time.Second)
		h := coordinatorHarness(t, generationSpec(base, end, time.Second), base.Add(-time.Nanosecond), end)
		for i := 0; i < 10; i++ {
			s, _ := h.root(false)
			if s.Unfinished == 4 {
				break
			}
			coordinatorStep(h, true)
		}
		s, _ := h.root(false)
		require.Equal(rt, int64(4), s.Unfinished)
		expected := map[string]bool{}
		admitted := map[string]bool{}
		active := map[string]bool{}
		disposed := map[string]bool{}
		for i := 0; i < 4; i++ {
			id := fmt.Sprintf("incarnation/automatic/%d/action/start", base.Add(time.Duration(i)*time.Second).UnixNano())
			expected[id] = true
			admitted[id] = true
			active[id] = true
		}
		policies := []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}
		for job := uint64(1); job <= 2; job++ {
			coordinatorBackfill(h, base, base.Add(2*time.Second), rapid.SampledFrom(policies).Draw(rt, fmt.Sprintf("backfill_policy_%d", job)))
			for i := 0; i < 3; i++ {
				expected[fmt.Sprintf("incarnation/backfill/%d/%d/action/start", job, base.Add(time.Duration(i)*time.Second).UnixNano())] = true
			}
		}
		var manuals uint64
		var completed, superseded int64
		queueID := func(q *scheduler2pb.QueuedOccurrence) string {
			return fmt.Sprintf("incarnation/backfill/%d/%d/action/start", q.BackfillId, q.Nominal)
		}
		finished := false
		for quantum := 0; quantum < 128; quantum++ {
			s, _ = h.root(false)
			if manuals < 8 && len(s.Backfills) > 0 && len(s.Manuals) < 2 {
				lease := s.Coordination.OwnerKey
				coordinatorManual(h)
				manuals++
				expected[fmt.Sprintf("incarnation/manual/%d/action/start", manuals)] = true
				s, _ = h.root(false)
				if lease != "" {
					require.Equal(rt, lease, s.Coordination.OwnerKey)
				}
			}
			// Initial contention is observed before permitting any completion.
			if quantum >= 3 && len(active) > 0 && (quantum%3 == 0 || !s.Coordination.Armed) {
				ids := make([]string, 0, len(active))
				for id := range active {
					ids = append(ids, id)
				}
				slices.Sort(ids)
				id := ids[rapid.IntRange(0, len(ids)-1).Draw(rt, "external_completion")]
				h.apply(decision.Event{Kind: decision.Complete, Incarnation: "incarnation", OperationID: id, ChainID: id + "/chain"}, true)
				delete(active, id)
				completed++
				s, _ = h.root(false)
			}
			if s.Coordination.Armed {
				previous := s.OperationId
				priorQueue := proto.Clone(s.RootState).(*scheduler2pb.RootState).Queue
				if rapid.Bool().Draw(rt, "reject_capacity_commit") {
					snapshot := cloneNodes(h.durable)
					coordinatorStep(h, false)
					require.Equal(rt, snapshot, h.durable)
				}
				coordinatorStep(h, true)
				s, ctx := h.root(false)
				if s.OperationId != previous {
					id := s.OperationId
					require.True(rt, expected[id], "unknown start %s", id)
					require.False(rt, admitted[id])
					require.False(rt, disposed[id])
					op := s.Operations[id].Get(ctx)
					if op.BackfillId > 0 && len(priorQueue) > 0 {
						selected := 0
						for i, q := range priorQueue {
							if q.Policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER || q.Policy == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER {
								selected = i
							}
						}
						require.Equal(rt, queueID(priorQueue[selected]), id, "drain selection violated queued replacement order")
						for i, q := range priorQueue {
							replaced := i < selected && (i == 0 || q.Policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER || q.Policy == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER)
							if replaced {
								lost := queueID(q)
								require.False(rt, disposed[lost])
								disposed[lost] = true
								superseded++
							}
						}
					}
					admitted[id] = true
					active[id] = true
				}
			}
			s, ctx := h.root(false)
			require.Equal(rt, int64(len(active)), s.Unfinished)
			require.LessOrEqual(rt, len(active), 4)
			require.Equal(rt, completed, s.Started)
			require.Equal(rt, int64(96), s.Remaining)
			require.Equal(rt, end.UnixNano(), s.Cursor)
			require.Equal(rt, superseded, s.OverlapSuperseded)
			require.Zero(rt, s.CapacityDropped)
			require.Zero(rt, s.BufferDropped)
			require.LessOrEqual(rt, len(s.Queue), 4)
			require.LessOrEqual(rt, len(s.Operations), 4)
			tracked := ""
			for id := range active {
				require.Contains(rt, s.Operations, id, "unfinished operation was evicted")
				op := s.Operations[id].Get(ctx)
				if op.Policy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
					require.Empty(rt, tracked)
					tracked = id
				}
			}
			require.Equal(rt, tracked, s.BlockingOperationId)
			liveJobs := map[uint64]bool{}
			for _, b := range s.Backfills {
				liveJobs[b.Id] = true
			}
			queued := map[string]bool{}
			for _, q := range s.Queue {
				id := queueID(q)
				require.True(rt, expected[id])
				require.False(rt, queued[id])
				require.False(rt, admitted[id])
				require.False(rt, disposed[id])
				queued[id] = true
			}
			for job := uint64(1); job <= 2; job++ {
				if liveJobs[job] {
					continue
				}
				for i := 0; i < 3; i++ {
					id := fmt.Sprintf("incarnation/backfill/%d/%d/action/start", job, base.Add(time.Duration(i)*time.Second).UnixNano())
					require.True(rt, admitted[id] || disposed[id] || queued[id], "completed backfill lost accepted identity %s", id)
				}
			}
			if !s.Coordination.Armed && len(active) == 0 {
				finished = true
				break
			}
		}
		require.True(rt, finished, "recurring capacity did not finish bounded workload")
		require.Equal(rt, len(expected), len(admitted)+len(disposed))
		s, _ = h.root(false)
		require.Empty(rt, s.Backfills)
		require.Empty(rt, s.Manuals)
		require.Empty(rt, s.Queue)
	})
}
