package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/testlogger"
)

func publicBatchHarness(t *testing.T, count int, tweaks *schedulerconfig.Tweakables) *treeHarness {
	t.Helper()
	h := executingPublicTree(t, nil)
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly))
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	library := NewNilLibrary()
	library.tweaks = func(string) schedulerconfig.Tweakables { return *tweaks }
	require.NoError(t, registry.Register(library))
	h.registry = registry
	h.reload()
	h.now = generationStart().Add(time.Hour).UnixNano()
	s, ctx := h.root(true)
	request, _ := frozenRequest(t)
	request.LastCompletionResult = nil
	request.ContinuedFailure = nil
	require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: generationSpec(generationStart(), generationStart().Add(time.Duration(count-1)*time.Second), time.Second), Revision: s.Revision + 1, After: time.Unix(0, h.now), CatchupWindow: time.Hour, Request: request, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}))
	s.Paused = true
	commitGeneration(h, true)
	coordinatorStep(h, true)
	return h
}

func finishBatchOperation(h *treeHarness, id string) {
	h.t.Helper()
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: id + "/chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, true)
}

func TestPublicBackfillSkipBatchesSurviveFastCompletion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		size, starts int
	}{{"reference default", 450, 1}, {"two per group", 2, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tweaks := schedulerconfig.DefaultTweakables
			tweaks.MaxBufferSize = (tc.size + tweaks.GeneratorBufferReserveSize) * 2
			h := publicBatchHarness(t, 5, &tweaks)
			coordinatorBackfill(h, generationStart(), generationStart().Add(4*time.Second), enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
			s, _ := h.root(false)
			remaining := s.Remaining
			cursor := s.Cursor
			for step := 0; step < 30; step++ {
				s, _ = h.root(false)
				if len(s.Backfills) == 0 {
					break
				}
				previous := s.OperationId
				before := cloneNodes(h.durable)
				coordinatorStep(h, false)
				require.Equal(t, before, h.durable, "failed publication cannot consume group membership")
				coordinatorStep(h, true)
				h.reload()
				s, _ = h.root(false)
				if s.OperationId != previous {
					finishBatchOperation(h, s.OperationId)
				}
			}
			s, ctx := h.root(false)
			require.Empty(t, s.Backfills)
			require.EqualValues(t, tc.starts, s.Started)
			require.EqualValues(t, 5-tc.starts, s.PublicRuntime.Get(ctx).OverlapSkipped)
			require.Equal(t, remaining, s.Remaining)
			require.Equal(t, cursor, s.Cursor)
		})
	}
}

func TestPublicBackfillSkipGroupCapturesExistingBlocker(t *testing.T) {
	t.Parallel()
	tweaks := schedulerconfig.DefaultTweakables
	h := publicBatchHarness(t, 3, &tweaks)
	s, _ := h.root(true)
	s.Paused = false
	h.commit(true)
	id := publicProjectionAdmission(h)
	coordinatorBackfill(h, generationStart(), generationStart().Add(2*time.Second), enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	for step := 0; step < 8; step++ {
		s, _ := h.root(false)
		if s.Backfills[0].SkipBatchRemaining > 0 {
			break
		}
		coordinatorStep(h, true)
	}
	s, _ = h.root(false)
	require.Positive(t, s.Backfills[0].SkipBatchRemaining)
	finishBatchOperation(h, id)
	for step := 0; step < 10; step++ {
		s, _ = h.root(false)
		if len(s.Backfills) == 0 {
			break
		}
		coordinatorStep(h, true)
	}
	s, ctx := h.root(false)
	require.Empty(t, s.Backfills)
	require.EqualValues(t, 1, s.Started)
	require.EqualValues(t, 3, s.PublicRuntime.Get(ctx).OverlapSkipped)
}

func TestPublicBackfillZeroBatchCapacityRecovers(t *testing.T) {
	t.Parallel()
	tweaks := schedulerconfig.DefaultTweakables
	tweaks.MaxBufferSize = 100 // reserve consumes all available capacity
	h := publicBatchHarness(t, 2, &tweaks)
	coordinatorBackfill(h, generationStart(), generationStart().Add(time.Second), enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	coordinatorStep(h, true)
	s, _ := h.root(false)
	require.Greater(t, s.Coordination.At, h.now)
	require.Empty(t, s.Operations)
	before := s.Backfills[0].Continuation.DisposedThrough
	h.now = s.Coordination.At
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.Greater(t, s.Coordination.At, h.now)
	require.Equal(t, before, s.Backfills[0].Continuation.DisposedThrough)
	tweaks.MaxBufferSize = 104 // two candidates, no new command required
	h.now = s.Coordination.At
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.NotEmpty(t, s.OperationId)
	require.EqualValues(t, 1, s.Backfills[0].SkipBatchRemaining)
}

func TestPublicBackfillBatchRevisionAndPolicyReset(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			tweaks := schedulerconfig.DefaultTweakables
			h := publicBatchHarness(t, 3, &tweaks)
			coordinatorBackfill(h, generationStart(), generationStart().Add(2*time.Second), enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED)
			coordinatorStep(h, true)
			coordinatorStep(h, true)
			s, _ := h.root(false)
			require.Positive(t, s.Backfills[0].SkipBatchRemaining)
			finishBatchOperation(h, s.OperationId)
			s, ctx := h.root(true)
			request, _ := frozenRequest(t)
			request.LastCompletionResult = nil
			request.ContinuedFailure = nil
			require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: generationSpec(generationStart(), generationStart().Add(2*time.Second), time.Second), Revision: s.Revision + 1, After: time.Unix(0, h.now), CatchupWindow: time.Hour, Request: request, Policy: policy}))
			commitGeneration(h, true)
			for step := 0; step < 20; step++ {
				s, _ = h.root(false)
				if len(s.Backfills) == 0 {
					break
				}
				previous := s.OperationId
				coordinatorStep(h, true)
				s, _ = h.root(false)
				if s.OperationId != previous {
					finishBatchOperation(h, s.OperationId)
				}
			}
			s, ctx = h.root(false)
			require.Empty(t, s.Backfills)
			if policy == enumspb.SCHEDULE_OVERLAP_POLICY_SKIP {
				require.EqualValues(t, 2, s.Started)
				require.EqualValues(t, 1, s.PublicRuntime.Get(ctx).OverlapSkipped)
			} else {
				require.EqualValues(t, 3, s.Started)
				require.Zero(t, s.PublicRuntime.Get(ctx).OverlapSkipped)
			}
		})
	}
}

func TestPublicBackfillBatchCapacityTracksConcurrentRanges(t *testing.T) {
	t.Parallel()
	tweaks := schedulerconfig.DefaultTweakables
	h := publicBatchHarness(t, 2, &tweaks)
	for count := 1; count <= 4; count++ {
		coordinatorBackfill(h, generationStart(), generationStart().Add(time.Second), enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
		s, ctx := h.root(false)
		require.EqualValues(t, 450/count, s.backfillBatchCapacity(ctx))
	}
	tweaks.GeneratorBufferReserveSize = 490
	s, ctx := h.root(false)
	require.EqualValues(t, 2, s.backfillBatchCapacity(ctx))
}

func TestPublicBackfillBatchBudgetRetainsVacancyLease(t *testing.T) {
	t.Parallel()
	tweaks := schedulerconfig.DefaultTweakables
	h := publicBatchHarness(t, 2, &tweaks)
	coordinatorBackfill(h, generationStart(), generationStart().Add(time.Second), enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	coordinatorStep(h, true) // scan publishes a candidate and reserves its vacancy
	s, ctx := h.root(false)
	owner := s.Coordination.OwnerKey
	require.NotEmpty(t, owner)
	state := s.stateFor(ctx, "")
	state.Root.Unfinished = 4
	planned, ready, _ := s.planWorkFor(ctx, state)
	require.Nil(t, ready)
	require.Equal(t, owner, planned.OwnerKey)
	tweaks.MaxBufferSize = 100
	// A newly competing producer cannot invalidate the older reserved candidate.
	state.Manuals = []decision.ManualCommand{{ID: 1, Nominal: h.now, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}}
	planned, ready, wake := s.planWorkFor(ctx, state)
	require.Nil(t, ready)
	require.Equal(t, owner, planned.OwnerKey)
	require.True(t, wake.After(time.Unix(0, h.now)))
	tweaks.MaxBufferSize = 104
	state.Root.Unfinished = 0
	planned, ready, _ = s.planWorkFor(ctx, state)
	require.NotNil(t, ready)
	require.Equal(t, owner, planned.OwnerKey)
	require.Equal(t, owner, ready.key)
}
