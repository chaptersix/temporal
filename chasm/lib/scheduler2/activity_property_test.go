package scheduler2

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

// TestActivityNodeProperties uses at most35 command/result/grant events per case (excluding setup).
// Idle closure is disabled; arbitrary execution-clock rollback and delayed start
// evidence are permitted. The oracle consumes accepted inputs and first matching
// evidence, never reducer output, to derive the anchor and operation accounting.
func TestActivityNodeProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		h := newTreeHarness(t)
		h.now = generationStart().UnixNano()
		s, ctx := h.root(true)
		s.Diagnostics = chasm.NewDataField(ctx.(chasm.MutableContext), &scheduler2pb.Payload{Data: []byte("small activity fixture")})
		template, _ := frozenRequest(t)
		require.NoError(rt, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: &schedulepb.ScheduleSpec{}, Request: template, Revision: 1, After: time.Unix(0, h.now), CatchupWindow: time.Minute, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}))
		commitGeneration(h, true)
		coordinatorStep(h, true)
		expectedAnchorNano := h.now
		revision := int64(1)
		sequence := uint64(0)
		started := int64(0)
		type operation struct {
			id                        string
			nominalNano, deadlineNano int64
			phase                     decision.Phase
			observed                  bool
			observedNano              int64
			chain                     string
		}
		type command struct {
			sequence    uint64
			nominalNano int64
		}
		records := map[string]*operation{}
		var history []string
		var pending []command
		check := func() {
			s, ctx := h.root(false)
			require.True(rt, s.ActivitySet)
			require.Equal(rt, expectedAnchorNano, s.ActivityAt)
			require.Equal(rt, revision, s.Revision)
			require.Equal(rt, sequence, s.ManualSequence)
			require.Equal(rt, started, s.Started)
			require.Len(rt, s.Operations, len(records))
			require.Len(rt, s.Manuals, len(pending))
			require.LessOrEqual(rt, len(records), 4)
			independentUnconfirmed := int64(0)
			metadataUnconfirmed := int64(0)
			for id, want := range records {
				field, ok := s.Operations[id]
				require.True(rt, ok, "missing retained operation %s", id)
				got := field.Get(ctx)
				require.Equal(rt, want.phase, decision.Phase(got.Phase))
				require.Equal(rt, want.observed, got.StartObserved)
				require.Equal(rt, want.observedNano, got.StartObservedAt)
				if want.phase == decision.Pending || want.phase == decision.Ambiguous {
					independentUnconfirmed++
				}
				if decision.Phase(got.Phase) == decision.Pending || decision.Phase(got.Phase) == decision.Ambiguous {
					metadataUnconfirmed++
				}
			}
			require.Equal(rt, independentUnconfirmed, s.UnconfirmedStarts)
			require.Equal(rt, metadataUnconfirmed, s.UnconfirmedStarts)
			require.Equal(rt, independentUnconfirmed, s.Unfinished)
		}
		accept := func(nominalNano int64, publish bool) {
			s, ctx := h.root(true)
			err := s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(time.Unix(0, nominalNano)), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL})
			if len(pending) == 4 {
				require.Error(rt, err)
				h.reload()
				return
			}
			require.NoError(rt, err)
			commitGeneration(h, publish)
			if publish {
				sequence++
				pending = append(pending, command{sequence, nominalNano})
				expectedAnchorNano = max(expectedAnchorNano, h.now)
			}
		}
		grant := func() {
			active := 0
			for _, op := range records {
				if op.phase == decision.Pending || op.phase == decision.Ambiguous {
					active++
				}
			}
			if len(pending) == 0 || active == 4 {
				return
			}
			s, _ := h.root(false)
			h.now = max(h.now, s.Coordination.At)
			next := pending[0]
			pending = pending[1:]
			if len(records) == 4 {
				var oldest *operation
				for _, op := range records {
					if op.phase != decision.StartOnly {
						continue
					}
					if oldest == nil || op.nominalNano < oldest.nominalNano || (op.nominalNano == oldest.nominalNano && op.id < oldest.id) {
						oldest = op
					}
				}
				require.NotNil(rt, oldest)
				delete(records, oldest.id)
			}
			id := fmt.Sprintf("incarnation/manual/%d/action/start", next.sequence)
			records[id] = &operation{id: id, nominalNano: next.nominalNano, deadlineNano: h.now + decision.RetryHorizon, phase: decision.Pending}
			history = append(history, id)
			coordinatorStep(h, true)
		}
		result := func(event decision.Event, publish bool) {
			h.apply(event, publish)
			if !publish || event.Incarnation != "incarnation" {
				return
			}
			op := records[event.OperationID]
			if op == nil {
				return
			}
			switch event.Kind {
			case decision.Acknowledged:
				if op.phase != decision.StartOnly {
					op.phase = decision.StartOnly
					op.chain = event.ChainID
					started++
				}
				if event.StartObserved && !op.observed && event.ChainID == op.chain {
					op.observed = true
					op.observedNano = event.StartObservedAt
					expectedAnchorNano = max(expectedAnchorNano, event.StartObservedAt)
				}
			case decision.Complete:
				if op.phase != decision.StartOnly {
					op.phase = decision.StartOnly
					op.chain = event.ChainID
					started++
				}
			case decision.Expire:
				if op.phase == decision.Pending && h.now >= op.deadlineNano {
					op.phase = decision.Ambiguous
				}
			default:
				rt.Fatal("unsupported result in activity oracle")
			}
		}
		// Five starts force cap4 eviction. The oldest nominal carries the largest
		// activity sample; its eviction must leave the independent high-water mark.
		for i := 0; i < 5; i++ {
			h.now++
			accept(generationStart().Add(time.Duration(i)*time.Second).UnixNano(), true)
			grant()
			id := history[len(history)-1]
			sampleNano := h.now - 1
			if i == 0 {
				sampleNano = h.now + int64(100*time.Second)
			}
			ack := decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: id, ChainID: id + "/chain", StartObserved: true, StartObservedAt: sampleNano}
			complete := decision.Event{Kind: decision.Complete, Incarnation: "incarnation", OperationID: id, ChainID: id + "/chain"}
			if i%2 == 0 {
				result(ack, true)
				result(complete, true)
			} else {
				result(complete, true)
				result(ack, true)
			}
			check()
		}
		require.NotContains(rt, records, history[0])
		result(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: history[0], ChainID: history[0] + "/chain", StartObserved: true, StartObservedAt: expectedAnchorNano + int64(time.Hour)}, true)
		lastID := history[len(history)-1]
		result(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: lastID, ChainID: lastID + "/chain", StartObserved: true, StartObservedAt: expectedAnchorNano + int64(time.Hour)}, true)
		check()
		for step := 0; step < 12; step++ {
			h.now += int64(rapid.IntRange(-3, 8).Draw(rt, "clock_delta")) * int64(time.Second)
			action := rapid.IntRange(0, 7).Draw(rt, "action")
			publish := rapid.Bool().Draw(rt, "publish")
			target := history[rapid.IntRange(0, len(history)-1).Draw(rt, "target")]
			event := decision.Event{Incarnation: "incarnation", OperationID: target, ChainID: target + "/chain"}
			switch action {
			case 0:
				accept(h.now+int64(rapid.IntRange(-30, 30).Draw(rt, "manual_nominal"))*int64(time.Second), publish)
			case 1:
				grant()
			case 2, 3, 4, 6:
				event.Kind = decision.Acknowledged
				event.StartObserved = action != 3
				event.StartObservedAt = h.now + int64(rapid.IntRange(-5, 200).Draw(rt, "observation_offset"))*int64(time.Second)
				if action == 4 {
					event.Kind = decision.Complete
					event.StartObserved = false
				}
				if action == 6 {
					event.OperationID = "evicted-or-unknown"
				}
				result(event, publish)
			case 5:
				event.Kind = decision.Expire
				if op := records[target]; op != nil {
					h.now = max(h.now, op.deadlineNano)
				}
				result(event, publish)
			case 7:
				nextRevision := revision
				if rapid.Bool().Draw(rt, "new_revision") {
					nextRevision++
				}
				h.apply(decision.Event{Kind: decision.Update, Incarnation: "incarnation", Revision: nextRevision, Limited: true, Remaining: 1, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}, publish)
				if publish && nextRevision > revision {
					revision = nextRevision
					expectedAnchorNano = max(expectedAnchorNano, h.now)
				}
			default:
				rt.Fatal("unreachable activity action")
			}
			check()
		}
		// Retained terminal wrong-chain/duplicate samples cannot move the anchor.
		ids := make([]string, 0, len(records))
		for id := range records {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for _, id := range ids {
			op := records[id]
			if op.phase != decision.StartOnly {
				continue
			}
			h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: id, ChainID: "wrong", StartObserved: true, StartObservedAt: expectedAnchorNano + int64(time.Hour)}, true)
			check()
			break
		}
	})
}
