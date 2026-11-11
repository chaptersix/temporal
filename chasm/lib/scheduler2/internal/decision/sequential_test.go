package decision

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func TestSequentialProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		budget := int64(rapid.IntRange(1, 8).Draw(rt, "budget"))
		state := State{Root: Root{Incarnation: "inc", Limited: true, Remaining: budget}}
		var now, cursor, starts, admissions, deadline int64
		var operation, chain string
		var phase Phase
		var frozen []byte
		counted := false
		commands := rapid.SliceOfN(rapid.SampledFrom([]string{"due", "ack", "complete", "expire", "retry", "old_ack"}), 1, 128).Draw(rt, "commands")
		for index, command := range commands {
			now += int64(time.Second)
			event := Event{Incarnation: "inc", Now: now, OperationID: operation, ChainID: operation + "/chain"}
			switch command {
			case "due":
				event.Kind = Due
				event.Nominal = now
				event.CatchupDeadline = now
				event.Request = []byte(fmt.Sprintf("request-%d", index))
				cursor = now
				// Independent SKIP relation: unfinished occupancy blocks admission even
				// before acknowledgment or after expiry; completed occupancy is reusable.
				if (operation == "" || phase == Completed) && budget > 0 {
					budget--
					admissions++
					operation = fmt.Sprintf("inc/automatic/%d/action/start", now)
					phase = Pending
					chain = ""
					counted = false
					deadline = now + int64(60*time.Second)
					frozen = bytes.Clone(event.Request)
				}
			case "ack", "complete":
				event.Kind = Acknowledged
				if command == "complete" {
					event.Kind = Complete
				}
				if operation != "" {
					if !counted {
						starts++
						counted = true
						chain = operation + "/chain"
					}
					if command == "complete" {
						phase = Completed
					} else if phase != Completed {
						phase = Started
					}
				}
			case "expire":
				now += int64(60 * time.Second)
				event.Now = now
				event.Kind = Expire
				if operation != "" && phase == Pending && now >= deadline {
					phase = Ambiguous
				}
			case "retry":
				event.Kind = RetryableFailure
				if operation != "" && phase == Pending && now >= deadline {
					phase = Ambiguous
				}
			case "old_ack":
				event.Kind = Acknowledged
				event.OperationID = "old/operation"
				event.ChainID = "old/chain"
			default:
				rt.Fatalf("unsupported command %q", command)
			}
			next, err := Step(state, event)
			if err != nil {
				rt.Fatal(err)
			}
			state = next
			expectedRoot := Root{Incarnation: "inc", Limited: true, Remaining: budget, Cursor: cursor, Started: starts}
			if operation != "" && phase != Completed {
				expectedRoot.Unfinished = 1
				expectedRoot.BlockingOperationID = operation
			}
			if state.Root != expectedRoot {
				rt.Fatalf("root mismatch after %s: got %+v expected %+v", command, state.Root, expectedRoot)
			}
			if operation == "" {
				if state.Operation != nil {
					rt.Fatal("operation created without admission")
				}
				continue
			}
			op := state.Operation
			if op == nil || op.ID != operation || op.ChainID != chain || op.Phase != phase || !bytes.Equal(op.Request, frozen) {
				rt.Fatalf("operation mismatch after %s: got %+v expected id=%s phase=%d chain=%s", command, op, operation, phase, chain)
			}
			if starts > admissions {
				rt.Fatal("accounting exceeded admissions")
			}
			// Mutating the command input after the step must not alter frozen request.
			if len(event.Request) > 0 {
				event.Request[0] ^= 0xff
				if !bytes.Equal(state.Operation.Request, frozen) {
					rt.Fatal("admission request aliases event")
				}
			}
		}
	})
}
