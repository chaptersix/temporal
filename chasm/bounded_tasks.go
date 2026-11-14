package chasm

import (
	"reflect"

	"go.temporal.io/api/serviceerror"
)

type boundedTaskCounts struct{ regular, immediate int }

func (n *Node) recordBoundedTask(component any, immediate bool) error {
	b := n.bounded
	if b == nil {
		return nil
	}
	if b.issued == nil {
		b.issued = make(map[any]boundedTaskCounts)
	}
	counts := b.issued[component]
	maxRegular, maxImmediate := -1, -1
	for _, slot := range b.layout.components {
		if slot.typ == reflect.TypeOf(component) {
			maxRegular = max(maxRegular, slot.limits.NewPerTransaction)
			maxImmediate = max(maxImmediate, slot.limits.ImmediatePerTransaction)
		}
	}
	if node, ok := n.valueToNode[component]; ok {
		if index, exists := b.layout.byPath[boundedPath(node.path())]; exists {
			maxRegular = b.layout.components[index].limits.NewPerTransaction
			maxImmediate = b.layout.components[index].limits.ImmediatePerTransaction
		}
	}
	if maxRegular < 0 {
		return serviceerror.NewInternal("task owner is not in the bounded layout")
	}
	if immediate {
		if counts.immediate >= maxImmediate || b.totalImmediate >= b.layout.totalImmediate {
			return serviceerror.NewInternal("bounded immediate enqueue limit exceeded")
		}
		counts.immediate++
		b.totalImmediate++
	} else {
		if counts.regular >= maxRegular || b.totalRegular >= b.layout.totalRegular {
			return serviceerror.NewInternal("bounded new task limit exceeded")
		}
		counts.regular++
		b.totalRegular++
	}
	b.issued[component] = counts
	return nil
}
func (n *Node) seedBoundedTaskCounts() error {
	for component, tasks := range n.newTasks {
		for range tasks {
			if err := n.recordBoundedTask(component, false); err != nil {
				return err
			}
		}
	}
	for component, tasks := range n.immediatePureTasks {
		for range tasks {
			if err := n.recordBoundedTask(component, true); err != nil {
				return err
			}
		}
	}
	return nil
}
func (n *Node) checkBoundedTasks() error {
	if n.bounded == nil {
		return nil
	}
	index, exists := n.bounded.layout.byPath[boundedPath(n.path())]
	if !exists {
		return serviceerror.NewInternal("bounded task component slot is absent")
	}
	limits := n.bounded.layout.components[index].limits
	attrs := n.serializedNode.GetMetadata().GetComponentAttributes()
	if len(attrs.PureTasks) > limits.Pure || len(attrs.SideEffectTasks) > limits.SideEffect {
		return serviceerror.NewInternal("bounded persisted task limit exceeded")
	}
	counts := n.bounded.issued[n.value]
	if counts.regular > limits.NewPerTransaction || counts.immediate > limits.ImmediatePerTransaction {
		return serviceerror.NewInternal("bounded attached component task limit exceeded")
	}
	return nil
}
func (n *Node) countBoundedImmediateExecution() error {
	if n.bounded == nil {
		return nil
	}
	if n.bounded.executed >= n.bounded.layout.maxImmediate {
		return serviceerror.NewInternal("bounded immediate execution limit exceeded")
	}
	n.bounded.executed++
	return nil
}
