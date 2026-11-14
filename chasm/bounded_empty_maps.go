package chasm

import (
	"fmt"
	"reflect"

	"go.temporal.io/api/serviceerror"
)

// WithRequiredEmptyMaps declares legacy DATA maps that must remain empty in
// this slot. Names are persisted field names (including any name tag). This
// does not permit component maps, map entries, or fallback to ordinary traversal.
func (slot BoundedComponentSlot) WithRequiredEmptyMaps(names ...string) BoundedComponentSlot {
	slot.emptyMaps = append([]string(nil), names...)
	return slot
}

func validateEmptyMapDeclaration(slot BoundedComponentSlot) error {
	wanted := make(map[string]bool, len(slot.emptyMaps))
	for _, name := range slot.emptyMaps {
		if name == "" || wanted[name] {
			return fmt.Errorf("invalid or duplicate required-empty map %q", name)
		}
		wanted[name] = true
	}
	matched := make(map[string]bool, len(wanted))
	for field := range fieldsOf(reflect.New(slot.typ.Elem())) {
		if !wanted[field.name] {
			continue
		}
		if matched[field.name] {
			return fmt.Errorf("ambiguous required-empty map field %s", field.name)
		}
		if field.err != nil {
			return field.err
		}
		if field.kind != fieldKindSubMap {
			return fmt.Errorf("required-empty field %s is not a legacy DATA map", field.name)
		}
		value := reflect.New(field.typ.Elem()).Elem().Interface().(interface{ valueType() reflect.Type }).valueType()
		if err := validateDataMapType(value); err != nil {
			return fmt.Errorf("required-empty map %s: %w", field.name, err)
		}
		matched[field.name] = true
	}
	for name := range matched {
		delete(wanted, name)
	}
	if len(wanted) != 0 {
		return fmt.Errorf("required-empty map declaration names unknown fields: %v", wanted)
	}
	return nil
}

func (n *Node) validateRequiredEmptyMaps() error {
	root := n.root()
	if root.bounded == nil {
		return nil
	}
	for index, node := range root.bounded.slots {
		if node == nil || !node.currentTreeNode() {
			continue
		}
		if err := root.validateRequiredEmptySlot(index, node); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) validateRequiredEmptySlot(index int, node *Node) error {
	slot := n.bounded.layout.components[index]
	if len(slot.emptyMaps) == 0 {
		return nil
	}
	for _, name := range slot.emptyMaps {
		if collection, ok := node.children[name]; ok && (!collection.isMap() || len(collection.children) != 0) {
			return serviceerror.NewInternalf("required-empty map %v.%s has persisted entries or invalid kind", slot.path, name)
		}
	}
	if node.value == nil {
		return nil
	}
	for field := range fieldsOf(reflect.ValueOf(node.value)) {
		descriptor, ok := n.bounded.layout.fields[index][field.name]
		if ok && descriptor.requiredEmpty && field.val.Len() != 0 {
			return serviceerror.NewInternalf("required-empty map %v.%s is nonempty", slot.path, field.name)
		}
	}
	return nil
}
