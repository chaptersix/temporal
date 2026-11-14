package chasm

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"go.temporal.io/api/serviceerror"
)

// BoundedTaskLimits are schema limits for one fixed component slot. Zero forbids
// the task category. They are not application queue capacity limits.
type BoundedTaskLimits struct {
	Pure                    int
	SideEffect              int
	NewPerTransaction       int
	ImmediatePerTransaction int
}

// BoundedComponentSlot declares a component at one fixed path. Construct slots
// with BoundedComponent so the component type is checked by Go.
type BoundedComponentSlot struct {
	path      []string
	typ       reflect.Type
	limits    BoundedTaskLimits
	emptyMaps []string
}

// BoundedComponent declares a fixed component slot and its task limits.
func BoundedComponent[C Component](path []string, limits BoundedTaskLimits) BoundedComponentSlot {
	return BoundedComponentSlot{path: slices.Clone(path), typ: reflect.TypeFor[C](), limits: limits}
}

// BoundedDataLayout opts a root into a fixed component topology with DATA-only
// collections. The same descriptor must be registered by live and nil libraries.
// It bounds framework traversal; addressed payload bytes and application work
// must be bounded separately by the component implementation.
type BoundedDataLayout struct {
	Components             []BoundedComponentSlot
	MaxImmediateExecutions int
}

// WithBoundedDataLayout declares a root's fixed layout. Unsupported topology is
// an error, never a request to fall back to full traversal.
func WithBoundedDataLayout(layout BoundedDataLayout) RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		copyLayout := layout
		copyLayout.Components = slices.Clone(layout.Components)
		for i := range copyLayout.Components {
			copyLayout.Components[i].path = slices.Clone(layout.Components[i].path)
			copyLayout.Components[i].emptyMaps = slices.Clone(layout.Components[i].emptyMaps)
		}
		rc.boundedLayout = &copyLayout
	}
}

type boundedField struct {
	requiredEmpty bool
	kind          fieldKind
	typ           reflect.Type
}
type compiledBoundedLayout struct {
	components     []BoundedComponentSlot
	byPath         map[string]int
	fields         []map[string]boundedField
	maxImmediate   int
	totalRegular   int
	totalImmediate int
}
type boundedTree struct {
	layout         *compiledBoundedLayout
	slots          []*Node
	structure      nodeWorkset
	bytes          nodeWorkset
	touched        nodeWorkset
	issued         map[any]boundedTaskCounts
	totalRegular   int
	totalImmediate int
	executed       int
}

func boundedPath(path []string) string { return strings.Join(path, "\x00") }

func compileBoundedLayout(rc *RegistrableComponent) (*compiledBoundedLayout, error) {
	descriptor := rc.boundedLayout
	if descriptor == nil {
		return nil, nil
	}
	if len(descriptor.Components) == 0 || descriptor.MaxImmediateExecutions < 0 {
		return nil, errors.New("bounded layout requires component slots and nonnegative immediate execution limit")
	}
	layout := &compiledBoundedLayout{components: slices.Clone(descriptor.Components), byPath: make(map[string]int), fields: make([]map[string]boundedField, len(descriptor.Components)), maxImmediate: descriptor.MaxImmediateExecutions}
	for i, slot := range layout.components {
		if err := layout.addSlot(i, slot); err != nil {
			return nil, err
		}
	}

	root, ok := layout.byPath[""]
	if !ok || layout.components[root].typ != rc.goType {
		return nil, errors.New("bounded layout root must match its registered component")
	}
	if rc.goType.Implements(reflect.TypeFor[VisibilityMemoProvider]()) && !rc.goType.Implements(reflect.TypeFor[VisibilityMemoVersionProvider]()) {
		return nil, errors.New("bounded memo provider requires MemoVersion")
	}
	for i, slot := range layout.components {
		fields, err := layout.compileFields(slot)
		if err != nil {
			return nil, err
		}
		layout.fields[i] = fields
	}
	for _, slot := range layout.components {
		if len(slot.path) == 0 {
			continue
		}
		parent, ok := layout.byPath[boundedPath(slot.path[:len(slot.path)-1])]
		if !ok {
			return nil, fmt.Errorf("bounded component parent is undeclared: %v", slot.path)
		}
		field, ok := layout.fields[parent][slot.path[len(slot.path)-1]]
		if !ok || field.kind != fieldKindSubField || field.typ != slot.typ {
			return nil, fmt.Errorf("bounded component path does not match a component field: %v", slot.path)
		}
	}
	return layout, nil
}

func (layout *compiledBoundedLayout) compileFields(slot BoundedComponentSlot) (map[string]boundedField, error) {
	if err := validateEmptyMapDeclaration(slot); err != nil {
		return nil, err
	}
	result := make(map[string]boundedField)
	for field := range fieldsOf(reflect.New(slot.typ.Elem())) {
		if field.err != nil {
			return nil, field.err
		}
		info := boundedField{kind: field.kind}
		switch field.kind {
		case fieldKindData:
			continue
		case fieldKindSubMap:
			if !slices.Contains(slot.emptyMaps, field.name) {
				return nil, fmt.Errorf("unsupported field in bounded layout: %v.%s", slot.path, field.name)
			}
			info.requiredEmpty = true
		case fieldKindMutableState, fieldKindUnspecified:
			return nil, fmt.Errorf("unsupported field in bounded layout: %v.%s", slot.path, field.name)
		case fieldKindDataMap:
			info.typ = field.val.Interface().(interface{ dataType() reflect.Type }).dataType()
			if err := validateDataMapType(info.typ); err != nil {
				return nil, err
			}
		case fieldKindSubField:
			info.typ = field.val.Interface().(interface{ valueType() reflect.Type }).valueType()
			if info.typ.Kind() != reflect.Pointer || info.typ.Elem().Kind() != reflect.Struct {
				return nil, fmt.Errorf("bounded field must have a concrete type: %v.%s", slot.path, field.name)
			}
			if info.typ.Implements(reflect.TypeFor[Component]()) {
				path := append(slices.Clone(slot.path), field.name)
				index, ok := layout.byPath[boundedPath(path)]
				if !ok || layout.components[index].typ != info.typ {
					return nil, fmt.Errorf("bounded component field is undeclared: %v", path)
				}
			} else if !info.typ.Implements(protoMessageT) {
				return nil, fmt.Errorf("bounded field is not DATA or component: %v.%s", slot.path, field.name)
			}
		case fieldKindParentPtr:
			// ParentPtr resolves only registered ancestor components using existing checks.
		default:
			return nil, fmt.Errorf("unsupported bounded field kind %v", field.kind)
		}
		result[field.name] = info
	}
	return result, nil
}

// initializeBoundedLayout performs a full validation only at cold construction
// or initial root attachment. Ordinary mutations maintain slots pointwise.
func (n *Node) initializeBoundedLayout() error {
	root := n.root()
	if root.bounded != nil {
		return root.validateBoundedNode(nil, root)
	}
	rc, ok := root.registry.ComponentByID(root.ArchetypeID())
	if !ok || rc.compiledBoundedLayout == nil {
		return nil
	}
	root.bounded = &boundedTree{layout: rc.compiledBoundedLayout, slots: make([]*Node, len(rc.compiledBoundedLayout.components))}
	if err := root.seedBoundedTaskCounts(); err != nil {
		return err
	}
	for path, node := range root.andAllChildren() {
		if err := root.validateBoundedNode(path, node); err != nil {
			return err
		}
		node.enqueueBoundedState()
		if node.subtreeIsDirty {
			root.bounded.touched.add(node)
		}
	}
	return nil
}

func (n *Node) validateBoundedNode(path []string, node *Node) error {
	b := n.bounded
	if b == nil {
		return nil
	}
	if node.serializedNode == nil || node.serializedNode.Metadata == nil {
		return serviceerror.NewInternal("bounded layout contains missing node metadata")
	}
	if index, ok := b.layout.byPath[boundedPath(path)]; ok {
		return n.validateBoundedComponent(index, node, path)
	}
	if len(path) == 1 && slices.Contains(reservedRootChildren, path[0]) && node.isData() && len(node.children) == 0 {
		return nil
	}
	if n.isBoundedFieldNode(path, node) {
		return nil
	}
	if len(path) > 1 {
		if owner, ok := b.layout.byPath[boundedPath(path[:len(path)-2])]; ok {
			field, ok := b.layout.fields[owner][path[len(path)-2]]
			if ok && field.kind == fieldKindDataMap && node.isData() && len(node.children) == 0 {
				return nil
			}
		}
	}
	return serviceerror.NewInternalf("unexpected bounded layout node at %v", path)
}

// isBoundedFieldNode checks direct DATA and collection fields of a fixed slot.
func (n *Node) isBoundedFieldNode(path []string, node *Node) bool {
	if len(path) == 0 {
		return false
	}
	parent, ok := n.bounded.layout.byPath[boundedPath(path[:len(path)-1])]
	if !ok {
		return false
	}
	field, ok := n.bounded.layout.fields[parent][path[len(path)-1]]
	if !ok {
		return false
	}
	if field.requiredEmpty && node.isMap() && len(node.children) == 0 {
		return true
	}
	if field.kind == fieldKindDataMap && node.isMap() {
		return true
	}
	return field.kind == fieldKindSubField && !field.typ.Implements(reflect.TypeFor[Component]()) && node.isData() && len(node.children) == 0
}

func (n *Node) validateBoundedMutation(mutation NodesMutation) error {
	if n.bounded == nil {
		return n.initializeBoundedLayout()
	}
	for encoded := range mutation.UpdatedNodes {
		path, err := n.pathEncoder.Decode(encoded)
		if err != nil {
			return err
		}
		for depth := 0; depth <= len(path); depth++ {
			node, ok := n.findNode(path[:depth])
			if !ok {
				return serviceerror.NewInternal("bounded replication ancestor is absent")
			}
			if err := n.validateBoundedNode(path[:depth], node); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *Node) checkBoundedRemoval() error {
	if n.bounded == nil || n.applyingMutation || n.isData() {
		return nil
	}
	if n.isMap() && len(n.children) > 0 {
		return serviceerror.NewInternal("bounded collection removal requires incremental DATA cleanup")
	}
	for _, slot := range n.bounded.slots {
		if slot == nil {
			continue
		}
		within := false
		for ancestor := slot; ancestor != nil; ancestor = ancestor.parent {
			if ancestor == n {
				within = true
				break
			}
		}
		if !within {
			continue
		}
		for _, child := range slot.children {
			if child.isMap() && len(child.children) > 0 {
				return serviceerror.NewInternal("bounded component removal requires incremental DATA cleanup")
			}
		}
	}
	return nil
}

func (layout *compiledBoundedLayout) addSlot(i int, slot BoundedComponentSlot) error {
	if slot.typ == nil || slot.typ.Kind() != reflect.Pointer || slot.typ.Elem().Kind() != reflect.Struct {
		return errors.New("bounded component slot requires a concrete struct pointer")
	}
	for _, segment := range slot.path {
		if segment == "" || strings.ContainsRune(segment, 0) {
			return errors.New("bounded component path has an invalid segment")
		}
	}
	key := boundedPath(slot.path)
	if _, exists := layout.byPath[key]; exists {
		return fmt.Errorf("duplicate bounded component path %v", slot.path)
	}
	if slot.limits.Pure < 0 || slot.limits.SideEffect < 0 || slot.limits.NewPerTransaction < 0 || slot.limits.ImmediatePerTransaction < 0 {
		return errors.New("bounded task limits must be nonnegative")
	}
	if slot.limits.NewPerTransaction > int(^uint(0)>>1)-layout.totalRegular || slot.limits.ImmediatePerTransaction > int(^uint(0)>>1)-layout.totalImmediate {
		return errors.New("bounded task limits overflow")
	}
	layout.totalRegular += slot.limits.NewPerTransaction
	layout.totalImmediate += slot.limits.ImmediatePerTransaction
	layout.byPath[key] = i

	return nil
}

func (n *Node) validateBoundedComponent(index int, node *Node, path []string) error {
	slot := n.bounded.layout.components[index]
	registered, ok := n.registry.rcByGoType[slot.typ]
	if !ok || !node.isComponent() {
		return serviceerror.NewInternalf("bounded component type mismatch at %v", path)
	}
	attrs := node.serializedNode.GetMetadata().GetComponentAttributes()
	if attrs.TypeId != registered.componentID && (attrs.TypeId != 0 || reflect.TypeOf(node.value) != slot.typ) {
		return serviceerror.NewInternalf("bounded component type mismatch at %v", path)
	}
	n.bounded.slots[index] = node
	if err := n.validateRequiredEmptySlot(index, node); err != nil {
		return err
	}
	return node.checkBoundedTasks()
}
