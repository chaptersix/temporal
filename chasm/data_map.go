package chasm

import (
	"reflect"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

// DataMap stores independently addressed protobuf DATA records. Reads and writes
// clone their values; callers cannot mutate stored records through aliases.
// Handles may only be used under the execution lease with a current context.
// Unlike Map, DataMap exposes no enumeration or mutable Field references.
type DataMap[D proto.Message] struct{ Internal dataMapInternal }

type dataMapInternal struct{ state *dataMapState }
type dataMapState struct {
	root       *Node
	epoch      uint64
	owner      *Node
	ownerValue any
	field      string
	delta      map[string]proto.Message
	removed    map[string]*Node
}

// NewDataMap creates a staging collection that can attach to one component field.
func NewDataMap[D proto.Message](ctx MutableContext) DataMap[D] {
	c := dataMapContext(ctx)
	dataMapMust(validateDataMapType(reflect.TypeFor[D]()))
	return DataMap[D]{Internal: dataMapInternal{state: &dataMapState{root: c.root, epoch: c.epoch, delta: make(map[string]proto.Message)}}}
}

func (DataMap[D]) dataType() reflect.Type { return reflect.TypeFor[D]() }

// Get returns a detached value and its presence. Corrupt records fail through
// framework panic recovery, rather than being reported as absent.
func (m DataMap[D]) Get(ctx Context, key string) (D, bool) {
	s, c := m.Internal.checked(ctx)
	var zero D
	if v, ok := s.delta[key]; ok {
		if v == nil {
			return zero, false
		}
		typed, ok := v.(D)
		if !ok {
			dataMapMust(serviceerror.NewInternal("DataMap staged entry has an incompatible type"))
		}
		return common.CloneProto(typed), true
	}
	collection := s.collection()
	if collection == nil {
		return zero, false
	}
	child, ok := collection.children[key]
	if !ok {
		return zero, false
	}
	if !child.isData() {
		dataMapMust(serviceerror.NewInternal("DataMap entry is not DATA"))
	}
	// Preserve the captured epoch while removing mutable access.
	immutable := *c
	dataMapMust(child.prepareDataValue(&immutable, reflect.TypeFor[D]()))
	v, ok := child.value.(D)
	if !ok {
		dataMapMust(serviceerror.NewInternal("DataMap entry has an incompatible type"))
	}
	return common.CloneProto(v), true
}

// Put stages a detached copy of value, replacing the addressed record.
func (m DataMap[D]) Put(ctx MutableContext, key string, value D) {
	s, _ := m.Internal.checked(ctx)
	if !reflect.ValueOf(value).IsValid() || reflect.ValueOf(value).IsNil() {
		dataMapMust(serviceerror.NewInternal("DataMap cannot store nil"))
	}
	s.delta[key] = proto.Clone(value)
	s.dirty()
}

// Delete stages removal of the addressed record. Missing records remain absent.
func (m DataMap[D]) Delete(ctx MutableContext, key string) {
	s, _ := m.Internal.checked(ctx)
	collection := s.collection()
	var child *Node
	if collection != nil {
		child = collection.children[key]
	}
	if child == nil {
		delete(s.delta, key)
		return
	}
	if !child.isData() {
		dataMapMust(serviceerror.NewInternal("DataMap entry is not DATA"))
	}
	s.delta[key] = nil
	s.dirty()
}

func dataMapContext(ctx Context) *immutableCtx {
	provider, ok := ctx.(interface{ dataMapContext() *immutableCtx })
	if !ok {
		dataMapMust(serviceerror.NewInternal("DataMap requires an execution context"))
	}
	c := provider.dataMapContext()
	if c.epoch != c.root.epoch {
		dataMapMust(serviceerror.NewInternal("DataMap context transaction has ended"))
	}
	return c
}

func (c *immutableCtx) dataMapContext() *immutableCtx { return c }

func (m dataMapInternal) checked(ctx Context) (*dataMapState, *immutableCtx) {
	c := dataMapContext(ctx)
	s := m.state
	if s == nil || s.root != c.root {
		dataMapMust(serviceerror.NewInternal("DataMap belongs to another execution or is uninitialized"))
	}
	if s.owner == nil {
		if s.epoch != c.epoch {
			dataMapMust(serviceerror.NewInternal("DataMap staging transaction has ended"))
		}
	} else {
		if s.owner.value != s.ownerValue || s.root.valueToNode[s.ownerValue] != s.owner || !s.owner.currentTreeNode() {
			dataMapMust(serviceerror.NewInternal("DataMap owner has been removed or replaced"))
		}
	}
	return s, c
}

func (s *dataMapState) collection() *Node {
	if s.owner == nil {
		return nil
	}
	n := s.owner.children[s.field]
	if n != nil && n.serializedNode.GetMetadata().GetCollectionAttributes() == nil {
		dataMapMust(serviceerror.NewInternal("DataMap field is not a collection"))
	}
	return n
}

func (s *dataMapState) dirty() {
	if s.owner != nil {
		if s.root.touchedDataMaps == nil {
			s.root.touchedDataMaps = make(map[*dataMapState]struct{})
		}
		s.root.touchedDataMaps[s] = struct{}{}
		if s.root.pendingDataMaps == nil {
			s.root.pendingDataMaps = make(map[*dataMapState]struct{})
		}
		s.root.pendingDataMaps[s] = struct{}{}
		s.owner.markSubtreeDirty()
	}
}

func validateDataMapType(t reflect.Type) error {
	if t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Struct || !t.Implements(protoMessageT) || t.Implements(reflect.TypeFor[Component]()) {
		return serviceerror.NewInternal("DataMap requires a concrete protobuf DATA type")
	}
	return nil
}

// dataMapMust follows Field's panic convention: framework boundaries recover
// these errors, so application code cannot accidentally ignore corrupt state.
func dataMapMust(err error) {
	if err != nil {
		// nolint:forbidigo // Panic is intended here for framework error handling.
		panic(err)
	}
}

func (n *Node) syncDataMap(field fieldInfo) error {
	internal := field.val.FieldByName(internalFieldName)
	m, ok := internal.Interface().(dataMapInternal)
	if !ok {
		return serviceerror.NewInternal("DataMap has an incompatible internal representation")
	}
	if m.state == nil {
		if _, exists := n.children[field.name]; exists {
			return serviceerror.NewInternal("DataMap cannot replace an existing collection")
		}
		m.state = &dataMapState{root: n.root(), epoch: n.epoch, delta: make(map[string]proto.Message)}
		internal.Set(reflect.ValueOf(m))
	}
	s := m.state
	if s.root != n.root() {
		return serviceerror.NewInternal("DataMap attachment belongs to another execution")
	}
	if s.owner == nil {
		if _, exists := n.children[field.name]; exists {
			return serviceerror.NewInternal("DataMap cannot replace an existing collection")
		}
		if s.epoch != n.epoch {
			return serviceerror.NewInternal("DataMap staging transaction has ended")
		}
		s.owner, s.ownerValue, s.field = n, n.value, field.name
	} else if s.owner != n || s.ownerValue != n.value || s.field != field.name {
		return serviceerror.NewInternal("DataMap cannot attach to multiple or replaced owners")
	}
	return s.sync()
}

func (s *dataMapState) sync() error {
	n := s.owner
	if n.value != s.ownerValue || s.root.valueToNode[s.ownerValue] != n || !n.currentTreeNode() {
		return serviceerror.NewInternal("DataMap owner has been removed or replaced")
	}
	collection := n.children[s.field]
	if collection != nil && !collection.isMap() {
		return serviceerror.NewInternal("DataMap field is not a collection")
	}
	for key, value := range s.delta {
		if value == nil {
			if err := s.deleteEntry(collection, key); err != nil {
				return err
			}
			continue
		}
		if collection == nil {
			collection = newNode(n.nodeBase, n, s.field)
			collection.initSerializedCollectionNode()
			collection.setValueState(valueStateNeedSerialize)
			n.children[s.field] = collection
			if err := n.validateBoundedNode(collection.path(), collection); err != nil {
				return err
			}
		}
		child, exists := collection.children[key]
		if exists && !child.isData() {
			return serviceerror.NewInternal("DataMap entry is not DATA")
		}
		if !exists {
			child = s.removed[key]
			if child == nil {
				child = newNode(n.nodeBase, collection, key)
				child.initSerializedNode(fieldTypeData)
			}
			path, err := child.getEncodedPath()
			if err != nil {
				return err
			}
			delete(n.mutation.DeletedNodes, path)
			collection.children[key] = child
			if err := n.validateBoundedNode(child.path(), child); err != nil {
				return err
			}
		}
		child.setValue(value)
		child.setValueState(valueStateNeedSerialize)
	}
	// Discard backing capacity so a later point delta cannot inherit a large
	// constructor transaction's map iteration cost.
	s.delta = make(map[string]proto.Message)
	return nil
}

func validateDataMapFields(t reflect.Type) error {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	value := reflect.New(t)
	for field := range fieldsOf(value) {
		if field.typ != nil && genericTypePrefix(field.typ) == "*"+chasmDataMapTypePrefix {
			return field.err
		}
		if field.kind != fieldKindDataMap {
			continue
		}
		typ := field.val.Interface().(interface{ dataType() reflect.Type }).dataType()
		if err := validateDataMapType(typ); err != nil {
			return err
		}
	}
	return nil
}

func (s *dataMapState) deleteEntry(collection *Node, key string) error {
	if collection == nil {
		return nil
	}
	child, ok := collection.children[key]
	if !ok {
		return nil
	}
	if !child.isData() {
		return serviceerror.NewInternal("DataMap entry is not DATA")
	}
	if s.removed == nil {
		s.removed = make(map[string]*Node)
	}
	s.removed[key] = child
	return child.delete(false)
}
