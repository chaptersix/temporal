package chasm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFieldIsSetStagedAndEmpty(t *testing.T) {
	t.Parallel()
	require.False(t, NewEmptyField[*protoMessageType]().IsSet())
	require.True(t, NewDataField(nil, &protoMessageType{}).IsSet())
	require.True(t, DataPointerTo(nil, &protoMessageType{}).IsSet())
}
func (s *fieldSuite) TestFieldIsSetDoesNotMaterialize() {
	serialized := testComponentSerializedNodes()
	serialized["SubData1"].Data.Data = []byte{0xff}
	node, err := s.newTestTree(serialized)
	s.NoError(err)
	ctx := NewMutableContext(context.Background(), node)
	component, err := node.Component(ctx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	field := component.(*TestComponent).SubData1
	data := field.Internal.node
	s.Nil(data.value)
	state := data.valueState
	s.True(field.IsSet())
	s.Nil(data.value)
	s.Equal(state, data.valueState)
	s.Equal([]byte{0xff}, data.serializedNode.Data.Data)
	// Presence never attempts the decode that normal access must perform.
	s.Panics(func() { field.Get(ctx) })
}
