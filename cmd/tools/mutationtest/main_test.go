package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApply(t *testing.T) {
	const text = "a one b two c"

	got, err := apply(text, edit{Begin: "one", Replacement: "1"})
	require.NoError(t, err)
	require.Equal(t, "a 1 b two c", got)

	got, err = apply(text, edit{Begin: "one", End: "two", Replacement: "X "})
	require.NoError(t, err)
	require.Equal(t, "a X two c", got)

	got, err = apply(text, edit{Begin: "one", End: "two"})
	require.NoError(t, err)
	require.Equal(t, "a two c", got)

	_, err = apply(text, edit{Begin: "missing"})
	require.Error(t, err)
	_, err = apply("x x", edit{Begin: "x"})
	require.Error(t, err)
	_, err = apply(text, edit{Begin: "one", End: "absent"})
	require.Error(t, err)
	_, err = apply(text, edit{Begin: "two", End: "one"})
	require.Error(t, err)
}
