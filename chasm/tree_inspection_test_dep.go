//go:build test_dep

package chasm

import "slices"

// MaterializedPathsForTest observes which attached node values are in memory.
// It does not load values, synchronize fields, or change dirty state. Call it only
// under the execution lock. Newly prepared and deserialized values both count;
// deleted/detached values and repeated decode calls are not represented.
func (n *Node) MaterializedPathsForTest() []string {
	var paths []string
	var visit func(*Node, string)
	visit = func(node *Node, path string) {
		if node.valueState >= valueStateSynced {
			paths = append(paths, path)
		}
		for name, child := range node.children {
			childPath := name
			if path != "" {
				childPath = path + "/" + name
			}
			visit(child, childPath)
		}
	}
	visit(n, "")
	slices.Sort(paths)
	return paths
}
