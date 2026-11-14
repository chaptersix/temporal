//go:build !test_dep

package packedstore

func (*Batch) observeRead(int, int)  {}
func (*Batch) observeWrite(int, int) {}
