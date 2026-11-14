//go:build test_dep

package packedstore

type observationBackend interface {
	ObservePackedRead(entries, encodedBytes int)
	ObservePackedWrite(entries, encodedBytes int)
}

func (b *Batch) observeRead(entries, encodedBytes int) {
	if observer, ok := b.backend.(observationBackend); ok {
		observer.ObservePackedRead(entries, encodedBytes)
	}
}
func (b *Batch) observeWrite(entries, encodedBytes int) {
	if observer, ok := b.backend.(observationBackend); ok {
		observer.ObservePackedWrite(entries, encodedBytes)
	}
}
