package timezonesource

import (
	"archive/zip"
	"bytes"
	_ "embed"
	"errors"
	"sync"
	"syscall"
)

//go:generate go run ./cmd/tzbundle

//go:embed bundle.zip
var ownedBundle []byte

type bundleEntry struct{ start, size int64 }

var bundleIndex = sync.OnceValues(func() (map[string]bundleEntry, error) {
	reader, err := zip.NewReader(bytes.NewReader(ownedBundle), int64(len(ownedBundle)))
	if err != nil {
		return nil, err
	}
	entries := make(map[string]bundleEntry, len(reader.File))
	for _, file := range reader.File {
		if file.Method != zip.Store {
			return nil, errors.New("owned timezone bundle is not stored")
		}
		start, err := file.DataOffset()
		if err != nil {
			return nil, err
		}
		size := int64(file.UncompressedSize64)
		if size < 0 || start < 0 || start > int64(len(ownedBundle)) || size > int64(len(ownedBundle))-start {
			return nil, errors.New("owned timezone bundle entry outside bounds")
		}
		if _, exists := entries[file.Name]; !exists {
			entries[file.Name] = bundleEntry{start, size}
		}
	}
	return entries, nil
})

// Embedded returns detached bytes from the owned bundle that matches the build
// toolchain's time/tzdata asset. The parity test is an upgrade gate.
func Embedded(name string) ([]byte, error) {
	entries, err := bundleIndex()
	if err != nil {
		return nil, err
	}
	entry, ok := entries[name]
	if !ok {
		return nil, syscall.ENOENT
	}
	return bytes.Clone(ownedBundle[entry.start : entry.start+entry.size]), nil
}
