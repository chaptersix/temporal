package timezonesource

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func sourceZip(t *testing.T, method uint16) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := zip.NewWriter(&output)
	for _, data := range []string{"first", "second"} {
		entry, err := writer.CreateHeader(&zip.FileHeader{Name: "Zone", Method: method})
		require.NoError(t, err)
		_, err = entry.Write([]byte(data))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	return output.Bytes()
}
func TestStoredZipAcceptedSemantics(t *testing.T) {
	t.Parallel()
	data := sourceZip(t, zip.Store)
	first, err := readStoredZip(bytes.NewReader(data), int64(len(data)), "fixture.zip", "Zone")
	require.NoError(t, err)
	require.Equal(t, []byte("first"), first)
	// Go's reader ignores CRC, choosing the first matching central-directory entry.
	directory := bytes.Index(data, []byte{'P', 'K', 1, 2})
	require.NotEqual(t, -1, directory)
	binary.LittleEndian.PutUint32(data[directory+16:], 1234)
	again, err := readStoredZip(bytes.NewReader(data), int64(len(data)), "fixture.zip", "Zone")
	require.NoError(t, err)
	require.Equal(t, first, again)
	_, err = readStoredZip(bytes.NewReader(data), int64(len(data)), "fixture.zip", "Missing")
	require.ErrorIs(t, err, syscall.ENOENT)
	compressed := sourceZip(t, zip.Deflate)
	_, err = readStoredZip(bytes.NewReader(compressed), int64(len(compressed)), "fixture.zip", "Zone")
	require.EqualError(t, err, "unsupported compression for Zone in fixture.zip")
	for n := 0; n < len(data); n++ {
		_, err := readStoredZip(bytes.NewReader(data[:n]), int64(n), "fixture.zip", "Zone")
		require.Error(t, err)
	}
	binary.LittleEndian.PutUint32(data[len(data)-22+12:], ^uint32(0))
	_, err = readStoredZip(bytes.NewReader(data), int64(len(data)), "fixture.zip", "Zone")
	require.EqualError(t, err, "corrupt zip file fixture.zip")
}
func TestReadSourceFilesAndZip(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	path := filepath.Join(directory, "Zone")
	require.NoError(t, os.WriteFile(path, []byte("input"), 0600))
	got, err := ReadSource(directory, "Zone")
	require.NoError(t, err)
	require.Equal(t, []byte("input"), got)
	got, err = ReadSource("", path)
	require.NoError(t, err)
	require.Equal(t, []byte("input"), got)
	_, err = ReadSource(directory, "Missing")
	require.Equal(t, syscall.ENOENT, err)
	zipPath := filepath.Join(directory, "zones.zip")
	require.NoError(t, os.WriteFile(zipPath, sourceZip(t, zip.Store), 0600))
	got, err = ReadSource(zipPath, "Zone")
	require.NoError(t, err)
	require.Equal(t, []byte("first"), got)
	require.NoError(t, os.WriteFile(path, make([]byte, (10<<20)+1), 0600))
	_, err = ReadSource(directory, "Zone")
	require.EqualError(t, err, "time: file "+path+" is too large")
}
