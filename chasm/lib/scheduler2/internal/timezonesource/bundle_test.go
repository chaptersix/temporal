package timezonesource

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:staticcheck // Verify the build toolchain, not an unrelated go binary found on PATH.
func TestOwnedBundleToolchainParity(t *testing.T) {
	t.Parallel()
	path := filepath.Join(runtime.GOROOT(), "src/time/tzdata/zzipdata.go")
	tree, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	require.NoError(t, err)
	var embedded []byte
	ast.Inspect(tree, func(node ast.Node) bool {
		value, ok := node.(*ast.ValueSpec)
		if !ok || len(value.Names) != 1 || value.Names[0].Name != "zipdata" {
			return true
		}
		require.Len(t, value.Values, 1)
		literal, ok := value.Values[0].(*ast.BasicLit)
		require.True(t, ok)
		require.Equal(t, token.STRING, literal.Kind)
		decoded, err := strconv.Unquote(literal.Value)
		require.NoError(t, err)
		require.Nil(t, embedded)
		embedded = []byte(decoded)
		return false
	})
	require.NotEmpty(t, embedded)
	require.Equal(t, embedded, ownedBundle)
	digest := sha256.Sum256(embedded)
	require.Equal(t, "b2d18a7c8fa8142097a48c99609fb3c92db5ee98bc740294e57eab8ae9f94779", hex.EncodeToString(digest[:]))
	archive, err := zip.NewReader(bytes.NewReader(embedded), int64(len(embedded)))
	require.NoError(t, err)
	require.Len(t, archive.File, 598)
	for _, file := range archive.File {
		reader, err := file.Open()
		require.NoError(t, err)
		expected, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		actual, err := Embedded(file.Name)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		clear(actual)
		again, err := Embedded(file.Name)
		require.NoError(t, err)
		require.Equal(t, expected, again)
	}
}

//nolint:staticcheck // Verify the build toolchain source fingerprints.
func TestTimezoneLoaderSourceUpgradeGate(t *testing.T) {
	t.Parallel()
	for path, expected := range map[string]string{
		"src/time/zoneinfo.go":         "198928e8d2573d88d8743db57b86777a26506f17ef2b0283882a9f5c884979b7",
		"src/time/zoneinfo_read.go":    "812b0025c6e4face22e709053183d59e03e71e3c1459187350f4088587f4a134",
		"src/time/zoneinfo_unix.go":    "d2001a2351a080f1a47ebb3999be74f99caceb4a8f1ea6369a3c843fa0442f08",
		"src/time/zoneinfo_windows.go": "831f1d99fc109507783841c65f0e196f063a1ce6490354869bf6df68aa626434",
		"src/time/tzdata/tzdata.go":    "168b73df4bab177f54c1cd852adfe2b85cf6cca11a8d5756003057dca8f02aa3",
	} {
		data, err := os.ReadFile(filepath.Join(runtime.GOROOT(), path))
		require.NoError(t, err)
		digest := sha256.Sum256(data)
		require.Equal(t, expected, hex.EncodeToString(digest[:]), "review source parity before refreshing %s", path)
	}
}

func TestOwnedBundlePreparesEveryEntry(t *testing.T) {
	t.Parallel()
	archive, err := zip.NewReader(bytes.NewReader(ownedBundle), int64(len(ownedBundle)))
	require.NoError(t, err)
	environment := NewEnvironment(SourceConfig{LookupEnv: func(string) (string, bool) { return "", false }, Embedded: Embedded})
	resolver := NewResolver(environment)
	for _, entry := range archive.File {
		snapshot, err := resolver.Resolve(entry.Name, nil)
		require.NoError(t, err, entry.Name)
		require.NotNil(t, snapshot.Prepared())
		if entry.Name != "UTC" {
			require.Equal(t, "embedded", snapshot.Source())
		}
	}
}
