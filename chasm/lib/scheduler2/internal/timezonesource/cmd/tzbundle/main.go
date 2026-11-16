// Command tzbundle copies the selected Go toolchain's actual embedded timezone
// bundle into the owned scheduler source asset.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
)

func main() {
	if err := generate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

//nolint:staticcheck // Extract the bundle from the exact toolchain that built this generator.
func generate() error {
	path := filepath.Join(runtime.GOROOT(), "src/time/tzdata/zzipdata.go")
	source, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	tree, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
	if err != nil {
		return err
	}
	data, err := extractBundle(tree)
	if err != nil {
		return err
	}
	if err := os.WriteFile("bundle.zip", data, 0644); err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	sourceDigest := sha256.Sum256(source)
	metadata := struct {
		Toolchain, Source, SourceSHA256, BundleSHA256 string
		Bytes                                         int
	}{runtime.Version(), "src/time/tzdata/zzipdata.go", hex.EncodeToString(sourceDigest[:]), hex.EncodeToString(digest[:]), len(data)}
	encoded, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile("bundle_provenance.json", append(encoded, '\n'), 0644)
}

func extractBundle(tree *ast.File) ([]byte, error) {
	var data []byte
	for _, decl := range tree.Decls {
		group, ok := decl.(*ast.GenDecl)
		if !ok || group.Tok != token.CONST {
			continue
		}
		for _, spec := range group.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Names) != 1 || value.Names[0].Name != "zipdata" {
				continue
			}
			if data != nil || len(value.Values) != 1 {
				return nil, errors.New("ambiguous zipdata constant")
			}
			literal, ok := value.Values[0].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return nil, errors.New("zipdata is not a string literal")
			}
			decoded, err := strconv.Unquote(literal.Value)
			if err != nil {
				return nil, err
			}
			data = []byte(decoded)
		}
	}
	if data == nil {
		return nil, errors.New("zipdata constant missing")
	}

	return data, nil
}
