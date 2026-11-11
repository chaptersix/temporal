package flakereport

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCodeOwnersLastMatchWins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "CODEOWNERS")
	require.NoError(t, os.WriteFile(path, []byte(`
* @temporalio/server
/service/history/ @temporalio/history
/service/history/queues/ @temporalio/queues @temporalio/server
*.pb.go @temporalio/generated
`), 0644))

	owners, err := loadCodeOwners(path)
	require.NoError(t, err)
	require.Equal(t, []string{"@temporalio/queues", "@temporalio/server"}, owners.owners("service/history/queues/task.go"))
	require.Equal(t, []string{"@temporalio/history"}, owners.owners("service/history/api.go"))
	require.Equal(t, []string{"@temporalio/generated"}, owners.owners("service/history/message.pb.go"))
}

func TestValidateSourcePath(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "service"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "service", "file.go"), []byte("package service"), 0644))

	path, ok := validateSourcePath(root, "service/file.go")
	require.True(t, ok)
	require.Equal(t, "service/file.go", path)
	_, ok = validateSourcePath(root, "../outside")
	require.False(t, ok)
	_, ok = validateSourcePath(root, "missing.go")
	require.False(t, ok)
}
