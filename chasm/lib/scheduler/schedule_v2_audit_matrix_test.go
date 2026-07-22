package scheduler_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScheduleV2AuditMatrixHasEveryRetainedFinding(t *testing.T) {
	matrix, err := os.ReadFile(filepath.Join("schedule_v2_audit_matrix.md"))
	require.NoError(t, err)

	var rows []string
	for _, line := range strings.Split(string(matrix), "\n") {
		if strings.HasPrefix(line, "| SCH-") {
			rows = append(rows, line)
		}
	}
	want := []string{
		"SCH-009", "SCH-014", "SCH-017", "SCH-018", "SCH-019", "SCH-020", "SCH-023", "SCH-024", "SCH-025", "SCH-026",
		"SCH-027", "SCH-028", "SCH-029", "SCH-030", "SCH-031", "SCH-032", "SCH-033", "SCH-035", "SCH-036", "SCH-037",
		"SCH-038", "SCH-039", "SCH-041", "SCH-043", "SCH-044", "SCH-046", "SCH-048", "SCH-049", "SCH-052", "SCH-053",
		"SCH-054", "SCH-055", "SCH-056", "SCH-057", "SCH-058", "SCH-059", "SCH-060", "SCH-061", "SCH-062", "SCH-064",
		"SCH-066", "SCH-067", "SCH-068", "SCH-070", "SCH-071", "SCH-072", "SCH-073", "SCH-074", "SCH-076", "SCH-077",
		"SCH-078", "SCH-079", "SCH-080", "SCH-081", "SCH-082", "SCH-083", "SCH-084", "SCH-085", "SCH-086", "SCH-087",
		"SCH-088", "SCH-089",
	}
	require.Len(t, rows, len(want))
	for i, row := range rows {
		columns := strings.Split(row, "|")
		require.Len(t, columns, 11)
		require.Equal(t, want[i], strings.TrimSpace(columns[1]))
		for column := 2; column < len(columns)-1; column++ {
			require.NotEmpty(t, strings.TrimSpace(columns[column]))
		}
	}
}
