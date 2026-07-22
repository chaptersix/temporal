package schedules

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGenerateWorkflowID(t *testing.T) {
	baseWorkflowID := "my-workflow"
	nominalTime := time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC)

	actual := GenerateWorkflowID(baseWorkflowID, nominalTime)
	require.Equal(t, "my-workflow-2024-06-15T10:30:45Z", actual)
}

func TestGenerateRequestID(t *testing.T) {
	nominalTime := time.Now()
	actualTime := time.Now()

	// No backfill ID given.
	actual := GenerateRequestID(
		"nsid",
		"mysched",
		10,
		"",
		nominalTime,
		actualTime,
	)
	expected := fmt.Sprintf(
		"sched-auto-nsid-mysched-10-%d-%d",
		nominalTime.UnixMilli(),
		actualTime.UnixMilli(),
	)
	require.Equal(t, expected, actual)

	// Backfill ID given.
	actual = GenerateRequestID(
		"nsid",
		"mysched",
		10,
		"backfillid",
		nominalTime,
		actualTime,
	)
	expected = fmt.Sprintf(
		"sched-backfillid-nsid-mysched-10-%d-%d",
		nominalTime.UnixMilli(),
		actualTime.UnixMilli(),
	)
	require.Equal(t, expected, actual)
}

func TestGenerateRequestIDBoundsOversizedInputs(t *testing.T) {
	requestID := GenerateRequestID(
		strings.Repeat("n", 1000),
		strings.Repeat("s", 1000),
		1,
		strings.Repeat("b", 1000),
		time.Unix(1, 0),
		time.Unix(2, 0),
	)
	require.LessOrEqual(t, len(requestID), maxRequestIDLength)
	require.Equal(t, requestID, GenerateRequestID(
		strings.Repeat("n", 1000), strings.Repeat("s", 1000), 1, strings.Repeat("b", 1000), time.Unix(1, 0), time.Unix(2, 0),
	))
}
