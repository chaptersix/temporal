package scheduler

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	sdklog "go.temporal.io/sdk/log"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	serverlog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestNewScheduleSpecLogInfoRedactsTimezoneData(t *testing.T) {
	timezoneData := []byte("embedded tzif data")
	spec := &schedulepb.ScheduleSpec{
		TimezoneName: "Custom/Zone",
		TimezoneData: timezoneData,
	}

	info := NewScheduleSpecLogInfo(spec)

	digest := sha256.Sum256(timezoneData)
	require.Contains(t, info.Spec, "Custom/Zone")
	require.NotContains(t, info.Spec, "timezoneData")
	require.Equal(t, len(timezoneData), info.TimezoneDataSize)
	require.Equal(t, hex.EncodeToString(digest[:]), info.TimezoneDataSHA256)
	require.Equal(t, timezoneData, spec.TimezoneData, "logging must not mutate the persisted spec")
}

func TestRecordComputeLimitExceededLogsScheduleIdentityAndSpec(t *testing.T) {
	const (
		namespace   = "test-namespace"
		namespaceID = "test-namespace-id"
		scheduleID  = "test-schedule"
	)
	timezoneData := []byte("embedded tzif data")
	digest := sha256.Sum256(timezoneData)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	expectedLog := logger.Expect(
		testlogger.Warn,
		"schedule spec next-time search hit the compute limit",
		tag.WorkflowNamespace(namespace),
		tag.WorkflowNamespaceID(namespaceID),
		tag.ScheduleID(scheduleID),
		tag.String("spec", "Custom/Zone"),
		tag.Int("timezone-data-size", len(timezoneData)),
		tag.String("timezone-data-sha256", hex.EncodeToString(digest[:])),
	)
	s := &scheduler{
		StartScheduleArgs: &schedulespb.StartScheduleArgs{
			Schedule: &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{
				TimezoneName: "Custom/Zone",
				TimezoneData: timezoneData,
			}},
			State: &schedulespb.InternalState{
				Namespace:   namespace,
				NamespaceId: namespaceID,
				ScheduleId:  scheduleID,
			},
		},
		logger: sdklog.With(
			serverlog.NewSdkLogger(logger),
			"wf-namespace", namespace,
			"schedule-id", scheduleID,
		),
	}

	s.recordComputeLimitExceeded()

	require.Equal(t, int64(1), expectedLog.MatchCount())
}
