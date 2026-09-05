package scheduler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
)

func TestMigrationResearchForeignCHASMDestinationMustNotAcknowledgeHandoff(t *testing.T) {
	client := &mockSchedulerClient{
		migrateErr: serviceerror.NewAlreadyExists("independently created destination with different schedule state"),
	}
	a := newTestActivities(client, testNamespaceID)
	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: testNamespaceID,
	})
	require.Error(t, err, "source must not acknowledge handoff without proving the destination belongs to this migration")
}
