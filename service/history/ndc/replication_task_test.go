package ndc

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.uber.org/mock/gomock"
)

type replicationTaskTestDeps struct {
	controller      *gomock.Controller
	clusterMetadata *cluster.MockMetadata
}

func setupReplicationTaskTest(t *testing.T) *replicationTaskTestDeps {
	t.Helper()

	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).Return("some random cluster name").AnyTimes()

	t.Cleanup(func() {
		controller.Finish()
	})

	return &replicationTaskTestDeps{
		controller:      controller,
		clusterMetadata: clusterMetadata,
	}
}

func TestValidateEventsSlice(t *testing.T) {
	t.Parallel()

	eS1 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 2,
			Version: 2,
		},
	}
	eS2 := []*historypb.HistoryEvent{
		{
			EventId: 3,
			Version: 2,
		},
	}

	eS3 := []*historypb.HistoryEvent{
		{
			EventId: 4,
			Version: 2,
		},
	}

	v, err := validateEventsSlice(eS1, eS2)
	require.Equal(t, int64(2), v)
	require.Nil(t, err)

	v, err = validateEventsSlice(eS1, eS3)
	require.Equal(t, int64(0), v)
	require.IsType(t, ErrEventSlicesNotConsecutive, err)

	v, err = validateEventsSlice(eS1, nil)
	require.Equal(t, int64(0), v)
	require.IsType(t, ErrEmptyEventSlice, err)
}

func TestValidateEvents(t *testing.T) {
	t.Parallel()

	eS1 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 2,
			Version: 2,
		},
	}

	eS2 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 3,
			Version: 2,
		},
	}

	eS3 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 1,
		},
		{
			EventId: 2,
			Version: 2,
		},
	}

	v, err := validateEvents(eS1)
	require.Nil(t, err)
	require.Equal(t, int64(2), v)

	v, err = validateEvents(eS2)
	require.Equal(t, int64(0), v)
	require.IsType(t, ErrEventIDMismatch, err)

	v, err = validateEvents(eS3)
	require.Equal(t, int64(0), v)
	require.IsType(t, ErrEventVersionMismatch, err)
}

func TestSkipDuplicatedEvents_ValidInput_SkipEvents(t *testing.T) {
	t.Parallel()
	deps := setupReplicationTaskTest(t)

	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	slice2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
		},
		{
			EventId: 14,
		},
	}

	task, _ := newReplicationTask(
		deps.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1, slice2},
		nil,
		"",
		nil,
		false,
	)
	err := task.skipDuplicatedEvents(1)
	require.NoError(t, err)
	require.Equal(t, 1, len(task.getEvents()))
	require.Equal(t, slice2, task.getEvents()[0])
	require.Equal(t, int64(13), task.getFirstEvent().EventId)
	require.Equal(t, int64(14), task.getLastEvent().EventId)
}

func TestSkipDuplicatedEvents_InvalidInput_ErrorOut(t *testing.T) {
	t.Parallel()
	deps := setupReplicationTaskTest(t)

	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	slice2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
		},
		{
			EventId: 14,
		},
	}

	task, _ := newReplicationTask(
		deps.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1, slice2},
		nil,
		"",
		nil,
		false,
	)
	err := task.skipDuplicatedEvents(2)
	require.Error(t, err)
}

func TestSkipDuplicatedEvents_ZeroInput_DoNothing(t *testing.T) {
	t.Parallel()
	deps := setupReplicationTaskTest(t)

	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	slice2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
		},
		{
			EventId: 14,
		},
	}

	task, _ := newReplicationTask(
		deps.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1, slice2},
		nil,
		"",
		nil,
		false,
	)
	err := task.skipDuplicatedEvents(0)
	require.NoError(t, err)
	require.Equal(t, 2, len(task.getEvents()))
	require.Equal(t, slice1, task.getEvents()[0])
	require.Equal(t, slice2, task.getEvents()[1])
}

func TestResetInfo(t *testing.T) {
	t.Parallel()
	deps := setupReplicationTaskTest(t)

	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.New(),
		RunID:      uuid.New(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId:   13,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		},
		{
			EventId: 14,
		},
	}

	task, _ := newReplicationTask(
		deps.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		nil,
		"",
		nil,
		false,
	)
	info := task.getBaseWorkflowInfo()
	require.Nil(t, info)
	require.False(t, task.isWorkflowReset())
}
