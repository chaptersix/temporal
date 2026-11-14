//go:build test_dep

package scheduler2

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common/namespace"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type preparedReadSnapshot struct {
	source      *pb.DefinitionPreparationSource
	revision    int64
	owner       [32]byte
	after       time.Time
	count       int
	description *workflowservice.DescribeScheduleResponse
}

func capturePreparedRead(s *Scheduler, ctx chasm.Context) (preparedReadSnapshot, error) {
	if s.Closed {
		return preparedReadSnapshot{}, serviceerror.NewNotFound("schedule is closed")
	}
	if !s.HasGenerator || !s.Generator.Get(ctx).PreparedDefinition.IsSet() {
		return preparedReadSnapshot{}, serviceerror.NewFailedPrecondition("prepared schedule definition missing")
	}
	p := s.Generator.Get(ctx).PreparedDefinition.Get(ctx)
	source, revision, _, err := p.SourceSnapshot(ctx)
	if err != nil {
		return preparedReadSnapshot{}, err
	}
	return preparedReadSnapshot{source: source, revision: revision, owner: [32]byte(p.OwnerId)}, nil
}

func capturePreparedDescription(s *Scheduler, ctx chasm.Context, _ struct{}) (preparedReadSnapshot, error) {
	snapshot, err := capturePreparedRead(s, ctx)
	if err != nil {
		return snapshot, err
	}
	snapshot.description, err = s.DescribePublic(chasm.ContextWithValue(ctx, publicSpecBuilderKey{}, (*legacy.SpecBuilder)(nil)))
	if err != nil {
		return snapshot, err
	}
	snapshot.count = 10
	if s.Limited {
		snapshot.count = int(max(int64(0), min(int64(10), s.Remaining)))
	}
	snapshot.after = ctx.Now(s)
	if cursor := time.Unix(0, s.Cursor); cursor.After(snapshot.after) {
		snapshot.after = cursor
	}
	if updated := snapshot.description.Info.UpdateTime.AsTime(); updated.After(snapshot.after) {
		snapshot.after = updated
	}
	return snapshot, nil
}

func (f *publicTestFrontend) describePreparedSchedule(ctx context.Context, namespaceID namespace.ID, request *workflowservice.DescribeScheduleRequest) (*workflowservice.DescribeScheduleResponse, error) {
	snapshot, err := chasm.ReadComponent(f.bind(ctx), chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: request.ScheduleId}), capturePreparedDescription, struct{}{})
	if err != nil {
		return nil, err
	}
	future, err := f.evaluatePreparedRead(ctx, snapshot, snapshot.after, nil, snapshot.count)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	snapshot.description.Info.FutureActionTimes = future
	return snapshot.description, nil
}

func (f *publicTestFrontend) matchingPreparedSchedule(ctx context.Context, namespaceID namespace.ID, request *workflowservice.ListScheduleMatchingTimesRequest) (*workflowservice.ListScheduleMatchingTimesResponse, error) {
	snapshot, err := chasm.ReadComponent(f.bind(ctx), chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID.String(), BusinessID: request.ScheduleId}), func(s *Scheduler, cc chasm.Context, _ struct{}) (preparedReadSnapshot, error) {
		return capturePreparedRead(s, cc)
	}, struct{}{})
	if err != nil {
		return nil, err
	}
	if request.StartTime == nil || request.EndTime == nil {
		return nil, serviceerror.NewInvalidArgument("missing or invalid query")
	}
	end := request.EndTime.AsTime()
	future, err := f.evaluatePreparedRead(ctx, snapshot, request.StartTime.AsTime(), &end, 1000)
	if errors.Is(err, legacy.ErrComputeLimitExceeded) {
		return nil, legacy.ErrScheduleSpecLimitHit
	}
	if err != nil {
		return nil, err
	}
	return &workflowservice.ListScheduleMatchingTimesResponse{StartTime: future}, nil
}

// evaluatePreparedRead runs only after the read lease exported immutable source.
// It shares the live resolver with ingress and source tasks, including its TTL,
// native source frame and exact TZif loader cache semantics.
func (f *publicTestFrontend) evaluatePreparedRead(ctx context.Context, snapshot preparedReadSnapshot, after time.Time, end *time.Time, count int) ([]*timestamppb.Timestamp, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	stage, err := f.preparation.RestoreDefinition(snapshot.source, snapshot.revision, snapshot.owner)
	if err != nil {
		return nil, err
	}
	source, err := f.preparation.ResolveDefinition(stage)
	if err != nil {
		return nil, err
	}
	zone, err := occurrence.NewResolvedLocation(source.Prepared())
	if err != nil {
		return nil, err
	}
	var future []*timestamppb.Timestamp
	for range count {
		warn, maximum := f.computeLimits()
		header, reader, err := stage.Prepared(DefinitionEvaluationPolicy{WarnIterations: warn, MaxIterations: maximum})
		if err != nil {
			return future, err
		}
		result, err := evaluatePreparedReadNext(ctx, header, reader, zone, after)
		if err != nil {
			return future, err
		}
		if !result.Found || result.Actual.IsZero() || (end != nil && result.Actual.After(*end)) {
			break
		}
		after = result.Actual
		future = append(future, timestamppb.New(after))
	}
	return future, nil
}

func evaluatePreparedReadNext(ctx context.Context, header occurrence.PreparedSpecHeader, reader occurrence.PreparedSpecReader, zone *occurrence.PreparedLocation, after time.Time) (occurrence.PreparedSpecResult, error) {
	var work occurrence.PreparedSpecState
	for {
		if err := ctx.Err(); err != nil {
			return occurrence.PreparedSpecResult{}, err
		}
		next, _, err := occurrence.AdvancePreparedSpec(work, header, zone, after, reader, occurrence.MaxPreparationSteps)
		if err != nil {
			return occurrence.PreparedSpecResult{}, err
		}
		work = next
		if result := work.Result(); result.Done {
			return result, nil
		}
	}
}
