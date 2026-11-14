package scheduler2

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
)

// UpdateConfig replaces the local definition and schedule controls together.
// Generation.After is used only by initial configuration, not by updates.
type UpdateConfig struct {
	Generation     GenerationConfig
	Paused         bool
	Limited        bool
	Remaining      int64
	PauseOnFailure bool
}

// Update replaces a local configuration with bounded framework request deduplication.
// Public conflict tokens and visibility changes belong to the frontend adapter.
func Update(ctx context.Context, ref chasm.ComponentRef, requestID string, config UpdateConfig) error {
	_, _, err := chasm.UpdateComponent(ctx, ref, func(s *Scheduler, cc chasm.MutableContext, config UpdateConfig) (chasm.NoValue, error) {
		return nil, s.UpdateConfiguration(cc, config)
	}, config, chasm.WithRequestID(requestID))
	if errors.Is(err, chasm.ErrRequestIDAlreadyUsed) {
		return nil
	}
	return err
}

// UpdateConfiguration validates and publishes one full configuration replacement.
// Admitted requests and queued occurrence identities retain their original values.
func (s *Scheduler) UpdateConfiguration(ctx chasm.MutableContext, config UpdateConfig) error {
	plan, err := s.prepareConfiguration(ctx, config)
	if err != nil {
		return err
	}
	return s.commitGeneration(ctx, plan)
}

func (s *Scheduler) prepareConfiguration(ctx chasm.Context, config UpdateConfig) (*generationPreparation, error) {
	if s.Closed {
		return nil, serviceerror.NewNotFound("schedule is closed")
	}
	if !s.HasGenerator {
		return nil, serviceerror.NewFailedPrecondition("schedule requires initial configuration")
	}
	if config.Generation.Revision <= s.Revision {
		return nil, serviceerror.NewFailedPrecondition("configuration revision must advance")
	}
	if config.Remaining < 0 {
		return nil, serviceerror.NewInvalidArgument("remaining actions must be nonnegative")
	}
	return s.prepareGeneration(ctx, config.Generation, &config)
}
