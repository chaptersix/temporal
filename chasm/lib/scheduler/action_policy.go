package scheduler

import (
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/scheduler/internal"
)

func policySelection(builtin enumspb.ScheduleOverlapPolicy, custom *schedulepb.CustomOverlapPolicy) internal.PolicyIdentity {
	return internal.PolicyIdentity{Builtin: builtin, Custom: custom.GetName()}
}

func actionPolicies(action *schedulepb.ScheduleAction) *internal.PolicyRegistry {
	return implementation(action).Policies()
}

// ValidateScheduleActionPolicies validates action-specific selections independently of legacy validation settings.
func ValidateScheduleActionPolicies(schedule *schedulepb.Schedule, patch *schedulepb.SchedulePatch) error {
	return ValidateActionPolicyOverrides(schedule.GetAction(), schedule.GetPolicies(), patch)
}

func ValidateActionPolicyOverrides(action *schedulepb.ScheduleAction, policies *schedulepb.SchedulePolicies, patch *schedulepb.SchedulePatch) error {
	registry := actionPolicies(action)
	configured := policySelection(policies.GetOverlapPolicy(), policies.GetCustomOverlapPolicy())
	if policies.GetCustomOverlapPolicy() != nil && (configured.Custom == "" || configured.Builtin != 0) {
		return serviceerror.NewInvalidArgument("custom overlap policy requires a name and cannot be combined with overlap_policy")
	}
	if _, err := registry.Resolve(internal.PolicyIdentity{}, configured); err != nil {
		return err
	}
	validate := func(builtin enumspb.ScheduleOverlapPolicy, custom *schedulepb.CustomOverlapPolicy) error {
		if custom != nil && (custom.GetName() == "" || builtin != 0) {
			return serviceerror.NewInvalidArgument("custom overlap policy requires a name and cannot be combined with overlap_policy")
		}
		_, err := registry.Resolve(policySelection(builtin, custom), configured)
		return err
	}
	if trigger := patch.GetTriggerImmediately(); trigger != nil {
		if err := validate(trigger.GetOverlapPolicy(), trigger.GetCustomOverlapPolicy()); err != nil {
			return err
		}
	}
	for _, backfill := range patch.GetBackfillRequest() {
		if err := validate(backfill.GetOverlapPolicy(), backfill.GetCustomOverlapPolicy()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) resolvedPolicy(builtin enumspb.ScheduleOverlapPolicy, custom *schedulepb.CustomOverlapPolicy) (internal.PolicyIdentity, error) {
	return actionPolicies(s.Schedule.GetAction()).Resolve(policySelection(builtin, custom), policySelection(s.Schedule.GetPolicies().GetOverlapPolicy(), s.Schedule.GetPolicies().GetCustomOverlapPolicy()))
}
