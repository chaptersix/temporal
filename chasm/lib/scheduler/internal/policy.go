package internal

import (
	"fmt"
	"maps"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const BufferLatestPolicyName = "temporal.buffer_latest"

type PolicyIdentity struct {
	Builtin enumspb.ScheduleOverlapPolicy
	Custom  string
}

func (p PolicyIdentity) String() string {
	if p.Custom != "" {
		return p.Custom
	}
	return p.Builtin.String()
}

func (p PolicyIdentity) IsZero() bool { return p.Builtin == 0 && p.Custom == "" }

type ExecutionOperations struct{ Cancel, Terminate bool }

type PolicySnapshot struct {
	Occurrence BufferedStartSnapshot
	Running    []ExecutionSnapshot
	Waiting    []BufferedStartSnapshot
	Selected   *BufferedStartSnapshot
	Now        time.Time
}

type PolicyDecision struct {
	Start, Overlap, Wait, Cancel, Terminate bool
	Replace                                 []BufferedStartSnapshot
}

type PolicyDefinition struct {
	Identity PolicyIdentity
	Requires ExecutionOperations
	Plan     func(PolicySnapshot) PolicyDecision
}

type PolicyRegistry struct {
	policies      map[PolicyIdentity]PolicyDefinition
	defaultPolicy PolicyIdentity
}

func NewPolicyRegistry(definitions []PolicyDefinition, defaultPolicy PolicyIdentity, operations ExecutionOperations) (*PolicyRegistry, error) {
	r := &PolicyRegistry{policies: make(map[PolicyIdentity]PolicyDefinition), defaultPolicy: defaultPolicy}
	for _, policy := range definitions {
		id := policy.Identity
		if id.IsZero() || (id.Builtin != 0 && id.Custom != "") || policy.Plan == nil {
			return nil, fmt.Errorf("invalid overlap policy registration %s", id)
		}
		if _, ok := r.policies[id]; ok {
			return nil, fmt.Errorf("duplicate overlap policy %s", id)
		}
		if policy.Requires.Cancel && !operations.Cancel || policy.Requires.Terminate && !operations.Terminate {
			return nil, fmt.Errorf("overlap policy %s requires unsupported execution operations", id)
		}
		r.policies[id] = policy
	}
	if !defaultPolicy.IsZero() {
		if _, ok := r.policies[defaultPolicy]; !ok {
			return nil, fmt.Errorf("unregistered default overlap policy %s", defaultPolicy)
		}
	}
	return r, nil
}

func (r *PolicyRegistry) Resolve(override, configured PolicyIdentity) (PolicyIdentity, error) {
	for _, id := range []PolicyIdentity{override, configured} {
		if id.Builtin != 0 && id.Custom != "" {
			return PolicyIdentity{}, serviceerror.NewInvalidArgument("only one overlap policy selector may be set")
		}
	}
	id := override
	if id.IsZero() {
		id = configured
	}
	if id.IsZero() {
		id = r.defaultPolicy
	}
	if _, ok := r.policies[id]; !ok {
		return PolicyIdentity{}, serviceerror.NewInvalidArgumentf("unsupported or missing overlap policy %s", id)
	}
	return id, nil
}

func (r *PolicyRegistry) plan(id PolicyIdentity, snapshot PolicySnapshot) PolicyDecision {
	snapshot.Running = slices.Clone(snapshot.Running)
	snapshot.Waiting = slices.Clone(snapshot.Waiting)
	if snapshot.Selected != nil {
		selected := *snapshot.Selected
		snapshot.Selected = &selected
	}
	return r.policies[id].Plan(snapshot)
}

func BuiltinPolicy(policy enumspb.ScheduleOverlapPolicy) PolicyDefinition {
	return PolicyDefinition{
		Identity: PolicyIdentity{Builtin: policy},
		Requires: ExecutionOperations{Cancel: policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, Terminate: policy == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER},
		Plan: func(s PolicySnapshot) PolicyDecision {
			if policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
				return PolicyDecision{Start: true, Overlap: true}
			}
			if len(s.Running) == 0 && s.Selected == nil {
				return PolicyDecision{Start: true}
			}
			switch policy {
			case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:
				return PolicyDecision{Wait: len(s.Waiting) == 0}
			case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:
				return PolicyDecision{Wait: true}
			case enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER:
				return PolicyDecision{Start: len(s.Running) == 0, Wait: len(s.Running) > 0, Cancel: len(s.Running) > 0}
			case enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
				return PolicyDecision{Start: len(s.Running) == 0, Wait: len(s.Running) > 0, Terminate: len(s.Running) > 0}
			default:
				return PolicyDecision{}
			}
		},
	}
}

func BufferLatestPolicy() PolicyDefinition {
	return PolicyDefinition{Identity: PolicyIdentity{Custom: BufferLatestPolicyName}, Plan: func(s PolicySnapshot) PolicyDecision {
		if len(s.Running) == 0 && s.Selected == nil {
			return PolicyDecision{Start: true}
		}
		decision := PolicyDecision{Wait: true}
		for _, waiting := range s.Waiting {
			if waiting.CustomOverlapPolicy != BufferLatestPolicyName {
				continue
			}
			if waiting.ActualTime.After(s.Occurrence.ActualTime) || waiting.ActualTime.Equal(s.Occurrence.ActualTime) && waiting.Occurrence > s.Occurrence.Occurrence {
				return PolicyDecision{}
			}
			decision.Replace = append(decision.Replace, waiting)
		}
		return decision
	}}
}

func WorkflowPolicies() *PolicyRegistry {
	definitions := make([]PolicyDefinition, 0, 6)
	for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL} {
		definitions = append(definitions, BuiltinPolicy(policy))
	}
	r, err := NewPolicyRegistry(definitions, PolicyIdentity{Builtin: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}, ExecutionOperations{Cancel: true, Terminate: true})
	if err != nil {
		log.NewCLILogger().Fatal("invalid scheduler overlap policy registry", tag.Error(err))
	}
	return r
}

func ActivityPolicies() *PolicyRegistry {
	definitions := make([]PolicyDefinition, 0, 6)
	for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL} {
		definitions = append(definitions, BuiltinPolicy(policy))
	}
	definitions = append(definitions, BufferLatestPolicy())
	r, err := NewPolicyRegistry(definitions, PolicyIdentity{}, ExecutionOperations{Terminate: true})
	if err != nil {
		log.NewCLILogger().Fatal("invalid scheduler overlap policy registry", tag.Error(err))
	}
	return r
}

func (r *PolicyRegistry) clone() *PolicyRegistry {
	return &PolicyRegistry{policies: maps.Clone(r.policies), defaultPolicy: r.defaultPolicy}
}
