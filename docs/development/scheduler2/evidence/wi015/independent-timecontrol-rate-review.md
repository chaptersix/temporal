# Independent time-control gate and rate-policy review

Reviewer: api_seam. No source edits. Reviewed timeskipping.go/tests, automaticDemand,
generator arm, public patch re-arming, CHASM runtime gate and defaultFindNextTargetTime,
and rate-policy-investigation.md against both original scheduler backends.

## Cached future candidate after control re-arm

No behavioral finding in the narrow gate correction. public patch can set Generator.WakeAt
to now while retaining a cached future pending candidate. automaticDemand explicitly
returns that candidate's future Actual time rather than runnable work; reconcileCoordinator
persists the corresponding future logical timer. Generator.arm only updates generator
metadata; its legacy GenerateHandler is invalid, so the stale WakeAt is not a separate
valid timer obligation that must be serviced. Treating stale WakeAt alone as due work
incorrectly prevents the framework's bounded fast-forward before that future candidate.

The corrected exception requires a non-excluded pending candidate strictly after now.
A missing pending candidate still needs scan; an excluded pending candidate still needs
bounded disposal; Actual==now still needs admission/disposition. All those remain blocked.
The earlier guards continue blocking closed/paused roots, unfinished/unconfirmed starts,
pending starts, effects, execution queue, manual/range work, vacancy lease and due
coordinator obligation. No persisted candidate, wake or request identity is changed.
The framework then independently scans valid persisted logical timers, refuses to skip
past any due timer, and caps progress at the earliest timer/fast-forward boundary. Reload
reconstructs this choice entirely from persisted generator/coordinator metadata.

Unit cases distinguish future cached candidate, due candidate, excluded candidate, due
coordinator and all authority blockers. Author reports unit PASS0.697s; actual three-port
rerun was still running at review time. No new independent execution or full recovery
matrix is claimed by this source review.

## Existing rate-policy distinction

rate-policy-investigation.md accurately identifies the backend difference. Old CHASM
invoker_tasks.startWorkflow consults getRateLimiterPermission only on attempt1; that
helper returns zero/nil and explicitly delegates rate control to APS. It issues the
actual frontend Start RPC. V1 worker fx subscribes SchedulerNamespaceStartWorkflowRPS,
scales rate per worker multiplicity/totalworkers, configures its reservation limiter,
and activities.waitForRateLimiterPermission either waits locally or returns a rate-limit
sleep instruction. ScheduleV1Suite.TestRateLimit is explicitly a V1 scenario.

Under the selected existing-CHASM execution policy, scheduler2's actual frontend RPC
retains frontend admission controls without introducing the distinct V1 worker reservation
limiter. This is supported as a backend-policy distinction, not proof of namespace-wide
scheduler-only throughput equivalence. The V1 rate test must not be labeled passed on
scheduler2. No APS throughput or overload-recovery measurement is provided here; future
sustained-backpressure verification remains required. No production config change,
routing or migration was performed.
