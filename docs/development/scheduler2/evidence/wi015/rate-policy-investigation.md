# Scheduler start-rate policy source investigation

The V1 `ScheduleV1Suite.TestRateLimit` configures
`SchedulerNamespaceStartWorkflowRPS=1` and expects fewer than10 starts across10
schedules after5seconds. It explicitly applies only to V1. This is an externally
configured behavior, so its non-applicability requires a policy decision, not just
a mechanical test exclusion.

The existing CHASM implementation calls `getRateLimiterPermission` only on the
first start attempt (`chasm/lib/scheduler/invoker_tasks.go`, startWorkflow). That
function returns zero delay and nil error, with an explicit comment that rate
limiting is currently through APS. The V1 worker alone subscribes to
SchedulerNamespaceStartWorkflowRPS and constructs its per-worker reservation
limiter (`service/worker/scheduler/fx.go`, activities.go). These are distinct
backend policies already present in the reference sources.

D59 selects existing CHASM public execution semantics and actual authenticated
frontend StartWorkflowExecution transport. Scheduler2 therefore retains that
CHASM policy: no separate V1 worker namespace start reservation limiter. Calls
still traverse the actual frontend service and its admission controls. This is
not a promise of namespace-wide scheduler-only throughput equivalence with V1,
nor a measured APS/capacity guarantee. M4 must exercise ordinary transport
backpressure and retained start authority under sustained overload separately.

This is a source-backed policy decision pending independent review. Do not mark
the original V1 rate-limit test as passing on scheduler2 or infer a quantitative
throughput claim from this investigation.
