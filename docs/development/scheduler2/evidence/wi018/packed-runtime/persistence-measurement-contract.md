# Combined prepared and packed persistence witness

Use a fresh local test frontend selecting newPackedPreparedPublicSchedule and the
bounded library. Assert all persisted selectors and absence of legacy collections;
never convert an execution. Keep current service transport, blob and state limits.

Reuse actual History SQLite test infrastructure, not synthetic ExecutionInfo fields.
Read the concrete persisted execution through TestCluster.ExecutionManager().
GetWorkflowExecution using its actual namespace/business/run/archetype and history
shard. Report proto.Size(State), CHASM framed portion, ExecutionInfo/ExecutionState,
MutableStateStats and DBRecordVersion separately. SQLite physical page/file size is a
different metric; do not label decoded proto bytes as SQL row or disk bytes. No empty
execution-envelope substitution is allowed in this witness.

Workload matrix: N1000/10000, shared1KiB input baseline and1280KiB input matching the
existing accepted within-transport fixture in tests/schedule2_payload_test.go. Reuse
that fixture's accepted size rather than raising limits. Stage the definition and
named timezone through the real preparation service and finish source/preview tasks
before snapshot. Record prepared source/page cardinality and fingerprint. A later
large-definition matrix is distinct from this fixed-definition comparison.

Use one actual admission transition per History update lease, preserving immutable
request freezing and count/deadline/Remaining/Unfinished behavior. Do not seed DATA or
collapse10000 publications into an unbounded batch. A test-only adapter may expose the
existing transition entrypoint with fixed quanta0 for setup; measured transactions use
live quanta. The local frontend's workflow-start wrapper returns a deterministic
retryable Unavailable to retain pending records while physical dispatch and retry still
run. Keep offered admissions, accepted admissions, pending records, attempted starts
and errors separate. Any state-limit rejection is evidence: retain prior accepted
state, error and ownership/count checks. Do not reduce public capacity or increase the
limit to obtain a pass. Queue comparison requires an actual retained blocker.

Capture full persisted images at setup boundaries and after observed physical retry;
reopen/reload the execution through existing History test hooks for a cold point read.
Report wall durations and actual work counters where the service exposes them. The
existing frozen mock-node whole-transaction vectors are supporting evidence, not a
substitute for service/SQL measurements. This is a WI018 storage acceptance witness,
not the complete WI017 calibrated old/new baseline/10x load matrix.

Shared integration prerequisite: root-owned prepared frontend must expose a private
packed selector; production/default and existing prepared frontend remain unchanged.
Framework agent owns the isolated test-only admission helper and new physical witness
test. Root and calendar retain source-service/prepared frontend fixes while this is
implemented. Final tests must use a frozen post-fix source and preserve failed runs.
