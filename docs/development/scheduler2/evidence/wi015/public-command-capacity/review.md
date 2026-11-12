# Public combined pending-command capacity

Old scheduler.handlePatch counts each TriggerImmediately and BackfillRequest as one
new backfiller, rejects existing+new greater than100, and returns FailedPrecondition
"too many concurrent backfillers". Parent's unchanged validation port exposed scheduler2
rejecting the fifth range under its internal verification profile.

PublicRevision!=0 now selects the public combined pending ingress limit100. Whole-patch
preflight counts all new trigger/ranges before time-skipping, pause, manual or backfill
mutation. AcceptManual and AcceptBackfill independently guard combined pending count,
so low-level component entry points cannot bypass it. Public manual ingress explicitly
selects the reducer's100-command profile; ordinary internal reducer events still use4.
The reducer independently enforces its public manual-only ceiling, while the component
checks the combined manual+range bound. Public revision is persisted, preserving profile
selection across reload. This change adds no schema fields; comments were regenerated.

The bound concerns commands awaiting enumeration/disposition. It does not raise accepted
execution queue or unfinished-operation caps, which remain4 and are an explicit unresolved
public capacity compatibility gap. Existing internal models cover their4-command profile;
no generalized100-command model checking or revised worst-case transaction proof is claimed.
The separate coordinator quantum limit is unchanged; per-quantum metadata cost can now
scale with up to100 pending ranges. No production routing or migration was performed.

## Executed checks

`CGO_ENABLED=0 go test -tags test_dep,integration ./tests -run
'^TestSchedule2(CreateScheduleAlreadyExists|PatchRejectsExcessBackfillers|ScheduleRejectsInvalidRequests)$'
-count=1 -timeout 3m`: PASS1.253s. Parent source ports/assertions were not relaxed.

`CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2
./chasm/lib/scheduler2/internal/decision -run
'Test(PublicCommandCapacity|PublicManualIngress|Manual|Backfill|PublicMutation)' -count=1`:
root PASS1.855s; decision PASS0.302s. New tests cover99ranges+1manual, atomic whole-patch
rejection before pause, failed publication retaining exact durable nodes, both low-level
guards at100, and100 pending manuals surviving reload without consuming execution capacity.
Existing internal manual/backfill boundary and mutation regressions run in that selection.

`CGO_ENABLED=0 make proto`: PASS after comment update. Logs and source hashes are in
manifest.json. Parent owns final combined lint and acceptance; oracle_review was requested
for independent review. No central plan/ledger status was edited.
