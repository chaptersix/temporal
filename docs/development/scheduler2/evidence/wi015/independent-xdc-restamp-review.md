# Independent scheduler2 local XDC restamp review

Reviewer: api_seam. Scope: new scheduler2_timeskipping_replication_test.go, additive
WithClusterScheduleFrontend option, XDC base forwarding, prior restamp suite, existing
onebox registration and HistoryTaskRecorder persistence interception. No source edits.

The new suite retains the original4h-future timer,2h fast-forward, standby offset>90min
and1min timestamp-tolerance assertions. Its selected test frontend is installed before
service start in each local TestCluster. The new option copies the supplied library slice;
base forwards the previously available factory/offline-library fields. Nil default fields
leave other suites unchanged. History registers the live library while frontend/admin
registers matching offline identities. There is no production scheduler routing/rollout
change; both clusters are constructed by local testcore.NewCluster, and the XDC remote
connections point to those test hosts. The old suite remains unchanged.

HistoryTaskRecorder delegates writes to the real ExecutionManager first and records
only successful AddHistoryTasks/Create/Update calls, so an observed ChasmTaskPure is
physical enqueue evidence rather than an unsuccessful attempted publication. The test
reads DatabaseMutableState for replicated offset. expectedFireTime=originalFireTime-skip
is correct when the initial accumulated skip is zero. It demonstrates restamping after
passive replication; it does not demonstrate firing the future timer, namespace failover,
standby cache reload or stale side-effect delivery. Author evidence explicitly preserves
those limits and the initial missing-base-forwarding failure.

## Requested strengthening

Initial snapshot typed-read active only and database-checked standby only, with merely
non-old persisted archetype assertions. Asked author for typed+database identity on both
local clusters, exact expected scheduler2 archetype equality and same run/incarnation,
and a run-scoped recorder filter. Also requested initial offset0 assertion (or delta
math), so the restamp equation's precondition is explicit. This is an evidence gap against
the requested review contract, not an identified production implementation defect.
Resolution and final source hashes will be appended after the author's correction.

## Resolution reviewed

Author added active database archetype equality against a locally registered scheduler2
library ID, standby typed ReadComponent on its own History context with exact run and
incarnation equality, standby database exact archetype equality, run-specific recorder
filter, initial standby offset0 assertion and distinct replacement physical task ID.
The local History ChasmEngine.ReadComponent/getExecutionLease path reads the selected
cluster's shard/cache; it does not redirect this typed read to the active cluster. Thus
the typed standby assertion is not disguised active evidence. These changes resolve the
requested identity/offset/publication evidence gaps. No remaining source finding.

Author reports run04 PASS12.298s after both-cluster identity strengthening. The final
initial-offset/distinct-task-ID assertions were not yet executed at this snapshot because
an unrelated concurrently introduced duplicate ContextMetadata method blocked compile.
This is not marked a final test pass; author owns its pending rerun and final manifest.
This reviewer ran no additional tests. Hashes below are independent final-source review
snapshots, preserving the author's original passing-source hashes.

Author follow-up: strengthened run06 PASS11.794s after the unrelated compile issue
was resolved; final source/log hashes are in xdc-restamp/manifest.json. The reviewer
inspected the strengthened source but did not execute this physical rerun.
