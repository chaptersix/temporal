# Independent physical recreation review

Parent read both actual frontend recreation fixtures, old-incarnation request and
callback token checks, Admin.AddTasks serialization, explicit queue-dispatch
barriers and the timer executor hook/default path. The Start fixture holds the
actual accepted response until a replacement exists, uses a new physical start
as a permit-release/merge barrier, waits for actual old callback SUCCEEDED, then
closes the shard and replays the captured old transfer envelope. It verifies no
extra Start RPC and unchanged replacement/closed old state.

Both cancel and terminate fixtures capture the actual old timer, bind its effect
ID/logical wake/target operation/chain, deliberately suppress initial dispatch,
then replay via Admin after close/recreate and shard reload. The timer observer
provides a dispatch barrier; zero effect RPCs, unchanged replacement and old child
still RUNNING verify rejection. The original suppressed delivery is not claimed
to have executed the side effect. These cases do not establish accepted-effect
lost-response behavior or arbitrary failover delivery.

The test-only namespace-scoped timer hook delegates to the unchanged active/standby
selection when absent or for other task categories. Default path retains its
original namespace and response behavior. The zero response when a test chooses
not to call its continuation is deliberate task suppression, not a production
routing path. No behavioral blocker found in these witnesses or the hook.
Author's combined result PASS3.854s is the execution evidence; this review did not
independently rerun it. Scope and failure logs remain in the author's manifest.
