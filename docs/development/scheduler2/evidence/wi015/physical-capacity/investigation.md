# Actual large-payload boundary investigation

First/third/fourth physical runs used near2MiB input, result and failure separately.
Two workflows completed (one success, one failure) and actual scheduler mutable
state retained about6.25MB, below the8MiB limit. The third start remained unresolved.
The second command ran during another owner's proto regeneration and did not compile;
its missing generated package error is retained separately from runtime failures.

A wrapper around the real frontend StartWorkflowExecution captured the decisive
error in physical04: grpc received message larger than max (6243479 vs4194304).
This is the existing aggregate RPC boundary, not the scheduler's old64KiB ceiling
or a demonstrated CHASM4MiB mutation cap. The enriched root read showed a Pending
third operation with Unfinished=1 and a retained pending start obligation after
admission. No result or failure bytes were truncated and no limit was raised.

The final test has two explicit profiles:1280KiB each (aggregate below4MiB) must
complete all three actions and verify successful-result plus failure-detail
carryover; near2MiB each must preserve two completed actions and retain the third
unresolved intent after actual transport rejection. This is a source/evidence-backed
correction to the original assumption that individually accepted blobs imply an
unbounded aggregate Start RPC. Neither profile proves arbitrary concurrent large
generations fit finite mutable-state capacity. Final execution evidence is pending.

## Verified final profiles

Physical06 passed both profiles in13.684s. The positive1280KiB-per-value profile
completed all3 real workflows and verified unchanged successful result and previous
failure details on the third start. The near2MiB profile verified the actual RPC
size rejection, two completed actions, a third unresolved start, original input
projection and positive reported mutable-state size below8MiB. Physical05's only
failure was the test assuming status.Code on the wrapped error returned the raw
gRPC code; the observed wrapper reports Unknown while preserving the explicit
size rejection. Final assertions check that actual rejection text and retained
authority, without guessing its wrapper type.

The observed6.25MB state is a concrete finite profile, not a universal limit proof.
The successful positive profile executes under unchanged configured limits.
