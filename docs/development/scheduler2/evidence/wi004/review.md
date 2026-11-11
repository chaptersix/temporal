# WI-004 acceptance review

Source: 9064bed65b0cf67659925986df04ea7ad27c8dcb.
Independent history_contract review found no acceptance blocker after the fixes
below and confirmed the final tests/model scope. Separate tests and model were
authored in isolated worktrees; the integrator owned schema/core/adapter changes.

Resolved behavior findings:

- Yield/Limited at the supported clock's upper boundary could schedule outside
  [2000,2101), making the successor fail forever. Clamp to the last supported
  instant; token progress permits same-instant resumption. Four-final-candidate
  range-edge regression reaches exhaustion without an unsupported clock.
- Paused, exhausted-budget and missed-catchup skips loaded carryover before
  admission selection. Select admission first; prepare/freeze carryover only for
  a new operation and before publishing any state or eviction. Poisoned carryover
  skips now advance the cursor without decoding that payload.
- Protobuf lint required an UNSPECIFIED zero enum; explicit mapping offsets
  preserve occurrence status values. Initial failing generation log retained.
- Test lint found an unused loaded context; scoped it to its actual read.

Passing evidence: registered Node.EachPureTask/ExecutePureTask execution, physical
pure-task publication, failed four-candidate batch/retry of the same token, fresh
reload, pending jitter/configuration supersession, duplicate token and clock
rollback, actual-time catchup, excluded-work yields, range-edge resumption, immutable
config inputs, cap 3/4/5/6 record tests, old retained/evicted callbacks and unfinished
capacity rejection. Actual success then failure then third admission preserves
both prior success and continued failure; retries ignore poisoned live carryover.

The explicit generation_v1.json replays twice through fresh CHASM trees, including
failed publication and second-request carryover. Rapid independently tracks up to
128 commands for 10000 scenarios (seed 1), budget 1–8, monotonic due events, completion,
acknowledgment, expiry, retry and stale acknowledgment. Expected state is not
derived by calling the implementation. Final race covers all scheduler2 packages
and offline registry; normal/minimal lint passes.

The independent six-occurrence model exhausts 3076 states at 100/200 bounds, with
seven safety assertions and six witnesses. Three model mutations detect failed-
publication cursor advance, stale timer progress and stale completion replacing
active identity. Four Go mutations detect pending-slot loss, missing successor,
unsupported deadline and newest-record eviction. Source is restored byte-for-byte
and the final profiles pass after restoration. Integrated model bytes match all
recorded case hashes; no repeat of identical exploration was needed.

Scope: strict sequential SKIP. Pending/Started/Ambiguous occupy until verified
completion (DIV6). Cap 4 retains records with oldest-completed-only eviction;
cumulative cursor/counts/carryover survive. Direct Due trusts actual-time eligibility
from its caller; generator supplies that check. Direct old/evicted IDs are no-ops.
Generator remains locally wired, not production-routed. No database crash proof,
authentication proof, liveness after permanent ambiguity, general overlap/manual/
backfill fairness, workflow-chain reset behavior, or model composition theorem.
M2 remains partial; this is not a full milestone acceptance.
