# WI-001 independent protocol model

Evidence ID: E-MODEL-001. Source base: `85b80a1836dbc73741cb52586297dc35e6c937ca`.
Contract input: WI-000 accepted commit `35de5b5a8`, contract version 1 including
review resolutions. This model was written from that contract without importing
or invoking the Go reducer, serializer, calendar evaluator, or fake remote.

Run locally with the FizzBee v0.5.4 macOS arm release used by WI-000:

```sh
python3 docs/development/scheduler2/model/run.py \
  --fizz /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz \
  --output /private/tmp/scheduler2-model-final-v2
```

The runner enforces a 300-second timeout per case, preserves full graphs in the
output directory, and requires the named invariant failure for each mutation.
Parser errors, tool errors, timeouts, and unexpected invariant failures do not
pass a mutation check. `evidence/results.json` records exact commands, exit codes,
source hashes and node counts. Text logs (trailing whitespace removed) and JSON error graphs are checked in;
full successful-state graphs remain at the local paths in the logs. Replay a
mutation by rerunning the command above, which deterministically constructs each
mutated specification and reproduces its counterexample. The FizzBee error-graph
JSON is its own model artifact format, not the production trace schema.

## State and transition correspondence

One schedule incarnation, predetermined due occurrence at time 1, one action,
one operation, one chain, capacity 1 and budget 1. Time is in seconds and is
abstracted to admission time 1, pre-expiry time 2 and expiry time 61. Only admitted
executions advance time, so the committed 60-second deadline is 61. This covers
ordering at the deadline, not every timestamp or clock rollback. Definition and
carryover snapshots are abstracted together as one immutable revision atom in
{0,1}; byte-level cloning, null/absence semantics and actual request generation
require independent Go/CHASM checks.

| Model transition | Contract event or obligation |
| --- | --- |
| PrepareAdmission | Tentative occurrence_due; no durable cursor/budget/task change |
| CommitAdmission | Atomic cursor, budget, frozen request, identity and task publication |
| FailCommit | commit_fail discards tentative state; allowed once |
| UpdateDefinition | update_definition affects future admissions only |
| DeliverStart | deliver_start issues saved request while before deadline |
| ExternalAccept | external_accept; fake remote retains first accepted identity/input |
| LoseAcknowledgment | Ambiguous rpc_result; logical task survives; allowed once |
| Acknowledge | Successful rpc_result, including after early completion/expiry |
| Complete | Authenticated matching complete, possibly before acknowledgment |
| Crash / Restart | Tentative admission discarded; committed state/request/task retained |
| AdvanceTime / Expire | advance_time; stop issuance at deadline, retain ambiguous intent |
| InvalidResult | Named stutter for unknown/stale/conflicting identity messages |

InvalidResult abstracts rejection as specified; it does not verify implementation
identity validation. Attempt saturates at 2 because retries beyond the first
repeat the same identity/input obligations. One crash and one lost acknowledgment
are independently possible in one execution. Remote in-flight work survives local
crash. Restart restores committed logical obligations; persistence serialization
is not modeled. No reset/delete/recreate, failover, general calendar, backfill,
other overlap policy, production deduplication horizon, or database crash claim.

## Assertions, fairness and results

Both 100- and 200-action explorations exhaust the same 1,028 unique states with
zero queued nodes. Six safety assertions and three existential witnesses pass.
Strict liveness reports `IsLive: true` for both temporal assertions:

- `ResolvesOrRetainsAmbiguity`: eventually permanently admitted and either
  completed or expired. Expiry is a retained ambiguous disposition, not success.
- `AcceptedWorkCompletes`: every accepted operation eventually completes.

FizzBee `fair` means weak fairness in v0.5.4. PrepareAdmission, CommitAdmission,
DeliverStart, ExternalAccept, Acknowledge, Complete, Restart, AdvanceTime and
Expire are weakly fair. FailCommit, LoseAcknowledgment and Crash can each occur
at most once. UpdateDefinition occurs at most once. InvalidResult may stutter
without bound; it cannot starve a continuously enabled fair action. Completion
fairness assumes eventual authenticated external completion and delivery; it is
stronger than merely preserving a task. Issuance can lose eligibility at expiry,
so this model does not claim every admitted action reaches remote acceptance.
These are checks over infinite executions of this finite abstraction under the
stated fairness, not liveness inferred from a bounded recovery trace.

| Injected mutation | Expected failed invariant | Counterexample |
| --- | --- | --- |
| Lost wakeup on lost acknowledgment | DurableObligation (S5) | `evidence/lost_wakeup-counterexample.json` |
| Cursor advancement on failed commit | AdmissionAtomicity (S1,S6) | `evidence/cursor_loss-counterexample.json` |
| Duplicate accounting after early completion | AccountingOnce (S3,S4) | `evidence/duplicate_accounting-counterexample.json` |
| Change operation identity on retry | StableRequest (S2) | `evidence/retry_identity-counterexample.json` |
| Read updated definition on retry | StableRequest (S2) | `evidence/retry_input-counterexample.json` |

All five fail their expected invariant. Witnesses separately establish reachable
lost-ack/update/retry with an older frozen request, completion before acknowledgment,
and live intent after restart. The model is an independent oracle for this scope;
implementation correspondence and CHASM loading/atomicity are separate gates.

## Harness challenge and limits

The initial runner expected a different FizzBee failure-message spelling and
correctly refused acceptance despite all mutations failing. It was corrected to
match each exact expected invariant, then all seven cases were rerun. Initial
runs remain in `/private/tmp/scheduler2-model-evidence` and
`/private/tmp/scheduler2-model-final`; accepted evidence comes from the `-v2` path.
The action bound is a finite exploration setting. Equal exhausted graphs at both
bounds support stabilization for this finite abstraction, not an unbounded
implementation proof. Physical dispatch queues, RPC cancellation, protobuf
ownership, remote deduplication expiry and persistence atomicity remain trusted
or independently tested boundaries.
