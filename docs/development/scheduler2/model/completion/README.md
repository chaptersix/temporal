# WI-003 independent completion model

Evidence ID: E-COMPLETION-MODEL-001.

Source base: `d6026f84040ca5f2e202ca76e738b7a218ad353c`, read-only contract input `work-items/WI-003.md`.
This symbolic FizzBee specification was authored independently from the completion
implementation. It imports no production code and shares no transition helper
with the reducer or CHASM tests. It extends the verification vocabulary rather
than modifying the accepted WI-001 model.

Replay with the already pinned local FizzBee v0.5.4 tool:

```sh
python3 docs/development/scheduler2/model/completion/run.py \
  --fizz /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz \
  --output /private/tmp/scheduler2-completion-model-accepted
```

The runner checks 100- and 200-action bounds, then three individual mutations.
Every case has a 300-second timeout. It saves exact commands, generated source
hashes, exit codes, node counts and logs in `results.json`; a parser error, timeout
or an invariant failure outside the named expected set does not count as the expected mutation detection.
Re-running deterministically reconstructs each mutated source and its error graph.
Full graphs remain in the local output directory; compact logs/results and JSON
counterexamples are retained in `evidence/`. Copied logs strip trailing whitespace.

## State and scope

One schedule incarnation, one admitted operation, one authenticated chain, one
remaining action. Initial success is absent, explicit null, or an old value;
initial failure is absent or an old failure. Symbolic values distinguish presence
from encoded null. `empty` success represents an empty/no-result payload list and
has the same clearing semantics as absent success. Opaque protobuf metadata and
empty-but-present failure messages map to a present symbolic failure; serializer,
size-limit and clone tests remain Go/CHASM obligations.

Admission freezes both initial carryover values and consumes budget once. An
acknowledgment or early completion binds the chain/count exactly once. Completion
preparation is tentative; failed publication drops it without durable changes.
Successful publication chooses the first terminal outcome. Later callbacks and
policy changes cannot modify the winning carryover or retroactively change its
pause decision. TogglePolicy can occur before publication, so pause is selected
from policy at commit, rather than policy at admission or tentative receipt.

Callbacks cover success absent/empty/null/value and failure statuses failed,
timedout, cancelled and terminated. Failure preserves the prior success and
replaces failure; only failed/timedout with enabled current policy pauses.
Metadata-only completion covers legacy status zero and each explicit terminal
status: success, failed, timedout, cancelled and terminated. It preserves both
payloads and wins against later payload callbacks. Explicit metadata failed or
timedout still applies the current pause policy; zero and other statuses do not. The model has no unpause command;
TogglePolicy changes the failure policy only. This is an explicit subset.

InvalidCompletion is a named stutter representing rejected old incarnation,
unknown operation, malformed status or conflicting chain. Its validation is a
trusted boundary, not a proof of implementation guards. External callback
authentication, protobuf ownership, 64-KiB value limits, failed-store rollback,
real task dispatch and persistence reload are tested independently. No sequential
admission, general overlap, retention or production routing is modeled.

## Independent assertions and mutations

`FirstCompletionCarryover` is a declarative table keyed by the first committed
outcome and the initial values, separately from transition writes. Other safety
assertions require one-time budget/count, invisible tentative writes, current
policy only at first completion, and immutable admitted input. Reachability
witnesses exercise explicit null, early completion followed by actual late ack,
duplicate after policy change, failed publication followed by completion, and
metadata completion winning before a later payload callback.

| Mutation | Required detected invariant |
| --- | --- |
| `OVERWRITE_DUPLICATE` | `FirstCompletionCarryover` or `CurrentPolicyOnlyAtFirstCompletion` |
| `COLLAPSE_NULL` | `FirstCompletionCarryover` |
| `ALIAS_ADMITTED_REQUEST` | `AdmittedRequestIsImmutable` |

No temporal/liveness assertion is made. There is no fairness assumption; arbitrary
policy changes and duplicate deliveries are safety interleavings. Any FizzBee
`IsLive: true` output is vacuous for this specification and is not liveness evidence.
Exhaustion at both bounds establishes finite-model stabilization only, not an
unbounded implementation proof or automated refinement correspondence.

## Recorded result

Accepted run: `/private/tmp/scheduler2-completion-model-accepted`. Both bounds
exhausted 173,584 model nodes / 162,732 unique states with zero queued nodes.
All five safety assertions and seven reachability witnesses passed. All three
mutations failed a named expected invariant. Full counterexamples and exact
commands/source hashes are in the accompanying evidence. The earlier `-first`
and `-final` output directories are retained but do not describe the final source: the
accepted version additionally requires an actual late acknowledgment and a
payload-specific duplicate for those witnesses. No production bug was injected.
