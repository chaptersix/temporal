# WI-006 independent keyed ALLOW_ALL model

Evidence ID: E-ALLOWALL-MODEL-001. Contract input: WI-006 D35–D37 at
`a81fa9f57`. No production reducer, adapter, or property-test code is imported.

```sh
python3 docs/development/scheduler2/model/allowall/run.py \
  --fizz /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz \
  --output /private/tmp/scheduler2-allowall-model-bounded
```

Pinned FizzBee v0.5.4 checks bounds 100 and 200, one concurrent atomic action,
with 300 seconds per case, then three separate sensitivity mutations. The runner
requires baseline success and each mutant's named failed invariant; parser errors,
timeouts, or unrelated invariant failures never pass. Exact commands, source
hashes, exit codes and graph counts are recorded in `evidence/results.json`.

## Finite fixture and transition mapping

The model starts at a saturated reachable fixture: tracked intent 1 and ALLOW_ALL
intents 3–5 are Pending; accepted waiter 2 retains BUFFER_ALL. Four retained records
are also four unresolved intents. This assumes the initial admissions, rather than
checking their construction from an empty root. New ALLOW_ALL ingress 6 and 7 and
queued admission 2 allow seven total admissions, including terminal eviction.
Numerical IDs also denote strictly ordered nominal times. The definition has
already changed from revision 1 to 2; initial requests remain revision 1 and later
admissions capture revision 2. Mid-run arbitrary definition updates are outside
this finite fixture. Root policy does not override explicit incoming or queued
resolved policy.

`AutomaticAllowIngress` bypasses the older non-ALLOW waiter and blocker, but full
unresolved capacity consumes the source identity into a capacity-drop journal
without admission or budget debit. Every admission evicts the smallest nominal
terminal record if retention is full. Started tracked operations are unresolved;
ALLOW acknowledgments produce StartOnly, not workflow Completed. `unfinished`
and `blocker` are independent summaries checked against all retained records.
The latest admitted identity is deliberately retained as a decoy: keyed results
must address older identities independently of it.

`KeyedEvidence` nondeterministically selects any retained unresolved identity and
acknowledgment or early completion, permitting reordered results. An independent
start-evidence journal checks accounting and a terminal-evidence journal checks
settlement. Acknowledgment followed by completion of tracked work is distinct
from ALLOW start-only settlement. Representative older intent 3 may expire to
Ambiguous and still accept its result. Other intents remain Pending/Started until
settled; simultaneous ambiguity of several intents is not explored.

ALLOW requests contain absent carryover and false continued-failure regardless of
existing carryover/failure. ALLOW evidence never changes carryover or pause; tracked
completion updates the symbolic carryover to its own identity. The model collapses
successful, failed, and metadata ALLOW callback payloads into one ignored payload
class. Actual absent/null/value/failure semantics remain WI-003 and implementation
tests. Frozen request observation journals are independent persistent admission
snapshots, not protobuf serialization checks.

`Drain` handles one accepted BUFFER_ALL waiter using current revision/carryover.
With a non-ALLOW blocker, it waits. With no blocker but four unresolved intents,
it retains the eligible queue without a drain task or debit. `ChangeEligibility`
abstracts current pause/budget/catchup ineligibility and allows disposal without a
free slot. Terminal evidence directly re-arms drain after capacity frees, including
after both remaining source occurrences are consumed. Tokens are never reused
following retirement. The queue-length-one abstraction does not recheck multi-entry
FIFO traversal or prefix-disposal batching, which remain WI-005 obligations.

## Checks and explicit trust limits

Nine safety assertions check keyed accounting, addressed settlement, unfinished
capacity, separate blocking ownership, absent ALLOW carryover/pause effects,
frozen request/policy, oldest-terminal-only eviction, budget/queue conservation,
and drain obligation without a capacity retry loop. Six existential witnesses
require capacity ingress drop, older Ambiguous acknowledgment, capacity queue wait,
subsequent exhausted-generator capacity recovery, new ALLOW ingress bypass before
queued admission, and a fifth admission with eviction.

| Mutation | Required failed invariant |
| --- | --- |
| `IGNORE_OLDER_RESULT` | `KeyedEvidenceCountedOnce` or `TerminalEvidenceSettlesItsIntent` |
| `ALLOW_RESULT_LEAKS` | `NoAllowCarryoverOrPause` |
| `LOSE_CAPACITY_WAKE` | `DrainObligationWithoutSpin` |

Failed publication, restart, duplicate/evicted result and stale physical delivery
are explicit stuttering environment transitions. They declare the transaction,
lookup and token-validation boundary, rather than proving its implementation.
Exact incarnation/chain checks, conflicting chains, physical retries, selective
loading, protobuf persistence, executor permits and DB atomicity require concrete
tests. Retention proofs apply to published metadata only; no runtime memory or
serialization claim follows. Source/calendar semantics and public projections are
outside this model. There is no automated Go refinement proof.

No liveness assertions or fairness assumptions are present. FizzBee's `IsLive: true`
output is vacuous here. Exhaustion at both action bounds establishes only finite
safety and witness reachability for this fixture, not unbounded liveness.

## Exploration history

The first broader fixture run exposed a witness reading an already evicted record;
the witness now requires retention before reading it. Later broad explorations
were superseded to make the checked state space tractable: the current fixture
begins at saturation after a definition update, bounds ambiguity to one older key,
and canonicalizes unordered ghost journals. These are explicit scope limits,
not claims that incomplete explorations passed. Acceptance requires all current
baseline and mutation runs recorded below.

## Accepted result

Both 100 and 200 bounds exhausted the same graph in
`/private/tmp/scheduler2-allowall-model-bounded`: 109320 visited nodes, zero queued;
`Valid Nodes: 73988 Unique states: 18220`. All nine safety assertions and six
existential witnesses passed. All three mutants failed their specified invariant.
Compact logs, command/hash metadata, and mutant counterexamples are retained here;
full graphs remain at the recorded local paths.

After the accepted runner started, timeout cleanup was hardened to kill the whole
process group, including the FizzBee wrapper's descendants. No accepted case timed
out. The model and success criteria did not change; `runner-metadata.json` records
this difference and final runner hash. Abandoned exploration logs and the earlier
witness failure remain evidence of unsuccessful runs, separately named.
