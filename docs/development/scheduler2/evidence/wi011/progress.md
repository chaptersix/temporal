# WI-011 implementation progress

Base4e0b5d4d4, isolated sch-redesign-updates. D55/D56 are recorded in
design/atomic-update.md and independently source-reviewed. New full local update
prepares source and controls together, rejects before writes, and preserves frozen
requests/waiter policies. No public conflict-token or routing claim.

Normal scheduler2/registry baseline and focused update race pass. Five named Go
mutants are detected and sources restored. Composition tests pass for historical
lease rebase and current template/carryover under frozen queued identity. A saved
JSON update replay is under final verification.

Initial focused test compared proto internals using require.Equal; replaced by
proto semantic comparison and strengthened retained request byte assertions. Initial
composition fixture retained carryover for ALLOW_ALL; corrected to accepted
start-only clearing behavior. Initial lint caught one complexity excess and a
missing default switch; terminal check moved to the existing public initial
configuration entry point, and the switch now rejects an unknown fixture.

Instrumentation first measured byte/node/task boundaries and cold1/warm0 History
GetWorkflowExecution calls. A test_dep-only observer now reads attached materialized
node state without loading it. Tree initialization already prepares the root for
visibility, so its pretransition baseline is1, not0. That incorrect initial assertion
is preserved in materialization-initial.log. The observer counts attached prepared
or deserialized values, not total repeated decode calls or physical database IO.

Replay first compared empty serialized spec bytes as nil versus allocated empty;
the assertion now decodes and compares the complete protobuf. These failures were
fixture/measurement expectation errors, not accepted evidence or ignored checks.

WI-011 remains open for final saved replay/instrumentation checks, current mapping
reconciliation, independent coverage audit and remaining generated coverage. No
M2–M4 completion claim or local integration has occurred.

Final saved replay, final-automatic-intent idle protection and materialization
measurements pass focused race (replay-measure-race.log). Root is already prepared
by tree reload; after-transition attached materialization and serialized mutation
measurements are recorded before postpublication assertion reads.

The first10000-case seed1 atomic-update property passes. Independent review confirms
its accepted-input oracle, but identifies missing PendingStarts/logical-start task,
blocker and chain projections. Those are being added before final acceptance;
updates-exploration-initial preserves this narrower run. Its25-event limit is a
valid loose upper bound; actual maximum executed shape is19 transitions plussetup.
Assertions inside shared harness helpers use outer testing.T; direct oracle
assertions use Rapid.T. Only the latter are claimed to participate in shrinking.
Final staged-source lint passes (all-lint.log), before subsequent property/early
failure/chain test additions; those additions require focused final checks.

The strengthened update relation passes10000 cases, seed1, at a maximum19
transitions plussetup (updates-exploration-final). It checks exact pending-start
identity/wake, persisted logical StartTask identity/time, blocker, chain and frozen
60-second deadline in addition to control/source/queue/request/accounting state.
A one-fault loss of PendingStarts is detected and shrunk to the first accepted
update. The retained shrunk.fail is the original Rapid output; the standalone
update_obligation_v1.json supplies explicit initial state and accepted replacement.
The same fault fails TestUpdateSavedReplay, and both mutation runs restore source
bytes exactly. These checks establish reproducibility for this fault, not universal
shrinking of shared testing.T harness failures.

Full scheduler2/registry race passes (final-race.log), including strengthened
properties, early failure and normalized chain checks. That run predates numeric
boundary tests and the second JSON replay; their focused normal tests pass in
configuration-limits.log, occurrence-limits.log and obligation-replay.log. Final
focused race and lint still remain. Numeric tests cover below/at/above limits and
recovery; two adapter scan/effect neighbor additions are in progress.

Final numeric race passes, including effect-request131072-byte neighbors for both
kinds and recovery (effect-limits-race.log). Final staged-source lint passes with
zero issues (acceptance-lint.log). Its prior failure was a missing default branch
in an occurrence test fixture; the branch now fails explicitly on an unknown kind,
and the corrected test reran under race. acceptance-lint-initial.log is retained.
Current fingerprints are refreshed; independent acceptance review remains pending.
