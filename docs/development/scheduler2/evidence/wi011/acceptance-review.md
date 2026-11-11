# WI-011 and M2 acceptance review

Review workspace: `/private/tmp/sch-redesign-updates`, WI-011 working changes over
`4e0b5d4d429b81e3372b4c73073fb7b3011a5797`. Final commit and command fingerprints
belong to the integrator's `checks.json`; this review does not identify uncommitted
changes solely by the base SHA.

## Conclusion

No remaining behavioral or coverage blocker was found for WI-011 or the literal
M2 gate after the numeric-boundary additions. This is an assessment of complete
local policy/update/backfill/callback behavior and its properties/mappings, plus
reviewed Lean statements and executable enumeration specification. It is not an
acceptance of M3 or M4, production routing, or an exhaustive product-state proof.

The integrator must attach final source identity and command dispositions to the
ledger and reconcile milestone/work-item status. Historical failed runs must stay
visible. This review itself does not change plan checkboxes.

## Scope and independence

Reviewed the literal milestone and gate-ownership tables and resource-limit
paragraph in `CHASM_SCHEDULER_REDESIGN_PLAN.md`, WI-011 requirements,
`design/atomic-update.md`, `design/current-mappings.md`,
`design/m2-coverage.md`, `profile.json`, and the current local update preparation
and publication path in `update.go` and `generator.go`. Revisited the current Lean
executable/proposition scope and the accepted per-slice evidence boundaries.

The final pass inspected the independently authored configuration and occurrence
limit tests, exact construction of their inputs, rejection observations and
recovery expectations. It also reconciled the numeric matrix with existing
capacity, retention, retry and executor tests rather than requiring redundant
runs. Earlier passes reviewed the generated update relation, saved replay,
failure/chain cases and actual CHASM composition tests.

This reviewer authored the scan/effect boundary tests and some earlier composition
and mapping artifacts; those are not claimed as independently reviewed by their
author. Production atomic-update code, configuration/occurrence boundary tests,
and the strengthened generated relation were authored by other agents. Parent
review and execution cover this reviewer's contributions.

## Gate assessment

| Requirement | Assessment and evidence boundary |
| --- | --- |
| Complete implemented-policy properties and mappings | All six policies, keyed results, accepted manual/backfill work, current coordinator ownership, effects and lifecycle are represented in the current transition/projection matrix. Legacy admission handlers are explicit stutters. Accepted WI-002–WI-010 properties/models remain factors at their recorded revisions, with current differences called out. |
| Atomic updates and composed behavior | The update wrapper uses bounded framework request deduplication. The component validates authority/revision and prepares source, controls, activity and work/idle preflights before publishing replacements. Admitted requests/effects are not rebuilt. Directed tests cover historical lease rebase, pending supersession, current payload/carryover on queued admission, early failure, rejected update/handoff and saved replay. No new defect was identified in this path. |
| Generated relation and replay | The strengthened 10000-case relation independently expects accepted controls/source, phases, counters, immutable requests, queue identity, blocker, chain and logical pending-start obligations. Its maximum19-transition fixture uses two operations, a bounded waiter and explicit grants; it does not drive automatic scans or establish CPU fairness. The saved JSON replay reproduces the dropped-start-authority mutation after the generated counterexample. Helper failures using the outer testing.T can limit shrinking; this is disclosed rather than generalized into a full shrink guarantee. |
| C7 local contract | Normalized first-execution chain identity, first terminal completion and intermediate continued-as-new rejection are explicit. Actual routing/authentication and reset normalization remain M3; an undefined local chain contract is no longer being deferred. |
| Numeric limits and recovery | Valid neighboring inputs cover template/spec/effect bytes, seed bytes, term/range counts, probe/output limits, catchup and idle duration. Existing cap4 admission/queue/retention tests cover saturation and release. New adapter7/8/9 exclusions check finite continuation recovery; deadline neighbors check stop-issuance boundaries. Structural one-candidate/one-handoff and two-kind effect bounds are distinguished from configurable capacities. |
| Reviewed Lean statements/executable specification | `Enumeration.lean` independently defines finite ordered-source state, scanning and update behavior, with proposed soundness/completeness, uniqueness, resumability, batching, pending preservation, update-boundary and probe-bound propositions. These are definitions of propositions, not proved theorems. The reviewed executable/examples satisfy the M2 deliverable; final proofs and correspondence remain M3. |
| P3.3 observations | Attached materialized values, modified nodes and serialized node/task bytes are observed at the real Node transaction boundary. Cold/warm History reads count persistence API calls. Neither poisoned DATA nor these observations is presented as physical database IO or a production cost bound. |

The new configuration fixtures use valid protobuf request padding and exact
serialized sizes, rather than triggering unrelated invalid workflow IDs. The
occurrence fixtures use valid duplicate terms/ranges and valid unknown protobuf
padding, assert unchanged input, and independently enumerate expected results
after recovery. Requested over-limit probe/output budgets are rejected and then
retried from the unchanged continuation. No test merely compares the implementation
against a second invocation of the same implementation for its expected output.

The effect request-byte limit is reachable through accepted local chain evidence;
it must not be dismissed as structurally impossible. Both request types now test
131071/131072/131073 bytes, including unchanged DATA/tasks after deliberately closing
the rejected transaction, and recovery after completion of the same bound chain.

## Findings resolved during this review sequence

1. Initial generated update expectations omitted pending-start tasks, blocker and
   chain. These projections were added and the strengthened exploration passed.
2. Early failure before acknowledgment and the local chain contract needed explicit
   update composition. Directed failure/chain tests and contract clarification close
   those gaps.
3. Plan419's numeric neighbors were incomplete. The new configuration, occurrence,
   adapter and effect-byte fixtures close the identified gaps; the matrix names
   existing capacity/recovery evidence where sufficient.
4. Root profile concurrency wording could imply one shared start/effect permit.
   The integrator added an adjacent scope clarification: Start1 and Effect1 are
   separate per-library limits, with combined maximum2. This is documentation of
   actual behavior, not a new global concurrency guarantee.

## Recommended plan checklist disposition

These recommendations follow the wording of each item, not a rule that every
unchecked Phase1–4 item must close together with M2.

| Item | Recommendation | Reason |
| --- | --- | --- |
| P1.4 | Close | Independent models expanded with completion, sequential/buffered admission, ALLOW_ALL, effects, manual, backfill arbitration and lifecycle. Recorded factorization, bounds, sensitivity and assumptions satisfy expansion; a single exhaustive product model is not stated as this item's requirement. |
| P2.3 | Close for the enumerated local reproductions | The current F1–F8 reconciliation plus policy, atomic-update, budget, early/duplicate/stale result, full-queue backfill, manual identity, rollback, retention, real History recreation and idle/quiescence traces cover the listed behaviors. This does not close the separately required routed public/API differential reproductions in Phase5. |
| P2.5 | Keep cross-milestone open | Current structured-spec and reducer properties meet the local M2 evidence need, but this item explicitly refers to section7 properties as a whole. Full boundary-biased spec-processing coverage and SP correspondence remain M3. Record the substantial completed local subset without checking off that broader obligation. |
| P3.3 | Close | Attached preparation/materialization counts, modified nodes, serialized node/task bytes and cold/warm persistence API reads now exist. Explicit measurement semantics are essential: this does not assert a count of every decode invocation or actual disk IO. Production scaling/cost evidence belongs to M4. |
| P3.5 | Close for current adapter expansion | Each implemented expansion followed its recorded contract/model decision. No public frontend adapter is silently asserted by this checkbox; Phase5 separately owns that integration. |
| P3.6 | Close for current adapter/executors | Applicable full and incremental race runs, controlled concurrent permit tests, cancellation, retry-horizon boundaries and detached immutable request ownership are covered. Future adapter additions require new applicable checks. |
| P4.4 | Close for current production paths | Current directed and saved replay coverage plus event/task/control mappings and targeted mutations cover the paths added through WI-011. Per-slice trace adapters are allowed; no common all-event trace importer or checked refinement theorem is inferred. Future paths must extend coverage. |
| P4.5 | Close | Real Node Rapid relations generate commands, event order and publication choices, independently check committed projections and include bounded recovery suffixes across the accepted factors. The update fixture's explicit grants and helper shrinking limit remain recorded. This item does not require an exhaustive Cartesian product or infinite-execution proof. |

P2.5 remaining open is consistent with the milestone table's explicit M3 ownership
of full spec-processing checks. It must not disappear from the next work-item plan.

## Evidence inspected and finalization

- `updates-exploration-final/manifest.json`: 10000 strengthened cases, exit0.
- `final-race.log`: scheduler2 packages and library registry pass at their recorded
  snapshot. Later additions have separate focused evidence.
- `update-mutations/manifest.json`, `update-obligation-mutation/manifest.json` and
  `update-obligation-replay-mutation/manifest.json`: named sensitivity evidence;
  the replay manifest records detected failure and exact source restoration.
- `boundary-final-race.log`: configuration neighbors, automatic/backfill scan
  neighbors, effect deadline neighbors, replay and occurrence limits pass.
- `effect-limits-race.log`: final effect request-byte and occurrence cases pass.
- `acceptance-lint.log`: final staged lint reports zero issues. Earlier lint and
  fixture failures remain separate artifacts.
- `replay-measure-race.log`, `history-read-race.log`, `failure-chain-race.log`:
  transaction observations, persistence-read boundary and composed local chain
  evidence, respectively.

Final numeric race and staged lint success were reported by the integrator and
their archived log endings were inspected. No tests were rerun for this review.
The integrator may append the final committed SHA and refreshed fingerprint/command
manifest reference below; this document does not substitute a passing older log for
later source changes.

## Boundaries retained for M3 and M4

Finite factor models and manual transition mappings are not a mechanically checked
Go refinement or an exhaustive composition proof. Backfill liveness remains
conditional on fair service, eventual usable capacity/evidence and successful
publication; a finite scenario or CPU rank argument gives no universal elapsed-time
bound. Real database atomicity and external deduplication horizons remain explicit
trusted boundaries.

M3 still requires completed resumability/batching proofs, axiom audit, executable
Lean–Go correspondence, full spec-processing checks, routed API/callback/chain
integration, differential results and shared functional coverage. M4 still owns
load/state-growth and actual persistence-cost evidence and the explicit
sentinel/migration support boundary. These duties are not reduced by M2 acceptance.
