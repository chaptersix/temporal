# Evidence ledger

Base: 85b80a1836dbc73741cb52586297dc35e6c937ca; comparison:
9ab3a9f770da20df7d94bcc0030f28eec7b0b947. No implementation/model results yet.

- E4 provenance: `git worktree list --porcelain`, `git reflog show sch-redesign --date=iso`,
  original `git status --porcelain=v1` and `git diff --binary`; files beside this ledger.
  Original diff is evidence only, not an applied contribution. W1-W4.
- E5 source inspection: ../investigation.md and ../contract.md; three independent
  read-only investigations at base SHA. H1-H7, C1-C8 relevant slice; no model or
  runtime claim. Fix inventory commands: `git log --oneline 9ab3a9f77..HEAD --
  chasm/lib/scheduler`, `git log --all --oneline --not HEAD -- chasm/lib/scheduler`.
- E6 tooling: `go version` => go1.27.3 darwin/arm64; `command -v lean/lake` absent;
  `shasum -a 256 /private/tmp/fizzbee-v0.5.4.tar.gz` => pinned hash in contract.
  Tool availability only, not model execution. FizzBee path:
  /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz.
- E7 baseline: `go test -tags test_dep ./chasm/lib/scheduler/internal -count=1`
  passed (0.231s), after sandbox initially denied Go cache access and approved retry.
  No changed code. This validates baseline internal package only, not M1.

Unmodeled: database crash recovery, failover, full calendars, all API behavior.
All future executable results require source/diff identity and standalone traces;
none of the above inspection records satisfies an implementation gate.

- E8 independent WI-000 review: three contract gaps found and resolved in contract.md
  Review resolutions (deadline/late results, identities/chain binding, fairness).
  M0 accepted for bounded slice; no M1 implementation evidence.

## Accepted M1 evidence

Code source: **f9d62cf99ba5a6e72d987b9bdb9f315c98efd40e**, clean working tree
when accepted profiles started. Each copied manifest includes per-source SHA256,
exact command, Go/Python versions, settings, source SHA and empty dirty diff.
Baseline remains 85b80a1836dbc73741cb52586297dc35e6c937ca; comparison baseline
9ab3a9f770da20df7d94bcc0030f28eec7b0b947 and included fixes are unchanged.
No differential public API comparison is claimed; DIV1 freezes retry input.

| ID | Kind / claim | Artifact and result |
|---|---|---|
| E9 | bounded-model-check; S1-S6, conditional liveness | [model manifest](m1/model/manifest.json), [results](m1/model/results.json); 1028 states, empty frontier at both 100/200 bounds; two strict temporal assertions pass; five targeted mutations fail expected invariants. Combined source 549fd46de; model unchanged at final code revision. |
| E10 | property-test; C3-C6, S1-S5/S8 | [exploration manifest](m1/exploration/manifest.json); Rapid v1.2.0, seed 1, 10000 scenarios x128 events plus completion recovery; pass. Independent expectations, no infinite liveness claim. |
| E11 | CHASM-test/unit-test; S2-S6/S8, H1-H3/H5-H7 | [regressions manifest](m1/regressions/manifest.json), [log](m1/regressions/0.log); real tree serialization, fresh reconstruction, failed publication, physical/logical task validation, null-library identities and immutable requests pass. |
| E12 | simulation; V1-V5 and refinement | [simulation manifest](m1/simulation/manifest.json); 100 Rapid command scenarios, saved explicit trace, independent per-event projections and dependency-preserving shrink pass. Six-event minimized retry-input mutation is committed under scheduler2/testdata. |
| E13 | race/cancellation/aliasing | [race manifest](m1/race/manifest.json); scheduler2 packages + all-library registry pass under -race; pre-issue cancellation, accepted/cancelled RPC, deadline, task-bound late acknowledgment, rollback and detached payload tests. |
| E14 | code generation/lint | [generation log](m1/generation.log); make proto passed, final generated schema unchanged since 5229df153. [lint log](m1/lint.log); make lint-code-fast with base85b80a183 and FIX=false passed both normal/minimal checks, 0 issues. |
| E15 | selective loading | E11 log: 3 poisoned payload values avoided (selected request, unrelated request, diagnostics), 2 modified nodes, 268 serialized mutation bytes. Poisoned values fail when deliberately accessed. DB reads/bytes not measured by fake store; no fetch-reduction claim. |
| E16 | challenge/review | [review](m1/review.md), [in-flight rollback mutant](m1/rollback-mutant.log); all four behavioral findings resolved, final independent reviewer reports no M1 blocker. |

Replay commands are in ../README.md and each manifest. FizzBee counterexample
JSON remains under ../model/evidence; Go traces under
../../../../chasm/lib/scheduler2/testdata. Model abstraction: one occurrence,
revision atoms, time collapsed to pre-expiry/expiry, one crash/failure/lost ack.
Go replay maps event kinds explicitly and checks a separately authored switch
oracle; serialization/event names are shared, decision logic is not. Model-to-Go
mapping is manual and no automated cross-language refinement is claimed.

Trusted: atomic DB publication and production cache eviction, eventual fair
physical delivery and authenticated completion, fake remote identity retention
through the 60-second horizon. Unmodeled: real DB crash recovery, full calendars,
failover, other policies, public API wiring, migration/retention and production
callback authentication. Unsupported profiles emit nonzero status, not passes.

Original tracked diff was byte-compared after execution and is identical to E4.
All original untracked entries remain; unrelated spec_processor_reference_test.go
and unrelated worktrees appeared during the session and were not modified.

## Accepted WI-002 evidence (partial M2)

Code: **637fe096df2a9b7de6db3e4865e5d9a53f10cc02**; prerequisite M1 acceptance
00b65430a52d6d43e7e0c9150ced94a81c5e169f. Required evidence below passes for
standalone enumeration; this does not accept M2's policy/callback/backfill gate.
Final Lean completeness/uniqueness statement correction at 7f80bd82d compiles;
Go source unchanged. WI-002 acceptance includes that independently reviewed fix.

| ID | Kind / claim | Artifact and result |
|---|---|---|
| E17 | source-inspection / contract | [D22–D26](../enumeration-contract.md), [review](wi002/review.md); SC1–SC7 bounded semantics, DIV2–DIV4 explicit; autonomous acceptance, no human pause. |
| E18 | property-test / unit-test / replay | [manifest](wi002/enumeration/manifest.json), [log](wi002/enumeration/0.log); independent second-scan oracle, 10000 Rapid cases seed 1; periods1–6s, phases0..period-1, jitter0–6000ms, probes1–8, outputs1–4, split windows across30s. SP1–SP4/SP6 examples/properties; SP5 update examples and selected SP7 pinned US gap/fold cases. Versioned complete JSON replay, malformed-input/state and ownership checks pass. |
| E19 | unit-test / race / lint | [checks](wi002/checks.json), [combined](wi002/combined.log), [race](wi002/race.log), [lint](wi002/lint.log); all scheduler2 + existing scheduler tests, race and normal/minimal lint pass. No schema regeneration required. |
| E20 | executable specification (not theorem) | [Lean source](../lean/Enumeration.lean), [examples](../lean/Examples.lean), tool/hash/commands in checks.json; Lean4.24.0 compilation and discriminating examples pass; SP1–SP6 proposition definitions, no completed proofs or correspondence claim. |
| E21 | mutation-test / independent review | [mutations](wi002/mutations/manifest.json), [review](wi002/review.md), [restored baseline](wi002/restored.log); all three source mutants detected; source restored and focused suite passes. |

The initial invalid-generator minimized Rapid artifact is preserved in occurrence/
testdata/generator-regressions; it is not a production defect reproduction. The
standalone pending-jitter replay carries full spec, seed, revision, windows and
expected results. No new production dependency or CHASM routing is introduced.

Original checkout received additional concurrent edits during WI-002. This task
made no writes there; original untracked entries remain. Its tracked patch now
differs from the M1 snapshot due to new files/hunks; checks.json records its hash
and original-status.txt records inventory. Task worktrees remain retained; see
[registry](../worktrees.md). All execution and commits are local.

## Accepted WI-003 evidence (partial M2)

Source **aee04d35b22d8085e133a09f58df426249f5a89c**, with clean source at final
completion/race profile start. Dependency c945ae3ba (accepted WI-002). D27/D28
resolve bounded C6/C7/C4 completion behavior; this does not accept full M2.

| ID | Kind / claim | Artifact and result |
|---|---|---|
| E22 | source-inspection / contract | [WI-003](../work-items/WI-003.md), [review](wi003/review.md); first completion wins, explicit absence/null/failure, current pause policy, failure/status validation; DIV5 follows current CHASM no-result clearing. |
| E23 | bounded-model-check / reachability | [model](../model/completion/completion.fizz), [results](../model/completion/evidence/results.json), [mapping](../model/completion/README.md); 173584 nodes /162732 unique states, empty frontier at 100/200; five safety assertions/seven witnesses and three mutants pass expected checks. No liveness claim. |
| E24 | property-test / CHASM-test / replay | [manifest](wi003/completion/manifest.json), [log](wi003/completion/0.log); 10000 Rapid cases seed 1, first callback plus1–16 duplicates, bytes0–32; presence/status/identity/policy matrix, actual serialize/reload/failed commit, exact resource boundaries/recovery, aliases and poisoned selective-loading checks pass. Complete JSON replay is scheduler2/testdata/completion_v1.json. |
| E25 | generation / race / lint | [checks](wi003/checks.json), [generation](wi003/generation.log), [race manifest](wi003/race/manifest.json), [lint](wi003/lint.log); make proto, all scheduler2 packages plus nil registry, full race and normal/minimal lint pass. |
| E26 | mutation / review / provenance | [Go mutants](wi003/mutations/manifest.json), [review](wi003/review.md), [registry](../worktrees.md); all three source mutants caught and restored, independent findings resolved. Original checkout untouched by task, worktrees retained, local commits only. |

The model's initialized carryover is an explicit abstraction; the prior M1 model
covers retry identity/horizon, and composition is not proved. Continued-as-new,
reset, retention, sequential action admission and full overlap/backfill are future
work. No second action or production callback route is claimed by WI-003.

## Accepted WI-004 evidence (partial M2)

Source **9064bed65b0cf67659925986df04ea7ad27c8dcb**; clean source for final profiles.
Prerequisite c19c343fe (WI-003); contract prerequisite b57e10e18. D29–D31 cover
strict sequential SKIP, cap4 retention and independent generation tokens.

| ID | Kind / claim | Artifact and result |
|---|---|---|
| E27 | source-inspection / contract | [WI-004](../work-items/WI-004.md), [review](wi004/review.md); Pending/Started/Ambiguous occupancy, actual-time catchup, completed-only eviction, bounded generator token/deadline. DIV6 prevents lost-ack overlap. |
| E28 | bounded-model-check / reachability | [source](../model/sequential/sequential.fizz), [results](../model/sequential/evidence/results.json); 3076 states and empty frontier at 100/200, seven safety assertions/six witnesses, three named mutants detected. Six occurrences and cap4; no liveness or automated refinement claim. |
| E29 | property-test / CHASM-test / replay | [manifest](wi004/sequential/manifest.json), [log](wi004/sequential/0.log); 10000 Rapid scenarios up to 128 commands, budget 1–8; real pure-task dispatch, failed multi-output publication, physical successors, second/third-request carryover, retention, jitter/update/clock/resource boundaries, poisoned skipped-path carryover and input ownership pass. Explicit generation_v1.json replay succeeds twice. |
| E30 | generation / race / lint / mutation | [checks](wi004/checks.json), [race](wi004/race/manifest.json), [lint](wi004/lint.log), [Go mutations](wi004/mutations/manifest.json); make proto, full scheduler2+registry race and normal/minimal lint pass. Four Go defects detected; restored source passes final profiles. |
| E31 | review / provenance | [review](wi004/review.md), [registry](../worktrees.md); two behavior findings fixed, independent acceptance review clear. Original checkout untouched by task; worktrees retained and commits local. |

Atomic DB publication and fair physical delivery remain assumptions; CHASM node
serialization, task validation/reconstruction and rollback publication are tested.
Other overlaps, queue admission/drain, manual/backfill fairness and general retention
remain future work. The generation task is not selected by production routing.

## Accepted WI-005 evidence (partial M2)

Source **443587c3e**, clean at final buffer/race profile start. Prerequisite256ac1df6;
contracte706a6340. D32–D34 define bounded automatic queues and independent drain.

| ID | Kind / claim | Artifact and result |
|---|---|---|
| E32 | source-inspection / contract | [WI-005](../work-items/WI-005.md), [review](wi005/review.md); FIFO BUFFER_ONE/ALL, cap4, no enqueue debit, current eligibility/template/carryover with queue-time workflow identity. |
| E33 | bounded-model / reachability | [results](../model/buffer/evidence/results.json), [mapping](wi005/review.md);69,207 nodes/69,196 unique states at100/200, empty frontier, seven safety/seven witnesses, four mutants. |
| E34 | property / CHASM / replay | [manifest](wi005/buffer/manifest.json), [log](wi005/buffer/0.log);10000 Rapid cases seed1 up to128 commands, capacity boundaries, current adoption, exhausted-generator wake, failed publication/reload, stale/early tasks, rollback, disposal/refill, bidirectional queue ownership and standalone buffer_v1.json replay. |
| E35 | generation / race / lint / mutation | [checks](wi005/checks.json), [race](wi005/race/manifest.json), [lint](wi005/lint.log), [mutations](wi005/mutations/manifest.json); regenerated schema, race/registry and scoped normal/minimal lint pass; five Go mutants detected and restored. |
| E36 | review / provenance | [review](wi005/review.md), [registry](../worktrees.md); independent review clear after ownership test; initial replay/lint failures preserved and resolved. Original checkout untouched by task; worktrees retained, commits local. |

Manual mapping hides intermediate model drain rearms and compares token freshness,
not counter equality; no automated correspondence or liveness proof. Drain payload
loading is selective; generation still reads its combined definition/template.
CHASM node publication/reload tests do not prove backend crash atomicity or fair
task delivery. ALLOW_ALL, cancel/terminate and manual/backfill remain M2 work.

## Accepted WI-006 evidence (partial M2)

Source **58ad6c3d6**, clean at final allowall/race profile start. Prerequisite8065ee20c;
contracta81fa9f57. D35–D38 establish keyed ALLOW_ALL and transaction-wide isolation.

| ID | Kind / claim | Artifact and result |
|---|---|---|
| E37 | source-inspection / contract | [WI-006](../work-items/WI-006.md), [review](wi006/review.md); separate blocking/count summaries, frozen policy, start-only terminal intents, capacity drop/wait and data-map task index. |
| E38 | bounded-model / reachability | [results](../model/allowall/evidence/results.json), [scope](../model/allowall/README.md);109320 visited/73988 valid/18220 unique states at100/200, nine safety/six witnesses, three mutants. Saturated fixture/one ambiguous older key/one waiter; no liveness or automated refinement. |
| E39 | property / CHASM / replay | [manifest](wi006/allowall/manifest.json), [log](wi006/allowall/0.log);10000 Rapid scenarios5–128 commands, keyed accounting/policy/ownership oracle, four same-instant tasks, cap4 recovery/eviction, old immutable retry, mixed blocker/queue, early callback, config-only disposal, exact pending index, failed publication/reload and standalone allowall_v1.json replay. |
| E40 | generation / race / lint / mutation | [checks](wi006/checks.json), [race](wi006/race/manifest.json), [lint](wi006/lint.log), [mutants](wi006/mutations/manifest.json); schema regenerated, full scheduler2/registry race and scoped normal/minimal lint pass; six source mutants detected/restored. |
| E41 | review / provenance | [review](wi006/review.md), [initial real isolation failure](wi006/isolation-initial.log), [registry](../worktrees.md); poisoned unrelated metadata and request remain unread through full commit/reload after D38 fix. Independent review clear. Original checkout untouched by task, initial untracked paths retained. |

Data maps/root PendingStarts prevent CHASM cleanup from preparing unrelated operation
values. This is value-deserialization evidence, not fewer physical DB reads. The local
layout change has no migration support. Root summary/adapter composition is checked by
concrete tests; model failed-publication/stale-delivery steps declare trusted boundaries.
Real storage-backend crash recovery, cancel/terminate, manual/backfill, public APIs,
proof correspondence and load evidence remain subsequent work under the active goal.

## Accepted WI-007 evidence (partial M2)

Implementation6175ef959; independent tests/models integrated4e4f3b605. Final source688eba86d passes all item gates after the independent review corrections.
See [acceptance review](wi007/review.md); no gate is waived.

| ID | Kind / claim | Artifact and current result |
|---|---|---|
| E42 | source-inspection / contract | [WI-007](../work-items/WI-007.md), D39–D42; fenced cancel/terminate, binding-time horizon, no effect-driven occupancy release, eligibility-before-replacement. Source review clear. |
| E43 | bounded-model / reachability | [results](../model/effects/evidence/results.json), [scope](../model/effects/README.md); protocol90058 visited/72728 valid/18168 unique, selection121/121/99, binding176/176/138 at100/200; twelve safety/fourteen temporally strengthened witnesses and six mutants. Manual factorization, no composition/liveness/refinement proof. |
| E44 | property / CHASM / replay | [manifest](wi007/effects/manifest.json);10000 Rapid scenarios and real CHASM effect publication/reload, frozen retry, poisoned partitions, delayed binding, cancellation and saved replay pass. Exact-deadline and stale physical-task checks pass. |
| E45 | generation / race / lint / mutation | [race](wi007/race/manifest.json), [mutations](wi007/mutations/manifest.json); final full scheduler2/registry race and scoped lint pass; eight source mutants detected/restored, including permit removal and deadline issuance. |
| E46 | review / provenance | [initial fixture failure](wi007/independent-initial.log), [initial lint](wi007/lint-tests-initial.log), [original checkout inventory](wi007/original-status.txt); fixture corrected, witness review corrections retained, all item review gaps resolved; full milestone gates remain open. |

The product model's actual300s timeout is retained; accepted factored models
separate transport from selection and supplement delayed binding. They do not
cover all eligibility/transport products or all-ineligible queues. Concrete tests
cover CHASM publication/value isolation, not physical database atomicity. Original
checkout received no task writes; no production routing, migration or rollout.


## Accepted WI-008 evidence (partial M2)

Implementation97e9a7eca, review fixf525eacab; prerequisite0c07f4a24.

| ID | Kind / claim | Artifact and current result |
|---|---|---|
| E47 | source-inspection / contract | [WI-008](../work-items/WI-008.md), D43–D46; command identity separate from operation retention, manual eligibility overrides, bounded accepted-wait ingress. |
| E48 | framework-test | [History dedup tests](wi008/framework-dedup.log); both request-ID storage layouts, empty IDs, count/age retention and type separation pass. Not a scheduler2 public API test. |
| E49 | implementation baseline | [generation](wi008/generation.log), [reviewed baseline](wi008/reviewed-baseline.log), [source lint](wi008/lint-source.log); schema generation, existing scheduler2/registry tests and initial scoped lint pass. |

Independent review found unnecessary template loading for policy-only disposal;
fixf525eacab avoids it and adds deterministic equal-nominal retention ordering.
Final source12740ef1d resolves the review findings; M2–M4 remain incomplete.

| ID | Kind / claim | Artifact and result |
|---|---|---|
| E50 | bounded-model / reachability | [model results](../model/manual/evidence/results.json), [limits](../model/manual/README.md); commands259 unique, admission17434 unique at100/200; nine safety/fifteen witnesses/eight mutants, manually factored with framework boundary. |
| E51 | property / CHASM / replay | [manual manifest](wi008/manual/manifest.json);10000 scenarios1–128 commands, actual request-ID DATA child, failed publication, stable identities, same-time records, pause/budget/catchup bypass, capacity recovery, poisoned partitions, current queued payload/carryover and stale stored tasks. Saved version1 replay. |
| E52 | race / lint / mutation / review | [checks](wi008/checks.json), [race](wi008/race/manifest.json), [lint](wi008/lint.log), [mutants](wi008/mutations/manifest.json), [review](wi008/review.md); all pass, eight Go defects detected/restored. Original checkout untouched by task, worktrees retained. |


## WI-009 work in progress

| ID | Kind / claim | Artifact and current result |
|---|---|---|
| E53 | design review / counterexample | [arbitration design](../design/backfill-arbitration.md), [WI-009 D47–D50](../work-items/WI-009.md); readiness-only turns and insert-only vacancy turns can starve accepted work. Reviewed concrete vacancy lease addresses those traces under stated external-capacity assumptions; implementation/model gates remain. |
| E54 | property / unit / source review | [range initial run](wi009/range-initial.log);10000 independent interval cases with probe budgets1/2/8 and directed inclusive/fractional/jitter/exclusion/nominal-rebase tests pass. Helpers retain candidate across waits; caller must preserve fixed end and publish disposal with handoff. Not CHASM integration or fairness evidence. |

WI-009 remains active. No coordinator, backfill API, integrated fairness, model
refinement or milestone completion is implied by this helper checkpoint.


E54 helper checkpoint sourceff9ddd706: [clean-source profile](wi009/range/manifest.json)
and [checks](wi009/range-checks.json) record10000 cases, occurrence package race and
scoped lint passing. These checks do not satisfy the pending WI-009 integration gate.


### WI-009 coordinator verification in progress

| ID | Kind / claim | Artifact and current result |
|---|---|---|
| E55 | independent bounded model / conditional liveness | [model scope](../model/backfill/README.md), [parent rerun](wi009/model-integrated-results.json); arbitration7876 unique/cursor144 unique at100/200, eight safety/six witnesses/one weak-fair liveness assertion, six named mutants detected. One contested operation slot; no cap4 refinement claim. |
| E56 | real Node integration / public adapter | [request checks](wi009/request-tests.log), [directed full queue](wi009/fullqueue.log), [mixed sample](wi009/mixed-initial.log), [delayed capacity sample](wi009/capacity-initial.log); local wrapper dedup/publication/rejection, sole admission owner, fullqueue+tracked+threeALLOW reservation recovery,100 cases each randomized workload. Larger exploration/race pending. |
| E57 | independent review / sensitivity | [composition audit](../design/backfill-composition.md), [seven Go mutations](wi009/mutations/manifest.json), [scoped lint](wi009/lint-reviewed.log); token preflight and upper-clock clamp review fixes, seven named implementation defects detected/restored. Initial rejection test failure was framework transition bookkeeping, preserved in [log](wi009/request-tests-initial.log). |

These are intermediate observations on the isolated coordinator worktree, not
WI-009 acceptance. Model extension for concurrent cap4, final exploration/race,
remaining cursor/arbitration boundary tests and review remain outstanding. The
original checkout and its unrelated modifications are preserved.


E56 final exploration: [manifest](wi009/backfill/manifest.json) records10000 cases
each for two Rapid workloads. [Checks](wi009/checks.json) link full race and later
boundary/replay/overlap race runs. Version1 coordinator JSON replay is independent
of Rapid and covers rejected publication, reload and revision rebase with complete
frozen requests and resource-turn projections. E57 final lint passes; runner review
fix preserves the [failed classification run](wi009/model-reviewed/manifest.json)
and [corrected model run](wi009/model-final/manifest.json).

E58 cap4 supplement: [scope](../model/backfill/CAPACITY.md), [final results](wi009/model-final/capacity/results.json)
check tracked+ALLOW counted occupancy directly:36674 unique states at100/200,
four safety/six witnesses/one conditional liveness and four named mutants. This
removes the one-slot abstraction for that fixture; independent review retains
CPU fairness, policy/definition change, two-ready-request and refinement limits.
Final independent item acceptance review is pending; no full M2–M4 claim.


WI-009 accepted at source **bd7b316fd** after independent final review found no
blocking item gap. E53–E58 retain their finite bounds and trusted framework/CPU
fairness boundaries. [Review record](wi009/review.md) maps resolved findings and
remaining milestone obligations. WI-010 is the next proposed item; M2 is partial.


## WI-010 terminal-fence increment (work item remains open)

| ID | Kind / claim | Artifact and current result |
|---|---|---|
| E59 | source contract / review | [D51 and proposed lifecycle design](../design/lifecycle.md), [clock investigation](../design/lifecycle-clock.md); terminal-fence source review clear; first-success clock and idle design remain separate open implementation. |
| E60 | real Node / History engine | [terminal checks](wi010/terminal-checks.json), [History log](wi010/history-s8.log); close publication/reload, all late result kinds, poisoned partitions, new-control rejection/prior dedup success; actual engine allocates different runs for one business ID and fences old in-flight start result. Captured persistence, not database proof. |
| E61 | generation / race / lint / sensitivity | [review](wi010/terminal-review.md), [five mutants](wi010/terminal-mutations/manifest.json); schema, full scheduler2/registry baseline, focused component/History race and scoped lint pass. Anchor/idle/L3 and remaining lifecycle/update compositions are not complete. |


## WI-010 activity and idle increment (accepted)

| ID | Kind / claim | Artifact and current result |
|---|---|---|
| E62 | real Node / activity properties | [progress and limits](wi010/activity-progress.md), [10000-case seed1 manifest](wi010/activity-exploration/manifest.json), [token preflight regressions](wi010/idle-preflight.log); pass. Original manifest overstates scenario event bound; actual bound35 plus setup is documented and runner corrected. |
| E63 | independent finite lifecycle model | [model scope/mapping](../model/lifecycle/README.md), [results](wi010/lifecycle-model-results.json); activity146 and idle1104 unique states at100/200; five mutants including temporal lost rearm detected. Conditional fairness, no Go refinement claim. |
| E64 | baseline / lint / History race | [baseline](wi010/activity-idle-baseline.log), [staged-source lint](wi010/staged-lint.log), [History race](wi010/activity-idle-history-race.log), [component race](wi010/activity-idle-race.log); pass. Later composed tests and sensitivity are recorded in E65. |
| E65 | composed Node replay / sensitivity | [two saved traces](../../../../chasm/lib/scheduler2/lifecycle_replay_test.go), [composed race](wi010/composed-race.log), [five Go mutants](wi010/activity-idle-mutations/manifest.json); pass. Real hold-removal and poisoned-payload checks included; expanded History recreation evidence follows in E66. |
| E66 | retention/pause and exact-run recreation | [checks/fingerprints](wi010/activity-checks.json), [retention/pause race](wi010/retention-pause-race.log), [History delete/recreate race](wi010/history-delete-race.log); pass. Engine-routed old handler inputs with colliding numeric tokens and in-flight start/cancel effect; captured persistence, not physical queue processor or database. |

WI-010 accepted after [independent review](wi010/acceptance-review.md) and
[final staged-source lint](wi010/acceptance-lint.log). Intermediate failures remain
recorded with their resolutions. WI-011 is the next dependency-ordered M2 item;
this acceptance does not close M2–M4.


## WI-011 atomic update and current mappings (accepted)

| ID | Kind / claim | Artifact and current result |
|---|---|---|
| E67 | contract/source review | [atomic update](../design/atomic-update.md), [current mapping inventory](../design/current-mappings.md), [coverage audit](../design/m2-coverage.md); D55/D56 coherent, remaining M2 gaps explicit. |
| E68 | Node update / replay sensitivity | [progress](wi011/progress.md), [baseline](wi011/baseline.log), [update race](wi011/update-race.log), [five mutants](wi011/update-mutations/manifest.json); whole replacement, rejection preflight, bounded request replay and immutable requests pass; [saved replay race](wi011/replay-measure-race.log) passes. |
| E69 | composed transitions / measurement | [composition race](wi011/composition-instrumentation-race.log), [History read boundary](wi011/history-read-race.log); historical lease rebase and queued current input pass, cold1/warm0 API reads measured. [materialized-node measurements](wi011/replay-measure-race.log) pass; no database IO reduction claim. |
| E70 | generated atomic-update relation | [strengthened10000-case manifest](wi011/updates-exploration-final/manifest.json), [full scheduler2 race](wi011/final-race.log), [early failure/chain race](wi011/failure-chain-race.log); exact pending-start tasks, blocker, chain, immutable deadline and accepted-input controls/source/request projection pass. [Shrunk fault](wi011/update-obligation-mutation/manifest.json) and [standalone replay sensitivity](wi011/update-obligation-replay-mutation/manifest.json) detect lost start authority and restore source. Shared testing.T helper failures are not claimed to shrink. |
| E71 | numeric boundary and recovery | [configuration](wi011/configuration-limits.log), [enumeration](wi011/occurrence-limits.log), [focused race](wi011/boundary-final-race.log); limit neighbors, rejected publication and recovery pass, including scan8 and effect deadline. [Effect request-byte race](wi011/effect-limits-race.log) passes both effect kinds; [complete matrix](../design/m2-coverage.md) records structural bounds and recovery. [Independent acceptance review](wi011/acceptance-review.md) passes; M2 complete. |

WI-011 and M2 accepted E67–E71 with [final scoped lint](wi011/acceptance-lint.log) and [source fingerprints](wi011/checks.json). M3/P2.5 and M4 remain open.

## WI-012 explicit functional backends and inventory (accepted)

| ID | Claim | Evidence |
|---|---|---|
| E72 | Baseline inventory and applicability audit | [242 baseline rows](../design/functional-coverage.md), [machine-readable source hashes](../design/functional-coverage.json); refinements preserve public/observability obligations, scheduler2 rows pending. |
| E73 | Existing backend setup preservation | [checks](wi012/checks.json), [full shared suites](wi012/shared-suites.log), [final descriptor cases](wi012/descriptor-expectations.log), [mixed backend](wi012/mixed-backend.log), [helper race](wi012/helper-race.log), [lint](wi012/lint-final.log), [independent review](wi012/review.md); pass. No scheduler2 frontend execution claimed. |

## WI-014 local public persistence and visibility (accepted)

| ID | Claim | Evidence |
|---|---|---|
| E74 | Real public SQLite lifecycle and independent identity | [checks](wi014/checks.json), [public run](wi014/public-accepted.log); paused empty-spec profile, request replay, persisted archetype, retained old run, no old fallback. |
| E75 | Typed query isolation and API boundary preservation | [public run](wi014/public-accepted.log), [component review](wi014/component-review.md), [seam review](wi014/seam-review.md); old same-ID row, alias collision, pagination, internal-field cleanup, metadata, invalid input and authorization. |
| E76 | Regression/race/lint | [default shared suites](wi014/default-shared.log), [race](wi014/race-accepted.log), [lint](wi014/lint-accepted.log); pass with limits/source timing in checks. |

WI-013 proofs are accepted separately below; WI-015 full execution remains open. These entries do not
claim full scheduler2 shared-suite parity or generated SchedulerService transport.

## Active WI-015/WI-016 checkpoint (not acceptance)

WI-015 [runtime checkpoint](wi015/runtime-checkpoint.json) records actual workflow
completion/CAN transport, held acknowledgment reconciliation, retained ambiguous
start authority, runtime race and separate public mutation counter checks. Full
public behavior remains unfinished. WI-016 boundary-biased calendar verification
found and fixed a London fold error; 10,000 generated cases and lint pass, with
independent review and mutation evidence being completed. Neither checkpoint closes M3.

### WI-015 execution checkpoint update (not acceptance)

[Archived checkpoint](wi015/execution-checkpoint/manifest.json) records full29 shared
methods, scheduler2/callback race, checkpoint lint, and both actual payload-capacity
profiles passing. [Visibility ports](wi015/visibility-ports/manifest.json) record
five additional public scenarios and lifecycle unit verification, pending independent
review. Three unchanged lifecycle/time-skipping cases still fail; physical recovery
and remaining applicability rows are open. Checks predate subsequent source changes
and must not be treated as final WI015 or M3 acceptance.

WI-015 additional checkpoint: actual SQLite shard reload with lost start response
and identical request/first-run retry passes3.387s (evidence/wi015/recovery-checkpoint.json;
physical-start-recovery.md). Six metadata/index ports including idle query pass
3.365s after context/import cleanup (visibility-idle/manifest.json). Consolidated
port lint initially reported22 issues; owners are fixing style/deprecation findings.
Two of three unchanged progress cases pass after bounded immediate coordination;
weekly execution-timeout progress remains under repair. No M3 acceptance claim.

## Integrated runtime verification checkpoint (M3 remains open)

The bounded two-quantum coordinator pump, due Start transfer dispatch and cached
future candidate time-skipping gate fix now pass all18 unchanged lifecycle
contract leaves (34.673s). Three extra time-control cases pass10.268s. Independent
reviews preserve per-quantum arbitration/logical wake fences and identify expanded
public definition/state bounds as outside earlier finite proof profiles.
Public combined100 ingress passes exact validation and rollback/reload tests;
four spec ports and three catchup/buffer/result parity ports pass. Evidence is
under wi015/coordinator-progress, timecontrol-ports, public-command-capacity,
spec-ports and behavior-ports. Current whole scheduler2/callback core passes
(root18.375s); consolidated lint04 passes0issues. Archive: consolidated-checkpoint.
All-scheduler2 public combined run is in progress. Remaining coverage includes
local-cluster physical restamping, listing/metadata parity, stale physical fences
and full capacity/time-domain compatibility. M3 is not accepted; M4 remains pending.

The combined public checkpoint completed PASS140.002s for all TestSchedule2 cases
present at compilation. Core packages and normal/minimal lint passed. Exact
commands, staged source hashes, logs and evidence limits are in
wi015/consolidated-checkpoint/manifest.json. This local source checkpoint is not
WI015/M3 acceptance; subsequent listing/XDC ports and compatibility audit remain.

## Physical restamp and metadata increment

The checkpoint8dd3cda3a scheduler2/callback race run passes (root193.095s); this
predates the next source changes. Two-localcluster standby restamping passes
11.794s with independent typed and persisted scheduler2 identity on both clusters,
same run ID, initial offset0 and a distinct successfully persisted timer task at
the expected shifted deadline. Independent review passes; this proves task
publication/restamp, not later stale-envelope consumption or failover.

Initial listing and ScheduleId alias/custom query ports pass2.190s. The missing
Patch/Delete attribution provider is fixed: public metadata+old/new closed-mutation
comparison pass1.174s; skeleton/empty/closed unit pass0.646s. Consolidated lint for
the listing/XDC/provider increment passes; the last closed-mutation test was added
after that lint checkpoint. Artifacts: wi015/xdc-restamp, listing-ports,
context-metadata, consolidated-checkpoint. Original setup, compile and behavioral
failures are retained separately. Parent source review and independent api review
record precise scope; no migration parity is inferred.

M3 remains open. The read-only remaining-public-contract-audit identifies fixed4
public capacity and timestamp-domain mismatches. Configured capacity semantics,
full source-backed timestamp acceptance, and actual close/recreate stale physical
delivery are assigned for implementation; M4 load/state-growth remains pending.

### Authorization and remaining compatibility progress

Actual frontend denial checks pass for all8 schedule APIs with zero selected
backend dispatches (wi015/authorization/manifest.json, PASS1.176s). Initial fixture
cleanup failure is preserved and corrected by restoring ordinary delegation during
cleanup. Existing authorized/persisted identity tests remain separate evidence.

Capacity and wide timestamp work is implementing, not accepted. The configured
capacity counterexample reproduces the prototype4-slot mismatch; completed tracked
occupancy must retain10 canonical workflow-close-time records. Timestamp baselines
establish V1/old CHASM pre2000 interval support and a known old-CHASM ScheduledTime
ignore defect; V1/API semantics govern exact manual timestamps. Optional wide time
fields preserve values outside nanosecond range without wrapping. Independent
review, expanded recovery and combined regression/race/lint gates remain required.

### Physical old-incarnation replay checkpoint

Actual old Start/late result/old callback and both cancel/terminate timer-envelope
replays pass together3.854s after delete/recreate plus CloseShard. Actual queue
interceptors provide delivery barriers; old callback token binds original run and
request ID, effect payload binds ID/logical wake/target chain. No extra external
RPC or replacement mutation is observed. Parent source review passes with limits:
this establishes stale-before-RPC effect rejection, not lost accepted effect
responses or arbitrary failover. Evidence: wi015/recreation-physical. These tests
and capacity/timestamp changes still require final combined verification.

### Composed public compatibility checkpoint

Public capacity and wide timestamp runtime changes are implemented and independently
reviewed. Full scheduler2 public suite passes146.941s; all selected core/callback/
queue packages pass (root19.576s); normal/minimal lint passes. Unchanged V1 and
CHASM shared suites pass88.049s. Exact public capacity neighbors2/5 and retained
completion occupancy are verified; timestamp differential checks cover ancient
interval ranges and exact manual times through9999, with saved-state/reload tests.
All expanded public claims retain explicit separation from finite internal proofs.

The242-row functional overlay now attaches current evidence and named exclusions;
legacy ownership/token migration cases are not marked passed. Source-backed
migration-and-policy-boundaries.md records those limits. M3 acceptance audit,
composed race completion, integration of already accepted WI013 proof artifacts
and final correspondence revalidation remain open. M4 workload specification is
WI017 draft; cardinality-dependent metadata work remains its implementation and
measurement obligation. No milestone completion is inferred from this checkpoint.

## WI-013 exact enumeration proofs and Go correspondence (accepted)

| ID | Claim | Evidence |
|---|---|---|
| E77 | Actual executable/public enumeration proof obligations | [checks](wi013/checks.json), [final proof build](wi013/proofs/build11/manifest.json), [obligation map](wi013/proofs/review-repair-status.md);93 exports audited, finite raw-source hypotheses explicit. |
| E78 | Independent correspondence and reproducible profiles | [routine](wi013/runner-routine02/manifest.json), [exploration](wi013/runner-exploration01/manifest.json), [proofs runner](wi013/runner-proofs01/manifest.json); both10000-case properties with explicitseed1, exhaustive/direct/savedv1/v2 comparisons pass. |
| E79 | Defect sensitivity and repair provenance | [three core mutants](wi013/correspondence-mutations-build07/manifest.json), [replacement mutant](wi013/correspondence-update-mutation/manifest.json); direct Rapid failures, shrunk concrete replay, mutant failure and restored pass. |
| E80 | Independent review and checks | [public proof review](wi013/independent-proof-review.md), [composition review](wi013/independent-composition-review.md), [integrator review](wi013/integrator-review.md), [race](wi013/build08-race.log), [tagged lint](wi013/tagged-lint.log); runtime binary identity reuse explicit. |

No universal Go/calendar/JSON/persistence refinement claim. Remaining SP7 calendar
boundary and public functional M3 gates remain open, as does M4.

## WI-016 bounded calendar verification (accepted)

| ID | Claim | Evidence |
|---|---|---|
| E81 | Independent bounded calendar oracle and pinned boundary cases | [Profile](wi016/verification-profile.json), [oracle review](wi016/independent-oracle-review.md); 10,000 generated cases, exact source timing and trusted primitives explicit. |
| E82 | London fold completeness correction | [Diagnosis](wi016/independent-fold-review.md), [non-author fix review](wi016/independent-calendar-review.md), [literal replay](wi016/calendar-fold-fix/manifest.json). |
| E83 | Final-source fault sensitivity and verification | [Three shrinking/replay mutations](wi016/spec-mutations/campaign02/manifest.json), [acceptance](wi016/acceptance.md), [lint](wi016/acceptance-lint.log), [integration provenance](wi016/integration-provenance.json); bounded SP7/P5.2 accepted. |

WI015 full public execution/shared coverage remains open; this is not M3 completion.

### Integration and independent closure audit

Runtime checkpoint6bcc1cb42 records composed public capacity/timestamp changes;
ff3645950 merges accepted WI013 proof/correspondence and WI016 calendar artifacts
from sch-redesign without modifying that checkout. Documentation conflicts retain
both accepted evidence and current runtime contract/progress. Composed race passes
(root193.913s plus callback/queue packages). Pinned proof/correspondence integration
revalidation is running separately.

Independent M3 closure review identifies remaining actual accepted-effect-response
loss across chain changes, combined virtual-clock/shard-reload/stale timer delivery,
and the stronger bounded-metadata-work requirement. Two logical quanta do not
bound growing public scans/copies; this requirement remains open and cannot be
closed merely by deferring it to performance reporting. These findings now drive
implementation. M3 is not accepted; M4 workload design remains preparatory.

Integrated finite proof/correspondence revalidation passes (integration-correspondence.md).
The bounded-work gap reaches CHASM ordinary-map hydration/synchronization and clean
subtree traversal; source investigation now requires additive lazy/delta collection
and dirty-subtree support, not only scheduler paging. WI018 is being specified with
independent design review and count-based hot/cold scope gates. WI015/M3 remain open.

### WI018 implementation preparation and recovery review

WI018 now has a draft in design/bounded-public-work.md and a dependency record in work-items/WI-018.md. Independent framework review identifies five protocol gates: durable fair dispatch claims, closed-root cleanup ownership, transaction-scoped lazy handles, complete task invalidation, and exact indexed queue equivalence. Instrumentation and baseline growth witnesses are assigned before collection implementation. No bounded-work acceptance is claimed.

The clock recovery witness passes both physical delivery orders (final run05,21.442s); root independently reviewed its source and verified manifest fingerprints against the tested checkpoint. Evidence: wi015/clock-recovery/independent-review.md. The effect recovery witness passed four cases, but root found its original completion barrier could be satisfied by a retry; the corrected exact original-invocation barrier passes run07,8.163s and awaits final independent inspection. Prior passing runs do not establish that stronger causal assertion. M3 remains open and M4 remains preparatory.

### WI018 measured baseline and first implementation prerequisites

Checkpoint60ad60b62 adds operation-site counters with disabled production observers and a deterministic ordinary-map witness. At N4/5/1000, each hot mutation visits30/35/5010 iterator nodes and copies8/10/2000 map keys while decoding zero unrelated DATA. Complete serialized node size remains65 bytes (DATA7 bytes); output size alone therefore misses the work violation. Full CHASM tests pass0.494s and required lint passes. Scope and excluded work are recorded in wi018/baseline/README.md.

The opt-in DataMap API/ownership protocol is specified and implementation is assigned, with default traversal unchanged until the separate worklist stage. Dispatch/cleanup review resolves the physical task context deadline and closed-data ownership. Queue index review is testing a monotone automatic-time invariant before accepting any cross-index range bound. Definition preparation must also cover list-preview rebuilds and per-term range expansion. Fixed-width time-key tests pass0.269s; this is a representation prerequisite only. WI018/M3 remain incomplete and M4 measurements remain pending.

### Queue expiry counterexample

The new reducer regression TestBufferClockRollbackAllowsNonPrefixExpiry passes0.275s and scoped normal/minimal lint passes. Three automatic entries with Actual109,102,110 require removing the middle entry at drain110/catchup3 while preserving the last. This rejects the proposed monotone-Actual simplification without changing accepted clock behavior. Evidence: wi018/queue-nonprefix/. A two-generation queue with an atomically published drain predicate and bounded physical cleanup is under design review; no implementation or complexity acceptance yet.

### DataMap and bounded range preparation capability checkpoint

DataMap implements point DATA access with detached ownership, coalesced deltas and transaction/owner checks. Full CHASM suite passes0.435s; independent directed runs pass0.448s/0.415s. Review discovered and repaired delete/reinsert tombstones across multiple synchronization passes. Actual child-only replication, serializer failure with durable reload, nil-library detached reading and unchanged raw-map behavior are covered. Evidence: wi018/datamap/ and independent-datamap-review.md. Default full-tree traversal remains; this capability alone does not close the work bound.

The isolated calendar range compiler uses sixteen-value source pages and at most64 elementary steps per call. Fixed/mixed budget partitions,1025 duplicate ranges, reference UTC candidates, corruption and failed-read rollback checks pass (final0.806s). Review fixes reject invalid reversed endpoints and corrupt fixed masks. It is not wired into persisted scheduling or timezone/candidate evaluation. Evidence: wi018/calendar-preparation/ and independent-preparation-review.md.

Combined required make lint-code-fast passes normal/minimal/vet with zero issues (wi018/combined-lint02.log). A standalone secondary count-trie prototype is undergoing separate review and is not included in these capability acceptance claims. M3 and M4 remain open.

### Secondary count-index checkpoint

The canonical Patricia secondary trie passes its final routine2.286s and independent review. Tests include100 Rapid traces of150 operations, shared roots, full96-bit depth and staged overflow rollback. At maximal depth the observed point operation uses1255 reads/290 writes/193 release visits; fixed source-derived ceilings are recorded in wi018/count-trie/review.md. Scoped required make lint-code normal/minimal/vet passes with zero issues. This is a standalone in-memory primitive, not the CHASM persistence adapter or complete queue index.

Single-root10000-key value/ref storage is1,879,906 bytes (2,519,874 including bare IDs); framework overhead and outer-index duplication are excluded. No claim that the complete layout fits the existing execution size limit is made. The two-generation queue protocol preserves a completed drain predicate while finite old-generation cleanup proceeds, and now has an implementation direction using shared atomic batches. M3/M4 remain open.

### Versioned memo comparison capability

The optional VisibilityMemoVersionProvider now lets opted components compare a fixed persisted revision on transaction close instead of assembling/cloning the full Memo. Raw providers retain prior behavior; cold/passive cache refresh preserves existing visibility task authority. Full CHASM tests pass0.474s, root independent memo tests pass0.413s, and post-generation required fastlint03 passes with zero issues. Evidence: wi018/memo-version/ and independent-memo-version-review.md.

This capability is not yet enabled by scheduler2. Providers must atomically version every semantic Memo change; rejected-publication coverage reconstructs the prior committed image and is not an actual database-fault experiment. Actual export remains unchanged and output-proportional. Bounded-layout worklists and scheduler integration remain open. M3/M4 remain incomplete.

### DataMap full-transaction baseline and worklist implementation

The new default DataMap fixture passes0.447s and independent source review. At N4/5/1000 it performs zero all-key hydration/copy/sync, but hot point transactions still visit37/43/6013 iterator nodes. Only the addressed DATA is decoded after reload; small root/addressed serialization stays51 full-node protobuf bytes. This is evidence of the remaining traversal gap, not an overall speedup or bounded-work claim. Raw counts/failures and exact source hashes are in wi018/datamap-work/.

Required fastlint passes on a detached6e80c11cf checkout plus the exact test, avoiding concurrent unfinished framework edits. The reviewed worklist implementation plan classifies every hot/cold/export loop, keeps global snapshot traversal unchanged, defines fixed component/task slots and transaction worksets, and forbids silently falling back to full scans. Descriptor/cache implementation is underway; public scheduler opt-in waits for complete gates.

### Persisted calendar preparation capability

The registered CalendarPreparation component now stores typed source pages and its fixed cursor using DataMap. Source staging is outside ordinary mutation; installation is capped at16 pages and mask preparation at64 elementary steps. Actual CHASM serialization/reload after each phase, failure-discard replay, source replacement, poisoned unrelated source and nil-library decoding are covered. Root review fixed uninitialized staging acceptance; final full scheduler2 tests pass11.518s, nil-library checks0.663s, scoped lint zero issues. Evidence: wi018/definition-preparation/ and independent-definition-preparation-review.md.

The component remains unselected by Scheduler/Generator. Superseded source pages still require a durable cleanup-owner FIFO before adoption; current source replacement fences and retains them. Counters for Advance are not full-transaction work evidence. Timezone/candidate preparation, framework worklists and runtime integration remain incomplete; M3/M4 stay open.

### Atomic index batching checkpoint

Count-index updates now share one staged keyed backend batch with outer/root metadata, publish once, and poison the batch on any error. Touched edges validate bit progression and immediate aggregates; malformed referenced pages fail before publication. Final count/batch routine passes3.606s; independent review and exact source hashes are archived in wi018/index-batch/. Required fastlint passes in the isolated verification worktree with frozen source. This proves standalone backend atomicity, not CHASM persistence atomicity.

The outer prototype's10000-key footprint exceeds8MiB before framework overhead. It is not accepted for adoption on that evidence; fixed-capacity canonical buckets are being investigated to reduce amplification without changing public capacity semantics. Framework and scheduler integration, storage accounting and M4 measurements remain open.

### Superseded definition-page ownership

CalendarPreparation now retains an exact FIFO cleanup descriptor for each replaced source, including its installed frontier and seven field counts. Cleanup preflights/deletes at most16 pages from one owner per call; new replacements cannot overtake the head. Rejected publication/reload, partial installation, corruption, empty owners, exhaustion and current-source preservation tests pass. Full scheduler2 tests pass12.473s, scoped lint zero issues, and root independent source review finds no blocker. Evidence: wi018/definition-cleanup/ and independent-definition-cleanup-review.md.

This closes the standalone component's missing cleanup-owner discovery mechanism. Runtime maintenance scheduling, bounded framework transactions and adoption remain unimplemented; M3/M4 stay open.

### Bounded framework and remaining index/calendar gates

The opt-in fixed-slot framework now has passing directed tests for cross-component DATA-driven task invalidation, attached/detached lifecycle, cold task refresh, timer regeneration, logical duplicate delivery and serializer/structure failure followed by reconstruction of the prior image. Root independently ran `CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm -run 'TestBounded' -count=1`: PASS0.495s on the in-progress worktree. This directed run is not final frozen-source acceptance, full persistence-fault evidence, or scheduler runtime integration. Independent review and consolidated lint remain required.

The canonical16-key bucket index passes its standalone behavior/reference gates, but the10000-entry permuted-time witness still occupies11,394,071 raw key/value bytes before CHASM overhead. Evidence: wi018/bucket-index/review.md and manifest.json. Compact tagged encoding and key representation are under investigation; no public-capacity reduction is authorized or used to close this gap.

The prepared-calendar contract now requires resolved timezone fingerprints and restart of unfinished evaluation when named-zone resolution changes, preserving published candidates. A supplied immutable timezone capability permits pure resumable evaluator implementation; it does not by itself bound Go timezone lookup work or implement production named-zone resolution. Those remain explicit runtime-adoption gates. WI018/M3 remain implementing and M4 measurements remain pending.

### CHASM index adapter integration in progress

The private keyed index adapter now stages opaque pages and owning roots through DataMap in the same enclosing CHASM transaction. Publication/discard/reload tests pass. A new full-close locality witness passes0.662s with identical complete work counters at4/5/1000 unrelated records: zero tree visits/map key scans, three addressed DATA decodes and seven serialized nodes (387 full-node protobuf bytes in that build). A poisoned unrelated record remains unread and retained after mutation publication/reload. Evidence: wi018/chasm-index/. This proves the measured operation's isolation, not the growing index's worst-case bound or full scheduler runtime integration.

Initial actual CHASM footprint accounting still exceeds8MiB at10000 sequence records. Canonical compact count encodings are being verified, with independent source review and frozen-source reruns required. Framework review also identified and repaired receiver-subtree behavior and retained-map capacity costs; final framework acceptance is pending. M3 and M4 remain open.

### Accepted fixed-layout framework capability

The opt-in bounded DATA layout and transaction worklists are verified as a framework capability. Full CHASM tests pass0.533s; root independent full suite passes0.485s; peer directed bounded tests pass0.425s. Scoped required lint passes normal/minimal/vet with zero issues. All13 final source hashes match both author and independent review manifests. Evidence: wi018/bounded-framework/ and independent-layout-review.md.

Review fixes preserve child receiver scope and deferred sibling work, refresh system/passive slot caches, enforce transaction-wide task budgets, and discard historical delta/workset map capacity. Fixed-count, cross-slot task, lifecycle, timer, failure/reload, nil-handler detached read and complete export gates pass within the recorded scope. Scheduler2 does not yet opt into this layout. Application index/key/payload bounds, dispatch, incremental calendar integration and public physical recovery remain WI018/M3 requirements; M4 measurements remain pending.

### Resumable prepared-calendar prerequisite

The standalone prepared-calendar evaluator now advances fixed phases over prepared masks with value-only continuation and resolved-timezone identity checks. Known DST fold/gap outputs, mixed/single budget equivalence, restart fencing and distinct local/UTC horizon behavior pass. Final full occurrence tests pass1.100s; root independent full run passes1.050s; scoped lint zero issues. Evidence: wi018/prepared-calendar/ and prepared-calendar-contract.md. Earlier vacuous exhaustion-only fixture passes are explicitly superseded.

This is not runtime adoption or a bounded timezone implementation. Go timezone lookups, production named-zone resolution, union/exclusion/jitter and list-preview integration remain open. WI018/M3 and M4 remain incomplete.

### Compact queue index and CHASM adapter capability

The final canonical bucket/sequence index and DATA adapter pass standalone reference, shared-owner, corruption, read/publication-failure and actual CHASM reload checks. Inline sequence leaves retain exact time without a redundant secondary owner; collapse preserves other shared owners. Final author index tests pass1.449s, independent100Rapid seed2 passes1.457s. Root composed tests pass core CHASM0.493s, scheduler217.405s and all internal packages; required fastlint passes normal/minimal/vet zero issues. Final12 source hashes match the independent manifest. Evidence: wi018/inline-leaf/ (earlier failed storage encodings retained).

At10000 permuted-time records, the actual index alone occupies6,610,295 CHASM node/path bytes across43891 nodes, down from9,528,787. Queue records, concurrent generations, operations/payloads, whole-record wrapper and database/heap overhead remain excluded; this is not end-to-end execution fit or a new public capacity limit. Actual queue facade/decision integration is now assigned per queue-runtime-integration.md. WI018/M3 and M4 remain open.

### Queue runtime integration in progress

The decision summary/change seam and persistent queue facade are being implemented against the accepted compact index. Root stateFor now validates indexed queue summaries and propagates errors through admission, coordinator preview/execution, idle planning, generator/backfill previews and effect reads. Coordinator demands/plans return errors before publishing successor control/tasks. The indexed state path does not reconstruct the finite queue slice.

Directed consumer tests pass0.681s, including rejection of unknown format, negative count and mixed indexed/slice representations before root/task publication, plus existing capacity/update/backfill cases (wi018/queue-runtime/consumers03.log). This is an in-progress shared-tree run; independent review and lint remain required. Public creation still does not activate indexed queues. Facade publication/maintenance, prospective overlay planning and full consumer activation are the next dependency-ordered gates. M3/M4 remain open.

### Indexed queue vertical-path witness

The in-progress runtime consumer path now publishes indexed append/drain mutations before the existing nonrejecting publication phase, preserves QueueState through root replacement, derives public action time/missed counts from the completed plan, and uses logical membership for capacity/visibility projections. Coordinator work processes at most one source cleanup record before its normal quantum, keeps debt wake authority and prevents time skipping across cleanup.

A real tree/coordinator witness passes0.659s: four automatic occurrences create one active operation plus three indexed waiters; rejected drain publication restores prior serialized nodes; successful drain selects one and records debt; three physical WorkTasks clean the source without another admission and preserve the remaining projection. Existing full scheduler2 tests passed17.059s before the later arithmetic correction. Evidence: wi018/queue-runtime/vertical02.log and composed01.log. Public creation remains unactivated pending complete facade/review/lint gates.

Independent review exposed signed overflow in the old `now-Actual` catchup comparison. The finite reference now compares exact instants against the threshold, with a retained old-code counterexample. Matching public expiry and automatic-demand paths use exact comparisons; extreme-nanosecond regression tests pass. This is an explicit arithmetic defect correction, not an undocumented oracle adjustment. M3/M4 remain open.

### New local public schedules select indexed queues

newPublicSchedule now initializes the indexed QueueState and DATA collections. Existing finite New/records retain the slice profile; no existing execution migration or production routing changed. Public tests inspect logical membership or explicit test projection, and a dedicated creation/reload test asserts indexed identity plus an empty finite slice after admissions. Full scheduler2 package passes17.123s; activation/vertical/corrupt-consumer directed tests pass0.684s. Evidence: wi018/queue-runtime/public-package01.log and activation02.log.

Independent runtime review found projection/time-skip callers bypassed queue validation. The shared scalar accessor now validates; no-error CHASM projection callbacks raise invariant failures using the DATA-decoding convention (the direct tests prove a panic, not outer-engine recovery), and time skipping fails closed. Malformed format/count/mixed-state tests cover these direct consumers. Full TestSchedule2 integration completed successfully in161.968s (local command: CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep,integration -p 2 -parallel 4 ./tests -run '^TestSchedule2' -count=1 -timeout=10m). Log: wi018/queue-runtime/public-integration01.log.gz. This run began before the later validated scalar accessor and finite-publication guard; it is initial integration evidence, not a final frozen-source rerun. Independent runtime review and five directed tests pass0.651s with no remaining source finding. Final frozen checks/lint, complete queue storage and physical maintenance gates remain. Large two-generation storage exploration exposed default-layout subtree walks on each DATA mutation; Scheduler bounded-layout adoption remains open. WI018/M3/M4 remain open.

### Queue and timezone composed checkpoint verification

Root composed verification passes core CHASM0.454s, scheduler220.259s, decision0.621s, occurrence1.039s and queueindex2.025s (wi018/queue-runtime/composed03.log.gz). Coordinator cleanup now also tests arrivals through ordinary admission, capacity rejection, and rejected publication at every cleanup step before successful publication/reload. Frozen source debt decreases by exactly one per coordinator step despite arrivals; final logical projection retains the survivors and admitted arrivals. The harness models atomic persistence; this is not an actual SQLite crash matrix. Two fixture failures are retained: a future nominal correctly caused no admission, and the third arrival correctly hit the finite harness capacity4. Assertions now exercise those actual semantics without changing production capacity.

Independent prepared-timezone review passes with all7 frozen hashes matching and full occurrence tests1.068s. The capability replaces hot time.Location lookups with fixed-page offset/rule evaluation and unchanged-continuation error behavior. It does not yet persist timezone pages, resolve named zones, or activate runtime calendar union/jitter handling.

Full queue storage measurement includes records, index pages and ChasmNodes map framing. Actual Scheduler initial10000-entry six-class image is8,128,337bytes; a test-only bounded-layout owner with identical queue types measures partial two-generation cleanup plus1250 arrivals at5,252,240bytes. Other execution metadata, operations and request payloads are excluded. Actual Scheduler cleanup still triggers default subtree walks, so bounded-layout integration is required; no full-transaction bound or universal8MiB fit is claimed. Details and terminated exploratory runs: wi018/queue-storage/README.md. WI018/M3 remain implementing; M4 remains pending.

Final queue checkpoint gates: required fastlint03 passes normal/minimal/vet with zero issues; frozen queue source hashes match checkpoint-source.json. Final public integration02 passes: `ok  	go.temporal.io/server/tests	168.258s`. The final test includes the validated projection accessor and finite-publication guard. Independent supplemental coordinator arrival/failed-maintenance test passes0.643s. This accepts the indexed queue runtime slice, not completion of WI018 or M3. Prepared timezone capability is committed atd5425fa2c. Next slices are operation/request DATA storage, reference ownership and dispatcher readiness plus persisted timezone preparation.

### Earliest-ready index capability

SequenceEarliest selects exact minimum(time,sequence) through bounded validated paths without mutation or owner changes. Root independent150 Rapid seed18 passes2.312s; author full queueindex100 Rapid seed1 passes2.139s. Actual CHASM adapter publication/discard/reload test passes0.656s. Required fastlint passes normal/minimal/vet zero issues on isolated frozen source matching the execution worktree; shared-tree typecheck failure from concurrent ungenerated operation-store schema is retained separately. Independent review finds no blocking defect. Evidence: wi018/ready-index/.

Source-derived read-counter ceiling is50054 (test ceiling52000), with fixed-size pages and no backlog-sized loop. The combined64/96-bit path witness uses12692 reads, zero writes/releases. This establishes the chronological query required by dispatch-cleanup-contract.md; dispatcher claim/RPC/recovery and fixed task set are not yet implemented. Operation and persisted-timezone work continue; WI018/M3 and M4 remain open.

Named timezone source audit found that actual Go1.27.3 fallback order is platform sources, embedded data, then GOROOT, after the optional ZONEINFO override. Temporal server imports time/tzdata, so a GOROOT-only resolver would change behavior. Root recorded exact local source hashes, cache/Local semantics and the remaining embedded-bundle access requirement in wi018/named-timezone-resolution-audit.md and named-timezone-source.json. Persisted timezone pages do not by themselves resolve this runtime-adoption gate.

### Addressed operation storage and persisted timezone checkpoints

The operation adapter now prepares at most three addressed record changes, checks immutable request replacement/counts before publication, and routes selected operation/request reads through the exclusive representation. Root review fixed Describe format-validation bypass and added malformed representation, overflow/underflow and no-semantic-write rejection tests. Full scheduler2 tests pass18.830s; independent operation/timezone directed checks pass scheduler2 0.640s and occurrence1.147s. Keyed operation transitions remain explicitly inactive until retention, payload ownership, projections and dispatcher conversion finish. Evidence: wi018/operation-store/.

TimezonePreparation now persists fixed89-byte headers and bounded230-byte transition pages, installs/cleans at most16 pages per call, retains old installed prefixes in FIFO owners, and rejects stale calendar bindings. Independent review covers codec/staging/ownership and nil-library detached reading. Tests exercise reload, both DST fold candidates, poisoned source isolation, installation rollback, every cleanup-page fault, partial replacement and context epochs. Runtime named resolution, union/exclusion/jitter and maintenance scheduling remain open. Evidence: wi018/persisted-timezone/.

Both source sets match author and root independent manifests. Required combined fastlint passes normal/minimal/vet with zero issues. Final TestSchedule2 public integration passes167.136s. This accepts these prerequisites, not WI018 or M3 completion. Whole Scheduler bounds and M4 remain pending.

### Exact retirement index capability

The fixed88-byte retirement key preserves legacy signed Nominal then generated operation-ID suffix ordering. An owned Patricia index supports bounded minimum/insert/delete through the same atomic backend batch; sorted16-key buckets avoid per-record full-width branch overhead. Independent source review finds no blocking defect. Author full queueindex passes4.190s; root150 Rapid seed19 passes1.614s; actual CHASM17-to16 collapse publication/discard/reload test passes0.688s. Required fastlint passes on an isolated frozen source checkout matching execution; an earlier shared-tree run's unrelated new timezone-source style findings are retained. Evidence: wi018/retirement-index/.

Source ceilings are2820 read-counter increments and2116 changes, with explicit704-bit/88-byte/16-key CPU constants. Raw10000-entry keys/values total1,390,761bytes, excluding CHASM and execution framing. This primitive is not yet selected by runtime retention. The operation/request adapters and persisted timezone checkpoint is7270828e0; blob ownership and named source resolution are the next active slices. WI018/M3 remain open, and M4 results remain pending.

### Payload ownership, retirement adapter and effect boundary verification

Blob reference ownership now preflights fixed owner deltas before publication, including request replacement, completion self-eviction, public definition and generator definition. Missing content with a positive counter rejects instead of silently reusing a corrupt owner; legacy records retain their existing payload-read isolation. Actual retirement selection validates maintained RetirementCount against its bounded index and publishes addressed record/index changes together. Keyed operation runtime activation remains gated.

The effect boundary checks at most two valid intents and their request membership before hot loops; cleanup visits old intent IDs only. Independent review accepts retaining existing bounded effect tasks alongside the planned start dispatcher. Addressed effect request storage and physical task-count/churn evidence remain outstanding.

Final composed scheduler2 package run passes18.892s; public TestSchedule2 integration passes166.195s; required fast lint passes normal/minimal/vet with zero issues, including the added independent duplicate/orphan/missing-request tests. Independent effect/overlap/idle checks pass scheduler2 1.696s and decision0.830s. Root verified all14 blob author manifest hashes against source. Exact commands and logs: wi018/ownership-checkpoint/; independent review: wi018/effect-bound/ and wi018/operation-retirement/. Public integration exercises the current default profile, not activation of the still-gated operation store. WI018/M3 remain implementing; M4 measurements remain pending.

Local integration checkpoint: a301d38ce (payload ownership, operation retirement adapter and effect validation). Readiness dispatcher work proceeds from this frozen increment; no runtime activation or milestone completion is implied.

### Owned timezone source capability accepted

The named source resolver owns the exact Go1.27.3 embedded bundle, preserves audited Unix source precedence and cache/Local behavior, and exposes immutable prepared source snapshots. Root review corrected empty-GOROOT fallback, verified all frozen source hashes, and independently ran the full source package (PASS0.719s). Author full04 PASS0.714s and lint04 normal/minimal/vet zero issues. Evidence: wi018/named-timezone-source/root-review.md, manifest.json and logs. This capability is not yet selected by Scheduler; native Windows Local and persisted native offsets, startup integration and calendar continuation remain open. WI018/M3 and M4 remain incomplete.

Readiness integration in progress: additive ReadySequence, PendingCount/NextReadySequence, StartDispatchControl/StartClaim and ReadyRecord definitions generated successfully with make proto; Scheduler now has the addressed ReadyRecords field. This is schema preparation only, with no task registration, readiness publication or profile activation yet. Adapter implementation and dispatcher claim/recovery follow start-dispatch-integration.md. Owned timezone capability committed8477d0f51.

Dispatcher integration continues: extracted the existing RPC issue/observation path for reuse by finite and claimed starts; existing task/activity regressions pass0.821s. Fixed-slot token planning now distinguishes absent wake from Unix epoch zero, fences replacement/disarm, and checks exhaustion before mutation. Its first directed run did not compile because concurrent native timezone methods were not yet present; retain that failure as incomplete verification, not a product-test result. The calendar package subsequently compiled with those methods and passed its author suite. Projection review identified shared canonical index-reference pages across SequenceIndex owners; running projections therefore use a separate ProjectionPages DATA collection. Task/projection schema generation passed; native source schema generation is in progress. No profile activation or M3 acceptance is claimed.

Dispatcher control rerun passes0.856s after native source dependencies compile, covering epoch-zero wake, same-time replacement, disarm fencing, exhaustion and existing task/activity cases. Both task/projection and native-source make proto runs completed successfully. No compile hold remains. Full readiness/claim/recovery integration and required final lint are still pending; these directed results are not runtime activation evidence.

Actual start claim/recovery handlers and operation-record readiness hooks are implemented but the complete operation profile remains gated. Registration exposes the fixed dispatch/recovery task identities; selected record publication prepares readiness/token/index changes before writes and omits legacy PendingStarts/per-operation tasks. The shared StartHandler permit/RPC path is retained. Exact recovery expiry and negative-result expiry fencing were strengthened after independent review. Claim tests pass0.709s: staged claim discard/reload, committed recovery with fresh sequence and unchanged logical retry/deadline, captured deadline after lock delay, failed commit without RPC, cancellation after commit without RPC, and mismatched expiry rejection. These use the real CHASM tree with injected engine publication outcomes, not SQLite fault evidence.

Composed scheduler2/... passes21.015s; independent SequenceAtOrAfter150 Rapid seed20 passes0.559s with existing failure corpus replay. Source review accepts its bounded read-only two-path selection. Public default-profile regression is running separately; it does not prove keyed-profile activation. Native source review found persisted timestamp normalization must reject invalid protobuf timestamps before reconstruction; that correction is underway. Projection/refcount/runtime activation, fixed effect DATA storage, framework layout adoption and whole-transaction measurements remain open.

### Reviewed runtime prerequisite integration checkpoint

Readiness/claim/recovery, fixed effect requests, addressed public projections and native Windows timezone capabilities have passed their scoped independent reviews and final composed validation. Root verified34 author-owned source hashes. Required combined fast lint passes all modes with zero issues; default TestSchedule2 public regression passes166.101s. Exact final composed results, source hashes, commands and scope limits are recorded in wi018/runtime-integration-checkpoint/. Claim03 passes0.674s after repairing an unsupported test-context wrapper; fixed snapshot clock conversion and shared permit witnesses remain active.

The OperationStore runtime gate is still present. Next integrate complete fresh private selectors, selected physical start/recovery/result paths, required-empty legacy-map framework declarations and MemoVersion, then prepared calendar startup/continuation. These remaining gates prevent accepting WI018 or M3. M4 remains unmeasured.

Local checkpoint35edb407d integrates the reviewed runtime prerequisites. Final composed validation passes CHASM0.486s, scheduler2 20.728s, decision0.758s, occurrence1.143s, queueindex3.626s and timezonesource0.677s. Required combined lint and default public regression passed as recorded above. Next: complete private fresh-profile activation, followed by selected-runtime physical evidence and bounded-layout adoption; milestone gates remain open. Raw diagnostic logs and Rapid counterexample comments retain original whitespace intentionally.

### Complete private addressed runtime and memo integration

A fresh private newAddressedPublicSchedule constructor now installs queue, payload/blob owners, addressed operations/requests/readiness, fixed effect slots and public action/running projections before generation and initial patches. applyTransition accepts this complete profile; incomplete selectors still reject and the default public constructor remains unchanged. Selected time skipping checks pending/claim authority and Close disarms the slot while retaining historical claim membership. Actual coordinator manual/automatic admission and physical dispatcher tests pass, covering success, callback-before-ack, immutable retries, canceled issuance with durable recovery/discard, stale replay and Close during RPC. Evidence: wi018/addressed-runtime/. These are real-tree/mock-engine commit witnesses, not SQL failure evidence.

A separate persisted MemoRevision is required because acknowledgments/callbacks change memo content without advancing PublicRevision conflict tokens. The new provider uses a virtual1 baseline for old zero-field records, reserves the next revision before publication, and shares it across nested list/pause writers. Root hooks cover public patches/updates, transitions and generator pause overrides; invalid or missing reservations fail closed. Existing low-level publication fixtures now reserve explicitly. The first composed run also exposed an old retry test selecting the final physical task even when a visibility task followed it; it now asserts exactly one new StartTask by type and retains all timer/logical-wake assertions. Final composed run passes CHASM0.445s, scheduler2 19.011s and all subpackages. Required final lint/public regression and bounded-layout activation remain pending for this increment.

The framework required-empty legacy DATA-map capability is implemented separately with cold/hot/replication and final publication guards. Actual Scheduler limits/registry still require source-derived task bounds and shared live/nil registration. Whole-spec calendar union/exclusion/following-candidate/streamed-jitter implementation is underway; no prepared runtime activation or full-transaction bound is claimed. WI018/M3 remain implementing and M4 remains pending.

Public regression and independent framework checks completed: `CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep,integration -p 2 -parallel 4 ./tests -run '^TestSchedule2' -count=1 -timeout=10m` passes170.163s, and `CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm -run 'TestBounded.*Empty|TestRequiredEmpty' -count=1` passes0.448s. Logs are preserved in wi018/addressed-runtime/addressed-memo-public01.log and required-empty-independent01.log. The public run exercises the default profile; bounded private registry witnesses and final combined lint remain outstanding. The source-derived task-limit audit is in wi018/bounded-scheduler-task-limits.md; its bounds still require actual registry integration witnesses. WI018/M3 remain implementing; M4 remains pending.

The opt-in bounded public frontend now selects the complete addressed constructor and the identical private bounded live/nil registry. `TestSchedule2BoundedSharedSuite` passes56.732s with actual service clients and persisted selector/required-empty-map assertions; no per-contract skips or finite fallback. Exact command: `CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep,integration -p 2 -parallel 4 ./tests -run '^TestSchedule2BoundedSharedSuite$' -count=1 -timeout=10m`; log: wi018/addressed-runtime/bounded-public01.log. This binary predates the subsequent PublicMetadata partition and prepared-definition schema, so final-source regression remains required. It also does not prove the future prepared calendar runtime or all physical fault matrices.

The100-command bounded constructor witness reaches the source-derived103 immediate enqueue/attempt ceiling; the100-command subsequent patch reaches102. Combined Work+Recovery and effect/visibility witnesses are being finalized independently. Additive definition-preparation and PublicActionMetadata schema generation passes; selected-profile context metadata now reads a separate DATA partition, with poison/reload/update verification underway. Calendar independent review reproduced a stale nominal output after excluded final-candidate exhaustion; repair/reverification are required before acceptance. WI018/M3 remain implementing, M4 pending.

### Bounded private runtime checkpoint verification

The selected bounded public shared suite passes again after PublicMetadata integration (56.220s; bounded-public02.log). Full framework/scheduler2 checks pass0.474s/22.759s; exact bounded task witnesses pass0.634s. The selected metadata poison/cold/update check passes0.709s and independent review0.686s. Corrected whole-spec pure suite plus independent exhaustion regression passes1.335s. Root verified all13 bounded-registry and pure-calendar author source hashes. Required combined fast lint passes normal/minimal/vet with zero issues after correcting two test-import style findings; initial failure logs remain preserved. Evidence resides in wi018/addressed-runtime/, bounded-runtime/ and prepared-spec/independent/.

This increment establishes the fresh private addressed runtime on bounded CHASM traversal, including the actual shared public contracts. It does not establish constant total hot work: prepared calendar runtime, asynchronous preview/static-dynamic list integration, complete handler/storage scaling and the full physical failure matrix remain open. Additive preparation schema is generated but its component integration is unfinished and excluded from this checkpoint. WI018/M3 remain implementing; M4 remains unmeasured.

Local checkpoint2baaa149a integrates the reviewed bounded private runtime, memo/context partitions and corrected pure prepared-spec evaluator. New persisted-definition implementation files remain outside that checkpoint. Original/integration checkout was verified clean and was not changed. The copied Farm32 license notice retains its source trailing blank line, like raw diagnostic evidence; exclude that verbatim notice from whitespace-only diff checks. Next: persisted definition/continuation installation, fixed list projection with bounded preview, and full-tree work/storage measurements.

Post-checkpoint integration: PublicListControl additive schema generation passes; Scheduler has inactive fixed control/static A/static B/notes fields. DefinitionPreparation is registered for nil/live decoding but not yet attached to the Scheduler bounded layout. The exact list storage contract is in wi018/public-list-storage-contract.md; persisted definition/codec contract is wi018/persisted-spec-contract.md. Standalone helper implementations and small whole-runtime measurement witnesses are under verification; no prepared runtime or list-profile activation is claimed.

Prepared runtime integration now executes automatic and backfill scan/handoff through fixed prepared pages when selected. Separate incomplete lookup work never advances occurrence ownership. Actual physical WorkHandler+Close and direct scan/transfer/cleanup tests pass with legacy definition/source fields poisoned. A hidden bundle-reuse source read and a disposal-after-admission/timezone-read dependency were found and corrected; exact evidence and remaining limits are in wi018/prepared-runtime/. Composed packages pass, with final combined lint/public regression still pending. Public prepared construction, source scheduling, staged replacement and preview hooks remain inactive until connected.

Whole-runtime measurement found10000 pending operations occupy18,975,700 framed bytes, exceeding8MiB. Removing all request records still leaves11,000,166 node/path bytes. A lossless static16-slot storage prototype measures7,947,424 framed bytes, but its mutable addressing and actual runtime integration are unproven. No capacity reduction or later request-freezing is accepted as a repair. Mutable packed-store work is under independent review; WI018/M3 and all M4 acceptance gates remain open.

The bounded public shared regression passes52.398s after the Request partition and prepared-path additions (prepared-runtime/prepared-bounded-public01.log). This still uses the bounded addressed public frontend, not the future prepared constructor. Selected ListControl Memo/SearchAttributes hooks separately pass actual close/poisoned-static and detached-read checks0.693s. Source request/paged-timezone schema generation passes; its pending source/service implementation remains under construction. Combined lint's root test unused-read finding is fixed; transient missing-method failures from concurrent source drafts are recorded distinctly. The latest stable-source run reports only two new paged-source complexity findings, assigned to their author; required final aggregate lint is therefore still pending.

Mutable packing review found and repaired touched-child range, allocator collision and child-frontier validation gaps; corrected full primitive suite passes1.311s. Complete mutable pending10000 isolated-issuance structural peak is8,371,789bytes, just16,819 below8MiB before unmeasured actual execution/prepared-state overhead. This is not accepted runtime fit. Lossless compact address encoding is the next measured experiment. The initial19MB actual runtime remains the authoritative current-layout finding. WI018/M3 remain implementing; M4 remains pending.

Selected list callback publication now passes the actual-tree transition test (0.686s), preserving an unfinished preview while updating recent results and failure-pause notes without reading the large definition or static list. Independent source review found no blocker. The previous failure used an ALLOW_ALL operation, whose callback is correctly ignored after acknowledgment; the corrected fixture admits SKIP before execution. Pending timezone recovery metadata generation passes; stable cache-eviction recovery implementation remains in progress. Final composed verification and aggregate lint remain pending. WI-018/M3 remain implementing; M4 remains pending.

Required combined fast lint06 passes both modes with zero issues, including the new transition test via intent-to-add. This validates the current increment; future recovery/service changes and final composed prepared-runtime acceptance still require their own checks.

The durable source preparation handler is now registered locally with fixed owner/token fencing, bounded detached response batches, cache-miss read exports, and outside-mutation source compilation. Pending work suppresses only new calendar scans and blocks idle/time skipping; completed candidates and other work remain eligible. Completion preflights scheduling rejection before page publication. Directed regressions pass1.731s and required lint02 passes both modes; independent physical handler tests and prepared constructor/update activation remain outstanding. See wi018/source-task/. WI018/M3 remain implementing; M4 remains pending.

Independent physical source-task tests pass0.690s: sixteen-page batches, cache-hit poisoned source, same-fingerprint retention, definition/timezone cache restoration, lost acknowledgment and discarded writes, stale owner/token and resolver failures. Valid final-batch rejection at exhausted source/coordinator/idle tokens leaves durable application state unchanged. Independent evidence is being archived under source-task-independent; composed service suite and final aggregate lint remain pending. No prepared frontend activation or M3 acceptance is claimed.

Staged generation and private prepared public constructor/update hooks are implemented. Source normalization/resolution happens in detached ingress staging; mutation uses fixed cursor metadata and preflighted definition replacement. Selected patch tests poison the full definition and still pass. Independent actual ingress tests pass0.661s; composed CHASM/scheduler2/subpackages/legacy tests pass (scheduler2 21.869s). Final lint/public regression, preview maintenance, per-job source checks and prepared frontend activation remain outstanding. Packed storage schema49 is generated for ongoing actual adapters. Evidence: wi018/staged-generation/ and wi018/staged-public/. WI018/M3 remain implementing; M4 remains pending.

Bounded public shared regression passes56.671s for the existing addressed frontend after staged ingress changes; prepared frontend activation is still pending. A fifth coordinator class now fairly services definition/timezone cleanup and prepared preview; each maintenance quantum handles at most16 cleanup pages or64 preview steps. SourceRequest recipient metadata now grants one automatic/backfill/preview logical lookup after outside-mutation resolution, retains grants across partial work, and fences old definitions/preview stamps. Directed integrated regression passes1.744s; isolated maintenance tests pass0.689s and scoped lint passes all modes. Independent grant-path tests remain in progress. Automatic refresh of completed preview windows and prepared frontend activation still require wiring. Typed packed record reads/publication hooks are integrated but packed constructor/queue adapters and actual storage measurements remain pending. WI018/M3 remain implementing, M4 pending.

Preview refresh now schedules the first future-time boundary and action-target changes without busy-looping exhausted windows; directed refresh tests pass0.679s. Independent physical source-grant tests pass0.734s. Required aggregate fast lint02 passes all modes after shared-code fixes and renaming archived Go copies to .go.txt (unchanged verified hashes) so tooling does not compile evidence artifacts. A composed regression found the fifth work class changed a saved legacy NextWork counter; root preserved four-class rotation for nonprepared profiles and uses five only for prepared executions. Directed coordinator/source/maintenance replay verification passes3.753s; composed03 is running. Actual fresh packed lifecycle tests pass0.681s, while independent atomicity review, full custom-decode instrumentation and actual whole-runtime measurements remain in progress. Prepared frontend shared-suite activation is being implemented; no milestone acceptance yet.

Composed03 passes all checked packages after the legacy rotation repair: CHASM0.511s, scheduler2 20.598s, decision0.871s, occurrence1.145s, packedstore1.392s, queueindex3.978s, source0.713s, legacy4.606s. Required aggregate fast lint04 passes both modes; lint03 only flagged a defer inside an independent test loop, changed to equivalent test cleanup. Packed independent tests pass0.700s for poisoned-Q cleanup, rejected-editor atomicity, corruption without fallback and queue/admission ownership. Prepared frontend/read helpers now use one PreparationService source provider for task and read projections; dedicated prepared public shared suite is running. Actual packed whole-handler instrumentation includes repeated custom decodes of cached DATA, and a short frozen-binary compile precedes1000/10000 measurements. Neither pending run is acceptance evidence yet. WI018/M3 remain implementing; M4 remains pending.

Prepared public activation exposed five failing shared contracts (shared01,98.531s): Basics, UpdateIntervalTakesEffect, ListSchedulesReturnsWorkflowStatus, ListSchedulesRecentActionsCapped, LimitMemoSpecSize. The two automatic scheduling contracts also fail in isolation16.265s; an actual one-second first-start witness passes9.292s, distinguishing slow physical progress from a fully stalled pipeline. Physical preview diagnostics identified repeated reset of unfinished output when its first time expired; this can starve a five-output window. Repair preserves unfinished snapshot progress and aligns source resolution with the reference whole-window compilation lifetime. Verification is in progress; no acceptance is implied. Logs: wi018/prepared-public-failures/.

Frozen actual packed measurement passes58.19s:10,000 pending operations use7,903,846 framed CHASM bytes initially and7,903,952 after claim/ack;10,000 queued entries use3,009,833 initially. These direct-definition fixtures exclude prepared pages and production execution metadata. Full cached DATA custom-decode work is recorded separately, so zero whole-tree counters do not imply zero decoding. Combined prepared+packed SQLite persistence measurements with1KiB and1280KiB shared inputs are underway. The current acceptance audit is wi018/current-acceptance-audit.md. Default-base fast lint revealed11 findings beyond the earlier HEAD-filtered increment; these are being repaired before final lint. WI018/M3 remain implementing; M4 remains pending.

Initial staged installation now explicitly starts the first automatic lookup: its existing source resolution grants that owner/revision-bound lookup through installation and bounded continuation quanta. Generic source installation requests still grant nobody; subsequent automatic lookups retain their own source checks. Independent review confirmed empty/replacement/pause handling. Directed staged/source/public tests pass0.699s; the first assertion incorrectly expected an unconsumed grant after immediate reconciliation and was replaced with recipient-before-install and completed-candidate-after-install assertions. With this correction UpdateIntervalTakesEffect passes in scheduling03, while Basics still fails. A mixed-calendar actual progress witness shows the first candidate spends about ten quanta yielding over expired2010/2011 calendar terms before its first acknowledgment; fixed year-mask lookup optimization is under investigation. The failure logs remain preserved.

Prepared preview physical progress now passes10.244s after retaining its whole-window source grant, preserving unfinished snapshots and using the remaining fixed64stepbudget across output boundaries. Small combined packed+prepared actual SQLite persistence passes13.317s: four operations use20,065 complete mutable-state protobuf bytes with1KiB input and1,329,859 with1280KiB input, including named-zone prepared pages. Larger measurement first exposed the configured1000 buffer capacity, so a separate explicit supported-unlimited-capacity witness is running without changing blob/state/transport limits. An opt-in visibility readiness gate is being implemented to defer initial storage publication until a coherent projection exists, instead of hiding rows only from List. M3/M4 remain open.

Actual combined prepared+packed SQLite unlimited-capacity run fails135.25s at the unchanged mutable-state limit:1KiB input admits8218 operations before rejection;1280KiB input admits6857. Both1000-operation cases pass. Retained full mutable-state protobuf sizes at rejection are8,524,038 and8,502,633bytes; server MutableStateStats.TotalSize uses different accounting (8,373,726 and8,376,854). The rejected admission preserves accepted counts. This supersedes any inference of10000-operation production fit from the simpler7.9MB fixture; lossless layout repair and remeasurement are required. Exact binary/manifest/logs are under wi018/packed-runtime/persistence/.

Opt-in visibility readiness now defers incomplete running projections with retryable dependency errors before export, while closed records and default providers keep their existing behavior. History tests pass0.810s and CHASM visibility1.177s, including retry after reload and closed export. Actual selected visibility contracts for workflow status, recent-action cap and Count now pass; LimitMemoSpecSize remains failing its15s visibility wait. Independent transition-helper review found no changed publication/error ordering; directed public/packed/operation/time-skip checks pass3.848s. Default-base lint02 was invalidated by concurrent new test imports; lint03 runs after imports stabilize. No M3 acceptance.
