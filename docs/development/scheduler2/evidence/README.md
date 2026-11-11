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
