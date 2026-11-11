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
