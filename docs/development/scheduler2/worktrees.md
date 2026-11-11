# Local worktree registry

| Item / owner | Path / branch | Base and prerequisites | Status / disposition |
|---|---|---|---|
| WI-000/WI-001 integrator | /Users/alex/repos/wt/sch-redesign/temporal / sch-redesign | 85b80a1836dbc73741cb52586297dc35e6c937ca; plan initially untracked | Existing isolated integration checkout, retained. |
| WI-001 model delegate | /private/tmp/sch-redesign-model / sch-redesign-model | 85b80a1836dbc73741cb52586297dc35e6c937ca; contract pending acceptance | Created locally, HEAD verified; no implementation assigned yet; retained. |
| Original, no owner edits | /Users/alex/repos/temporal / main | 9ab3a9f770da20df7d94bcc0030f28eec7b0b947 | Original tracked/untracked changes preserved. |

Creation command: git worktree add -b sch-redesign-model /private/tmp/sch-redesign-model 85b80a1836dbc73741cb52586297dc35e6c937ca.
All delegated investigations were read-only at base, with no file ownership overlap.
Integrator owns central plan, contract, records and evidence. Model contribution,
when assigned, owns only its model directory and must report exact revision,
commands, artifacts, unresolved assumptions and limits before integration.

## Final disposition

- Integrator sch-redesign: verified code f9d62cf99, subsequent documentation-only
  acceptance commit. Existing isolated checkout retained; all task work committed.
- Model sch-redesign-model at /private/tmp/sch-redesign-model: base 85b80a183,
  contract prerequisite 35de5b5a8 read explicitly; contributions 100ec6cec/3159ec6d1,
  integrated 733cca000/bf92288d9; clean and retained.
- Reducer sch-redesign-reducer at /private/tmp/sch-redesign-reducer: base 85b80a183,
  contract prerequisite 35de5b5a8; contribution fe9806be4 integrated 13e1a443c,
  later integrator fixes/refactoring recorded on sch-redesign; clean and retained.
- Harness sch-redesign-harness at /private/tmp/sch-redesign-harness: base 5229df153
  (explicit WIP adapter/schema prerequisite), reducer fe9806be4 transferred by
  cherry-pick 0b10def88; fixes 381f5722f/4315dd889 transferred explicitly;
  contributions 2bb96c0f1,71f9536db,f087e85f0 integrated 980c6f4e1,c4a90030c,21c4fc102;
  clean and retained at f087e85f0.
- Original main remains 9ab3a9f77. Original tracked diff byte-identical; original
  untracked files retained. Other actors created unrelated worktrees during the
  session; none were used, modified or removed by this task.

One integrator and at most three delegated agents. Expensive checks serialized.
No worktree, branch, failure evidence or unrelated change was deleted. Missing
configured signing key necessitated commit.gpgsign=false for local task commits.

## WI-002 continued execution

| Item / owner | Path / branch | Full base SHA / dependencies | Status / disposition |
|---|---|---|---|
| WI-002 integrator | /Users/alex/repos/wt/sch-redesign/temporal / sch-redesign | 00b65430a52d6d43e7e0c9150ced94a81c5e169f | Core/contracts, integration and acceptance; retained. |
| WI-002 oracle/replay, tooling_harness | /private/tmp/sch-redesign-enumeration-tests / sch-redesign-enumeration-tests | 46ff5128c78ddebda9e03b323644498bd8fe408e; transferred 4939d3cb5731f29a0182a7d2418256eb90fe0325 and e76c2ff9803fd7533f3eb0a6cb1679209e2bba63 prerequisite core fixes | Tests/testdata only; dc48caed5 and 136bc30dc30ba9797897327a5c977008f5fdd3dd integrated as eae24bae8 and d75e768d3; clean, retained. |
| WI-002 Lean, chasm_guarantees | /private/tmp/sch-redesign-enumeration-spec / sch-redesign-enumeration-spec | 00b65430a52d6d43e7e0c9150ced94a81c5e169f; D23–D26 contract provided explicitly | lean/ only; 005266ca83675717493b6a42e92367ae41058303 integrated as af9a0cfe6, integrator refined proposed completeness/uniqueness statements; clean, retained. |
| WI-002 contract/review, history_contract | Integration checkout, read-only | 46ff5128c initial, eae24bae8/final637fe096d reviews | Source semantics and fixed contribution review; findings resolved in evidence/wi002/review.md. |

Worktree inventory and final original status are in evidence/wi002. Original
checkout received new concurrent tracked/untracked edits; this task made no writes
there and all initially untracked entries remain. The original M1 tracked files'
patch segments remain; added changes mean the complete patch is no longer identical.
No unrelated worktree was changed. Parent executed local checks/commits where
older delegate sessions retained restrictive permissions; no escalation requested.

## WI-003 assignments

Base c945ae3bafb537a55d83d6627da158c46ff1b14c. Integrator owns reducer, adapter,
proto/generation, central records. history_contract read-only source/review.
Planned isolated delegates: tooling_harness owns new completion tests/testdata at
/private/tmp/sch-redesign-completion-tests (branch sch-redesign-completion-tests);
chasm_guarantees owns model/completion/ at /private/tmp/sch-redesign-completion-model
(branch sch-redesign-completion-model). Both consume WI-003 contract explicitly;
parent will transfer committed API prerequisite to tests before implementation.
Acceptance: independent discriminating cases/model, exact handoff/diff, no overlap.

WI-003 final disposition (all retained):

- Actual isolated creation base for both delegates was contract prerequisite
  d6026f84040ca5f2e202ca76e738b7a218ad353c (parent c945ae3ba), verified at creation
  and in evidence/wi003/worktrees.txt. Integration branch owns final acceptance.
- Tests: prerequisite d8a4e4182 transferred as fca1e1546; opaque-payload and status
  fixes8bba156ea/126915ba1 transferred as643254615/3ab52c7a5. Contribution
  b7b156b34 integrated as b0caad885. Parent changed one receive to await.Rcv.
  Worktree clean at b7b156b34, retained.
- Model: contribution7d08e4c59 integrated as cac1b4807; only model/completion/ owned.
  Both bounds and mutants passed; integrated model bytes match recorded hashes.
  Worktree clean at7d08e4c59, retained.
- Independent read-only reviewer: source fixes and final evidence reviewed;
  no acceptance blocker at aee04d35b. All routine choices resolved from evidence.
- Original checkout: no task writes, current inventory/hash recorded separately;
  all initially untracked entries retained. Concurrent unrelated changes remain.
  No push, production routing, rollout, migration, branch or worktree deletion.

WI-004 planned assignments: parent owns schema, generator and adapter/reducer;
history_contract read-only review. New model worktree
/private/tmp/sch-redesign-sequential-model owns model/sequential/; new tests
/private/tmp/sch-redesign-sequential-tests owns sequential/generator test additions.
Both start from committed WI-004 contract prerequisite, with explicit API patch
transfer before test implementation; source/creation identities verified below.
