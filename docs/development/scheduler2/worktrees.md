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
