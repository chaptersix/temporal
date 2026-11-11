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
