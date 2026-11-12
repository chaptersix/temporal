# Independent Lean implementation evidence

Ownership: Lean exact executable, schema, proof runner and this evidence only.
Production Go and central acceptance records are integrator-owned.

Accepted local build command:

```sh
python3 docs/development/scheduler2/lean/exact/build.py --out /private/tmp/scheduler2-wi013-lean-build03
```

Exit0. `build03/manifest.json` archives exact commands, source and tool hashes,
per-command outcomes/durations, and final binary hash/path. `build03/audit.log`
contains the exported theorem axiom audit. Build03 is a separate immutable binary
path; the earlier initial binary used by Go routine100 was never overwritten while
that run was active. The integrator owns correspondence manifests and test results.

The final build checks the actual executable loop's conservation, sufficient and
arbitrary-positive-budget completeness, cross-call uniqueness, adjacent/finite
window occurrence-sequence batching, probe/recursion bounds, source/cursor
initialization/preservation, suffix reconstruction, authority, pending stability,
and public helper relations. See `lean/exact/README.md` for precise hypotheses and
unproved boundaries. No universal Go implementation proof is claimed.

All earlier `kernel-*`, `frontier-*`, `controls-*`, `oracle-*` logs are retained.
Failures include record-layout/parser errors, a reserved Lean identifier, missing
library theorem names, and proof obligations requiring explicit Boolean/record
normalization. These are failed attempts, not accepted proof evidence. Parser
failures sometimes show temporary elaborator-inserted `sorry` expressions; final
source and successful axiom audits contain no unfinished proof/project axiom.

Initial native build01 failed because compiling with a repository-relative filename
generated a different module initializer name from `import Kernel`. Building from
the module directory resolved it. An intermediate manual multi-command build02
encountered a string-method compile error before its linker command; its empty link
log was not a successful complete build. That error was corrected before the
initial usable binary. The new Python runner awaits each command and fails fast,
so an old generated C file cannot turn a failed Lean compilation into a passing
build manifest. It also rejects nonempty output directories, pins Lean4.24.0 and
kills the entire process group on timeout.

This file is a contribution handoff, not WI-013/M3 acceptance. In particular the
old draft's stronger full-state batching equality, complete malformed-state
validation semantics and universal historical Peek/Dispose composition are not
claimed as proved. Current Go correspondence tests observe those concrete fields
and outcomes independently. Independent review must retain any additional required
theorem as explicit work rather than infer it from sequence equality.
