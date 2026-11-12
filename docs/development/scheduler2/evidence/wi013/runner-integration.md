# WI-013 named local runner evidence

`lean-correspondence` routine01 passed all nine required tests, including 100
checks per each of two Rapid properties, seed 1, the bounded exhaustive suite,
saved v1/v2 replays and directed boundary/validation cases. Oracle SHA256 is
`3e0b7213a8e63024cc7a219c003cbe4fff22e8b683029ce4694c15b58112db6d`. Exact command, source and fixture
hashes, build manifest reference, version, before/after focused provenance and logs
are archived under `runner-routine01/`. This run executed the runner snapshot SHA256
`d9ab98c7d41eb8dbf68712c761c49be489ec382bd898958e17fdf148da1c2289`.

After routine01, two runner-only followups were made:

1. `proofs` now invokes `lean/exact/build.py --out <output>/lean-build` using the
   installed pinned local Lean toolchain. It records build-manifest/source/tool/
   binary hashes and version. No build.py or Lean source changes were made.
2. `lean-correspondence` rescans the full production Go/proto/testdata `source_files`
   set after execution and fails on addition/removal/content drift. Earlier routine01
   recorded that set before execution and checked focused driver/fixture/runner/
   configuration/oracle provenance afterward; it did **not** execute this additional
   full-production postcheck. The Go command, source under test, oracle and generated
   bounds are unchanged by these followups. Existing unrelated profile commands and
   default seeds are unchanged. Unsupported-profile text and runner README were corrected.

Current runner SHA256: `32a59bde3b81f9d3ee5fdfa86b76cfd15839f89546e23253da89943a19c3b65d`. Routine01 remains valid evidence for its
recorded snapshot and test execution; it is not relabeled as testing this newer
runner source. Python AST parsing, JSON parsing, missing-required-argument rejection,
build11 binary provenance acceptance, mismatched-executable rejection and diff checks
were performed during implementation. No new Go driver or production edits were made.

The final named `proofs` profile was run once:

```sh
python3 docs/development/scheduler2/run.py proofs --output /private/tmp/scheduler2-wi013-runner-proofs01
```

It passed. Wrapper manifest/log/source diff and nested exact build logs/manifest are
archived in `runner-proofs01/`. The nested build passed all compilation and required
axiom-audit commands and emitted oracle SHA256 `3e0b7213a8e63024cc7a219c003cbe4fff22e8b683029ce4694c15b58112db6d`.
Wrapper manifest SHA256: `eb2e9e51a75fa1575fbe42a3fc54b4e859c9159800bc6f413cd0e6353c38b6d5`.
Nested manifest SHA256: `cd5c86e4c9c118e78a58cc17dcf33f54ceb58c5570b6ffacc02af1cbee39f928`.

No deployment, download, production routing or migration was performed. Formal proof
review and mutation/correspondence evidence remain distinct acceptance inputs.
