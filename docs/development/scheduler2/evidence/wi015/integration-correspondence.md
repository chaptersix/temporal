# Integrated finite proof and correspondence revalidation

At merged execution revision `ff3645950019ab521bae51e08622aec89138358e`, both named profiles passed. No runtime, schema, theorem or runner changes were made. All Go source fingerprints were stable before/after the correspondence run, and were independently rehashed during evidence archival (`integration-correspondence/current-source-hashes.json`).

Before reusing build11, verified its passed manifest, all11 theorem/wire/build input hashes, pinned Lean executable hash and exact oracle binary hash. Fresh named proof build then compiled/checks all required modules and93 axiom reports (92 dependent reports plus one axiom-free report), with all11 commands successful. The hardened runner checks every required audit target and rejects forbidden dependencies/unfinished proofs. Fresh build took14.685s and produced the identical oracle SHA256 `3e0b7213a8e63024cc7a219c003cbe4fff22e8b683029ce4694c15b58112db6d`; original build11 was not overwritten.

Routine correspondence passed all9 required tests:384 exhaustive traces,100 checks for each of two Rapid properties at explicit seed1, saved replay, controls/rejections, exact boundaries, budget edges, malformed wire and negative-input precedence. Named runner elapsed2.896s; Go package elapsed1.426s. Required-test list had no missing tests; source/input/oracle postchecks passed.

Commands from `/private/tmp/sch-redesign-execution`:

```
CGO_ENABLED=0 GOMAXPROCS=4 python3 docs/development/scheduler2/run.py proofs --output /private/tmp/scheduler2-wi015-integration-proofs01
CGO_ENABLED=0 GOMAXPROCS=4 python3 docs/development/scheduler2/run.py lean-correspondence --oracle /private/tmp/scheduler2-wi013-lean-build11/oracle --oracle-manifest /private/tmp/scheduler2-wi013-lean-build11/manifest.json --seed 1 --output /private/tmp/scheduler2-wi015-integration-correspondence01
```

Version: Lean4.24.0, commit797c613eb9b6d4ec95db23e3e00af9ac6657f24b; pinned executable SHA256 `9544be237d91099efe1db8cb97d0c359fa13aa94f565e9972bfb8b63abbcb437`. Version logs, full manifests, commands, proof/audit logs and Go test output are archived under `integration-correspondence/`. The fresh executable and intermediate compiled artifacts remain in the versioned local build directory.

This revalidates the accepted finite internal occurrence profile on integrated source. `NewPublic` wide timestamps, namespace-configured capacity, public callback orchestration, calendar correctness beyond the finite oracle, and whole-runtime work bounds are separate obligations. Successful compilation is not used to invent stronger theorem statements; the accepted theorem review remains the source of their premises/conclusions. No fresh exploration, race run or mutation campaign is claimed, and this does not close M3's bounded-transaction obligation.
