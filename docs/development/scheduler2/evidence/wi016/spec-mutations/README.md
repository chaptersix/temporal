# WI-016 overlay mutation gate

Run `python3 docs/development/scheduler2/evidence/wi016/spec-mutations/check.py --output /private/tmp/fresh-wi016-mutations`.
The output directory must be fresh. Production files are never rewritten; Go
`-overlay` maps one source file to an archived mutant text view when compiling a
new test binary. Tests execute in an isolated working directory so existing Rapid
failure files cannot bypass fresh generation/shrinking. `rapid.nofailfile=true`
prevents mutation artifacts from being written into the source fixture directory.

Final campaign02 uses the final strict-EOF replay reader. All three mutants passed
the sensitivity gate:

| Mutant | Assertion detection | Successful Rapid shrinks | Mutant standalone replay | Baseline standalone replay |
| --- | --- | ---: | --- | --- |
| fold-late-base | yes | 71 | fails | passes |
| jitter-ignore-lookahead | yes | 57 | fails | passes |
| ignore-exclusions | yes | 32 | fails | passes |

The manifest records each original/mutant/binary SHA256, exact overlay mapping,
commands, working directory, environment, exit code and elapsed time. Compiler
errors, panics, flaky tests, timeout or absent actual shrinking fail the gate.
Each final minimized scenario is explicit versioned JSON, not a random seed.
`replay-envelope-checks.json` separately verifies final baseline accepts whitespace
but rejects a second JSON object or trailing junk at the EOF guard.

All tracked source hashes before/after match. `checkout_unchanged` means the
manifest's explicitly listed relevant source/fixture set; the runner does not
claim unrelated agents' files were frozen. Final original baseline property also
passes. Fresh binaries remain under `/private/tmp/scheduler2-wi016-mutations02/`;
they are hashed but not copied into the repository. Overlay JSON records original
absolute execution paths. Re-running the runner recreates overlays in a fresh
output directory; standalone replay takes the JSON path via SCHEDULER2_SPEC_REPLAY.

Campaign01 preserves pre-EOF results and a hash-verified pre-EOF test source text
snapshot. Campaign02 repeats the mutation/shrink/replay gate on the strict reader;
no broader 10,000-case exploration was rerun for that reader-only delta. Earlier
profile logs and their exact bounds are in ../verification-profile.json. This is
mutation sensitivity evidence, not WI-016 or M3 acceptance.
