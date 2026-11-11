# WI-012 progress

Baseline source352dafa83, unchanged integration checkout. Both V1 and existing
CHASM shared TestDeletedScheduleOperations and TestCountSchedules pass on local
SQLite; worker-service selection matches the current backends. This is a baseline
for refactoring, not scheduler2 coverage. No new backend is registered or routed.

Command: `CGO_ENABLED=0 GOMAXPROCS=4 TEMPORAL_TEST_LOG_LEVEL=error go test -tags test_dep -p 2 -parallel 2 ./tests -run '^TestSchedule(V1|CHASM)SharedSuite$/(TestDeletedScheduleOperations|TestCountSchedules)$' -count=1 -timeout=5m -v -args -persistenceType=sql -persistenceDriver=sqlite`.
Exit0, package duration2.492s; compilation occurred before the test-run timing.

Descriptor refactor and full scenario inventory are in progress. Post-change
checks, independent review and local integration remain required.

Initial compilation caught two missed locals in existing CHASM-only helper groups
(undefined backend/unused context). Fixed before verification; compile-initial.log
retains the failed result. Independent review also corrected a mixed V1/CHASM
sentinel fixture's cleanup list. Final source is frozen, compiles, and full existing
shared suites plus descriptor/worker tests are running with an8m bound. Scoped
lint passes with zero issues. These are existing backend preservation checks,
not scheduler2 public behavior.

Both full existing shared suites and three new descriptor/helper cases pass in
shared-suites.log (176.000s, no skips or failures). A subsequent descriptor-only
refinement replaces the last two name-based behavior/config branches with named
carryover expectations and SDK options. Four affected backend/case combinations
pass in descriptor-expectations.log (13.369s), and lint-final.log has zero issues.
The mixed V1/CHASM public carryover differential passes in mixed-backend.log
(21.262s), including explicit setup/cleanup for both backends. No migration test
execution was added. Focused helper race is still running.

Parent inventory audit verified all seven source hashes against baseline352dafa83
and found no missing Test symbols in the four Schedule-focused source files.
Related files are selected by Schedule API scenarios, not every workflow test.
Six classification refinements retain observable obligations for scheduler2 even
where old mechanisms are implementation-specific; a concurrency permit does not
establish a namespace rate limit. New descriptor tests have a separate final-source
addendum. No scheduler2 shared/public pass is inferred from these baseline tests.

Final focused helper race passes. WI-012 accepted after independent source review,
parent inventory audit and all required checks. The exact final file fingerprints
and evidence scopes are in checks.json. Initial compilation failure is retained.
This acceptance closes the setup/inventory item, not the compound M3 checklist
items that also require scheduler2 execution and promoted public scenarios.
