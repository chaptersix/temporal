# Persisted calendar preparation slice

Standalone CalendarPreparation is registered in both live and nil libraries, but remains unattached and unselected by Scheduler/Generator. No runtime enumeration, tasks, configuration reads, timezone evaluation or routing changes.

StageCalendarSource performs input-proportional canonicalization, copying, hashing and page construction outside mutation. Fixed seven-field counts identify immutable revision/source pages. InstallNextPages preflights and installs at most sixteen pages, each containing at most sixteen ranges. Advance fetches only addressed DATA pages and uses the reviewed fixed-budget compiler; its continuation publishes only on success. SourceSnapshot enables detached staging after reload. Empty externally constructed stages are rejected before mutation.

Tests serialize actual CHASM NodesMutation and reconstruct NewTreeFromDB between phases. These are in-process CHASM persistence tests, not SQLite or physical queue evidence. They cover >1024 ranges, caller/stage/read aliases, unknown fields, incomplete install, failed install/advance/replacement publication with reload, stale revision, addressed corruption, nil-registry reads, and first-post-reload DATA counters. Poisoning the large Source blob demonstrates Advance does not hydrate it. Default tree traversal costs remain outside this slice's bounded helper claim.

Commands and results:

- make proto: PASS; expected scheduler2 generated files only retained.
- CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2 -run '^TestCalendarPreparation' -count=1: PASS 0.690s. Earlier compile failure due missing root metadata methods retained where log exists.
- CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2/... -count=1: PASS root 11.518s, decision 1.062s, occurrence 2.095s, queueindex 6.769s.
- CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/all -run '^Test(AllNilLibrariesRegistered|NewNilRegistry_TasksDecodable)$' -count=1: PASS 0.663s.
- make lint-code LINT_CODE_TARGETS='./chasm/lib/scheduler2' GOLANGCI_LINT_FIX=false GOLANGCI_LINT_BASE_REV=HEAD: final PASS zero issues. Initial unnamed same-type return lint fixed by names only; full tests precede that semantics-neutral signature-label change.

## Required before runtime adoption

Replacement retains old pages fenced by source identity. There is currently no superseded-source ownership index: old source ID plus seven page counts must be retained in a keyed FIFO descriptor with fixed root head/tail (or equivalent) to permit bounded cleanup. A full-map scan is not an acceptable cleanup implementation. This missing owner/discovery metadata and eventual cleanup are explicit debt; no lifetime-space bound is established. Runtime registration does not constitute runtime enablement or WI018 acceptance. Terminate returns Unimplemented for this unselected preparation component.
