# WI-000 source investigation

All source locations refer to integration base 85b80a1836dbc73741cb52586297dc35e6c937ca.
Three read-only delegates inspected history/contracts, CHASM, and tooling.

| Guarantee | Evidence | Consequence |
|---|---|---|
| H1 | service/history/chasm_engine.go:484-537,613-640; workflow/cache/cache.go:382-423; workflow/mutable_state_impl.go:7763-7802 | Commit state/tasks together; discard tentative state after error. |
| H2 | chasm/tree.go:1779-1870,3899-4009; chasm/task.go:66-85 | Persist logical tasks independently of physical attempts; validate until resolution. |
| H3 | chasm/ref.go:153-190; tree.go:467-510; nexus_completion.go:33-59 | Route callbacks using explicit incarnation/run reference plus operation identity. |
| H4 | service/history/chasm_engine.go:1425-1528; outbound_queue_active_task_executor.go:126-156 | Clone under lock, execute outside lock; no local fence on issued RPC. |
| H5 | chasm/tree.go:271-299,609-672,1336-1390,1987-2028; field.go:82-108 | Lazy payloads, eager node metadata/map wrappers; visibility reads summaries only. |
| H6 | chasm/context.go:167-175; tree.go:1771-1776; workflow/mutable_state_impl.go:1939-1940,7535-7555 | Pass execution time; adapter must not double-convert task deadlines. |
| H7 | chasm/registrable_component.go:247-262; registrable_task.go:170-210; chasm/lib/all/all.go:18-40; Makefile:198-234 | Independent library/component/task/proto names; nil library registration required. |

Paths under workflow above are relative to service/history. These are inspected
implementation guarantees, not execution evidence. chasmtest.Engine lacks rollback
and restart (test_engine.go:713-740). Required M1 harness must clone serialized
snapshots, discard old objects and restore with NewTreeFromDB; physical obligations
must also be checked. Its mock backend does not establish DB crash recovery.

## Historical defects

| ID | Revision | Inclusion | Reproduction and existing regression |
|---|---|---|---|
| F1 | 310879a37 | HEAD only | Jitter gap loses candidate; generator_jitter_test.go, generator_wakeup_test.go. |
| F2 | 7a98c28e7 | HEAD only | Empty carryover incorrectly forwarded; internal/payload_test.go. |
| F3 | d2cddffc3 | HEAD only | No-result success must clear old result/failure, preserve explicit null; scheduler_nexus_completion_test.go:603. |
| F4 | f12669973 | HEAD only | CHASM request replay mutates twice; chasm/request_ids_test.go. |
| F5 | 1193f14b9 | HEAD only | Duplicate update/patch; handler_test.go. |
| F6 | 48feb7009 | HEAD only | Execution virtual time; commit scheduler tests. |
| F7 | 5f5db96ca0d60617b92ef279e2e67f515e9e9b1b | Neither baseline | Last automatic start lost to idle close; idle_pending_start_test.go on upstream/schedv2-idle-lifecycle. |
| F8 | bf06a556af39a89b6d989d2c77c5ab345fffab18 | Neither baseline | Outstanding work/overdue idle needs reconciliation; same ref. |

HEAD only means included in 85b80a183, excluded from comparison 9ab3a9f77.
Migration fixes 2423c5c6b,7092fa307,49cb784f2 are deferred. No cherry-picks.
Original status and tracked diff are recorded under evidence. Original checkpoint
code is uncommitted and excludes physical-task publication/reconstruction. Original
spec_processor fix is superseded by F1. Neither is imported as implementation.

## Tool inventory

Go 1.27.3 effective (auto toolchain), FizzBee 0.5.4 under /private/tmp,
golangci-lint 2.13.2 available; 12 logical CPUs. Lean/Lake and Rapid absent from
inspected installation/module cache. Existing original idle model verifies idle
rearming only; its logs cannot count toward M1. Tool installation/downloads, if
needed, are local development only. No remote execution.
