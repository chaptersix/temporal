# Ordinary-map transaction work baseline

Stage 1 instrumentation checkpoint only; no bounded-layout implementation or WI-018 acceptance.

Final full package command: `CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm -count=1 -v` passed (0.494s). Final lint: `CGO_ENABLED=0 GOMAXPROCS=4 make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false` passed, zero issues. An import-group-only correction follows the final test. Production build: `CGO_ENABLED=0 GOMAXPROCS=4 go build -gcflags='-m' ./chasm` passed; the compiler reports disabled observer calls inlined. Production has no collector or global state.

The fixture stores N unrelated DATA entries and one future pure task, reloads the actual serialized tree, then performs two ordinary root-state updates and full CloseTransaction. Both hot transactions produce identical counts:

| N | tree iterator visits | copied map keys | synchronized map keys | delete-child inspections | DATA decodes | serialized DATA bytes | serialized complete node protobuf bytes |
|---|---|---|---|---|---|---|---|
| 4 | 30 | 8 | 4 | 5 | 0 | 7 | 65 |
| 5 | 35 | 10 | 5 | 6 | 0 | 7 | 65 |
| 1000 | 5010 | 2000 | 1000 | 1001 | 0 | 7 | 65 |

Each hot transaction enters synchronization once, serializes one node, validates one task, and reconstructs zero cold nodes. True reconstruction separately visits N+2 persisted nodes and hydrates N raw-map keys. A final isolated point read decodes exactly one DATA value. These counters demonstrate growing framework work despite unchanged serialized output and no unrelated payload decoding.

Counters measure named operation sites, not every instruction. `tree_visits` covers andAllChildren yields, including dirty-subtree marking, serialization, task/cleanup traversal; map-key copying, map sync/hydration, child deletion inspection, deserialize/serialize and task validation have distinct counters. Direct path lookup/ancestor checks, standalone Snapshot recursion, application loops, persistence I/O and mutable-state construction before NewTreeFromDB are not counted. `serialize_data_bytes` counts only encoded node DATA; `serialized_node_proto_bytes` uses proto.Size on complete serialized nodes, including metadata/tasks, and does not measure database envelope or physical I/O bytes. Counts include failed attempts; Reset is an observation boundary, not rollback. The optional per-tree observer requires the execution lock and has no global mutable state.

Preserved failed attempts are instrumentation-development evidence: baseline01 missing fixture interfaces; baseline02 omitted mock transition version causing empty snapshot; baseline03 an incorrect sync-entry-count assertion; chasm-tests01 observer nil nodeBase panic on a deliberately skeletal iterator test; lint02 import grouping. All were corrected before the final passing checks. None is a scheduler runtime counterexample. Raw JSON and source hashes accompany the final logs.
