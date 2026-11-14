# Whole selected-runtime measurement fixture plan

Use the actual Scheduler, private bounded live/nil registry and complete fresh
addressed constructor. Keep one fixed small public spec/action. No preparation
cardinality claim. Sizes N=4,5,1000,10000; run serially, first small fixtures before
allocating10000. No runtime source changes are required.

## Setup without quadratic harness overhead

Existing treeHarness.commit snapshots and cold-reloads every transaction. That
would turn10000 setup publications into repeated full-tree work. Add a test-only
hot commit helper: call actual Node.CloseTransaction, merge only UpdatedNodes and
DeletedNodes into the fixture's durable map using cloned protobufs, increment the
transition counter, and clear backend task capture. Keep the same live tree for
the next fresh lease. Never count fixture-map merging as handler work. Cold reload
exactly once after setup, and separately record that cold cost if available.

Populate using actual Scheduler.applyTransition with explicit coordinator grants
(the same setup authorization convention as treeHarness.apply), not direct index
or DATA writes. Each item closes and publishes its own real transaction. Override
only test context work quanta to zero during setup, preventing unrelated immediate
coordinator progress; measured handlers use the actual live context/quanta2.
Use configured MaxBufferSize=0, preserving public unlimited capacity semantics.

Families:

1. Pending operations: ALLOW_ALL, N unique accepted occurrences, all with immutable
   request snapshots and shared action blob. Readiness index and all maintained
   operation/request/blob counts must equal actual admitted state.
2. Queued backlog: one pending blocker plus N BUFFER_ALL arrivals. Queue records,
   class index, source controls, blocker request and blob owners all included.
3. If time permits, six-class queue variant via actual transition grants and
   supported manual/backfill/automatic policies. Label adversarial timestamp
   layout separately; simple-family size does not bound the earlier six-class
   random-time8,128,337-byte queue fixture.

## Measured real runtime work

For operations, execute claimStart (the actual locked physical dispatcher claim
handler) plus CloseTransaction with a captured deadline, then mergeStartClaim with
an acknowledgment plus CloseTransaction. Measure first hot transition after cold
reconstruction and second hot transition separately. The physical RPC itself is
not measured; the existing addressed physical task suite verifies that wrapper.
For backlog, release the blocker through actual Apply callback/ack as appropriate,
then run physical WorkHandler through EachPureTask and CloseTransaction, measuring
bounded drain publication and subsequent source maintenance separately. Capture
all automatic immediate executions inside CloseTransaction.

Attach WorkCountersForTest before each measured handler. Capture counters after
CloseTransaction and before durable-map merge, snapshot/export, or reload. Assert
zero tree_visits/map_hydration_keys/map_sync_keys/map_key_copies/cold_nodes during
hot work. Report full vectors, updated/deleted node counts and protobuf bytes,
regular/immediate tasks, and payload identity/count checks. Index paths can vary
with key depth; report observations without falsely claiming identical vectors
or inventing an empirical constant. The implementation's fixed trie-depth limits
remain the source of worst-case bounds.

## Complete storage scope

Measure every persisted CHASM node/path, not only QueuePages/QueueRecords. Report
node/protobuf/path totals by collection, and exact
proto.Size(WorkflowMutableState{ChasmNodes: durable}) map framing. Also report
framing with the actual fixture ExecutionInfo/ExecutionState when available,
explicitly identifying which backend fields are synthetic/absent. This is not a
claim to include SQL row encoding, compression or unrelated external histories.
Do not subtract initial controls/visibility/action payloads. Report measured
bytes against8MiB as an observation only; no inferred fit claim for omitted
payload distributions or adversarial six-class keys.

Freeze the compiled test binary and source manifest before expensive runs;
verify source hashes around compilation. Preserve raw per-size logs, source hash,
fixture bounds, setup time and any rejected/oversize outcome. If parent runtime
source changes, rerun against a new frozen manifest rather than attributing old
measurements to new code.
