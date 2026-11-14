# Actual prepared + packed SQLite persistence witness

This creates a fresh private packed+prepared schedule through the local frontend,
waits for the real named America/New_York source and list preview to install, and
admits one generated occurrence per actual History update. It uses the production
admitGenerated path including the nominal workflow-ID suffix and immutable request
freezing. Physical start tasks call a local frontend wrapper returning retryable
Unavailable; no workflow execution is created by that wrapper. This retains pending
work while leaving actual dispatch/claim/retry publication active.

Inputs are fixed shared payloads of 1 KiB and 1280 KiB. The latter reuses the accepted
within-transport dimension from tests/schedule2_payload_test.go. Supported public
MaxBufferSize=0 explicitly selects unlimited capacity. Transport/blob/state limits
are unchanged. This is a cardinality/storage witness, not calibrated offered-load
throughput, old/new comparison, all workload classes, or a production rollout.

Each successful test admission is checked for exactly one new operation. At the end,
operation, request and pending counts must all match actual successful admissions.
The concrete run/archetype comes from the live component ref. ExecutionManager reads
that real SQLite-persisted WorkflowMutableState and reports complete protobuf bytes,
CHASM map framing, actual execution info/state, node count, DB record version and
persistence MutableStateStats. Stats TotalSize is a server accounting metric and is
not necessarily equal to protobuf framing. Neither is SQLite page/file size; no disk
size claim is made. The live backend can publish retries between the readonly summary
and persistence read, so task/version details can advance while cardinality remains
stable. The test logs any rejected admission as a failure, then attempts to record
the retained image; it never silently calls a short accepted prefix a 10000 pass.

## Retained fixture corrections

small01 failed because the fixture added an index to the lease time, producing future
Due events after the first admission. The reducer correctly ignored them. The test
now uses the exact lease clock and its helper verifies actual admission rather than
counting successful UpdateComponent calls. Await checks were moved outside engine
callbacks to avoid recovered test-assertion panics. small02 then passed13.317s with
four accepted operations at each payload size; complete state bytes were20,065 and
1,329,859 respectively (real execution info/state included).

The first large run retained the default MaxBufferSize1000 inadvertently. Both target
1000 cases passed; target10000 cases stopped at exactly1000 under the existing public
capacity rule. This is not a storage-limit failure. finite-capacity contains its
original source/binary manifest and compressed full log; the original binary is also
preserved as /private/tmp/scheduler2-packed-persistence-finite.test. The corrected
unlimited run has its own source-manifest.json and binary.sha256. All3535 Go/module
source hashes were unchanged across its compile, after which source editing resumed.

The unlimited run failed in135.25s, providing a concrete storage-limit counterexample.
At1KiB input it accepted8218 operations before the next admission returned
InvalidArgument: Workflow mutable state size exceeds limit. The retained full protobuf
image was8,524,038 bytes; CHASM framing8,523,167; server stats TotalSize8,373,726.
At1280KiB input it accepted6857 operations, with the same limit error; retained full
protobuf8,502,633, CHASM framing8,501,787, server stats8,376,854. Counts remained exact.
Both1000 targets passed (full images1,022,080 and2,340,090 bytes). This disproves actual
10000 fit for these accepted workloads, despite the simpler mock fixture's7.9MB result.
No limit or capacity reduction is authorized as a repair. large02-summary.log contains
key values; large02.log.gz preserves the complete actual service log. environment.json records exact invocation
and host/toolchain settings. No universal-fit or completed M3/M4 claim follows from
these witnesses.
