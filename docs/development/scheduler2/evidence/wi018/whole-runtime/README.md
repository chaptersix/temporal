# Complete selected runtime measurement

The frozen binary identified by `binary.json` and `source-manifest.json` ran the
actual fresh selected Scheduler with bounded CHASM layout, actual transition
publication, and handler plus CloseTransaction work inspection. All measurements
are local. `large01.log` is complete; no extrapolation is used.

| Family | N | Initial CHASM map protobuf framing bytes | Nodes |
|---|---:|---:|---:|
| pending ALLOW_ALL operations | 1,000 | 1,897,928 | 7,017 |
| pending ALLOW_ALL operations | 10,000 | 18,975,700 | 70,018 |
| BUFFER_ALL queued entries plus blocker | 1,000 | 606,034 | 4,262 |
| BUFFER_ALL queued entries plus blocker | 10,000 | 6,052,710 | 42,510 |

The 10,000-pending fixture **fails the 8,388,608-byte storage gate**. The queue
fixture uses simple sequential nominal times and one class: its fit does not
supersede adversarial six-class/random-time queue evidence or prove arbitrary
input fits. Inputs use one shared 1 KiB payload and a 64-byte workflow ID. Definition
cardinality is one simple spec; prepared-definition integration was not active.

Measured first-after-cold claim and second-hot acknowledgement, and physical Work
queue drain and maintenance, each had zero full-tree visits, map hydration keys,
map synchronization keys, map key copies, and cold nodes. Index path work varies
with depth: this is bounded path evidence, not identical instruction counts. Cold
reconstruction and input cloning are reported separately. Setup commits incrementally
merge mutations outside measurement, without complete snapshot/reload per item.

The CHASM map is framed using WorkflowMutableState.ChasmNodes. Adding fixture
ExecutionInfo/ExecutionState contributes four bytes (both messages have empty
protobuf bodies). This is not a complete production execution, SQL row, physical
queue delivery, database failure, or universal storage-fit measurement.

## 10,000 pending breakdown

| Collection | Node protobuf bytes | Encoded path bytes |
|---|---:|---:|
| RequestRecords | 6,778,529 | 690,014 |
| OperationRecords | 2,669,639 | 710,016 |
| OperationPages | 2,118,561 | 1,820,006 |
| ActionRecords | 1,709,512 | 680,013 |
| ReadyRecords | 999,766 | 290,012 |

Complete groups and framing are in the log. Even removing the entire RequestRecords
collection leaves **11,000,166 node-plus-path bytes**, before remaining map-entry
framing. Request skeleton sharing alone cannot satisfy this fixture's gate.

`breakdown01.log` is a supplemental later-source directed test, identified separately
by `breakdown-manifest.json`; it does not replace the frozen large-source manifest.
Its first request record is 651 bytes: 578 wire bytes for a 575-byte request skeleton
and 73 bytes for the action reference. Skeleton fields: callback198, generated search
attributes191, workflow ID66, request ID56, identity39, namespace11, workflow type12,
reuse policy2. Header/input are already shared, not repeated. Operation metadata is
240 bytes, including occurrence ID31, action ID50, request ID56 and workflow ID66.

The original measurement lint01 failed only concurrently evolving list helper
complexity findings, not measurement code. Index agent's subsequent scheduler2
lint02 passed all modes. The supplemental test has its own required fastlint log.
