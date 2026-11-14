# Actual packed Scheduler measurement

The private fresh packed constructor runs under the bounded Scheduler registry. This
is actual applyTransition/claim/ack/coordinator publication followed by
CloseTransaction, not the earlier transformed storage prototype. Setup incrementally
merges returned node mutations; cold reconstruction and its full input copy are
reported separately. Default sizes 4/5 passed; the frozen binary also runs 1000/10000
with SCHEDULER_WHOLE_RUNTIME_LARGE=1.

The fixture has a simple interval definition and a shared 1 KiB workflow input.
Pending records use ALLOW_ALL; queue records use BUFFER_ALL with one blocker.
MaxBufferSize remains zero (unlimited). Setup disables immediate work quanta; measured
handlers use live quanta. No admission or request freezing behavior is changed.

Storage is the exact WorkflowMutableState ChasmNodes map wire framing of the complete
fixture tree, plus explicitly reported fixture ExecutionInfo/ExecutionState (empty
messages). It is not a full production execution/SQL row size or universal input fit.
The constructor currently uses the direct simple-definition path: prepared-definition
cardinality and named timezone source pages are separate dimensions, not implied by
this measurement. Prior prototype headroom is not an acceptance result.

Operation observations append to existing CHASM counters without changing old counter
indices. Packed page/control decoding counts every invocation, including repeated
parsing of cached Payload bytes, and records entries and encoded input bytes. Packed
serialization counts every staged page/control encode, including overwritten overlay
versions. Record decoding/encoding records codec/protobuf input/output byte volumes;
these are not CPU instruction counts and baseline input can be conservatively charged
on a raw-fallback codec path. Packed backend counters separately count DATA lookup
calls and returned raw bytes, even when framework decoding is cached. Final CHASM
serialize counters and updated node bytes remain separate to avoid conflating staged
work with persisted output. Cached-Payload regression verifies a second read has zero
framework DATA decodes while custom page/record/backend work remains nonzero and
repeatable. Each page has at most sixteen entries; aggregate per-handler work includes
all index operations and repeated page visits.

Reproduce from the frozen binary named by binary.sha256:

```
SCHEDULER_WHOLE_RUNTIME_LARGE=1 /private/tmp/scheduler2-packed-runtime.test \
  -test.run '^TestPackedRuntimeWorkAndStorage$' -test.v -test.count=1 -test.timeout=20m
```

source-manifest.json includes all 628 CHASM Go sources plus module files and was checked
unchanged before/after compilation. Source writes resumed only after binary completion.
The small and observation logs are archived alongside this document. The large run passed in 58.19s; final lint is recorded separately. No universal fit
claim is made here.

## Frozen actual results

| Family / N | Initial framed CHASM bytes | After measured handlers | Initial nodes |
|---|---:|---:|---:|
| Pending / 1000 | 776,121 | 776,576 | 2,087 |
| Pending / 10000 | 7,903,846 | 7,903,952 | 20,712 |
| Queue / 1000 | 304,963 | 305,090 | 555 |
| Queue / 10000 | 3,009,833 | 3,009,639 | 5,270 |

Empty execution message framing adds four bytes to each value. The largest measured
pending image leaves 484,652 bytes below 8 MiB after those four bytes; this is only this
fixture, not a budget guarantee for production metadata, other admitted request data,
prepared calendar/timezone pages, arbitrary key distributions, or intermediate images
outside the listed handlers. Large logs include small4/5 results as well.

All measured hot phases have zero whole-tree visits, collection hydration/key-copy/
full-sync counts and cold-node construction. Custom page work is not zero: it includes
all fixed-depth logical index lookups and repeated parsing. hot-counters.json preserves
the complete vectors (and large01.log includes cold reconstruction and exact physical
updated-node bytes), so no constant-work claim is inferred merely from framework cache
hits or absence of full scans. Actual public/SQL delivery and mutation fault matrix are
separate parent-owned verification gates.

## Post-measurement checks

The full default-base make lint-code-fast run found eleven findings (lint01.log):
existing/shared complexity in framework/runtime functions and an unused test context,
plus two operation_store functions touched by typed reads. The latter were extracted
into preparation/ownership helpers after this frozen measurement; directed packed,
operation store and readiness tests then passed0.792s (refactor01.log). Final aggregate
lint is pending parent-coordinated shared fixes. The measured binary intentionally
remains the exact pre-extraction version identified by its manifest.
