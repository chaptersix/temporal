# Independent WI017 workload design review

This is a read-only review of the WI017 draft against Phase6/P6.1–P6.4, with proposed executable parameters. No load runner, measurements, throughput result or M4 acceptance is claimed.

## Coverage and design findings

The draft preserves all six requested families, matched old-CHASM/scheduler2 comparisons, offered-versus-achieved load, local physical services, capacity recovery, retention/dedup tradeoffs and the unsupported migration boundary. Its ADLC sequence is appropriate. Three details need resolution in the executable runner before measurements:

1. **Define the rate unit and time window per family.** Describe requests/sec, due occurrences/sec, accepted range commands/sec, generated starts/sec and callbacks/sec are different quantities. A fixed population of gated workflows completed once cannot produce ten times as many callbacks over the same measurement window. For the fixed callback-storm witness, preserve population and compress release offsets by10; report active-burst offered intensity and total count separately from throughput over the fixed observation window. If sustained callback throughput is required, add a separately named replenishing-pool workload and count unavailable release targets as unachieved offered work. Do not present either as ten times achieved callbacks merely because the trigger driver ran faster.
2. **Specify instrumentation that is currently missing.** Existing captures provide service metrics and successful physical task writes. They do not establish decoded DATA bytes, preparation/serialization CPU, transaction lock wait, exact persistence read bytes, or serialized CHASM mutation bytes. Implement generic test-only hooks/wrappers at those boundaries, identically for both backends; mark any unimplemented metric unavailable. ChasmTotalSize and logical task histograms are not substitutes for per-transaction bytes or a complete task census (the logical-task histogram is conditional). Whole-process RSS/CPU includes workers, frontend/History and SQLite; label it accordingly.
3. **Bound observation overhead and measure cold state separately.** HistoryTaskRecorder retains all tasks, and in-memory histogram captures may retain all samples. A sustained10x run can benchmark recorder allocation and GC. Use bounded streaming/raw aggregation, count observation drops, and invalidate metrics whose samples were lost. Warm metadata reads and post-shard-reload reads exercise different decode paths; collect both using fixed read traces and explicit cache state. A process/shard restart at a known accepted-work count must not silently reset counters or change workload denominator.

These are implementation prerequisites, not requests for user permission. No additional infrastructure or production routing is needed.

## Finite pilot parameters

`workload-candidate.json` is a proposal for the first executable profile. It fixes seeds, populations, payload bytes, client concurrency and durations, and defines exact baseline/10x stimulus changes. It deliberately uses small local populations and records sample-size limits: a baseline with fewer than1000 observations should not claim a stable p99. Raw order statistics can still be reported with sample count and uncertainty.

Calibration runs are separate artifacts, not selected results. Choose one paired offered-rate configuration before measuring either backend. Retain all calibration attempts and both failures and successes. Do not tune server limits or worker behavior per backend. Repetitions alternate backend order to reduce host-temperature/cache/order effects; fresh dedicated namespaces and clusters prevent previous retention from leaking into comparisons. All arrival scheduling uses an absolute monotonic deadline trace with bounded in-flight requests; a missed dispatch deadline is recorded as driver backlog/drop, not rescheduled into a closed-loop rate.

## Instrumentable layer map

| Required measurement | Available local mechanism / required addition | Interpretation |
| --- | --- | --- |
| API latency and offered/accepted/rejected counts | Driver monotonic deadlines and RPC begin/end/status records | Include scheduling delay separately from RPC service latency; record timeouts/censored calls. |
| Nominal-to-start latency and actual throughput | Public RecentActions for bounded spot checks; actual workflow histories/start RPC observation for complete counts | RecentActions is capped and cannot be the workload census. |
| Backlog, queue age and live operation counts | Periodic typed scheduler reads + DatabaseMutableState snapshots, fixed cadence; source-aware adapter per backend | Public buffer counts differ from all retained records; compare semantic sets explicitly. |
| Successful physical task writes | `WithHistoryTaskRecorder`/`WithClusterHistoryTaskRecorder` | Recorder delegates persistence first and records only successful writes; count by exact namespace/business/run/task type. Use bounded replacement for sustained load. |
| Persistence calls, latency and bytes | Existing namespace/global metric captures for call/latency counters; add ExecutionManager/DataStore wrappers for exact request/response byte accounting | Distinguish API payload, protobuf serialized mutation and SQLite storage/WAL bytes. Avoid summing repeated references as new payload ownership. |
| Decode/preparation/serialization work and transaction contention | Required generic CHASM node-load/serialization and History lock acquisition/hold hooks or validated tracing | No current physical metric is claimed to supply these quantities. Count cached versus materialized reads separately. |
| CPU, allocation/heap, RSS and GC | Whole-test-process CPU profile/runtime statistics and OS RSS samples on dedicated run | Not scheduler-only CPU; include idle/control observation overhead and sample cadence. |
| State growth/retention | Fixed-cadence committed DATA-node serialization census and row/blob reference census; first/peak/post-drain snapshots | Track unique payload bytes, references, metadata records, and write amplification separately. |
| Restart cost and correctness | CloseShard/cache reload at a recorded accepted-work barrier, then same immutable request/dedup replay | Report latency to first successful read and to recovered progress, preserved authority and exact action count. |

## P6 mapping and acceptance boundary

- P6.1: six concrete matched workload definitions, calibration trace, old/new identity assertions, baseline/10x offered stimuli, source/environment/command fingerprints and raw distributions. Callback finite-burst intensity caveat must remain explicit.
- P6.2: each arrival family has stop-and-drain; long-lived/overload include configured capacity1000 and explicit1/disabled-limit correctness variants; fixed reload barrier and duplicate request/result probes before and after configured retention horizon. Count rejected commands separately from accepted commands waiting. Positive completed-history retention is time-based, so stopping intake and merely waiting without a maintenance transition may not physically reclaim immediately; record the actual maintenance event that performs reclamation.
- P6.3: publish measured costs and benefits, missing metrics, recorder overhead, scaled public-state scans, sample counts, resource caps/censored runs, proof-domain limits and unresolved discrepancies. Finite internal model bounds are not limits or throughput evidence for the expanded public profile.
- P6.4: local-only comparison; migration, sentinel ownership transfer, production registration, routing and rollout remain unsupported and unexecuted.

The proposal is implementable within the existing dedicated local cluster harness plus test-only observation wrappers. Approval of the written draft alone does not supply those metrics or complete P6.
