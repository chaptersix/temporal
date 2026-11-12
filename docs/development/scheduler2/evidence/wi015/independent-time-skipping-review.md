# Independent time-skipping review

Non-author review of the time-skipping runtime gate, issuance snapshot and History identity gate, with repository code/testing guidelines. No blocking behavior finding for this local explicitly registered profile. The reviewer authored public projection plumbing, so this artifact does not independently approve that implementation.

`IsExecutionSkippable` checks bounded control summaries and generator readiness without materializing request payloads. It refuses advancement while closed/paused/uninitialized, tracked or unconfirmed operations exist, queues/manuals/ranges/effects remain, or a coordinator reservation/due coordinator/generator obligation must execute. Future wakes remain eligible targets. This matches the existing scheduler's no-active-invoker/backfill/work-before-advance principle. ALLOW_ALL start-only records do not indefinitely block advancement after acknowledgment, while pending issuance remains blocked. There is no use of wall-clock time in the gate.

`startSnapshot` obtains the already detached request, calls the framework propagation helper once, validates the captured skipped duration exactly, and clones both config and propagation into the outgoing request. The same captured offset reconstructs post-RPC activity time; a later clock-state change does not alter that offset. Persisted admitted payload and callback bytes remain unchanged. The framework's real propagation helper preserves accumulated offset while respecting DisablePropagation and stripping the parent's FastForwardConfig. Existing scheduler invoker uses the same issuance-time semantics.

The History branch recognizes the generated dedicated scheduler2 identity only with a nonnil registry containing that component and the existing namespace scheduler time-skipping flag enabled. Unknown IDs, unregistered scheduler2 and default-disabled flag remain false. This does not register or route scheduler2 or broaden unrelated archetypes. Unit tests cover these boundaries and verify namespace lookup.

## Evidence checked

All seven source hashes matched `time-skipping/source-hashes.json` at review. Read the actual archived functional03 log: `ok go.temporal.io/server/tests 9.367s`. The test exercises both initial and later public patch configuration; actual History advances to a first tick12h ahead, a real child's one-day workflow timer completes, and its persisted state contains >11h initial skipped duration and enabled config without inherited fast-forward policy. Parent fast-forward completion is checked against existing behavior that disables its effective config. This is stronger than a mocked request assertion. Earlier failure logs remain retained and are not counted as passing evidence.

Unit tests cover clone isolation, preserved durable request, actual StartHandler outgoing propagation and snapshot observation offset, and each runtime-gate blocker/exact due boundary. These are finite cases, not a complete proof of all timer/range/overlap recovery interleavings. The parent owns full shared suite and consolidated lint. No new tests were run by this reviewer while the parent owned active validation.

<details><summary><strong>small</strong> — New time-skipping subtests omit parallelization annotations.</summary>

The new runtime/snapshot table subtests and functional test cases omit t.Parallel, without explaining an intentional serialization requirement. Repository testing guidance asks all tests/subtests to parallelize or document an exception. Per-case state is local; dedicated functional cluster cost may justify serial execution if explicitly recorded.

</details>

**Suggestion:** Add t.Parallel to independent unit subtests; document deliberate functional serialization using the repository opt-out convention if that is required by cluster ownership.

## Reviewed source hashes

- `chasm/lib/scheduler2/activity.go`: `530d8082a0978a495c2cef611ce4e5e94c471aaf3318922a9d1a40cb25ceb456`
- `chasm/lib/scheduler2/activity_test.go`: `c80f0143aa127e3f0f2580e94b1b6159e7454c7e2759bdf89fe9be6efccb43ff`
- `chasm/lib/scheduler2/timeskipping.go`: `64b64c611c7f1b572f2fa23b37091c17c2328466c8cf82fe323f0d6dee6d1e2e`
- `chasm/lib/scheduler2/timeskipping_test.go`: `84a488483712fa0efd39aa4d4c79c98117ede54254c062ba9d0d2960e635575d`
- `service/history/workflow/timeskipping.go`: `c8dba59686a596db3aa9600c6eaada6d0f53166f80762d8a4190a49a15e694db`
- `service/history/workflow/timeskipping_test.go`: `07caad3e6ababaeda5de1643f152076a360edb0c52241a7b19df689d45368916`
- `tests/schedule2_timeskipping_test.go`: `cb7fccbb07c585e75b21ebb04ece6511331ce46557e801d07e4a2d020600b172`
