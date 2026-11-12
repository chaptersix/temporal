# M3 SP7 / P5.2 spec-processing coverage audit

Read-only source audit of `/private/tmp/sch-redesign-execution`, HEAD
`2c69d8ad7a781bc3a35144b592317d7179526a56`. Only this document was written;
no production code/tests were changed or executed. Concurrent WI-015 edits are
outside this audit. WI-013 proof/correspondence artifacts were inspected separately
in `/private/tmp/sch-redesign-proofs`; they are not yet present in this execution
snapshot and must be integrated explicitly.

**Conclusion:** existing tests substantiate the M2 bounded UTC slice and two pinned
US DST examples. They do not yet satisfy the plan's boundary-biased generated SP7
coverage or all P5.2 combinations. This is a verification gap, not evidence of a
specific production calendar defect. The immediate prerequisite is extending the
independent oracle safely before feeding it sparse timezone/calendar cases.

## Contract and implementation boundary

- [Plan §7](../../../../CHASM_SCHEDULER_REDESIGN_PLAN.md#spec-processing-verification-track), SP1–SP7, requires independent instant scanning, pinned timezone data, boundary-biased Rapid, explicit minimized JSON replay and mutation sensitivity. P5.2 (line274) remains unchecked for separate jitter, DST gap/fold, exclusion, advancement, computation and batching checks.
- [D23–D26](../enumeration-contract.md) define integer-second nominal matches, millisecond actual jitter, inclusive spec boundaries/actual window, strict nominal advancement, duplicate coalescing, exclusion precedence, jitter bounded by the next **raw** nominal including exclusions, and retained revision-specific pending state. D25 explicitly leaves general historical non-hour transitions outside proved library correctness; it does not remove selected timezone-boundary examples or generated SP7 obligations.
- [Enumerator.New/Run/probe](../../../../chasm/lib/scheduler2/internal/occurrence/occurrence.go#L76) uses the legacy bounded compiler and [NextTimeCandidate](../../../../service/worker/scheduler/candidate.go#L15). That candidate method uses `rawNextTime`, exclusion matching, and next-raw jitter lookahead. [spec.go:348–410](../../../../service/worker/scheduler/spec.go#L348) implements union/dedup by minimum next nominal, interval arithmetic, exclusion matching and Farm32 jitter. Correctness of those calendar/search computations is precisely what a finite precomputed-stream Lean proof does **not** establish.

## Source-backed coverage matrix

`O` below is `chasm/lib/scheduler2/internal/occurrence/`. “Present” means inspected
assertions, not a fresh execution claim. Existing evidence provenance is E18/E54
and [M2 coverage limits](m2-coverage.md#L51).

| Obligation | Present source / actual assertion | Missing boundary coverage / exact next check |
| --- | --- | --- |
| SP7 gap / fold | [O/boundaries_test.go:172](../../../../chasm/lib/scheduler2/internal/occurrence/boundaries_test.go#L172), `TestPinnedTimezoneGapAndFold`: embedded New York TZif digest; 2024-03-10 local02:30 has no occurrence; 2024-11-03 local01:30 has literal UTC05:30 and06:30; budget1/output1 with JSON continuation through `collect`; independent local-field scanner agrees. | No generated transition-near specs; no cursors/actual windows exactly before/at/after transition or either repeated instant; no start in first fold occurrence followed by second; no gap-adjacent valid match in the same sequence. Add transition fixture tables plus Rapid offsets around these anchors. |
| SP7 exclusion timezone / fold precedence | [O/occurrence_test.go:143,166](../../../../chasm/lib/scheduler2/internal/occurrence/occurrence_test.go#L143) compares UTC interval/calendar union with second-field exclusions; [O/backfill_test.go:245](../../../../chasm/lib/scheduler2/internal/occurrence/backfill_test.go#L245) exercises fractional calendar/exclusion Yield then pending. | No exclusion applied to repeated local hour, no both-fold-instants excluded assertion, no inclusion/exclusion duplicate expression collision at transition, no UTC-vs-local exclusion discriminator. Use local01:30 exclusion on a broader inclusion to remove both fold instants; separately restrict UTC spec bounds to retain only one branch. |
| SP7 timezone / date boundaries | One pinned America/New_York fixture; [legacy calendar tests](../../../../service/worker/scheduler/calendar_test.go#L223) include Pacific DST and London repeated-hour regression. | Legacy tests use host `LoadLocation`, not pinned bytes; they do not exercise new serialized occurrence adapter. Add pinned London repeated-hour-start regression plus local midnight crossing UTC day/year, leap day/month end and DOM∧DOW conjunction. A selected non-hour transition (e.g. Lord Howe) can improve coverage while retaining D25's unproved general-library boundary; do not claim exhaustive timezone verification. |
| SP1 eligible first / strict advancement / no skipped occurrence | `scanOracle` directly enumerates seconds and matches fields; `TestBoundedEnumerationAgainstScan` checks complete sequence with duplicate intervals; generated sequence equality implies uniqueness/advancement for its dense UTC slice. [O/boundaries_test.go:18](../../../../chasm/lib/scheduler2/internal/occurrence/boundaries_test.go#L18) checks fractional start/inclusive end; `TestPrototypeBounds` covers range edges. | Rapid calendars vary only seconds on fixed2024-01-01; all other calendar fields are broad. Add explicitly varied minute/hour/day/month/year/DOW, range steps/unions, duplicate calendars and mixed interval/calendar same-instant matches. Assert each returned candidate strictly exceeds input nominal cursor and equals first independent eligible instant, including restart exactly at first fold instant and ±1ns bounds. |
| SP2 resumability / SP6 budgets | `collect` JSON-roundtrips each call, rejects input mutation, checks reported probes/output limits; budgets1/2/8 deterministic and1–8/1–4 generated. [O/limits_test.go:17,58,93](../../../../chasm/lib/scheduler2/internal/occurrence/limits_test.go#L17) covers1023/1024/1025 probes,127/128/129 outputs,64 terms/1024 ranges/64KiB ±1. `TestExcludedWorkYieldsWithoutExhaustion` checks exact excluded disposition/Yield; February30 distinguishes Impossible/Exhausted. | Dense UTC budget coverage is substantial. Add sparse calendar/exclusion chains near transitions, checkpoint/resume before both fold candidates, independently counted consumed raw candidates and terminal probe. Reported `Probes` is not an instrumented count of internal calendar operations. At-most-two raw searches is source-backed in candidate.go; CPU/search-year bound is an explicit contract limitation, not established by elapsed-time assertions. |
| SP3 batching | `TestEnumerationProperties` creates one UTC cut at0–30 seconds plus0–999ms and joins two fully collected windows against independent expected stream. Backfill interval Rapid covers inclusive/fractional windows with probe1/2/8. WI-013 proves finite-stream exact batching and tests UTC interval correspondence separately. | No generated finite partitions around timezone transitions or actual jitter boundaries; no calendar-generated full-state final equality test in this adapter suite. Add2–8 nondecreasing cuts selected from independent nominal/actual/transition values ±1ns, including repeated cuts, and compare concatenated sequence to whole independent expected results; compare final continuation/status for equivalently settled executions. Do not equate arbitrary per-call probe counts across output-limited schedules. |
| SP4 jitter bounds / stability / raw lookahead | UTC Rapid jitter0–6000ms; duplicate intervals; exclusions. [O/boundaries_test.go:36](../../../../chasm/lib/scheduler2/internal/occurrence/boundaries_test.go#L36) has literal2022 jitter golden beyond nominal EndTime and retained ownership; line229 errors on unsupported actual; replay recompiles every step. | No generated sparse next-raw distance, excluded-next-raw golden, sub-ms jitter boundary table, cap≈2^32ms table, seed variety in base occurrence property, or transition-crossing jitter/partition cross-product. Add independent literal/fixed-computation witnesses for zero/999999ns/1ms and next-raw distance−1ms/exact/+1ms, with an excluded next raw candidate and nominal end before lookahead. Assert `nominal≤actual<nextRaw`, including that excluded next raw still caps jitter. Farm32 agreement alone is not independent hash validation. |
| SP5 pending update interaction | `TestUpdateSupersedesFutureCandidates` covers eligible/excluded pending; definition identity/revision rejection; [O/backfill_test.go:73](../../../../chasm/lib/scheduler2/internal/occurrence/backfill_test.go#L73) exercises capacity ownership and historical rebase. WI-013 saved/generated UTC Update/Rebase sequences cover exact state. | No transition-local calendar/zone/seed update with a pending fold candidate or exclusion revision. Add one directed automatic update and one historical rebase at a fold boundary, maintaining the existing distinct restart-boundary contract. This verifies calendar composition, not a new update contract. |
| Empty/impossible/exhausted | `TestTerminalAndWindowStatuses` and backfill distinctions cover no inclusions, February30, full vs partial excluded scans and zero-work repeated terminal queries. | Add gap-only calendar over an explicit effective date range, plus a later valid match outside/inside nominal EndTime; assert distinction using actual origin/full-range semantics. Do not infer global Impossible merely because a narrow window has no due candidate. |
| Parsing/canonicalization | `TestMalformedDefinitionsRejected`, limits tests and legacy `spec_test.go`/`calendar_test.go` cover malformed structured specs, nils, duration/range limits, cron/calendar parsing and canonical forms. | Current independent occurrence oracle only understands explicitly populated structured calendars and intervals. Legacy syntax/cron/default fields need literal expected normalized fixtures or a separately specified parser expectation. Do not silently canonicalize expected specs with production code and call that independent parsing evidence. |
| Replay / mutation sensitivity | [O/replay_test.go:37](../../../../chasm/lib/scheduler2/internal/occurrence/replay_test.go#L37) saved pending-jitter full-input replay; `TestScanOracleDetectsDroppedPendingCandidate` tests missing persisted pending. WI-013 has separate executable-correspondence saved traces and actual source mutations. | No saved DST/exclusion/partition trace; base occurrence Rapid only reports draws on failure, not a full minimized explicit SP7 scenario. Add versioned fixture with spec, seed/revision, embedded timezone digest/data reference, windows/limits and expected observations; test standalone replay. Add targeted calendar mutations (fold second-instant loss, exclusion timezone/precedence, jitter lookahead skipping excluded raw, inclusive boundary error) with restored-source evidence. |

## Independent oracle limits that must be fixed before extension

[O/occurrence_test.go:53–103](../../../../chasm/lib/scheduler2/internal/occurrence/occurrence_test.go#L53)
is genuinely independent of production **next-time search**, but intentionally
limited:

1. It enumerates raw instants only from the cursor to `spec.EndTime + 2 minutes`.
   A sparse inclusion whose next raw match is later than that horizon is omitted
   from jitter lookahead, so the oracle can incorrectly leave jitter uncapped.
   Current dense intervals and zero-jitter DST examples avoid this; blindly adding
   sparse jitter cases would create a false oracle disagreement. Production raw
   lookahead is not limited by the effective nominal EndTime. Fix by requiring a
   proven complete finite fixture raw horizon, scanning through at least the next
   raw match or the maximum relevant jitter horizon, or using explicit independently
   constructed sparse raw lists/literal expected jitter where scanning is expensive.
   Record why stopping the scan cannot change any in-range jitter bound. Never
   obtain that lookahead from production `rawNextTime`/`NextTimeCandidate`.
2. It requires finite meaningful StartTime/EndTime and fully specified structured
   ranges. Omitted proto time bounds/default calendar fields, CronString and legacy
   Calendar/ExcludeCalendar are not independently normalized by this helper.
   `matchesRange` uses max(end,start)/max(step,1) for the valid generated domain;
   it is not a validator for arbitrary malformed inputs.
3. Shared trusted primitives are Go timezone conversion from identical TZif bytes,
   UTC/time binary encoding, protobuf values and Farm32. Literal gap/fold UTC
   expectations and the existing jitter golden reduce selected common-mode risk;
   the hash algorithm and timezone library are not verified here.
4. The oracle emits eligible Candidate values at revision1, not per-call excluded
   disposition traces or all continuation/status fields. Existing `collect` checks
   structural budgets/ownership and final sequence; it is not an independent full
   state-transition oracle. Keep raw candidate observations separately if asserting
   exact disposed/search/pending state at a generated cut.
5. The embedded New York fixture is byte-pinned (SHA256
   `e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95`), but its originating
   tzdb release is explicitly **unknown** in [fixture README](../../../../chasm/lib/scheduler2/internal/occurrence/testdata/README.md).
   This satisfies D25's exact-byte reproducibility for those dates; do not relabel
   it as a known IANA release. New fixtures should record release when available,
   byte hashes and source, otherwise retain the same honest unknown-release bound.

Legacy [FuzzCalendar](../../../../service/worker/scheduler/calendar_test.go#L373)
selects host zones, drops invalid parses, samples using global math/rand and compares
production `cc.next` with itself at related instants. It is useful supplementary
metamorphic coverage, not the pinned independent predicate oracle/boundary-biased
structured Rapid promised by SP7. No new fuzz campaign was run by this audit.

WI-013's separate raw fixture builder scans **UTC interval** matches and explicit
second exclusions; its generated small periods/windows and proof-domain ordered
stream are excellent enumeration checks but add no DST/calendar-parser coverage.
Its 10000-case count must not be merged into SP7's transition-case count.

## Concrete next implementation slice, in priority order

1. **SP7-A oracle boundary self-tests.** Add a test-only bounded structured-spec
   reference with explicit finite horizon/preconditions; preserve an independently
   specified raw list through jitter lookahead. Self-test a next raw match beyond
   EndTime+2min (e.g. hourly structured matches with a shorter effective EndTime and
   >1h jitter), an excluded next raw match, missing/default-field rejection or
   explicit support, and duplicate-union coalescing. Reuse production only for the
   actual side. Keep dense existing helper cases unchanged until equivalence is
   demonstrated.
2. **SP7-B directed pinned fixtures.** Reuse current NY bytes and add literal
   transition-adjacent matches, both fold instants with a cursor between them,
   local exclusion of both, next-raw excluded jitter bound, nominal/actual/end ±1ns,
   and pinned London repeated-hour-start regression. Add local midnight/day/year
   and leap-day/DOM∧DOW fixtures. Optional selected non-hour fixture remains a
   recorded finite example, not expansion to a universal timezone claim.
3. **SP7-C structured boundary-biased Rapid.** Generate valid specs directly from
   finite fixture families (no parse-filter loop): transition family, relevant
   local fields/range steps, optional duplicate/mixed inclusion, local exclusion,
   fixed revision/recorded seed, jitter selected at meaningful bounds, cursor/start/
   end selected at anchors ±1ns, and2–8 partitions biased to independently computed
   actual/nominal times. Keep windows small (e.g. max8h and bounded candidate count)
   and budget schedule from1/2/8 probes and1/2/4 outputs; retain existing cap±1 tests
   separately. Use zero-jitter transition families plus a smaller finite horizon
   jitter family to avoid an uncontrolled second-scan cost. Assert full expected
   output, strict nominal order/uniqueness, no earlier eligible skip, pending
   stability after JSON reconstruction, and semantically comparable final state.
4. **SP7-D replay and sensitivity.** Persist a transition/exclusion/partition trace
   independent of Rapid seeds; preserve minimized generated failure data and full
   provenance. Demonstrate at least one transition-sensitive and one jitter/
   exclusion-sensitive source mutation is detected, minimized and replayed, restoring
   production source afterwards. A mutation of the test oracle alone is not a
   substitute for demonstrating production-defect sensitivity.
5. **SP7-E evidence gate.** Record source hashes, tool/version, exact TZif hashes,
   commands, routine fixed seed and separately recorded exploration seeds, generated
   case bounds/counts and runtime. Start with a measured routine profile (e.g.100
   transition-family cases) and larger bounded exploration (e.g.1000 before raising
   to10000 after timing); do not copy dense-UTC10000 counts without measuring this
   scanner. Parent chooses final profile from measured cost, not an arbitrary wall-
   clock claim. Run tagged occurrence/affected legacy tests, race where appropriate,
   and required changed-code lint after implementation; this audit did not run them.

No material contract question currently blocks this slice: D23/D25 already specify
nominal/actual boundaries, exclusion priority, both-fold behavior and byte pinning.
Any actual oracle disagreement must preserve its smallest input and be resolved
against that contract rather than majority agreement with historical code. M3
SP7/P5.2 should remain open until these missing bounded checks and their independent
review/evidence are completed; universal timezone-library proof is not required.

## WI-016 acceptance update

The bounded SP7-A–E implementation and verification now pass, including the
source-reproduced London fold correction, final-source shrinking/replay sensitivity
and independent reviews. See work-items/WI-016.md and evidence/wi016/acceptance.md.
The audit above is the pre-implementation gap inventory; historical non-hour
transitions and trusted primitives retain their stated limits.
