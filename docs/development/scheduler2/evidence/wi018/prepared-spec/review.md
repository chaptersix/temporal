# Prepared whole-spec continuation: scoped capability

This checkpoint implements integration-map item1 only. No Generator, backfill, provider, preview publication, CHASM storage, task, or routing path consumes it yet. Definition staging/installation, actual runtime integration and composed transaction measurement remain open WI-018 gates. Parent owns plan/work-item/ledger and commits.

## Contract and source correspondence

AdvancePreparedSpec performs at most1..64 elementary phases and returns value-only fixed continuation. Its result accessor publishes only after completion. SourceID binds immutable terms, seed, wrapper bounds and invocation policy; reader ownership/record ordinal validation is a caller obligation. Terms/exclusions/seed windows are detached fixed-size values; no list or string is retained in state. The header and reader are trusted prepared capabilities, not arbitrary public inputs. Structural cursor checks cannot prove semantic history of an already committed prefix.

Default mode follows CompiledSpec.GetNextTime: nil start means epoch, fractional StartTime subtracts1second, raw union runs calendars and normalized intervals, wrapper rejects UTC year>2100 or nominal>end, exclusions loop, thresholds are sampled once by caller per logical job, then a following raw union ignores both wrapper and exclusions when capping jitter. Interval arithmetic deliberately retains Go truncation and overflow operators. The compute-limit error is the exact legacy sentinel; it returns original state and necessarily implies ComputeLimitWarning=true, just as the legacy error result does. Successful result carries the accumulated warning. Substeps do not consume raw-iteration budget.

ReturnExcluded mode is the automatic enumerator wrapper, not bare NextTimeCandidate: StartTime subtracts1nanosecond, exclusions produce a completed candidate whose Actual=Nominal, thresholds are disabled for this one-probe mode. Caller still owns SearchAfter, pending/disposal/admission, automatic/rebase range semantics and validation of the jittered result against its caller-specific accepted range. The wrapper UTC horizon is separate from calendar local-year exhaustion; a following raw calendar lookup can return a UTC2101 candidate. PreparedCalendar input now accepts public timestamp years1..9999, preserving local result-year cap2100 and source conversion behavior.

Any term/seed/timezone read error returns the exact input whole-spec state, even after successful earlier phases in the same quantum. An unfinished zone fingerprint mismatch returns ErrPreparedZoneChanged; caller restarts from Boundary. A completed candidate is unaffected by timezone refresh. Source replacement requires caller rebase; this primitive does not dispose or publish an admission.

## Exact bounded jitter

Nominal UTC MarshalBinary is15bytes. Short concatenations<=24 use the existing Farm32 implementation on a fixed buffer. Long concatenations preserve the dependency's tail initialization,20byte loop and final arithmetic, without retaining the arbitrary seed. SeedWindow reads at most24 bytes at an arbitrary offset; a persisted adapter can use24byte source pages and at most2 physical reads per window. The20byte hashing stride is not a20byte storage-page requirement. A single phase reads at most one window. No seed prehash is substituted for fingerprint(nominal+seed). The dependency's MIT attribution/license is included.

SeedBytes is bounded only by the representable existing concatenated Go byte-slice size (MaxInt-15), not a new definition or seed product limit. Ingestion remains proportional to source bytes; persistence/staging and physical page codec are not claimed by this capability. Timestamp prefix marshaling is constant-size. State has no dynamic slice/map/string; time.Time fields must be encoded by the eventual durable codec without ephemeral location pointers (all created result/local values are UTC and input boundaries should be normalized there).

## Per-quantum accounting

For budget B<=64, Steps<=B, TermReads<=B, SeedReads<=B. Each calendar step has its existing adapter bound of8 lookups and66 fixed timezone page reads per lookup; exclusion wall conversion is one additional fixed phase rather than repeated per exclusion. Calendar stats count those conversions/lookups/pages. Addressed storage wrapper reads/bytes and handler+CloseTransaction overhead remain the integration's measurement obligation. Tests at4/5/1000/10000 duplicate interval terms show the first4-step quantum reads exactly3 terms for every cardinality; complete results also match. This is not a full transaction/storage capacity claim.

## Validation

- Full occurrence suite final03 PASS1.146s, predecessor full02 PASS1.166s.
- Scoped lint02 normal/minimal/vet zero issues. Final lint03 covers the additional mixed-union, following exclusions/end and fixed-read-budget tests; terminal result is recorded below.
- Reference comparisons with budgets1/7/64 and JSON reconstruction across every quantum: mixed duplicate calendar/interval union, exclusions, jitter, NY first/second fold and spring gap, pre2000, year1, UTC2101, year9999, end exhaustion.
- Independent known UTC witnesses: NY fold2024-11-03T05:30Z and06:30Z; skipped02:30 local spring day returns2024-03-11T06:30Z.
- Fractional-start test distinguishes preview's StartTime-1second from automatic probe's StartTime-1nanosecond; excluded probe Actual=Nominal.
- Every addressed read reached in a one-step trajectory is faulted and must return original whole state; separate existing timezone tests cover all fixed-page faults and noninitial calendar rollback.
- Exact Farm32 seed lengths0/1/4/5/8/9/10/11/19/20/24/25/39/40/41/100/1001/10000 against the current dependency. Tail/block boundary and fault rollback cases included.
- Compute-limit iterations, zone refresh and malformed term cursor checks pass.

Initial directed01 failure exposed a test fixture issue: the reused calendar helper populated missing structured fields while the reference source did not. Both inputs are now explicitly populated before compiling; known outputs prevent vacuous exhaustion acceptance. Initial lint01 found only four style/complexity issues, corrected by named result value, reserved identifier rename, explicit default and union-completion extraction. Logs retain unsuccessful attempts. No public test or runtime claim is inferred from these isolated passes.

Final lint03 terminal PASS, normal/minimal/vet zero issues. All seven frozen source hashes rechecked after terminal; no source edits followed.

Independent review found and reproduced stale Nominal after an excluded final candidate exhausted the next raw union. Fixed the exhaustion transition to clear Nominal; regression compares full completed result at budgets1/7/64. Final full04 PASS1.123s; corrected lint04 pending. Earlier manifest is superseded by this seven-file freeze.

Corrected lint04 terminal PASS normal/minimal/vet zero issues; independent corrected full suite plus unchanged failing-witness overlay PASS1.335s. Independent review artifacts retain both original failure and corrected source hashes.
