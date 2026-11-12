# Bounded definition preparation and public projection contract

Owner: api_seam. Read-only implementation proposal; no code or acceptance claim. Scope is D59 bounded scheduler mutation work, preserving the current accepted spec and API domain. It does not reintroduce prototype size/term/range caps.

## Actual repeated work

`occurrence.NewPublic` calls the legacy bounded builder with MaxInt limits, marshals canonical form and hashes it. `currentEnumerator` repeats construction during coordination. `rawNextTime` visits every include calendar and interval; `excluded` visits exclusion calendars; `NextTimeCandidate` repeats the raw union for following nominal time before jitter. Eight probes only bounds calls to this work.

A term is not yet a bounded unit: `newCompiledCalendar` expands every range; `makeYearMatcher` appends every expanded year including duplicates, and every year lookup scans those values. `compiledCalendar.next` has calendar carry/search loops and DST fold/gap logic. `GetNextTime` loops across excluded candidates until its dynamic compute limit (disabled means effectively unlimited). Counting only terms, probes or returned future times misses these costs.

Every public transition currently calls `publishPublicList`: it clones the entire spec, CleanSpec processes it, and only later truncates top-level arrays in publicListProjection. publicFuture recompiles and invokes GetNextTime up to5 times for List. Thus an unrelated callback can pay full definition/range/exclusion work even when list output is small. Recent actions independently scan/sort all retained actions; keyed action/window storage is already assigned elsewhere.

## Definition storage and preparation

Keep canonical public definition bytes as immutable versioned DATA for exact Describe/Update and identity hashing. Validation/canonicalization of the submitted request remains proportional to that request and runs before the mutation callback wherever possible; neither malformed input nor a failed update can partially publish a new definition. Do not repeat full canonicalization/hash during ordinary coordination. Store the accepted canonical digest, jitter seed identity and revision in fixed control state.

Persist bounded compiled pages keyed by definition revision, include/exclude kind, term ordinal and page ordinal. A page contains a schema-fixed number of normalized terms, not an arbitrary repeated range list. The immutable source definition remains the authority for API projection; the compiled representation is semantically equivalent derived state. Activation records source revision and preparation progress. No automatic candidate is published until its required compiled pages are ready; accepted old-generation requests remain immutable and executable while the new definition prepares.

Compilation of a structured calendar folds ranges into fixed masks for second/minute/hour/day/month/weekday and a101-bit year mask for explicit years2000..2100. Preserve a separate `allYears` flag for empty Year: empty means wildcard, including earlier years admitted by the reference, not an empty mask or an implicit2000 lower bound. Duplicate/overlapping/stepped ranges set the same bits; repeated range count must not affect later evaluation cost. Preparation tracks term, field, range and range-value cursors, spending a fixed number of elementary operations per quantum. A range-value step is bounded by validated field domain. Interval representation is fixed duration/phase seconds with exactly the reference rounding/arithmetic semantics.

Do not repeatedly decode the entire canonical ScheduleSpec just to resume range preparation. Either persist bounded source-range pages as input-proportional ingress storage, or parse the addressed immutable source snapshot outside the execution mutation lock and publish bounded prepared page batches. The initial source snapshot/accepted request is an explicit input-byte cost; every ordinary mutation still touches a fixed number of compiled pages. If an outside-lock preparation cache is used, cache loss must cause a named re-preparation step, never an under-lock full compile fallback. No implementation may label repeated whole-spec clone/parse in every transaction as cold reconstruction.

The choice of staging input pages versus outside-lock immutable preparation is a framework integration decision to freeze with the actual byte/node counters. Do not accept a first implementation merely because it returns the same candidates: the first transaction after reload must also meet the hot control-work bound.

### Timezone dependency

`SpecBuilder.loadTimezone` accepts both standard timezone names and arbitrary validated TimezoneData, using time.LoadLocationFromTZData for the latter. A custom timezone's parse cost is input-dependent; a fresh legacy builder also loses its location cache. Pages must share one revision-bound timezone identity, not parse the TZif blob per term or per quantum. Reuse a validated immutable location prepared outside mutation and bind it by definition hash, or persist an equivalent bounded lookup representation. Reload/cache miss is an explicit preparation state; it cannot call LoadLocationFromTZData under an ordinary metadata/callback transaction. Source validation and exact Go timezone lookup/DST behavior remain the oracle. The implementation must record the trusted timezone-library lookup bound and actual cache-miss preparation path; no guessed fixed cost or substituted timezone semantics is approved here.

## Resumable raw candidate evaluator

Persist a small evaluator continuation separate from the public occurrence disposition cursor:

1. `include_union`: input searchAfter; include page/term cursor; current minimum nominal candidate; per-calendar search cursor.
2. `exclude`: finished nominal candidate; exclusion page/term cursor and matched flag. Stop early on the first match exactly as reference; excluded candidates do not compute jitter.
3. `following_union`: for a nonexcluded nominal, compute the next raw nominal over all include terms, even if that following instant itself will be excluded.
4. `publish_candidate`: apply the unchanged deterministic jitter formula using nominal, seed and min(configured jitter, following-minus-nominal), then atomically create the existing Pending candidate.

A calendar's year/month/day/hour/minute/second carry and DST offset traversal must itself be resumable if it can exceed the fixed elementary-step budget. Preserve the corrected calendarDate first-fold choice and next's dstoffset exactly; do not approximate with fixed UTC durations. Freeze a bounded cursor containing the reference local fields/offset/phase. The year horizon remains the reference2100; wide manual/backfill identity and pre-epoch public start semantics remain unchanged.

During any preparation/search yield, `DisposedThrough` and the admitted action budget remain unchanged. Do not advance `SearchAfter` as if a raw candidate were complete while only part of its union has been inspected. A revision change invalidates the entire partial evaluation by definition hash/revision; never combine old minimum with new terms. Named `definition_prepare` and `candidate_prepare` stutters expose outstanding work and a physical recovery wake. They must not spin with repeated immediate quanta or steal an existing coordinator candidate lease. Fairness is over completed ready candidates; a partial definition cannot be advertised as ready.

When publish_candidate finishes, the existing one-probe occurrence transition is the refinement target. Probe/output counters increment at this point, not once per term/page. Existing Empty/Impossible/Exhausted distinctions, FullRange and EligibleSeen semantics remain exact. Arbitrary batching/reload of preparation steps must produce the same nominal/actual/exclusion sequence as NextTimeCandidate.

## Public projections without hidden compilation

Separate static list-definition projection from dynamic counters/recent actions. Build the cleaned/truncated spec projection once per accepted definition and configured truncation policy, directly from the selected canonical entries instead of cloning every term first. Top-level truncation alone does not bound range bytes inside retained terms; store that input-derived projection separately so unrelated action updates do not reserialize it. Preserve public output exactly, including current configured truncation behavior; do not silently replace retained range lists with masks in the API.

The dynamic list projection contains only bounded recent-five entries, fixed counters/flags and a revision-tagged future-five preview. Updating a callback must not invoke NewCompiledSpec/GetNextTime. Maintain preview through the same incremental evaluator, using its own cursor/definition revision and compute-limit observation snapshot. Required list memo serialization may include the static projection bytes; account these addressed projection bytes separately and avoid full-definition scans while assembling it. If the visibility mechanism requires rewriting a large retained static projection each mutation, document and measure that necessary API-byte cost rather than claiming fixed bytes.

Future preview semantics require special care: current code starts after max(ctx.Now, root.Cursor, PublicInfo.UpdateTime), and dynamic hard compute limits return a partial list. Capture that anchor and limit snapshot at preview job creation; restart or filter/extend when the anchor moves. Do not publish stale-revision future times after Update, or return an early candidate now at/before the required anchor. List visibility is eventual, but that does not authorize indefinitely stale/incorrect predictions. The new stored-preview publication protocol must have a tested eventual convergence condition.

Describe and MatchingTimes are synchronous reads with required answers. Capture the current immutable definition and fixed root projection/anchor consistently in the engine read; perform request-specific compilation/enumeration outside the mutation critical section where the API adapter can preserve that snapshot. Their input-definition/output-cardinality costs are explicit read costs, not a fixed-work scheduler mutation. Preserve matching-time compute-limit errors, Describe partial-preview behavior, and dynamic configuration observations. Do not route Describe to an incomplete stored List preview merely to avoid computation. Nil-library detached reads must still decode source and stored projections without runtime handlers; no new runtime dependency is embedded in DATA.

## Required evidence

- Many duplicate ranges in one term and many duplicate explicit year ranges: preparation cost grows with input once, but subsequent evaluation has the same fixed page/step bounds. Test empty Year wildcard versus explicit2000..2100 mask, ancient interval/cursor cases and future empty ranges.
- >64KiB/100terms/1025ranges remain accepted with unchanged canonical API output; no revived prototype caps.
- Compare raw candidate and public GetNextTime outputs against existing builder for interval/calendar unions, exclusions, jitter following-raw behavior, London/NewYork folds/gaps and custom TimezoneData. Preserve raw warning/hard-limit counting separately from new scheduling yields.
- Reload after every evaluator phase, failed commit, update during partial union/exclusion/following scan, and competing manual/backfill work with retained leases. No incomplete minimum is admitted; no accepted work disappears.
- Instrument unrelated callback/list-metadata update with a huge valid spec: zero full-definition compile/range walks, fixed control visits, no historical payload reads. Account required static projection bytes explicitly.
- Compare List eventual preview and synchronous Describe/MatchingTimes after creation/update/clock movement and dynamic compute/truncation changes. Delayed preview jobs cannot overwrite newer revision/anchor data.

This design adds real refinement obligations beyond the finite occurrence theorem. Preserve old proofs as reference semantics; do not claim they prove the new preparation machine, timezone cache lifetime or public projection protocol without new correspondence evidence.
