# Source-level definition/projection slices

Owner: api_seam; proposed assignments, not permission to edit shared runtime. Parent assigns implementations. Start with slice1 only: it does not change scheduler behavior, legacy calendar code, timezone handling, List semantics or persisted schema.

## Slice 1 — bounded calendar range preparation primitive

Own new `chasm/lib/scheduler2/internal/occurrence/preparation.go` and `preparation_test.go`. No edits to occurrence.go or legacy scheduler packages. Introduce plain Go value types:

- CalendarHeader: fixed seven field range counts, explicitYear flag and validated canonical source identity.
- RangePage: fixed-capacity array of at most16 canonical Range values plus actual count; no protobuf pointers.
- CalendarMasks: six uint64 field masks, two uint64 explicit-year words and allYears bool.
- PreparationState: source identity, field/range/page cursor, next range value, partial masks and terminal flag.
- `PrepareCalendar(state, header, readPage, budget) (state, stats, error)`: fixed budget of elementary range-value/cursor steps, at most a derived fixed number of page fetches, no iteration over untouched ranges. Callback retrieves one indexed bounded RangePage, never the entire spec.

Input is already canonical/validated; still reject structurally impossible page counts/cursors and invalid range/domain values as internal corruption. Normalize End<Start and Step0 only as the existing canonical semantics permit; avoid overflow and never silently truncate a range. Empty Year produces allYears=true; explicit years set bits relative to2000 and retain the reference year domain. Other fields retain empty-mask meaning. Each finished range advances exactly once, including duplicates, and no final mask is exposed until all fields finish. A zero/negative budget is an explicit validation error. This produces a compiled matcher artifact, not a candidate or admission.

Test arbitrary budgets/partitioning and fresh state copies after every step against a simple full-range expansion reference. Include >1024 duplicate ranges, overlapping steps, all field extremes, explicit and empty years, unknown source identity/corrupt cursor, and source mutation after page snapshot. Require exact step/page bounds and identical finished masks. Convert masks back to a bounded canonical calendar only in tests, then compare legacy NewBoundedCompiledSpec/NextTimeCandidate for representative UTC queries: this checks the primitive preserves reference matching without changing legacy production code. No timezone/calendar-next implementation is rewritten.

Schema: none in this first slice. Do not claim durable continuation evidence from copying Go state; serialization is slice3. This independently reviewable primitive exposes whether range paging/step accounting is sound before larger integration.

## Slice 2 — visibility memo version gate (framework owner)

Proposed owned files `chasm/visibility.go`, `chasm/tree.go`, focused new `chasm/visibility_version_test.go`; coordinate with DataMap/worklist owner before edits. Optional interface:

```go
type VisibilityMemoVersionProvider interface {
    MemoVersion(Context) uint64
}
```

It supplements, never replaces, VisibilityMemoProvider. Zero means unsupported/uninitialized for the bounded profile, not 'unchanged'. Version comparison is a semantic application contract: every change to returned Memo content increments the persisted version atomically. The scheduler needs a dedicated ListRevision, since PublicRevision intentionally does not increment for every action/result/list change. Preflight MaxUint64 before writing any changed projection. A repeated no-op may keep the version; repeated request dedup must not consume a version unnecessarily.

Add a small cached `currentMemoVersion` and presence flag alongside currentMemo. For a versioned provider, newTreeInitSearchAttributesAndMemo and passive refresh read the version without Memo/Clone. On close, compare old/new version only; schedule visibility when unequal and update the cached version in the same successful-tree lifecycle. Avoid Memo(), proto.Equal and proto.Clone on this mutation path. Actual visibility export retains existing full Memo assembly/encoding and is charged as an output-proportional read, not hidden in mutation accounting.

Cold reload initializes cached version from the persisted current projection; equal old/new version means no extra task for an unrelated mutation, not proof that visibility storage has already caught up. The already-persisted visibility logical/physical task remains authority for pending export. If loading/replication replaces projection and revision, refresh cache and preserve existing task-generation rules; do not fabricate a revision change solely to compensate for cache absence. Existing default raw providers retain old equality/clone behavior. An opted bounded root with missing/zero version is invariant failure; it must not silently fall back to O(spec) comparison.

Failure/rollback: cache advancement on a tree whose persistence fails cannot suppress future visibility after reload. Existing failed-tree discard is required; tests must inject failure after version comparison/task creation then reconstruct committed tree and retry. An application error before close must not advance the cached version. NewTreeFromDB and detached nil-library decode must see the same provider contract. Library schema registration must reject version-only provider without Memo provider or bounded profile expecting a version whose root does not implement it.

Tests: large memo with a provider that panics/counts if Memo invoked during unrelated close; unchanged-version no update; changed-version task; duplicate/no-op; MaxUint64 failed publication; snapshot reload with pending task; passive child-only update; persistence rollback; default-provider unchanged behavior. Actual visibility queue task still returns the complete memo, including long retained range lists. Do not change scheduler runtime until this gate is reviewed.

## Slice 3 — persisted prepared calendar pages

Own new `chasm/lib/scheduler2/definition_preparation.go` plus tests; narrow proto changes in `proto/v1/state.proto` and generated outputs by one coordinated generator owner. Add source/compiled page records and fixed PreparationState representation matching slice1. Use the new DataMap API after independently accepted behavior. Root/Generator stores revision/digest and page frontier only; no growing root slices. The source canonical definition remains immutable DATA for API roundtrip/unknown fields.

Choose source-page staging mechanics using measured input costs: page the accepted canonical ranges before mutation, or snapshot the addressed definition for an outside-lock preparation worker and commit a fixed page batch. Never decode all source ranges merely to resume one page. API validation/canonicalization remains request-proportional; ordinary callbacks/coordination cannot invoke it. This choice requires parent approval of the exact count formula, not a guessed transport limit. Serialize/restore after every preparation phase; failed publication leaves frontier/pages unchanged.

Do not wire candidate production in this slice. Runtime activation and actual cold-cache timezone recovery are prerequisite gates below.

## Slice 4 — timezone and calendar-next gate, then raw evaluator

Owned files proposed `internal/occurrence/prepared_calendar.go`, `prepared_candidate.go` and differential tests. Keep existing legacy reference untouched. First establish the exact timezone supply contract: standard-name and custom TZif parsing occurs outside ordinary mutation; reuse one validated immutable location per revision; cache absence must yield a durable preparation obligation, never under-lock parse fallback. Scope source/clock semantics to current Go timezone behavior, including the corrected fold handling. Snapshot/cache lifetime and recovery tests precede enablement. No claim of bounded timezone parsing follows from term pages alone.

Only then implement bounded resumable calendar carries and the union/exclusion/following-raw/jitter phases described in definition-projection-contract.md. Expose an adapter matching existing occurrence probe completion; new substeps are named stutters. Actual existing State/SearchAfter/DisposedThrough/Pending changes happen only at completed probe. Preserve finite internal New path until new correspondence tests exist; public path selects persisted preparation by revision. Own coordinator/generator insertion points only after parent freezes those files.

## Slice 5 — static list projection and preview

Owned files proposed new `public_list_projection.go`, narrow public_projection.go/public_mutation.go/Memo callsites, and proto ListRevision/static-reference/preview-state fields. Static cleaned/truncated spec is prepared only for definition/truncation changes; bounded dynamic counts/recent-five/notes/paused metadata is updated pointwise. Note strings and retained range arrays are exact input-derived API bytes, not fixed-size values. The framework memo-version gate prevents their repeated cloning/comparison under unrelated mutation. Memo export assembles static+dynamic snapshot for actual visibility; preserve full encoded output.

Future-five preview uses incremental evaluator with revision, anchor=max(now,cursor,updateTime), remaining-budget and compute-limit snapshot. This is a separate projection cursor, never the authoritative occurrence cursor. A newer revision/anchor invalidates pending old preview writes. List eventual consistency allows bounded delayed export, not wrong-revision predictions. Parent must freeze refresh/convergence semantics and compare existing List tests before activation.

Synchronous Describe/MatchingTimes capture immutable current definition, anchor and projection under the engine read then calculate request-specific future results outside mutation. Keep exact errors/partial-result rules and configured compute thresholds. They must not return unfinished List preview as their answer. This adapter change is independently testable before replacing scheduler enumeration, but is not itself a bounded mutation proof.

## Assignment recommendation

Assign slice1 to a free independent implementer now; source review can begin without schema generation or scheduler conflicts. Framework owner adds slice2 after DataMap API stabilization and before bounded worklist acceptance. Keep slices3–5 specified but unimplemented until timezone supply and full Memo serialization paths have source-backed gates. No milestone or stronger bounded-work claim follows from a passing isolated mask compiler.
