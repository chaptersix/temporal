# Standalone persisted timezone checkpoint

The fixed codec, immutable outside-mutation staging, no-task TimezonePreparation component, CalendarPreparation binding, installation and FIFO reclamation are implemented. Source frozen in manifest.json; matching compressed source copies are retained. The generated schema snapshot also contains the operation-store additions, coordinated in one make proto run; those additions remain separately reviewed/owned.

## Exact bounds and ownership

The codec header is exactly 89 bytes; a transition page is 38 + 12*N bytes, N <= 16, maximum 230. The ordinary writer's protobuf control is at most 251 bytes, page wrapper at most 273 bytes, and superseded-owner descriptor at most 137 bytes (tags, maximum legal varints, three 32-byte control identities, 89-byte header). These bounds exclude CHASM node/path metadata and arbitrary unknown protobuf fields not produced by these writers. Variable original name, input TZif bytes and resolvedAt are in separate Source DATA. Their ingestion and snapshot export are input-proportional.

Restoration validates fixed header metadata and reads zero transition pages. Hot access obtains only the addressed page and validates owner/ordinal plus codec shape/fingerprint; the existing pure lookup bound remains <=66 reads per lookup. Installation preflights at most16 pages, then publishes that exact batch. Cleanup preflights at most16 pages from one FIFO head, then deletes that exact batch. Replacement retains the old installed prefix and advances a positive generation; same semantic fingerprint never reuses page ownership. Definition binding is independent of both semantic fingerprint and generation. SourceSnapshot explicitly returns the original bound definition ID for outside-mutation reconstruction.

The capability captures the current context and owner. Ending that transaction invalidates the reader via the existing DataMap misuse-panic contract; this is an API lifetime violation, distinct from missing/malformed addressed data returned as ordinary read errors. No context is serialized or put in staging.

CalendarPreparation and this child still use default framework traversal. This checkpoint does not prove full-transaction cardinality independence, opt into fixed worklists, persist candidate continuations, or attach to Scheduler/Generator. The lookup-page/decode assertions cover evaluation only, not transaction-close or snapshot traversal.

## Evidence

- Combined make proto: PASS exit0 after queue checkpoint release.
- Initial codec focused tests: PASS0.664s.
- Final directed calendar/timezone suite: occurrence PASS1.063s, scheduler2 PASS0.717s.
- Combined scoped required make lint-code: normal/minimal/vet PASS zero issues; new files added with intent-to-add before diff-filtered lint. Targets scheduler2 and internal/occurrence, base HEAD, fixes disabled.
- Full occurrence initial run PASS1.037s. Simultaneous full scheduler2 run failed only operation-store new test assertions on framework transition metadata; that agent corrected those fixtures and independently reports final full PASS18.830s. Parent owns composed full-suite acceptance.
- Real CHASM close/reload between installation batches and candidate quanta; fixed explicit cache seed reconstructed from detached Source; known NY first/second fold results; poisoned original Source DATA proves evaluation does not parse it; decoded page count bounded by pure requested reads.
- 600-transition/38-page fixture crosses multiple installation/cleanup batches. Installation bad second page fails before first write; discarded enclosing publication restores prior frontier/pages. Replacement retains partial16-page prefix, followed by complete38-page owner and UTC successor; cleanup takes exactly4 calls, removes only old prefixes and preserves UTC.
- Every one of the first16 cleanup pages is independently corrupted: error leaves all16 keys and owner cursor/control unchanged before discarded publication. Stale definition binding rejects access after calendar replacement; a new generation rebinds successfully.
- Nil-library detached whole-record reconstruction reads all38 pages; retaining a capability across transaction end fails through DataMap epoch validation.

Earlier logs remain: test compile failures were two missing MutableContext assertions, then the source poison fixture used slash instead of CHASM dollar path separator, then the one-step DST test had an insufficient100-call guard (expanded to10000 while retaining known exact expected results). These are fixture corrections, not accepted runtime evidence. First lint found codec rule validator complexity; extracted the single-rule predicate. Concurrent operation test import findings were fixed by that owner.

## Remaining gates

Independent final source review and parent checkpoint; actual named source resolution/cache/Local behavior from named-timezone-resolution-audit.md; owned embedded source mechanism; source-change coordination; candidate/union/exclusion/jitter continuations; full bounded-layout opt-in and end-to-end counters; integration and recovery. This slice does not close WI018, M3 or M4 and does not enable routing.
