# Persisted prepared timezone slice

Status: standalone implementation and directed verification complete; frozen source is in persisted-timezone/manifest.json. Root retains independent review, runtime activation and central acceptance. Combined timezone/operation proto generation completed after the queue checkpoint.

## Current source and exact scope

CalendarPreparation currently stores a detached canonical calendar source, bounded range pages, a fixed mask cursor and FIFO cleanup ownership. It has no timezone field and no candidate continuation. Its source hash includes calendar bytes/revision only. PreparedTimezone now supplies bounded custom-TZif conversion but its immutable pages live only in caller memory. This slice makes those pages durable and attaches an optional fixed preparation child. It does not implement union/exclusion/jitter, public future previews, production named resolution or coordinator routing.

Add a registered TimezonePreparation component with no tasks/config dependency, and a fixed `Timezone` Field on CalendarPreparation. Live and nil libraries register the same type. Existing calendar-only constructor/callers retain an empty optional child; no empty nodes are fabricated. An explicit binding method attaches or replaces the child after checking the currently installed calendar SourceId. Calendar source replacement leaves the old child physically owned, but PreparedTimezone access rejects its stale definition binding until a new timezone snapshot is bound.

## Outside-mutation staging

StageCalendarTimezone takes the already validated StagedCalendarSource, explicit resolved TZif bytes/name, explicit resolver wall instant, and positive timezone generation. It invokes PrepareTimezone and fixed-page encoding outside mutation. The bytes may be supplied by a future named resolver, but this API does not itself resolve names or prove source-order/TTL behavior. A separate UTC staging constructor handles the existing named empty/UTC special case without treating absent custom bytes as UTC. No custom-byte nil/empty fallback is introduced.

The immutable staging object owns a detached source blob, fixed encoded header and immutable encoded pages. DefinitionSourceId binds the calendar revision. SemanticFingerprint comes from actual prepared rules/cache. Owner SourceId hashes definition identity, positive generation and semantic fingerprint/name. The generation must increase on every replacement, including replacement under the same calendar revision. This prevents a newly installed snapshot from sharing page keys with a superseded cleanup owner even when its semantic rules are identical.

SourceSnapshot returns the detached original DefinitionSourceId as well as source and generation; reconstruction must use that original calendar revision, not a newly installed revision. Persist the original explicit resolvedAt instant with source bytes, so after a caller cache miss source reconstruction outside mutation reproduces the same loader-cache tuple. Persisted execution evaluation consumes the stored header/pages directly and never reseeds, parses, loads source bytes, or consults time.Location.

## Fixed header/page codec

A new occurrence codec exports a detached fixed-size header and at most16-entry transition page at an addressed ordinal, and restores an immutable capability from the fixed header plus an addressed page reader. It exposes no mutable backing array and performs no full-page-array construction on restore.

The versioned header encodes fingerprint, transition count, before-first offset, cache presence/start/end/offset and fixed extension/rule fields. The versioned page encodes fingerprint, ordinal, actual entry count and16-or-fewer (int64 instant,int32 offset) records. Decode rejects unknown versions, wrong lengths, bad identities/counts, impossible rule modes/ranges and inconsistent UTC/header forms. Preserve accepted unsorted/duplicate entries and arbitrary signed32-bit offsets; do not sort or impose a public transition cap. Structural decoding does not claim to prove the semantic derivation of a trusted persisted fingerprint.

All header work is constant-size. Every page decode/copy is bounded by16 entries. NewTimezonePreparation and replacement publish only fixed header/control plus the addressed original source DATA blob; the latter is explicit input-proportional ingestion, not an ordinary hot-work claim. Page installation separately preflights and writes at most16 pages, then advances its installed frontier atomically. An incomplete snapshot cannot supply a capability for evaluation.

## State and cleanup ownership

Additive protobuf control state contains definition/owner/fingerprint identities, positive generation, fixed header bytes, total/installed page counts, and FIFO cleanup head/tail/count. Source name, resolved-at timestamp and full raw source bytes remain a separate DATA field. Pages and superseded-owner descriptors use separate DataMaps. The fixed header codec gives an explicit max byte count, independent of input transition cardinality.

Replacement validates generation/definition/header/frontiers and FIFO exhaustion/collision before mutations. It appends the previous owner descriptor with exact installed page count and cursor, publishes the successor fixed state/source, and retains old pages. Cleanup addresses at most16 pages from one FIFO head, validates all addressed page identities before any delete, then deletes pages and advances/removes that owner atomically. Partial installation deletes only the installed prefix. New replacements cannot overtake an unfinished cleanup owner. Current snapshot ownership is never cleaned.

The TimezonePreparation child remains attached during active cleanup. No unbounded collection removal is allowed. Closure/production maintenance scheduling remains the future scheduler protocol, not invented here. As with CalendarPreparation, failed enclosing publication discards the candidate tree and restores the prior durable image; there is no in-place rollback promise.

## Evaluation and context ownership

CalendarPreparation obtains completed masks and its bound complete TimezonePreparation snapshot separately. A restored timezone capability captures the CHASM context/epoch in its fixed page reader; it must not be cached across transactions. Each new mutation restores from the small fixed header and addresses only pages requested by the pure evaluator. DataMap ownership checks reject stale handles. Reader failures return the original pure continuation under the existing Advance contract.

Timezone semantic fingerprint mismatch requires restarting only unfinished calendar/union/exclusion work at its original boundary; completed Pending candidates retain authority. Calendar DefinitionSourceId mismatch is a definition revision error, not a timezone refresh. This slice exposes enough identity to enforce both but does not persist a new candidate cursor or publish/admit work.

## Required evidence

Actual CHASM serialization/reload after installation batches; fixed header and page codec malformed/truncated tests; installation source mismatch and all-page preflight rollback; missing/corrupt addressed page errors; nil-library whole-record detached reading; poisoned original source proves evaluation does not parse it; exact known DST candidates using restored pages; failure publication preserves prior header/pages/cache identity; replacement during partial installation and repeated replacement FIFO cleanup; cleanup wrong-owner/page failure preserves earlier addressed pages. Test a timezone fixture with enough transitions for multiple installation/cleanup batches, not only common small zone files. Record full transaction counters separately from pure lookup counters and quantify encoded fixed header/page maxima.

## Remaining integration boundary

A production named resolver must still preserve Go ZONEINFO/platform/embedded/GOROOT lookup order (the actual loader implementation, documented in named-timezone-resolution-audit.md), Local behavior, cache errors and existing24hour refresh. It supplies actual resolved bytes/snapshot and resolvedAt outside mutation. No GOROOT-only shortcut is allowed. A definition may contain many calendars/intervals/excludes; union minima, exclusion cursors, jitter and public List previews must be incrementally evaluated using versioned identities. Existing legacy UTC-year horizon and published-candidate behavior remain required. Persisting one timezone snapshot alone does not close WI018 or M3.
