# Persisted whole-spec definition capability

Scope: standalone DefinitionPreparation and immutable staging/read adapters, pure canonicalization without resolution, fixed binary continuation codec, and public year1 inclusive-sentinel corrections. No live Generator/backfill/shared-provider/preview task activation is claimed. Root owns prepared_enumerator.go, shared source provider wiring, projection publication, task scheduling, component layout attachment, central plan/work item/ledger and full transaction evidence. Root added additive schema and all-profile/nil component registration; hashes of these evolving shared files are separate from the twelve owned frozen sources.

## Implementation

StageDefinitionSource performs source-proportional clone/canonicalization/validation/mask construction/hash/page encoding outside mutation. Exact canonical content+seed identity equals occurrence.NewPublic; revision page ownership is independently hashed. No range/term count cap is added. Absent timestamp values normalize to a canonical internal zero plus presence flag so staged and restored fixed headers are identical; explicit year1 zero time remains distinct via presence. Non-UTF8 seed bytes are preserved. CanonicalizeScheduleSpec reuses the exact canonicalizer and unrestricted bounded-builder validations without timezone loading; protobuf Clone's existing empty-custom-data normalization is preserved.

DefinitionPreparation control holds only version, owner/content IDs, positive revision, fixed header, contiguous installation prefix and cleanup FIFO counts. The full detached source resides in a separate DATA field read only by ingestion/reconstruction/export. New/Replace publication of that source is explicitly input-proportional admission work. All masks, intervals and seed pages are addressed; installed evaluation never decodes original ranges/spec/seed. SourceSnapshot remains available before installation for immediate Describe preparation outside mutation.

InstallNextPages validates every staged page and existing addressed key before any publication, then publishes<=16 pages. Replacement transfers only the installed old prefix to one FIFO descriptor; no page scan or immediate recursive removal. Cleanup preflights all addressed records and deletes<=16 pages from only the oldest descriptor. Retained readers capture immutable owner/header and are bound to CHASM context epoch. Timezone binding uses one separately versioned TimezonePreparation child; stageCalendarTimezone now delegates its unchanged body to a generic definition-ID binder. Old definition binding becomes unavailable after replacement until a new source generation is bound and installed. Old timezone prefixes remain in that child's existing cleanup FIFO.

SourceSnapshot timezone reconstruction returns the exact original binding separately, as in the prior calendar capability; caller must reconstruct against that owner. Standalone APIs do not schedule cleanup or choose refresh times. Runtime activation must supply fair maintenance scheduling and monotonic timezone generation across definition replacement.

## Representation and bounds

- Header: exactly91 bytes. Counts are64bit and page-count arithmetic is checked.
- Calendar term98 bytes; interval17; term page<=16 records plus2-byte header => maximum1570 encoded bytes.
- Seed page<=24 bytes plus2-byte header; arbitrary24-byte SeedWindow spans<=2 physical pages. Hash step remains20 bytes.
- Protobuf page wrapper maximum1618 bytes, tested at16 calendar records and maximum ordinal.
- Protobuf control<=229 bytes and cleanup descriptor149 bytes. Source input bytes, DATA path/envelope and CHASM transaction serialization are explicitly excluded from these record-only bounds.
- Continuation codec: exact328 bytes, explicit version/identities/five times/phase/index/minimum/nested calendar/hash/iteration/flags/jitter. No reflection/JSON; canonical booleans/nanos, host-int bounds, nested cursor shape, result completeness, immutable job identity and zone are validated. Internal wall time retains time.Time's signed-seconds domain; only public boundary enforces public domain plus exact year1-minus1ns sentinel.

The pure wrapper now accepts that inclusive sentinel, preserves default epoch clamp and year1 fractional semantics, ignores a raw calendar zero-time return, and treats raw interval zero-time as exhaustion, matching the existing reference's Go zero sentinel. Root's automatic prepared wrapper remains a separate review/validation scope.

## Validation

- Full source suite: scheduler2 PASS19.726s, occurrence PASS1.142s, legacy scheduler PASS4.263s.
- Final additional storage-bound/full nil-library fixtures: directed05 PASS0.750s.
- Pure state codec every-phase binary reconstruction and malformed wire checks PASS0.655s predecessor; sentinel+codec checks PASS0.644s.
- Canonicalization direct reference/immutability/error-order fixtures PASS0.791s. Malformed cron, conflicting names/custom data, nil calendars/intervals/ranges, invalid durations/exclusions covered; pure helper with unresolved names never resolves.
- Real reload before installation, after partial/full page work, and full nil-library DetachedExecution reconstruction after corrupting Source. Installed reads continue from pages only; escaped reader fails across epoch.
- Non-UTF8 seed windows at boundary offsets and complete source reconstruction preserve identity; mask/interval pages match staging after reload.
- Every one of the first16 install pages injected malformed before publication; state and addressed page existence unchanged. Corrupt last page in a16-page cleanup prevents any deletion; restored prefix reclaims in two quanta and leaves active owner intact.
- Timezone binding/replacement/original snapshot reconstruction and old binding refusal pass. Empty definitions and exact year1/year9999 metadata roundtrip; malformed page count/type/trailing bytes/identity/header flags rejected.
- Scoped lint01 normal/minimal/vet zero issues; final lint02 covers the latest fixture/comment changes and is archived after terminal.

Retained unsuccessful attempts are evidence, not acceptance: canonical01 overlapped root proto generation and failed missing generated imports; canonical02 had incorrect test assumption about protobuf empty-byte clone behavior; directed01 referenced nonexistent test convenience methods; directed02 exposed staged absent timestamps retaining epoch while persisted header canonicalized to zero, corrected in staging so headers are identical. No failed attempt is hidden or counted as a pass.

Remaining gates: independent source review; root composed final tests/lint; actual generator/backfill/provider/preview integration and scheduling; full hot-handler+CloseTransaction accounting and cold/source/export costs; restart/task/public suites on integrated runtime. Capability completion does not close WI-018/M3/M4.

Final lint02 terminal PASS exit0, normal/minimal/vet zero issues. All twelve owned source hashes rechecked after terminal.
