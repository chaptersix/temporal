# Whole-spec definition preparation contract

Parent authorized the pure checkpoint followed by this independent storage slice. No runtime/profile activation is included. The fixed accepted combined Manuals+Backfills bound remains100, with ingress/reconstruction validation owned separately.

## API and ownership

New files: definition_staging.go, prepared_definition.go, prepared_definition_codec.go and corresponding tests. Existing definition_preparation.go remains CalendarPreparation. New legacy helper file exports CanonicalizeScheduleSpec without loading a location or building legacy matchers; it preserves NewBoundedCompiledSpec ingress checks, canonicalization and validation order with no term/range caps. Its canonical result retains original timezone name/data and cron conflict semantics. Resolver selection stays outside this helper and outside CHASM callbacks.

- StageDefinitionSource(spec, seed, revision) constructs immutable StagedDefinitionSource outside mutation. It validates, canonicalizes, prepares all calendar masks, computes exact old definition content identity plus unique revision ownership, and encodes fixed pages. Work and allocation are proportional to submitted source; this is explicitly the ingestion/cold reconstruction boundary.
- NewDefinitionPreparation(ctx,stage), ReplaceSource(ctx,stage) publish fixed control and a detached source blob. Replacement requires increasing positive revision. Source persistence is input-proportional admission work, separately counted; it is not a constant-size hot mutation claim. Old installed page prefixes transfer to FIFO cleanup descriptors without a scan.
- InstallNextPages(ctx,stage,limit<=16) preflights every addressed page/owner/ordinal and then publishes at most16 pages. All validation/read failures precede writes.
- Prepared(ctx,policy) returns fixed occurrence.PreparedSpecHeader and occurrence.PreparedSpecReader only when all pages are installed. Policy snapshots WarnIterations/MaxIterations/ReturnExcluded and derives a fixed job identity from immutable content/revision/policy. Source blob is never read on this path. Reader capability remains valid only for the captured CHASM transaction epoch.
- SourceSnapshot(ctx) returns a detached canonical input, seed, revision and identity even before installation completes. It is explicitly a read/export/cold reconstruction operation so immediate Describe can prepare/evaluate an immutable snapshot without waiting for installation. It neither installs pages nor mutates control.
- BindTimezone(ctx,stage) attaches/replaces one existing TimezonePreparation child by exact definition owner identity; a new generic private staging helper binds timezone to a32byte definition identity without inventing a fake calendar. Existing calendar wrappers retain their behavior. Timezone generation/refresh remains independent from term installation.
- PreparedTimezone(ctx) returns the existing fixed lazy zone capability after bound timezone installation. Root combines this with Prepared for actual generator/preview work. New definition replacement does not silently reuse a child bound to the old definition.
- CleanupSuperseded(ctx,limit<=16) validates and removes at most16 pages from the oldest owner, advancing or deleting one descriptor. Old timezone prefixes retain their existing separate FIFO ownership; no child deletion until all descendants are reclaimed.

The component is registered for offline reconstruction without adding it to active bounded runtime layout. Parent owns shared library registration/layout and task scheduling; the staging slice does not schedule tasks.

## Fixed page representation

One component owns a single DATA map of term and seed pages. Logical term ordinals concatenate calendars, intervals, then exclusions; header records union and exclusion counts separately. Term pages contain at most16 tagged fixed records: calendar record is98 bytes (kind1+sourceID32+six masks48+year masks16+AllYears1), interval record17 bytes (kind1+interval8+phase8). Page format/count adds2 bytes, so maximum encoded page1570 bytes. Page decode validates type, exact length, masks and positive interval/nonnegative phase. Source ID for each calendar derives from fixed definition identity+ordinal; no original ranges are read hot.

Seed storage pages hold at most24 bytes plus format/count2 bytes. Arbitrary-offset SeedWindow<=24 reads at most2 pages. Hashing still advances20-byte arithmetic blocks. A single global ordinal space places seed pages after term pages. The exact expected page type/count derives from fixed header and ordinal; wrapper source ID/ordinal reject cross-owner substitution.

Fixed header encoding is91 bytes: version1, content identity32, union/exclusion/seed counts24, explicit start flag+seconds+nanos13, explicit end flag+seconds+nanos13, jitter nanoseconds8. Positive revision and owner ID live in control. Counts use64bit representation without new public caps; checked quotient/remainder arithmetic derives page counts. Timestamp metadata validates protobuf domain and exact nanoseconds; nil start is preserved separately from explicit year1 zero time. Input bytes/seed/full canonical source are outside fixed control.

## Exact additive schema request

Append these independent messages to chasm/lib/scheduler2/proto/v1/state.proto; no existing RootState or Generator field is needed for the standalone slice. Root owns regeneration and later child attachment.

```proto
message DefinitionPreparationState {
  // Format version identifies the fixed control representation.
  uint32 format_version = 1;
  // Source ID is the exact canonical definition and seed content identity.
  bytes source_id = 2;
  // Owner ID binds this revision's immutable pages.
  bytes owner_id = 3;
  // Revision increases on definition replacement.
  int64 revision = 4;
  // Header encodes fixed counts and wrapper bounds.
  bytes header = 5;
  // Total pages includes term and seed pages.
  uint64 total_pages = 6;
  // Installed pages is the published contiguous prefix.
  uint64 installed_pages = 7;
  // Cleanup head identifies the oldest superseded owner.
  uint64 cleanup_head = 8;
  // Cleanup tail identifies the next unused owner ordinal.
  uint64 cleanup_tail = 9;
  // Cleanup count equals tail minus head.
  uint64 cleanup_count = 10;
}
message DefinitionPreparationSource {
  // Canonical spec preserves the full detached public definition.
  bytes canonical_spec = 1;
  // Seed preserves exact seed bytes, including any non-UTF8 bytes.
  bytes seed = 2;
}
message DefinitionPreparationPage {
  // Owner ID identifies the revision that owns this page.
  bytes owner_id = 1;
  // Ordinal identifies the page in its owner's combined page space.
  uint64 ordinal = 2;
  // Encoded contains bounded calendar/interval records or seed bytes.
  bytes encoded = 3;
}
message DefinitionSupersededSource {
  // Owner ID retains cleanup authority for this revision.
  bytes owner_id = 1;
  // Header preserves the exact expected page shape and content identity.
  bytes header = 2;
  // Installed pages records the prefix present at replacement.
  uint64 installed_pages = 3;
  // Cursor identifies the next page to reclaim.
  uint64 cursor = 4;
}
```

## Acceptance gates

Canonicalization differential including malformed cron, conflicting TZ directives, custom nil/empty distinction, malformed intervals/ranges and nil records; caller-input immutability. Source and owned-source reconstruction retain exact identity, including explicit year1 timestamps and non-UTF8 seed bytes. Fixed page codec boundaries across16 terms/24 seedbytes and empty definitions. Nil-library reload before/mid/after installation; poisoned source blob must not affect installed evaluation. Every addressed install/cleanup fault occurs before publication. Old readers cannot read the next transaction epoch; old-definition binding remains explicit after replacement. Failed owner/revision installation leaves tree image unchanged. Multiple superseded prefixes drain16pages at a time without scanning other owners. Per-control/page protobuf size maxima and full path/CHASM overhead distinguished. Then parent performs source review, composed test/lint and runtime integration.


## Implemented continuation codec

occurrence.EncodePreparedSpecState and DecodePreparedSpecState(data, header, zone, boundary) use exact version-one328-byte records, with no reflection or JSON conversion. Layout: version1byte; source and zone identities64bytes; five times65bytes; phase/index/minimum17bytes; nested CalendarNextState135bytes; hash continuation26bytes; iteration count8bytes; four flags4bytes; max jitter8bytes. Times retain signed Unix seconds and nanos plus explicit zero flag; internal converted wall values may exceed protobuf timestamp years, while the public boundary is validated against years1..9999 plus the exact inclusive year1-minus1nanosecond sentinel. Integer fields use64bit encodings and reject host-int overflow. Bool tags, nanos, lengths, phase/cursor shape, invocation identity and zone fencing are checked without page reads. Semantic history remains the durable-owner obligation; a codec cannot prove a trusted prefix's arithmetic history.

The root owns prepared_enumerator.go and automatic/backfill Origin/SearchAfter/DisposedThrough/Pending/rebase/disposal behavior. This codec carries the pure subcomputation only. New direct sentinel tests also preserve legacy zero-time rules: raw calendar zero result is ignored, while a raw interval zero result exhausts the wrapper.

Canonicalization evidence corrected an earlier assumption: the existing protobuf Clone normalizes present-empty TimezoneData to nil before canonicalization. CanonicalizeScheduleSpec preserves that behavior, including cron conflict precedence. The independent resolver itself continues to distinguish nil and present-empty direct input. Staging does not silently preserve a byte presence distinction that the existing ingress already removes.
