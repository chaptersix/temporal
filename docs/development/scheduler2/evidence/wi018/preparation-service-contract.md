# Shared preparation service and bounded source refresh

This is the next WI-018 prerequisite. Existing pure evaluation/staging is frozen for review. Root owns shared library/frontend/handler/maintenance integration; the calendar owner implements the isolated service, pending-source capability, fixed response batches and tests. No default/offline source I/O or production routing is introduced.

## Startup and source/cache boundaries

Inject one process-scoped PreparationService into the live private frontend and task handlers. It owns one timezonesource.Environment and Resolver, with the actual wall clock used by the existing1000-entry/24hour source cache. Unix and Windows platform factory files select existing providers and owned embedded data; NewNilLibrary never constructs a process provider. Tests inject an environment and event clock. This establishes one explicit provider history, with process-immutable TZ/ZONEINFO assumption from the source audit; it does not claim equivalence to arbitrary prior stdlib Local cache history. Different server processes cannot share a pointer/cache; detached durable inputs/fixed job ownership are the cross-process contract.

A separate bounded process cache retains immutable StagedDefinitionSource and StagedTimezoneCandidate by their32-byte owner identities. Capacity/eviction affect only availability, not correctness; use the existing cache implementation with injected wall clock and a fixed capacity1000,24hour retention. No cache lookup, canonicalization, resolver operation or filesystem access is needed inside a mutation callback. A task obtains an immutable response batch first; callback validation/installation touches only fixed control and bounded records. An immutable complete source stage may be retained by outside-mutation caller code, but it is never persisted in root DATA.

The caller begins one source check before a new logical evaluation job. A job pins the response's semantic zone fingerprint across stutters, as a compiled reference call does. Do not approximate real TTL with execution ctx.Now()+24h: execution time may be skipped. Completed pending occurrence authority survives refresh. Source failure retries the one source task; it does not advance enumeration or silently fall back to UTC.

## Definition staging service API

- NewPreparationService(environment, wallClock) constructs the isolated cache/service; live platform factory uses the real wall clock.
- PrepareDefinition(spec, seed, revision) prepares canonical input and inserts immutable stage, then resolves canonical timezone input outside callbacks. It returns the stage and snapshot for creation/update or immediate read-only preview. Legacy protobuf clone normalization of present-empty custom bytes is preserved.
- Definition(owner) returns a cached immutable stage or miss, without I/O. On miss, the side-effect executor reads detached DefinitionPreparation.SourceSnapshot under the same owner/token fence, then calls RestoreDefinition(snapshot, revision, expectedOwner) outside callbacks. Owner/identity mismatch is stale, not an overwrite.
- DefinitionBatch(owner, frontier, limit<=16) returns a detached fixed-size batch from a cached stage. Cache misses are explicit; no Source.Get or parsing occurs in installation. DefinitionPreparation.InstallBatch validates exact owner/revision/header/frontier and all supplied bounded pages before publishing.
- ResolveDefinition(stage) delegates only to the shared resolver, returning its original explicit ResolvedAt snapshot.
- PrepareTimezone(stage, snapshot, reservedGeneration) creates a StagedTimezoneCandidate and inserts it by owner; TimezoneBatch(owner, sourceFrontier, transitionFrontier, limit<=16) returns at most16 total raw-source and compiled pages. Stage objects and batches retain no CHASM context.

Bounded batch values have private construction, fixed arrays of16 records, explicit count, fixed metadata and per-record bounded encoded bytes. Public accessors expose only owner/frontier/result information needed by root hooks. Definition source admission at actual client create/update remains an explicitly input-proportional boundary. Environment-driven refresh never publishes an arbitrary full source blob inside mutation.

## Pending timezone double buffer

The selected profile uses version2 source ownership even for initial timezone installation. Legacy standalone NewTimezonePreparation/BindTimezone remains version1 for existing capability tests; no migration of existing executions is enabled. Version2 retains the current active snapshot while a pending candidate's source and compiled pages are installed.

Serialize the complete TimezonePreparationSource outside mutation, including arbitrary name/TZif bytes, original ResolvedAt and native variant. Raw DATA pages contain<=1024 bytes. Pending metadata holds the byte count/hash and two contiguous frontiers. Compiled pages retain existing<=16-transition encoding. One batch installs<=16 total raw+compiled pages; raw source first, then compiled. No per-page task is created.

The pending page owner is SHA256(legacy staged timezone owner || SHA256(exact serialized source)). Binding only semantic fingerprint/generation/name is insufficient: cache loss may re-resolve identical rules at a different ResolvedAt while retaining the same semantic fingerprint. The exact serialized-source hash makes those distinct pending owners and prevents mixing old/new raw page prefixes under a reused owner.

Candidate activation requires both complete prefixes. It swaps only fixed active control and enqueues the prior active prefixes for FIFO cleanup. New candidate replacement enqueues its already installed raw/compiled prefixes; no frontier is reset in place or forgotten. LastGeneration remains monotonic across activation, aborted/partial candidates and definition replacement. Active Source is absent in version2; SourceSnapshot assembles exact bytes from SourcePages as a read/export operation and verifies SourceHash outside the hot path. No full-source leaf write occurs during refresh.

Cache loss with a complete pending raw-source prefix reconstructs the exact original snapshot and ResolvedAt outside mutation. With an incomplete raw prefix, absent bytes cannot be recovered from durable state. Re-read current source through the provider and re-prepare at the original fixed ResolvedAt, native initialization year and native word width retained in Pending. Accept only the exact original SourceHash and candidate owner. Thus unchanged source progresses even when stage-cache eviction occurs on every batch. If the bytes/API source changed, preserve active, queue the partial candidate for cleanup, reserve a strictly newer generation/token and stage the new source. Never write a different source into the old owner prefix. Repeated process crashes during source ingestion need eventual completion of individual bounded response transactions; unchanged source does not require its process cache entry to survive. No new source-size cap is imposed.

Cleanup spends a single<=16-page budget across compiled and raw-source prefixes of the oldest descriptor. Raw source pages use a separate DATA map with uint64 ordinals, avoiding a new uint32-byte-count restriction. Missing/malformed source or compiled pages fail before any deletion. Old active generation remains usable until atomic activation; complete Pending occurrence records do not require the old timezone capability afterward.

## One durable source obligation

Root persists SourcePreparationRequest (fixed owner/token/phase/reserved generation/candidate owner/pending flag). Task payload contains only definition owner and token. Validate checks closed/lifecycle state and exact fixed owner/token/pending; no source decode. There is at most one authoritative source side-effect obligation per execution.

Phases:1 definition-page installation,2 resolve/check timezone,3 pending-source installation. The executor reads a fixed work snapshot, reconstructs any cache miss outside mutation, obtains the bounded response and invokes one fenced mutation. Every successful batch or no-change source acknowledgment advances the token before scheduling a successor. A lost acknowledgment or duplicate physical task therefore fails the old token and cannot repeat activation. Update/replacement invalidates the owner/token before any new source work. Pending candidates also compare reserved generation and candidate owner in the response merge. Mismatches produce a stale response with no writes; no source failure changes enumeration state.

A resolve result semantically identical to the currently bound active snapshot acknowledges the source check without installing another source. The active original snapshot remains authoritative; latest cache metadata is not substituted into its owner. A changed fingerprint begins a pending source candidate, and unfinished evaluation restarts only after its installation/activation. Root ensures a bounded fair share for installation/cleanup versus automatic/manual/backfill work.

Task bound: one additional pending side-effect type/obligation and one task payload<=45 protobuf bytes (32byte owner plus maximum uint64 token). Each successful response emits at most one successor source task, or one existing pure Work wake when ready, never one task per page. Existing immediate/pure/side-effect per-transaction aggregate limits must be recomputed by root before activating the slot, including source-response transition and task serialization/validation. Stale physical duplicates contribute no authoritative live-state collection growth and are still measured separately.

## Exact additive schema request

Append to TimezonePreparationState (existing tags1..11 remain):
```proto
  // Last generation never decreases when a candidate is replaced or activated.
  uint64 last_generation = 12;
  // Source bytes is the complete active version-two source encoding length.
  uint64 source_bytes = 13;
  // Source hash binds the exact active source bytes and preparation instant.
  bytes source_hash = 14;
  // Pending owns an unselected source until both installation prefixes complete.
  TimezonePendingSource pending = 15;
```
Append to TimezoneSupersededSource (existing tags1..4 remain):
```proto
  // Source bytes determines exact expected raw-page sizes for this owner.
  uint64 source_bytes = 5;
  // Installed source pages retains the prefix present at supersession.
  uint64 installed_source_pages = 6;
  // Source cursor identifies the next raw page to reclaim.
  uint64 source_cursor = 7;
```
New messages:
```proto
message TimezonePendingSource {
  // Definition source ID binds the pending snapshot to one definition owner.
  bytes definition_source_id = 1;
  // Source ID binds this candidate's source hash, generation and semantic rules.
  bytes source_id = 2;
  // Semantic fingerprint identifies exact conversion and loader-cache rules.
  bytes semantic_fingerprint = 3;
  // Generation is reserved monotonically before candidate publication.
  uint64 generation = 4;
  // Header is the fixed prepared timezone header.
  bytes header = 5;
  // Total pages counts compiled transition pages.
  uint32 total_pages = 6;
  // Installed pages is the compiled transition prefix.
  uint32 installed_pages = 7;
  // Source bytes is the exact original serialized source length.
  uint64 source_bytes = 8;
  // Installed source pages is the contiguous raw-source prefix.
  uint64 installed_source_pages = 9;
  // Source hash identifies the exact original input and preparation instant.
  bytes source_hash = 10;
  // Resolved at preserves the original loader-cache seed for partial-source recovery.
  google.protobuf.Timestamp resolved_at = 11;
  // Native initialization year preserves the captured Windows transition window.
  int64 native_initialization_year = 12;
  // Source kind is canonical UTC0, TZif1, or Windows native2.
  uint32 source_kind = 13;
  // Native word bits preserves the source platform arithmetic width.
  uint32 native_word_bits = 14;
}
message TimezoneSourcePage {
  // Source ID identifies the immutable candidate owner.
  bytes source_id = 1;
  // Ordinal identifies this raw-source page.
  uint64 ordinal = 2;
  // Encoded contains at most1024 original source bytes.
  bytes encoded = 3;
}
message SourcePreparationRequest {
  // Definition owner ID fences replacement of the canonical input.
  bytes definition_owner_id = 1;
  // Token advances on every successful response before a successor is scheduled.
  uint64 token = 2;
  // Phase is definition pages1, timezone resolution2, or timezone pages3.
  uint32 phase = 3;
  // Timezone generation is the most recently reserved source generation.
  uint64 timezone_generation = 4;
  // Candidate source ID fences a pending immutable source installation.
  bytes candidate_source_id = 5;
  // Pending identifies the one current source task obligation.
  bool pending = 6;
}
message SourcePreparationTask {
  // Definition owner ID must match the current source request.
  bytes definition_owner_id = 1;
  // Token must match the current pending request.
  uint64 token = 2;
}
```

Root chooses the request field's owning control/tag and task registration. Calendar owner adds the TimezonePreparation.SourcePages DATA map plus isolated service/batch/pending methods; no shared runtime files are edited. Source-write/compile holds are coordinated around root generation.


## Recovery refinement before implementation acceptance

An exact-source owner by itself is insufficient for liveness under finite-cache eviction: unchanged custom data re-resolved at a new wall instant changes serialized ResolvedAt, so a large candidate can be repeatedly superseded before completion. Pending now also records the original resolution instant and native source parameters. Recovery reuses those parameters only while verifying exact SourceHash/owner equality, without assuming current bytes match. This permits progress with more simultaneous owners than cache capacity. Native word width must be retained as well as initialization year so cross-architecture reconstruction cannot silently alter native integer arithmetic. A regression will evict more than1000 owners between each bounded batch and verify stable-source monotonic progress; changed-source recovery must report mismatch with no durable writes.
