# Native Windows prepared timezone representation

Status: approved implementation and scoped verification complete; frozen owned source/evidence is in native-timezone/. Root owns shared schema acceptance and runtime integration. No production routing has been added.

## Source and semantic scope

Reference: reviewed Go1.27.3 src/time/zoneinfo_windows.go, src/time/zoneinfo.go and src/time/time.go. The existing owned-bundle audit captures source hashes. Windows named zones have no platform paths and use override, embedded, then nonempty GOROOT. Only Local needs this native representation. Windows Local uses exported syscall.GetTimeZoneInformation, not Unix TZ or IANA historical reconstruction. API failure becomes canonical UTC, matching Go.

Capture the native API result and an explicit initialization UTC year once in the shared source environment. The same environment supplies every prepared builder; expiration of a builder's24hour named cache does not recapture Local. The source provider samples its clock outside mutation after API capture, and retains the instant/year with the native record. Comparing independently initialized standard-library Local near a year boundary cannot assume identical hidden Now samples. Portable tests inject the exact native record/year. The documented process-immutable environment/shared startup assumption and prior-stdlib-history limits remain unchanged.

## Deterministic native input

A new versioned native source encoding contains:

-1 byte source-format version;
-1 byte source native word width (32 or64);
-8 byte signed initialization year;
-172 bytes corresponding to all logical Timezoneinformation fields in explicit byte order, without Go struct padding: Bias int32, StandardName32 UTF16 units, StandardDate8 uint16 fields, StandardBias int32, DaylightName32 UTF16 units, DaylightDate8 uint16 fields, DaylightBias int32.

Total182 bytes. A pure constructor decodes this owned input and derives normalized prepared semantics. Runtime capture is in a Windows-build-tagged file; source structs and deterministic conversion tests compile on Linux/Darwin/Windows. Names, Systemtime.Year and Milliseconds remain in source identity even where Go calendar conversion ignores them. No private Windows registry API is needed for offset semantics: Go's registry work only selects display abbreviations, which this private calendar capability does not expose. This does not claim equivalent formatted time.Location names.

Record source native word width to preserve arithmetic when restoring on a different-width host. Bias+StandardBias and Bias+DaylightBias use signed32bit modular addition first, as Go's field types require. Then convert to source native int, negate and multiply by60 using that native width. Persist/compute the resulting signed64bit value. On64bit source hosts the extrema can approach128,849,018,880 seconds, so signed32 TZif offsets cannot represent the native source generally. On32bit hosts the later arithmetic wraps; do not recompute with the replay host width.

## Exact native transformation

If StandardDate.Month==0, ignore StandardBias entirely; use -int(Bias)*60. Produce one alpha transition and standard before-first offset. Cache presence is true with [alpha,omega) and the same offset, exactly as Go.

Otherwise create standard and daylight zones with their native arithmetic offsets. Order the dates by Month, swapping indices when StandardDate.Month > DaylightDate.Month. Produce exactly400 transitions: two for each year initializationYear-100 through initializationYear+99. Each transition subtracts the pre-transition zone's offset from the pseudo-Unix civil timestamp. The pseudo-Unix calculation uses Go UTC Date normalization, weekday difference and its fixed first/fifth-week handling; year and millisecond Systemtime fields are ignored. For normal native records the before-first choice is standard offset: zone0 is used and is the first non-DST zone. Preserve emitted order rather than sorting in response to extreme offsets.

There is no POSIX extension and no seeded cache for the DST case. Before the first transition use standard offset; after the final transition retain the final zone indefinitely. Southern rules therefore retain daylight after their final yearly pair, rather than extrapolating an IANA rule. These boundary differences are observable and require directed tests. Malformed injected Systemtime records that would panic inside Go are not proof of accepted public timezone behavior; the provider receives native API records, while the pure decoder must reject unsafe structural values with an internal/source error rather than panic.

## Shared evaluator and codec widths

Widen private transition.offset, first, cache.offset and lookup result to int64. wall/date already perform int64 offset arithmetic; audit every conversion, hash and rules lookup boundary. TZif parsing still consumes signed32 offsets and its existing accepted byte/transition domain remains unchanged. POSIX grammar retains current bounded offsets and converts into the wider evaluator without narrowing. Cache fingerprints already encode offsets in64bits; preserve all old TZif semantic fingerprints.

Keep existing format1 header89bytes and page38+12*N bytes unchanged for TZif/UTC snapshots, including their decoders. Add a native-only format2 with header70bytes: version1 + fingerprint32 + transition count4 + first offset8 + cache presence1 + cache start8 + cache end8 + cache offset8. Native format2 has no extension fields. Format2 pages are version1 + fingerprint32 + ordinal4 + count1 + N*(instant8+offset8): at most294bytes for16 entries. Native transition count is1 or400, hence1 or25 pages; validate the native shape/cache mode when restoring. This is a format-specific invariant, not a cap on custom TZif input.

Add private source/codec kind to PreparedTimezone so Header/Page re-emit the correct version after restoration. Existing zero-value kind uses format1, preserving current PreparedUTC. Header decoding dispatches by version and exact length; addressed page decoding requires the matching header's format, fingerprint, ordinal and expected count. The generic maximum header remains89bytes; maximum page becomes294bytes while a dedicated format1 maximum remains230. Normal-writer page wrapper maximum becomes337bytes (source ID34 + ordinal6 + encoded length/tag297); control and cleanup descriptor maxima remain251/137 because native headers are smaller. Recalculate any instrumentation/fixtures that use a single page maximum and retain exact format1 assertions.

Fingerprint native semantics using a distinct versioned domain plus deterministic native source frame and derived fixed cache/transition semantics, so source algorithm and arithmetic width are explicit. No reinterpretation of native bytes as custom TZif and no source-name-only fingerprint.

## Source/staging/persistence integration

Snapshot needs an explicit source kind: canonical UTC, TZif, or native Windows. Current UTC inference from absent TZif bytes must not classify native Local as UTC. Provide detached native input access separately; existing TZif Data accessor remains exact for that kind. The shared environment's Local provider is platform-specific: Unix source search remains unchanged; Windows invokes the API capture and pure native preparation once. No platform falls back to the other's source algorithm.

Add an outside-mutation staging constructor for the native source or a typed resolved-snapshot constructor that handles all three source kinds. It still binds definition ID, positive owner generation and semantic fingerprint independently. Extend TimezonePreparationSource additively with native source bytes/source-kind discrimination; retain existing TZif fields and UTC meaning for already written local snapshots. The native frame and original capturedAt belong to addressed Source DATA. capturedAt must be representable by protobuf Timestamp; staging checks that representation, and persisted reconstruction rejects invalid seconds/nanos before AsTime normalization. Fixed header/control and bounded install/cleanup logic remain generic after codec dispatch. SourceSnapshot must return the original definition identity and exact variant for reconstruction. Coordinate any proto edits/generation with root and framework ownership.

## Required local verification before adoption

Portable injected native fixtures: no-DST StandardBias ignored; northern/southern transition pairs; first/last-window behavior; fifth-week correction; exact transition instants; both32/64-bit arithmetic, signed32 bias-add overflow and offset seconds beyond signed32; source year/word-width identity; codec1 byte-for-byte stability; codec2 round trips and every addressed malformed/missing page; restored-reader error rollback; native install/reload/cleanup and source reconstruction.

Windows-only syscall capture must compile under the repository Windows target. Cross-build and source-derived fixtures establish scoped local evidence, not physical Windows API execution. A real Windows subprocess differential is additional conditional platform evidence, not a new blanket local-completion blocker beyond the plan. Shared startup wiring, existing stdlib Local history limits, union/exclusion/jitter continuation integration and actual full-transaction work measurements remain explicit runtime gates. No production routing is enabled by this design.
