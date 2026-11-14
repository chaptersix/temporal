# Native Windows timezone capability checkpoint

The approved native representation is implemented and frozen in manifest.json (15 owned source/test files). Root owns the additive WindowsNativeData field5 and shared generated schema; schema-dependency.json preserves the current integration dependency separately from the owned freeze. No runtime scheduler consumer or routing was added.

## Exact behavior and compatibility

WindowsTimezoneSource captures all logical Timezoneinformation fields, source native word width and explicit initialization year in182 bytes. GetTimeZoneInformation is called only by a Windows-build-tagged outside-mutation provider. API error becomes UTC, matching Go. Valid native input uses the deterministic pure constructor, retaining source order even when extreme bias offsets make transitions unsorted. WordBits32/64 controls negation/multiplication overflow; Bias+StandardBias and Bias+DaylightBias first add as int32. No-DST ignores StandardBias. DST emits exactly400 transitions over initializationYear-100 through initializationYear+99, no extension or seeded cache; fixed mode emits one alpha transition/cache interval. Native source names and ignored native date fields are retained in identity but are not exposed as a formatted time.Location.

The shared private evaluator now stores first/cache/transition offsets and returns lookup offsets as int64. TZif parsing still sign-extends exactly signed32 input, and POSIX grammar offsets remain bounded ints converted to int64 at the lookup boundary. Existing TZif fingerprints are unchanged. Format1 header89bytes and page38+12*N bytes are preserved. Pre-change baseline SHA256 over complete header+all pages (including semantic fingerprint) is pinned for UTC, New York and London at explicit1704067200 seed; post-change tests match exactly. Existing complete TZif/calendar tests also pass.

Native format2 header is70 bytes; pages are38+16*N, maximum294bytes. Maximum generic header remains89; format1 page maximum remains separately asserted230. Native formats validate count1/fixed cache or count400/absent cache, format/fingerprint/ordinal/count at every addressed page. The same <=66-read lookup ceiling remains; source comparison covers unsorted extreme tables without sorting. Normal writer page wrapper maximum is337bytes; control/cleanup descriptor maxima remain251/137, excluding CHASM node/path/unknown-field overhead.

Typed source snapshots distinguish UTC/TZif/native explicitly; absent TZif bytes never cause native Local to become UTC. SourceHash hashes the native frame for native snapshots. A shared environment captures native Local once; builder24h cache expiry and new builders retain that same source/year/identity. The original captured instant and source frame are persisted separately from fixed hot control. StageResolvedCalendarTimezone/StageCalendarTimezoneSnapshot stage or reconstruct all variants outside mutation; zero snapshots, conflicting variants and missing timestamps fail at the boundary. Definition binding and positive generation remain separate from semantic identity.

## Verification

- Before width edits, V1 baseline capture PASS0.639s; golden hashes retained in native_compat_test.go and baseline log.
- Full occurrence/source/scheduler2 run PASS1.097s/0.641s/21.958s. Final directed run including source-boundary hardening PASS1.102s/1.184s/0.686s. Final source-reference test-format correction PASS0.650s.
- Scoped required lint02 occurrence+timezonesource PASS normal/minimal/vet zero issues. All new files intent-to-add. Initial combined lint found a test-only day-zero time.Date style issue (changed reference last-day calculation to first-next-month.AddDate(-1day)) and a concurrent projection helper complexity finding owned/fixed by framework. Parent combined acceptance covers scheduler2 staging/component alongside coherent projection source.
- Windows amd64 source-provider/test-package cross-build PASS; no physical Windows execution claimed.
- Native north/south exact transition instants, both ends of the200year window, fifth-week correction, no-DST ignored bias and signed32 sum/native32/native64 arithmetic extremes are directed.
- Source-derived Go Windows transition and Location.lookup reference checks every emitted transition and every transition-1/exact/+1 instant plus integer endpoints on an intentionally unsorted extreme-offset table. Exact offset/start/end triples and bounded page reads agree. The reference is preserved with original Go source hashes/copies.
- Date parity uses Go LoadLocationFromTZData with source-derived sorted native transitions for ordinary native offsets; wide fixed offsets compare public time.FixedZone. Known Pacific first/second fold and spring skipped-hour candidates are explicit UTC witnesses.
- Native source/codec round trips; every header truncation, addressed read failure and format mismatch; every addressed reader fault during a64step calendar quantum restores its original noninitial continuation.
- Actual CHASM serialization/reload across native25-page installation, SourceSnapshot reconstruction with original definition ID, poisoned Source isolation, failed-publication cleanup replay and bounded two-batch reclamation to a UTC successor pass. All resolved input variants reconstruct exact header/owner identity.

## Evidence limits and next integration

This is a standalone capability, not shared server-startup wiring. The source environment must be shared and its explicit immutable-environment/initialization boundary established before adoption; it cannot reconstruct arbitrary previously initialized stdlib Local history. Portable source-derived fixtures and Windows cross-compilation are scoped local evidence; real Windows API differential remains conditional additional platform evidence, not a newly imposed blanket local-completion gate.

The native frame is owned internal input, not a new public timezone API. Unsafe injected last-week month records that would panic in Go are rejected; other civil values that Go normalizes are not broadly rejected (including a non-last-week month13 fixture). Custom TZif accepted input ranges and allocation safeguards remain unchanged. All source preparation is outside mutation and input-proportional.

Default preparation-component framework traversal remains. Candidate persistence, incremental union/exclusion/jitter, shared provider/startup wiring, full bounded-layout integration and public runtime acceptance remain open. This checkpoint does not close WI018, M3 or M4.

## Root timestamp reconstruction correction

Root review found that protobuf Timestamp.AsTime normalizes malformed stored seconds/nanos. Source reconstruction now calls ResolvedAt.CheckValid before dispatching any source variant. Common outside-mutation staging likewise validates timestamppb.New(resolvedAt) before publishing an immutable stage, so later persistence cannot silently store an unrepresentable preparation instant. This is the internal preparation metadata's actual protobuf representation contract; pure TZif/native conversion and accepted custom timezone byte semantics are unchanged.

Directed tests reject negative/1e9 nanos and seconds just outside both protobuf range limits for UTC, TZif and native snapshots. Direct TZif/native staging rejects unrepresentable instants; valid year1 and year9999 endpoints survive exact nanosecond round trips. Final timestamp02 passes0.674s; initial timestamp01 test-import compile typo is retained as failed evidence. Gofmt/import formatting and diff-check pass. Parent final combined lint covers the changed scheduler2 files after coherent effect/projection integration; unchanged occurrence/source lint02 remains valid. Manifest/final-source now identify this corrected freeze.
