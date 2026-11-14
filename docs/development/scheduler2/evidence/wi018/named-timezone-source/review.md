# Owned timezone source checkpoint

Approved design steps1–2 implemented in internal/timezonesource, with no runtime caller. Frozen source and exact compressed copies are in manifest.json/final-source. No prepared timezone offset, persisted component or schema files were changed in this slice.

## Behavior

The checked-in408,467-byte stored zip comes from the actual Go1.27.3 time/tzdata embedded constant, not an assumed GOROOT fallback. SHA256 b2d18a7c8fa8142097a48c99609fb3c92db5ee98bc740294e57eab8ae9f94779,598 entries. go:generate uses Go AST parsing/strconv.Unquote to reproduce the asset/provenance. Tests require byte equality with the build toolchain asset and fixed reviewed loader-source fingerprints. The trusted asset has an immutable indexed reader that returns detached bytes. Generated/source provenance is explicit; no unsafe, linkname or private runtime access exists.

The new isolated source environment preserves actual named override/platform/embedded/GOROOT ordering and error precedence. It retains Go's unusual ignored readable-invalid override parser error, while platform parse errors remain eligible first errors. Invalid name rejection and empty/UTC shortcuts follow Go. A shared Environment owns one override capture and one Unix Local snapshot; each Resolver owns the existing common/cache capacity1000/TTL24hour cache including negative results. Exact expiry is inherited (strictly after creation+TTL), not sliding expiry. Nonnil custom bytes bypass named caching, including empty bytes as invalid input. Snapshot data is detached and preparation samples an explicit outside-mutation instant; cache reuse retains that instant and prepared identity.

The filesystem provider preserves Go's directory source10MiB limit and stored ZIP lookup: first matching duplicate, no CRC enforcement, unsupported compression error, exact local header/central directory fields, no comment-bearing tail shortcut. Allocations are preflighted against available bytes and host integer representation. Malformed ZIPs that Go's private reader might panic on return corrupt ZIP errors; no valid TZif/public transition cap was added.

All source resolution/preparation/cache work is outside CHASM mutations and input-proportional. The prepared persisted capability supplies the separate bounded evaluation path. This source package has no tasks, scheduling, coordinator routing or database writes.

## Verification

- Final full source package tests PASS0.701s, generator command package builds.
- All598 owned entries byte-compare against the toolchain's actual asset; detached returned bytes cannot mutate future reads; all598 names resolve/prepare successfully.
- Injected conflicting sources prove ordering through owned embedded before GOROOT; absent/corrupt/read-failed source cases prove error precedence.
- Exact24hour boundary then1ns expiry, changed success snapshot/seed, negative-result refresh,1000-entry eviction, once-captured override, custom bypass, malformed empty custom, special names, and all Unix Local branches are covered.
- Real cold subprocess comparisons with time.LoadLocation (time/tzdata imported as in server main): directory and stored ZIP overrides, readable invalid override, unsupported ZIP compression, absolute Local path and empty Local. Successful cases assert five actual offsets spanning1900, both2024 NY fold instants, spring transition and2050. These prove source choice/semantic offset witnesses; they do not assert equality of independently sampled hidden Go loader caches.
- ZIP tests preserve first duplicate/ignored CRC, reject every prefix truncation and oversized advertised central-directory allocation, and exercise actual files/error extraction/size behavior.
- Windows amd64 test-package cross-build PASS with final production source (before the final all-entry test addition); no Windows execution or native Local semantics claim.
- go generate rerun PASS with unchanged owned asset/provenance.
- Final scoped required make lint-code PASS normal/minimal/vet zero issues. Scope internal/timezonesource/..., baseHEAD, fixes disabled; all new files intent-to-add before diff-filtered lint. Deliberate runtime.GOROOT uses carry narrow documented staticcheck suppression because exact Go runtime fallback/build-toolchain verification is the contract, not lookup of an unrelated go binary on PATH.

Earlier failures are archived: initial compilation raced the coordinated proto regeneration and saw temporarily absent generated API packages. First lint found unchecked read-only closes, runtime.GOROOT deprecation, missing switch default and a test assertion style issue; fixed or narrowly documented. Earlier preliminary parent lint saw generator complexity/import findings before the completed implementation; extraction/formatting fixed them.

## Acceptance boundary

Independent source review and parent checkpoint remain. Runtime provider initialization must establish the explicitly documented process-immutable TZ/ZONEINFO assumption and shared builder ownership; this does not reconstruct arbitrary earlier standard-library Local/ZONEINFO history. Unix source capture is not a Windows Local implementation. The approved next native provider uses supported GetTimeZoneInformation, portable deterministic native fixtures and versioned int64 offset source/codec, retaining old format1 decoding; cross-platform local evidence must be reported with its limits. No user-facing rejection or forcedUTC substitute for Windows was introduced: this package remains unselected.

Native Windows preparation, shared startup wiring, union/exclusion/jitter continuations, persisted candidate progression and full runtime bounds remain open. WI018/M3/M4 are not closed by this checkpoint.

## Root review correction and final freeze

Root found that an empty runtime.GOROOT must disable fallback, while the original Unix provider appended the suffix unconditionally. The new pure gorootSource helper returns an empty path for empty input and appends only for nonempty input. A directed test asserts both cases and verifies zero source-reader calls for an environment with no fallback. This mirrors Go gorootZoneSource rather than probing /lib/time/zoneinfo.zip.

Final corrected full package suite04 PASS0.714s; final scoped lint04 normal/minimal/vet PASS zero issues. Both archived. manifest.json and final-source copies now identify this corrected freeze; earlier03 evidence remains historical. No runtime routing or native Windows behavior changed.
