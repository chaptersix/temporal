# Prepared timezone implementation and evidence

This checkpoint implements approved steps1–3 of prepared-timezone-contract.md: custom TZif preparation, parsed POSIX rules, fixed16-entry transition pages, offset lookup and UTC-only wall/instant conversion, and conversion integration into the unselected pure calendar evaluator. Source hashes are in manifest.json. Go1.27.3 parser/rule/date semantics are the source reference; adapted grammar/rule code has BSD attribution and prepared_timezone_license.txt. This is not named resolution, persisted page installation or scheduler runtime adoption.

## Implementation boundaries

Preparation preflights advertised counts against available input using uint64 byte totals before allocating zone/transition arrays. It preserves accepted positional unsorted/duplicate transitions, signed32-bit offsets, first-zone precedence and synthetic empty-transition behavior. It parses extension text outside mutation and preserves invalid-extension fallback, default rules, quoted names, Julian/DOY/month-week-weekday forms, signed rule times, southern swaps and reference returned bounds. Names/flags used only during preparation are not read by hot conversion.

The loader cache is seeded from an explicit resolvedAt argument. It uses the first adjacent interval containing that instant, including extension substitution. The immutable snapshot retains its fixed cache tuple. Fingerprint covers exact source bytes plus this observable tuple and a representation version; therefore it is stronger than minimal semantic identity and may distinguish source-byte differences with identical outputs. It never treats a name hash as resolved rules. PreparedLocationFromTZData also includes the name. No production time.Now call, execution-clock substitution, reflection, unsafe access or time.Location fallback remains in prepared-calendar evaluation.

PreparedTimezone owns its private page array; caller input is not retained. The test-injected reader exercises addressed errors and page identity/ordinal/length validation. The eventual persisted adapter must reproduce immutable source ownership and fixed-size pages. This implementation does not claim a full validator for forged semantic page contents under a matching trusted identity. Actual persistence encoding/byte budgets and durable page-installation/cleanup remain next gates.

Date conversion preserves Go's wall-as-UTC initial lookup and optional second lookup if adjusted UTC lies outside the first interval. Field extraction performs one indexed offset lookup then UTC-only arithmetic. The corrected one-hour fold helper compares the exact local civil fields and retains the reference's one-hour behavior. Local-year2100 exhaustion remains distinct from the future public wrapper's UTC-year limit.

## Bounds

An offset lookup performs at most66 addressed page reads: first/selected entry plus at most64 binary decisions. The count is a conservative machine-width bound, not a new transition-count restriction; the current TZif uint32 count gives a tighter bound. Each page has16 fixed entries and identity/ordinal/length metadata. Rule evaluation has no input text loops; its one week loop has at most4 iterations.

One calendar phase performs at most6 adapter conversions and8 offset lookups, hence at most528 page reads. A maximum64-step call has at most33792 page reads and touches at most16 fixed entry slots per read. Exact counters report actual conversions/lookups/page reads, including failing accesses. These are metadata-work bounds, not latency claims or yet a serialized CHASM-byte bound. Page reader implementations must preserve fixed lookup cost; the current direct private array does so.

## Tests and failures

Final full occurrence command: CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm/lib/scheduler2/internal/occurrence -count=1. Full04 PASS1.115s. Scoped make lint-code normal/minimal/vet passes with zero issues (lint02). Directed count log reports N4/5/1000/10000 page reads under the same ceiling.

Tests compare offsets, wall fields and exact Date instants to Go across NY/London years1900..2168, including negative Unix epochs. Custom cases cover signed32 offset extremes, all four first-zone choices, empty transitions, duplicates, v1/v2/v3, extension grammar/invalid fallback/southern rules, and truncation rejection before allocation. Known fold/gap UTC instants and explicit primitive/public horizon witnesses continue to pass. Extreme offsets produce real calendar candidates even when local year is below2000.

The crafted unsorted fixture proves the loader cache can return offset0 while binary lookup without that cache returns7200 for the same instant. Its chosen preparation instant is explicit. Comparison to a separately loaded Go location uses a known interval far from the current loader sample; this is not control over Go's hidden runtimeNow. Reload must retain the prepared cache; independent resolution may legitimately change fingerprint.

Every addressed page read in a multi-step calendar advance is failed in turn; the original noninitial continuation is returned unchanged. Missing or mismatched page metadata also fails. A nonexistent New York wall time explicitly requires Date's second offset lookup. Existing JSON continuation roundtrips and budget partition equality cover every calendar phase.

Historical build01/full01 include test-author setup failures (full01 missed schedulepb import). Lint01 identified copied-source style and a time.Now boundary violation; the constructor now requires explicit resolvedAt, and helpers were extracted without changing grammar. These logs are retained. No observed semantic mismatch remains in the covered matrix, but arbitrary accepted TZif parity is source-derived plus targeted differential evidence, not exhaustive proof.

## Remaining gates

Independent review; persisted fixed timezone page/header installation and cleanup; complete named resolver source-order/Local/TTL behavior; union/exclusion/list-preview integration; full scheduler transaction and physical recovery gates. Existing public runtime is unchanged. WI018/M3 and M4 remain open.
