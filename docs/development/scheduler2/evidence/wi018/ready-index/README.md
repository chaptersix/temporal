# Exact earliest ready entry

CountBatch.SequenceEarliest(index) returns the minimum pair (chronological CountKey, sequence), preserving the dispatch contract's ready-time priority and sequence fairness among equal times. It does not select the lowest sequence across all due times. The caller compares the returned exact time to its current time; the same result identifies the next future wake. Empty indexes return found=false, and a one-record inline root returns its own time/sequence.

The query reads the outer root and computes its chronological minimum by following the secondary count tree's validated left path to the first bucket key. At every count branch, existing progression, child-range, ordering and aggregate validation runs. At each outer branch, both child minima are computed, the parent's minimum must equal their minimum, and equal times choose the left sequence subtree. The selected inline leaf must match the original chronological minimum. Missing/corrupt referenced pages and inconsistent outer/secondary minima fail and poison the batch. No page/root/refcount encoding or ownership protocol changes.

## Source-derived work bound

A countMinimum call makes one tx.read (at most2 recorded reads, including backing load). It follows at most96 count branches. validateCountBranch reads two immediate children per branch, each at most2 recorded reads: total at most2+96*4=386. Bucket decoding inspects at most16 fixed-key/multiplicity entries, each canonical integer at most10bytes; canonical node size remains at most354bytes. There is no record-count-dependent loop.

The sequence root and root page each use readIndex,2 recorded reads apiece. The initial chronological minimum costs at most386. At most64 outer branches each load two children (4 recorded reads) and compute both minima (2*386). Therefore total Reads<=4+386+64*(4+2*386)=50,054. Tests use the conservative52,000 ceiling (65*800). Inline-leaf minima cost zero secondary reads. Writes, reference releases, canonical hash-input encoding and staged page changes are zero. These are index-local bounds; enclosing Scheduler/framework work and selected request bytes remain separate obligations.

## Evidence

Final routine: CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm/lib/scheduler2/internal/queueindex -count=1 -rapid.checks=100 -rapid.seed=1 -v

PASS2.139s.100 Rapid traces of80 operations compare every staged and committed query with an independent full-map ordering, check exact backing bytes unchanged by queries, and reuse the canonical ownership audit. Other cases cover empty and inline roots, sequence0/MaxUint64, equal-time ties, signed nanosecond extremes and public year1/year9999 timestamps, every backing-read failure with poisoned Commit/no Apply, a validly encoded inline leaf contradicting its parent summary, missing minimum pages, independent count ownership and shared canonical summaries across two generations. A full sequence-depth query measures840 reads; the combined64/96-bit key-depth witness measures12,692, both below the source-derived ceiling. Read metrics are corroborating witnesses, not the derivation.

Scoped required lint passes normal/minimal/vet with zero issues (lint01.log): CGO_ENABLED=0 GOMAXPROCS=4 make lint-code LINT_CODE_TARGETS=./chasm/lib/scheduler2/internal/queueindex GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false. Base revision3e85a5292. Source/dependency hashes are recorded in manifest.json. Root independently reviewed source and ran150Rapid checks with seed18; root owns that log and the actual CHASM adapter publication/reload test evidence.

This is a bounded read capability for the upcoming dispatcher. It does not activate dispatch, change tasks, establish its claim protocol or complete WI018/M3/M4. No schema migration, production routing or remote execution occurred.
