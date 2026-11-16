# WI-016 spec boundary fixtures

`America_New_York.tzif` is the pre-existing byte-pinned fixture; see README.md for
its unknown originating release. Its digest remains
`e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95`.

`Europe_London.tzif` was copied locally from
`/usr/share/zoneinfo/Europe/London` on 2026-11-11. The adjacent local
`/usr/share/zoneinfo/+VERSION` reports `2026c`. SHA256:
`c85495070dca42687df6a1c3ee780a27cbcb82f1844750ea6f642833a44d29b4`.
Tests embed and verify the exact bytes; no host-zone lookup or network is needed.
Go's TZif conversion, time binary encoding and Farm32 remain trusted primitives.

`spec_boundaries_v1.json` is a standalone pinned New York fold fixture with literal
UTC05:30/06:30 expectations and explicit partition/budget sequence. It contains the
complete structured spec and timezone bytes, seed, revision, cursor, expected
occurrences and schema/generator version. Replay twice without a generator:

```
CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2/internal/occurrence -run '^TestSpecBoundarySavedReplay$' -count=1
```

`spec_boundaries_fold_regression_v1.json` preserves the minimized explicit scenario
from the first London-inclusive routine run (100 cases, Rapid seed1). The initial
unmodified production search skipped first-copy London fold matches after
2024-10-27T00:00:00Z. Original failure logs are
`/private/tmp/scheduler2-wi016-routine02.log`,
`/private/tmp/scheduler2-wi016-london-golden-initial.log` and
`/private/tmp/scheduler2-wi016-fold-standalone-initial.log`; parent owns durable
ledger/evidence copies and the production fix. A smaller literal witness is
`TestSpecBoundaryLondonFoldGolden`: local01:00:30 has UTC00:00:30BST and
UTC01:00:30GMT, and cursorUTC00:00 must not skip the first. The test first asserts
both literal UTC/local mappings independently of production next-time search.

```
SCHEDULER2_SPEC_REPLAY=testdata/spec_boundaries_fold_regression_v1.json CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2/internal/occurrence -run '^TestSpecBoundarySavedReplay$' -count=1
```

Rapid's supplemental `.fail` data is preserved under testdata/rapid. Failures print
`SPEC_BOUNDARY_REPLAY=` followed by the complete JSON scenario; the final minimized
scenario can be saved and passed to the environment-selected replay above. Seed
alone is not required for replay. Expected output is independently computed by the
bounded predicate oracle; timezone literal cases additionally constrain shared
conversion risk.

The oracle accepts only explicit finite bounds, populated structured fields and
whole-second intervals. Its scan extends to EndTime plus the effective millisecond
jitter cap: any later raw match cannot lower an in-range candidate's jitter bound.
Raw matches beyond nominal EndTime and excluded raw matches still bound jitter.
The test-only scanner rejects scans exceeding12hours; the sparse multi-month London
fixture uses literal expected instants instead. Routine01 before adding London
passed100 generated cases in1.053s. Routine02 failed and shrank a product discrepancy. After the separately reviewed
calendar fix, routine100 seed1 passed in1.915s, exploration1000 seed17 in9.090s,
and exploration10000 seed29 in57.090s. Full occurrence package final source
routine100 seed1 passed in1.049s. Initial failures remain preserved. Final tagged
make lint-code-fast (FIX=false) passed with zero issues. Parent owns mutation
sensitivity, independent oracle review and final acceptance.
