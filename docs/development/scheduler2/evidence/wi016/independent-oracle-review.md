# WI-016 independent spec oracle review

Reviewed final `spec_boundaries_test.go`, its `collect` helper, explicit saved
fixtures and D23/D25/SP7-A–E requirements under testing/copilot guidance. This
reviewer did not author this oracle or its fixtures; the reviewer did author the
separately reviewed production fold repair. No oracle/test/production source was
changed during this review and no tests were run during parent mutation ownership.

## Conclusion

No blocking semantic finding for the stated finite fixture domain. This is a
source/fixture review, not a universal calendar proof or WI-016 acceptance. Parent
owns mutation sensitivity and evidence acceptance. Earlier London diagnosis and
non-author review of the repair remain separate artifacts.

## Independent checks

- Inclusion is independently enumerated on integer UTC seconds using explicit
  structured predicates and interval membership. Fields are conjoined; ranges and
  inclusion expressions are unioned; duplicate matches coalesce because each physical
  second is appended at most once. Exclusion is checked on local wall fields after
  collecting raw inclusion matches. Expected results do not call canonicalization,
  compiled matcher next-search, NextTimeCandidate or the production enumerator.
- The first scanned integer instant is strictly after the possibly fractional
  cursor. StartTime/EndTime filter nominal instants inclusively. UTC stepping and
  pinned UTC-to-local conversion naturally retain both repeated-hour copies and
  skip nonexistent local instants. New York/London literal cases independently
  constrain conversion. Generated replay timestamps are UTC; arbitrary offset-form
  serialized timestamps are not part of the exercised encoding/jitter domain.
- Let C=min(floor(jitter/ms),2^32-1)ms. For an in-range nominal N<=EndTime, a next
  raw match after EndTime+C is at least C after N and cannot lower the jitter cap.
  Therefore collecting raw candidates through EndTime+C is sufficient. The raw
  list intentionally retains excluded candidates and candidates beyond nominal
  EndTime for lookahead. Computing the cap before comparing the integer-second
  next-raw gap is equivalent to production's min-before-quantization on this domain.
  The 12-hour bound explicitly rejects costly reference scans; it is not a silent
  truncation of expectations. The sparse multi-month case uses an independent
  calendar literal instead of pretending this bounded scanner covers that horizon.
- Jitter arithmetic uses the shared time binary encoding and Farm32 primitive,
  with a separate integer multiply/right-shift implementation. Independent integer
  arithmetic confirmed hash492042837 gives 114ms at jitter999/1000ms, zero at1ms,
  and492042836ms at caps2^32-2 and2^32-1, i.e. January6 16:40:42.836UTC from January1.
  The literal cap tests intentionally bypass the 12-hour reference domain. They
  do not establish Farm32 correctness; that shared primitive remains explicit.
- `collect` marshals/unmarshals each returned continuation, checks input immutability,
  enforces per-call probe/output bounds, and rejects failure to settle within4096
  calls. The property therefore exercises serialized resumptions, not only in-memory
  continuation. Each sorted partition compares accumulated output to the independent
  due prefix; future pending checks full-state zero-probe Waiting stutter. The final
  whole-window comparison checks complete settled state and status as well as outputs.
  It does not claim identical intermediate stops across different resource partitions.
- Five generated families cover pinned NY gap/fold, pinned London fold, UTC leap day
  and NY local year boundary. Date/DOW/hour restrictions, stepped seconds/minutes,
  duplicate/mixed interval inclusion, local exclusion, fractionally offset bounds,
  jitter choices and nominal/actual/boundary ±1ns cuts are explicit finite choices.
  The final window exceeds every generated actual horizon. Sparse all-excluded work
  still exercises bounded probes via the existing collector; reference errors fail
  directly rather than skipping draws. This is sampled bounded coverage, not an
  exhaustive product of every timezone/calendar field or historical transition.
- Direct Rapid failure assertions emit complete explicit scenarios after shrinking.
  The saved NY and minimized London fixtures contain their specs/TZif bytes, revision,
  seed, cursor, window budgets and expected results; standalone replay does not need
  the generator seed. Replay recomputes the independent reference and rejects any
  mismatch with saved expectations before comparing production, then repeats from
  fresh state twice. Parent evidence must distinguish the selected fixture path:
  the default saved replay runs NY; the London regression is selected explicitly.

## Concrete nonblocking hardening finding

`TestSpecBoundarySavedReplay` rejects unknown fields but decodes only one JSON value;
it does not require EOF afterward. Thus valid JSON followed by another JSON object
or trailing junk is silently ignored. Checked-in fixtures are complete single
objects and their hashes are recorded below, so this does not invalidate the observed
runs or oracle comparisons. A follow-up EOF assertion would make the standalone
replay reader reject ambiguous/truncated extraction artifacts consistently with the
WI-013 reader. No source edit was made during the parent's mutation window.

## Evidence limits and fingerprints

Routine/exploration/lint results are owned and archived by proof_review/parent; this
review does not rerun or rebrand them. In particular the10000 seed29 exploration used
the documented pre-style-cleanup binary; final-source routine/lint evidence is separate.
Timezone conversion, time encoding and Farm32 remain trusted primitives; sparse literals
cover specific dates only. Neither production persistence/queue delivery nor frontend
semantics are established by these pure occurrence tests.

```json
{
  "chasm/lib/scheduler2/internal/occurrence/spec_boundaries_test.go": "4de5f547b0402f67149696620774341337c5c8c39b2e797ce5d1f70e4f6955ff",
  "chasm/lib/scheduler2/internal/occurrence/occurrence_test.go": "ed11486cbb44f6a29222bd1dbf769cf72eaa07aa942aa0b273807895f2dcee81",
  "chasm/lib/scheduler2/internal/occurrence/testdata/spec_boundaries_v1.json": "e0cbb7e330cb0e6aebda12d2dac72eb7bf630bd7d3004f07f175d33e2d832c1e",
  "chasm/lib/scheduler2/internal/occurrence/testdata/spec_boundaries_fold_regression_v1.json": "eea3d80d23e435fce51839f6eed646ef31e55a8abab091171e51aed2f0ca9349",
  "chasm/lib/scheduler2/internal/occurrence/testdata/spec_boundaries.md": "3046e63d383bdf5253021dd6724e532b65d0a1d419cfed48a03b661181873c05",
  "chasm/lib/scheduler2/internal/occurrence/testdata/Europe_London.tzif": "c85495070dca42687df6a1c3ee780a27cbcb82f1844750ea6f642833a44d29b4",
  "chasm/lib/scheduler2/internal/occurrence/testdata/America_New_York.tzif": "e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95"
}
```
