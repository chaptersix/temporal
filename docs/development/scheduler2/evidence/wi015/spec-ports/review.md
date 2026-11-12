# Public spec parity and definition-size profiles

Four standalone scheduler2 ports preserve the original CHASM scenario stimuli and assertions: mirrored include/exclude on Create and Update (configured1000 iteration limit), far-future interval preview and matching queries, and50 calendars plus50 exclusions with real workflow starts and advancing recent/future projections. Original tests are unchanged. The port changes are entry point, explicit scheduler2 backend/context, root parallelization, and use of t.Context instead of the deprecated environment context. Intentional legacy ExcludeCalendar field inputs retain a narrowly documented lint suppression. No skips, shortened scenarios, changed expected results, or fallback.

The initial run failed only ManyCalendars: occurrence.New rejected100 combined terms under the internal64-term prototype profile. The fixed run passes all four. The spec test retains its original public ListMatchingTimes assertion that the excluded minute has no matches.

## Reconciliation

The internal occurrence.New profile remains64KiB/64terms/1024ranges; existing boundary tests still assert its rejection behavior. NewPublic uses the same immutable canonicalization, semantic/range validation, deterministic identity, candidate generation, continuation checks, and scan budgets, but does not impose an extra definition-size ceiling. Public Create selects this constructor. ConfigureGeneration and automatic/backfill reconstruction select it from persisted PublicRevision, so a valid larger definition is not accepted at creation only to fail after reconstruction. Internal verification scenarios continue selecting New.

Both profiles retain the existing representable execution domain2000 through2100 inclusive, probe maximum1024, output maximum128, and negative/invalid-time checks. This change does not claim the full protobuf timestamp domain. It separates definition cardinality/byte acceptance from per-call candidate budgets. A single raw lookup costs work proportional to the accepted terms/ranges; bounded probes do not imply a fixed CPU bound independent of public spec size.

No frontend validation was removed. Existing semantic validation, workflow ID/action checks, search-attribute limits, memo/input blob limits, transport enforcement and mutable-state accounting remain. Source inspection confirms validateSchedulePayloadSize measures memo+action input, not serialized spec bytes: there is no claim that namespace BlobSizeLimit itself caps calendar definitions. No new guessed fixed definition limit is introduced. Larger arbitrarily distinct definitions or payloads can still exceed configured transport/persistence constraints; this evidence is not an aggregate-capacity guarantee.

The added boundary tests show the public profile accepts100terms,1025ranges and valid unknown protobuf data exceeding64KiB while the internal profile rejects each; input protos remain unchanged. One-probe/one-output execution emits the exact first candidate without duplicates,1025probe budgets still reject, and malformed interval/jitter/excluded range inputs still reject. Public/generation/backfill regression tests pass.

## Verification limits

Existing Lean kernel/public continuation theorems do not derive calendar parsing correctness from spec cardinality. Their algebraic continuation obligations are unchanged by this constructor selection. Existing Go/Lean correspondence fixtures still compile through the bounded internal New profile and are not evidence for arbitrary larger public specs. The new functional and boundary tests provide concrete larger-definition witnesses only. WI013 source fingerprints must be treated as historical snapshots; no claim of having rerun the Lean oracle or mutation profiles on this changed source is made here.

Exact commands, result durations and source hashes are in manifest.json. Initial failure, fixed pass and regression logs are retained separately. Parent owns non-author source review, consolidated lint and milestone acceptance; central plan and ledger were not edited.

Final post-lint functional rerun passes9.374s; definition-boundary rerun passes0.628s. These logs are retained as final-functional.log and final-limits.log. No production behavior changed after the previous passing run.
