# Independent shared-payload storage review

Non-author source review of the API owner's payload representation and integration; the reviewer authored earlier byte-ceiling and public projection changes, which are not independently approved here. Read the full new storage implementation and callers, comparing original inline request/carryover semantics. No blocking source defect found at this snapshot. Final expanded tests and real capacity run remain owner/parent validation obligations.

## Identity, reads and ownership

Blob IDs include semantic kind plus SHA-256 of stored bytes. Preparation compares bytes if an ID already exists; reads verify both digest and expected kind, reject missing references, and decode into fresh protobuf messages. Public definitions and public request/generator skeletons reject missing action references rather than falling back to silently empty inline data. A present failure without its required reference also rejects. Carryover result references decode exactly one Payload. Missing reference fields which themselves encode optional absence are distinguishable only where an independent presence flag exists; the format does not claim a general corruption-detection theorem for arbitrary lost optional metadata.

Public action skeletons retain all unsplit fields, including unknown envelope bytes. Payload-bearing fields move together into one immutable action bundle. Generated scheduled-by/time attributes stay in the request skeleton and override user fields at assembly; candidate bundle reuse normalizes those generated overrides only for comparison. Decoding and cloning keep assembled request/public messages detached from persisted DATA. Nil vs present-empty message fields are represented by protobuf field presence in the action bundle, not inferred from payload length.

## Transaction and generation lifetime

Admission freezes the request, computes request/carryover blobs and completes successor/retention validation before publishing blobs/control/DATA/tasks. Generator replacement prepares its skeleton/blob before publication. Public update may publish tentative generation before a later visibility/definition error, but it is inside the framework transaction: rollback must discard all tentative state and the request-ID record together. The explicit failed-publication test compares complete durable nodes and verifies no dedup acceptance.

The reachability collector walks current public action reference, current generation action reference, every retained live issuance skeleton's action/result/failure references, and current carryover. It never decodes blob contents. An old generation remains reachable through an admitted request after definition replacement; terminal request removal then allows reclamation when no other owner references its bundles. Effects use separate fully frozen effect request DATA, and terminal metadata fences late deliveries. There is no dangling-request regeneration from the latest definition.

## Completion semantics and deduplication

Callback preparation already canonicalizes accepted Payload/Failure wire data with deterministic protobuf serialization and retains unknown fields. Result storage wraps that Payload as a one-element Payloads message so carryover and admitted LastCompletionResult use the same typed blob bytes; materialization reverses that wrapper. Failure bytes are stored directly, and failure presence remains distinct from an absent failure even for empty wire. A failed completion retains the previous successful result; success clears failure through reducer semantics. Old issuance snapshots retain their own references after current carryover replacement.

The implementation removes the avoidable repeated persistence of the same near-limit input in public definition, generator template and admitted requests, and the same result/failure in carryover and future issuance. Hash comparison/assembly can still allocate and decode large data transiently. Distinct legitimate generations and distinct carryover histories can occupy multiple blobs. This is not a universal guarantee that all accepted concurrent definitions/results fit the default8MiB mutable-state limit; the near2MiB three-start physical test is a specific supported trace, not a capacity proof for arbitrary four distinct large generations.

## Tests reviewed and remaining evidence

Current tests cover two generations and GC, old request immutability and detached reads, shared carryover/request references, preserved live old carryover, nil/empty input, missing action references, failed replacement publication with no consumed request ID, unknown envelope field1000 canonical bytes, explicit-null result and present-empty failure, and missing result/failure/public/generator references. Reviewer requested additional unknown Payload/Failure field roundtrips because those use different wrapping paths; author is adding them. Preserve final test source/run fingerprints before acceptance.

Parent `TestSchedule2LargePayloadCarryoverCapacity` performs three actual manual starts with near2MiB input, first success, then large failure retaining success, then verifies both in workflow code and succeeds. It checks actual History-backed progress and reported mutable-state bytes below8MiB at each settled stage without raising limits. This source review does not mark its pending execution as passed.

## Source SHA-256 snapshot

- `chasm/lib/scheduler2/payload_storage.go`: `043b6fe91d5d6d87e76454daef31c2ce4db7383e67ab4bb4f596581daac50e1f`
- `chasm/lib/scheduler2/payload_storage_test.go`: `a0f268839c39f457a10e34ab0ec84a793ab64596d9df8b5c716311f41bf54f59`
- `chasm/lib/scheduler2/component.go`: `2b85286d7164890f82dd1f6bf0c0b6d612eb73f5f477e517ee32425084ded9c7`
- `chasm/lib/scheduler2/generator.go`: `58c0b9572da6758157431f11989b6ab45e46f91e4ddcd9ed1951f51108f3c316`
- `chasm/lib/scheduler2/completion.go`: `14987399aa4bc786f5f029e84f1a202f3a6473b8bbdfe5f6e8ff70757d12654b`
- `chasm/lib/scheduler2/public.go`: `75f0f8a0b8e3cb4ea88f83fca8610646630d346b630f5eb9c45516cee1f05c36`
- `chasm/lib/scheduler2/public_mutation.go`: `9d6ea8882dc6abe0794a8178ec200dfb1c1bbf7a32d06ae5e26bf90cc26be7a6`
- `chasm/lib/scheduler2/effects.go`: `99cb5050c85d6ce999d7ec25f6890070a562516c6fc0675b5f552c263273c57c`
- `chasm/lib/scheduler2/manual.go`: `fe461ca60a48a6ed233dabac493e4bc6b08b17fc9e4279fdab92e8a5699ae1c8`
- `chasm/lib/scheduler2/buffer.go`: `d7999e49797297a80c8f285d9a2f9e987e732ddf756ebf44affddece01dd599d`

## Final expanded storage-test snapshot

Author froze source/tests and ran all TestSharedPayload: PASS1.074s (`/private/tmp/scheduler2-wi015-payload-final-focused2.log`, inspected by reviewer). Outcome Payload and Failure now carry unknown protobuf fields through carryover preparation/materialization and request split/assembly; protobuf equality validates preservation. Explicit-null/present-empty failure and each missing-reference family are covered. Earlier full-root pass10.814s predates final read identity hardening; parent owns combined rerun. No additional source finding; actual capacity diagnostics remain separate.

- `chasm/lib/scheduler2/payload_storage.go`: `043b6fe91d5d6d87e76454daef31c2ce4db7383e67ab4bb4f596581daac50e1f`
- `chasm/lib/scheduler2/payload_storage_test.go`: `2c1cec13fd2ff4eb9dee7257cb458a867428f8681748d18f37da9f1cd1c9c863`
