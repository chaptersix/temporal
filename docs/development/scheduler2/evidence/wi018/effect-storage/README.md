# Fixed effect request storage

Selected OperationStore.StartDispatch executions store cancel and terminate request owners in two fixed DATA fields, CancelRequest and TerminateRequest. Legacy executions retain EffectRequests. A selected execution with any legacy request entries, or a legacy execution with either fixed field set, rejects mixed storage instead of falling back. No migration or public activation occurs in this slice.

Each fixed slot is absent or belongs to the unique validated EffectState of its kind. Metadata validation first enforces at most two effects, allowed unique kinds, canonical target-derived IDs and phases, and common blocking target. Field.IsSet probes both slots without loading bytes. Pending metadata requires its slot; matched membership must equal the two-slot presence count, detecting missing and orphan owners without enumeration or a maintained counter. Retention/replacement uses old metadata IDs/kinds, never old RPC bytes. On target change disappeared owners are cleared before new owners are written, so same-kind replacement cannot accidentally remove its fresh request.

The slot payload is a version1 byte envelope: version byte, canonical unsigned-varint owner-ID length, owner-ID bytes, then the exact deterministic RPC request bytes produced by freezeEffect. Blob-reference fields remain empty. Only addressed materialization decodes this envelope and verifies the exact expected owner; malformed versions, lengths, overlong varints and owner mismatch fail closed. Returned bytes are detached, and unmarshaled public request values remain detached as before. The first-execution-run fence, namespace/workflow identity, cancel RequestId, retry horizon and effect result protocol are unchanged.

## Metadata-only Field presence

Field.IsSet is exactly the negation of Internal.isEmpty. It reports a stored reference or staged value; it does not deserialize DATA, resolve a pointer, validate referenced contents or mark a node dirty. A dangling pointer can still be set. CHASM tests cover empty/staged/deferred fields and a real cold poisoned DATA node: IsSet preserves its nil materialized value and valueState, whereas Get fails on its invalid serialization. This is a presence API, not a substitute for TryGet/validation at use.

The scheduler no-read witness poisons both serialized fixed request DATA nodes, reloads the actual tree, and verifies validateEffects, stateFor and EffectHandler.Validate succeed using metadata alone. Addressed issuance still fails closed on the poisoned DATA. Replacing that target cleans the old slots and freezes new valid bytes without materializing the poisoned requests. Thus unrelated metadata/idle paths retain the prior no-request-read isolation.

## Evidence and scope

Directed tests (`directed01.log`) include CHASM presence semantics; discarded/committed fixed-slot publication and reload; exact retry bytes and first-run fence; caller mutation isolation; missing/orphan/mixed owners; owner/encoding corruption; actual prepareEffects freezing a new target's request despite old same-kind slots; and canonical-envelope validation. The task churn witness performs24 target replacements, each with three retry-wake updates, through actual CHASM task publication/reload. Logical effect task count remains at most2 and reaches0 after cleanup. It does not claim the external physical queue contains no stale envelopes; physical delivery/response-loss acceptance remains a separate parent gate.

All effect loops and slot probes are fixed by two kinds. Metadata validation performs no request DATA read. Addressed issuance loads one request; prepareEffects freezes at most two small RPC skeletons. Publication and cleanup update at most two fixed fields. Work depends on the touched request byte size, not queued/unfinished operation cardinality. This slice alone does not establish whole-Scheduler bounded layout or full physical task acceptance.

`manifest.sha256` freezes owned source and reviewed effect-handler source. Required combined lint and independent review are parent-owned. No live lint is owned by this agent.

Full routine verification (`full01.log`): `CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm ./chasm/lib/scheduler2 -count=1` passed, chasm0.446s and scheduler2 21.503s. Directed timings were chasm0.394s and scheduler2 0.713s. These tests include the retained finite effect semantics and the new fixed storage witnesses; independent parent lint/review remain separate recorded gates.
