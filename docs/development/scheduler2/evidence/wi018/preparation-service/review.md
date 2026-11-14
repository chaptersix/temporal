# Shared preparation service and addressed timezone installation

The 15 owned files in manifest.json are frozen for review. final-source preserves their exact bytes. shared-dependencies.json records separately owned schema/task dependencies; it does not claim their independent acceptance. This increment supersedes prepared-definition/manifest.json only for prepared_definition.go, whose validation/publication split was requested by runtime integration.

## Behavior and bounds

One injected Environment/Resolver and real wall clock serve outside-mutation preparation. Stage caches preserve the resolver's 1000-entry/24-hour policy. Process factories are explicit live construction; offline/nil libraries do not perform source I/O. Cold source reconstruction and actual client definition admission remain input-proportional outside hot transitions.

Timezone version 2 retains active data while a pending owner installs at most 16 combined raw-source and compiled pages per response. Raw records contain at most 1024 source bytes. Owner identity includes exact serialized source bytes and timestamp, definition owner, generation and semantic rules. All addressed reads, page validation and overwrite checks precede writes. Completion swaps the fixed active control; superseded partial and active prefixes remain queued for bounded cleanup. Legacy standalone version 1 remains supported without migration.

Pending recovery retains original resolvedAt and native initialization year/word width. Re-resolving an evicted candidate reconstructs that original frame and must match the entire descriptor and source hash. Changed source supersedes instead of mixing prefixes. Full-source read/export assembles bytes outside mutation; hot prepared lookup reads only the active fixed header and compiled pages.

Detached definition replacement plans validate before root publication; publication methods have no error return. Caller must retain the same transaction and avoid intervening definition writes.

Record-only protobuf bounds tested: raw source page 1072 bytes, pending descriptor 306, active-plus-pending control 616, cleanup descriptor 170, source task 45. These conservative maxima exclude CHASM/path/transaction overhead. Compiled page bounds remain the existing prepared timezone codec bounds. No new public source-size cap is imposed.

## Validation

- directed06: PASS 0.668s, including every addressed batch fault before writes, cleanup fault rollback, partial supersession, source hash rejection, stable-source recovery after 1001-owner eviction before every batch, native restart-frame recovery, detached preflight/publication, and record bounds.
- lint01: PASS normal/minimal/vet. Later lint02 terminated on the unrelated packed_editor.go:322 QF1008 selector finding; no owned service finding. Parent owns final combined lint after that correction.
- Windows amd64 package cross-build: terminal exit 0; this is compilation evidence, not execution on Windows.
- full01's old source-task fixture failed only on normal mutable-access TransitionCount bookkeeping. The independent task owner corrected that fixture. Parent subsequently reported composed31514 PASS: CHASM 0.475s, scheduler2 21.869s, occurrence 1.122s, decision 0.426s, packedstore 1.519s, queueindex 5.221s, timezonesource 0.706s, legacy scheduler 4.722s. Parent retains that composed log.

Earlier draft compile/lint failures remain archived rather than being described as acceptance passes. Parent owns task delivery/token tests, shared startup wiring, frontend ingress, preview maintenance and final public-runtime acceptance. This capability does not itself claim production routing, rollout, migration, or complete milestone acceptance.
