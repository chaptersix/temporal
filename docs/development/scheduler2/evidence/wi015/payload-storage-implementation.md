# WI-015 payload storage implementation checkpoint

Public roots now store one typed immutable ActionPayloadBundle for input/header/memo/
user search attributes/user metadata, plus independent completion result/failure blobs.
Root PublicActionRef, GenerationDefinition.ActionRef, operation Payload references and
CarryoverState references identify stable owned DATA in PayloadBlobs. Internal
PublicRevision0 fixtures retain inline representation through the same logical helpers.
Public missing action references fail closed rather than falling back to current or
inline content. Missing referenced blobs, wrong types/hash identities and malformed
protobuf decode return errors.

Explicit typed split/assembly retains detached skeleton fields and unknown protobuf
fields. Generated scheduled-by/time attributes remain in the operation skeleton and
merge over user attributes. A candidate static bundle is reused when generated reserved
keys or an empty user map would otherwise cause avoidable copies. Result uses a Payloads
wrapper in storage so the same blob can feed an issuance request; current CarryoverState
logical bytes remain the single Payload contract. Failure presence remains independent
of serialized empty bytes. No dynamic dependency is persisted and no reflection-driven
serialization framework was introduced.

All admission storage preparation occurs after logical validation and before publication.
Payload owners, small envelopes, successor tasks and terminal cleanup publish together.
Root replacement preserves PublicActionRef. Generation updates assemble the current
immutable template; prior admitted envelopes continue referencing their old bundles.
Terminal requests are reclaimed while metadata remains for stale delivery fencing.
GC walks current public/generator references, the bounded live request map and current
carryover references, then removes unreachable owner entries. It reads reference metadata
rather than decoding every blob. Hash/collision verification and materialization do read
immutable blobs with the caller context; possible mutable-read write amplification is
not claimed solved by persistent byte sharing.

The actual selected Create/Update paths split public action data; Describe assembles a
detached definition. Patch and pause-note projection update small public skeletons while
retaining the same current action ref. Visibility continues reading compact list DATA.
Actual runtime request assembly returns detached input/carryover values; cancellation/
termination identity extraction uses only the small frozen request skeleton.

## Executed gates and evidence limits

- `CGO_ENABLED=0 make proto` passed; generation log archived. Scheduler2 schema/output
  changed intentionally alongside already-in-flight internal callback metadata changes.
- Full scheduler2 root tests passed10.814s in payload-root.log before final typed-hash
  read validation and final missing-ref/unknown-outcome tests were added.
- Final `CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2 -run
  '^TestSharedPayload' -count=1` passed1.074s. Covers multi-generation immutable requests,
  actual DATA reload, terminal reachable-only GC, shared result/failure references, root
  carryover replacement while pending request retains its snapshot, nil versus empty
  input, explicit-null result, allocated-empty failure, missing references, unknown
  envelope/Payload/Failure fields, and failed replacement publication preserving exact
  durable nodes and request-ID state.
- Deterministic near2MiB input→success→failure→third-admission fixture passed3.907s.
  Third request assembles all three large fields with exactly3 owner blobs and
  Started2/Unfinished1. It uses real CHASM Node close/reload with mock publication,
  **not** actual History database accounting.

Logs and exact source hashes: `payload-storage-checkpoint/manifest.json`.
Independent review belongs to oracle_review; final combined lint/race/History gates and
physical capacity acceptance belong to the integrator.

The first actual History capacity run reached input+result+failure persisted state of
about6.25MiB but failed to observe third completion. Parent's physical04 diagnostic
captured the exact failure: gRPC rejected the assembled Start request,6243479 bytes
against4194304 receive limit. Admission retained the pending operation; this was not
an8MiB CHASM state failure. Parent reports capacity06 PASS13.684s with a1.25MiB-per-field
positive profile and near2MiB-per-field negative profile asserting retained authority,
without limit overrides. Physical logs and acceptance belong to the integrator; this
reviewer did not execute those tests. Arbitrary simultaneously distinct maximum-size
generations/outcomes still have finite-state and transport capacity constraints; this
representation removes avoidable copies but does not prove all such workloads fit8MiB.

The final lint-driven refactor separates typed action/search-attribute extraction and
bundle reuse from splitRequest; focused TestSharedPayload passed1.074s afterward in
payload-refactor.log. A subsequent named-results-only signature adjustment resolves
confusing-results lint without behavior changes; the integrator owns final combined gates.
