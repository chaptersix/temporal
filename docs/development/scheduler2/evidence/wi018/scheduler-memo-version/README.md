# Scheduler memo revision integration

RootState.memo_revision (field48) is independent of the public conflict revision. This distinction is required: start acknowledgment and successful completion can change recent public list results while leaving PublicRevision unchanged. MemoVersion returns only the effective scalar and never assembles Memo or reads runtime configuration.

New public records persist1. An older record whose field is absent/zero has stable virtual baseline1; its first memo-changing publication persists2, not1. This preserves a framework cold-cache baseline without a migration or eager write. Finite records with no memo also return nonzero baseline1. Public conflict tokens are unchanged by this mechanism.

## Reservation and publication

reserveMemoVersion preflights effective+1 with overflow rejection before any writes. A root-pointer-owned reservation is placed on chasm.ContextWithValue, preserving its concrete CHASM context type. The same context flows through nested transition/public/generation writers. Nested reservation and publication reuse the one prepared value even if the first publication reaches MaxUint64; no late second increment can wrap or reject a partially published transaction. Publication without a matching reservation is a framework-discard invariant failure. A stale reservation whose root revision has moved outside its original or prepared value is rejected; reservations are transaction-scoped, never retained across engine leases.

The helper publishes the prepared scalar idempotently. It does not promise minimal revision increments when an explicit projection replacement happens to contain identical bytes; it guarantees that equal observed revisions imply equal memo content. Actual visibility export still assembles Memo at its explicit export boundary.

Root-owned hooks reviewed:

| Writer | Preflight / publication |
| --- | --- |
| New public constructor | Stores initial revision1 with initial PublicList |
| applyTransition | Reserves after closed/incarnation checks when a list exists; preserves revision across RootState replacement |
| publishTransition | Consumes on actual Paused change, including private/legacy list-bearing roots with PublicRevision0 |
| UpdatePublic / PatchPublic | Reserve before writes; nested initial/apply patch calls reuse context reservation |
| publishPublicList | Consumes reservation for every list replacement |
| projectPublicPause | Uses the same outer reservation as transition/list publication |
| commitGeneration / publishGeneration | Reserve/consume when an existing list's dynamic Paused override changes, even without replacing stored list |

Memo itself clones stored PublicList and overrides Paused from the root. The generator and private-transition paused-only hooks are therefore necessary, independently of list replacement. Custom user memo remains separately owned by the existing Visibility component.

## Evidence

`final-directed.log` passes all MemoVersion tests in0.694s. Tests cover virtual-zero baseline to persisted2, actual legacy public zero records, nonzero getter without memo materialization, ContextWithValue compatibility with DataMap writes, nested publication at MaxUint64, stale/different-root/missing reservations, actual acknowledgment/callback memo changes with unchanged PublicRevision (both callback orders), discarded publication and reload, overflow before patch/ack/failure callback with deliberate commit-after-rejection comparison, pause-on-failure multiple writers consuming one final counter, generation Paused-only projection, and private PublicRevision0 list-bearing transition projection.

`lint01.log` is the required scoped scheduler2 lint, normal/minimal/vet terminal PASS with zero issues. `manifest.sha256` freezes owned helpers/tests; `reviewed-hooks.sha256` records the reviewed parent hook/schema snapshot. Root owns final composed tests and checkpoint.

The retained initial `directed01.log` failure came from an invalid test setup: poisoning PublicList before cold reload fails in SearchAttributes initialization, which independently reads the compact list. The provider no-materialization test was corrected to exercise MemoVersion directly with an unreadable staged list. This evidence does not claim that the whole Scheduler cold load avoids PublicList reads; it proves the revision getter performs no such read and preserves framework memo-cache correctness. Framework provider/cache behavior remains covered by the existing chasm visibility-version tests and the parent composed gates.
