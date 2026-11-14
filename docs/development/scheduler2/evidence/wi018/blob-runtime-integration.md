# Transactional blob ownership integration

Concrete follow-on to operation-runtime-integration.md. Source audit after the addressed operation adapter; no implementation change or activation yet. Existing finite and existing public records remain supported without migration. Root owns checkpoint and activation decisions.

## Complete owner/write map

| Owner identity | Existing owner fields | Writers/releases | Preflight source |
| --- | --- | --- | --- |
| public-definition | Root.PublicActionRef | storePublicDefinition from newPublicSchedule and UpdatePublic | old root ref; new bundle prepared from accepted ScheduleAction |
| generator-definition | Generator.Definition.ActionRef | configureGeneration via ConfigureGeneration/UpdateConfiguration | old addressed Definition when HasGenerator; new splitRequest action ref |
| request/<operationID> | Requests/RequestRecords Payload.ActionRef,ResultRef,FailureRef | prepareTransitionRecords admission, terminal settlement, retention eviction; public completion-window eviction | operationRecordPlan already knows addressed changed IDs; read old request only when changeRequest |
| carryover | Carryover.ResultRef,FailureRef | applyTransition prepareCarryover then publishTransition replacement | old Carryover only when next.Carryover changes; new preparedCarryover refs |

No owners are held by queued occurrences, action-history projections, diagnostics, PublicList, or effects: effect RPC payloads contain no action/result/failure references. A retained operation without a request owns no blob. Generator rejects result/failure refs in templates; retain this validation. Close retains owner records/refcounts for execution deletion and performs no release scan. Changes to public pause/notes/remaining controls that do not replace PublicActionRef have zero ownership delta.

Current collector call sites to remove for the selected refcount layout: component.applyTransition, generator.configureGeneration, public_mutation.newExecutingPublicSchedule and UpdatePublic. The collector's raw Request and PayloadBlobs scans may remain only in the old layout; no mixed layout and no implicit reconstruct-count fallback.

## Additive schema and helpers

Use Root.PayloadStore -> PayloadStoreControl{format_version=1}; immutable BlobRecords DataMap[contentID]->Payload{Data}; independent BlobReferences DataMap[contentID]->BlobReference{owner_count uint64}. A blob count aggregate is unnecessary for current consumers; do not add an unmaintained placeholder. Reject unknown versions and mixed legacy PayloadBlobs with selected keyed layout. OperationStore and PayloadStore selectors are separate because ownership integration must be testable before dispatcher activation; new public runtime activation remains separately reviewed.

Content identity remains kind+'-'+SHA256(bytes), exactly current prepareBlob/payloadBlob behavior. No renaming, mutable content updates or deduplication weakening. DataMap records are detached. Reference IDs remain existing string fields so request/template wire identity does not change.

Functions:
- blobRecord(ctx,id) -> (Payload,present), put/delete publication adapters select the exclusive layout.
- ownerRefs(payload) -> fixed [3]string; carryoverRefs -> fixed [3]string; singleRef -> fixed [3]string. Empty entries ignored; duplicate nonempty IDs count once per owner.
- BlobOwnerChange{OwnerKey string, Before [3]string, After [3]string}. At most6 distinct owner changes per ordinary bounded transition; duplicate owner keys are rejected or explicitly coalesced before preparation, never silently applied twice.
- prepareBlobDelta(ctx, owners []BlobOwnerChange, prepared payloadPreparation) -> (BlobDeltaPlan,error). At most36 old/new reference slots, at most36 distinct counter point reads. Plan holds only changed count values, new content values and zero-reference deletions; it publishes no fields while preparing.
- publishBlobDelta(ctx, plan): publish new blobs and count values, delete zero-count counter/blob pairs. Called in the same enclosing transaction as the prepared owner record writes. No errors after successful preflight except the framework transaction's normal publication failure.

For the legacy profile the plan can retain prepared blob publication and old collector behavior until new-profile activation; no keyed owner may use the legacy collector. Keep legacy source/test oracle independently accessible.

## Immutable-request plan seam

Extend operationRecordPlan with its fixed changed request-owner tuples, captured during prepareOperationRecord. A changed request is inspected once for both existing immutable-replacement checks and its old refs. Metadata-only changes do not read issuance records or blobs. Admission has empty Before; terminal/removal has empty After; unchanged request has no owner delta. If an operation's request was already released, duplicate terminal callback has empty-to-empty ownership and cannot decrement again. The plan must own detached copies; no caller can alter a prepared reference tuple after validation.

applyTransition prepares recordPlan and carryoverRecord first, combines their owner changes, then prepares BlobDeltaPlan using the already collected new blob bytes. All blob counter errors precede stageQueueAppend/stageQueueDrain. Publish the blob plan and record/root changes only after every queue/token/retention/identity check succeeds. Existing new request snapshot remains immutable through template/carryover updates.

Public completion-window eviction is currently performed inside publishPublicTransition. Move its bounded ten-window computation to an explicit preflight PublicTransitionPlan, returning at most one victim and the prepared next window/actions metadata. Include that victim's request deletion in the same operationRecordPlan and BlobDeltaPlan before any publication. Even though valid completed operations normally released their request on terminal transition, do not rely on that to leave an unaccounted writer. If completion self-evicts the addressed operation, coalesce its update+delete into one final deletion before the duplicate-ID guard. Preserve exact stable close-time ordering and callback-before-ack semantics. This is required before keyed ownership activation, not optional cleanup.

## Definition update sequencing

Split storePublicDefinition into preparePublicDefinition(ctx,definition) -> {skeleton,actionRef,preparedBlobs,ownerChange} and nonrejecting publishPublicDefinition. The old public ref remains a live owner until the replacement plan commits.

Split configureGeneration's prepublication section into prepared generation data/control plus its one owner change; publication consumes this prepared object. Existing single-generation APIs can prepare and publish one blob delta. UpdatePublic already replaces generation before public definition and can later reject visibility validation: make the outer update prepare both owner changes and merge both prepared-blob maps before publication. Two references to identical new bytes yield owner_count+2; old/new net-zero sharing must never transiently delete live content. Preserve root/generator revision/token/idle/coordinator checks and existing enclosing CHASM rollback; visibility input validation must finish before semantic owner publication. Do not expose a partially updated reusable component on error.

Creation starts empty; initialize keyed collections before preparing the public owner. newExecutingPublicSchedule then adds generator owner under the same transaction. Either combine creation plans or prepare the second against the first staged counts, explicitly testing first-owner sharing. Initial patch admission must observe both staged owners, not an uninitialized count namespace. No collector is needed after a complete refcount path.

## Count and content rules

Aggregate signed additions/removals by unique content ID across all owner changes; do not perform per-owner deletes before netting. Use checked uint64 arithmetic. Existing owners require existing positive count records; missing/zero counters, underflow, overflow and new referenced IDs without existing ownership or prepared bytes reject unchanged. A zero net count change does not create a write or delete/reinsert content. Prepared bytes with no final owner are not published (splitRequest/reuseActionBundle can discard a candidate bundle).

For an existing blob, preparing newly supplied bytes preserves current exact content collision validation. For release-only count changes do not hydrate/hash immutable blob bytes: the persisted owner/count invariant already establishes its identity, and delete is addressed. Missing/corrupt content is detected when materialized or compared against newly supplied bytes, while missing/corrupt ownership metadata fails immediately. This distinction preserves payload isolation rather than reading large old results merely to release their final reference. A future cheap DATA existence API could validate physical presence without decoding; the current DataMap API has no such method, so do not invent one in the facade.

All owner/count edits remain in one CHASM NodesMutation; rejected persistence rehydrates prior counts/content/owners together. There is no external refcount service, periodic correctness sweep, global owner map clone, eager close cleanup, or background decrement job for individually removed requests. Offline whole-record audits may recount owners as independent evidence only.

## Dependency order and verification

1. Add BlobReference/control schema and keyed adapters; pure bounded delta planner plus lifecycle tests. Coordinate one proto generation with other active schema work.
2. Extend operationRecordPlan owner tuples and preflight public completion retention. Integrate applyTransition, including terminal and legacy retirement releases. Keep incomplete operation runtime gate intact.
3. Split public/generator preparation/publication and integrate all creation/update owners. Replace collector in selected layout; retain old profile independently. Add addressed carryover replacements and close retention witnesses.
4. Independently review every owner writer from the table, then select the refcount layout only for new local public executions if parent approves complete dependent gates. No existing-record migration or public capacity changes.

Required traces: same blob shared by public+generator+many requests; template A->B while request A remains immutable; carryover A->B while an old request retains A; duplicate refs within one owner; sharing/net-zero replacement; callback-before-ack; repeated terminal callbacks; completion-window self-eviction; malformed/missing count; count overflow/underflow; unused prepared candidate; every read/publication failure followed by prior-image reload. Poison unrelated blob bytes and assert zero unrelated decode bytes. Assert the same36-reference ceiling at N4/5/1000, with full transaction counters once bounded layout is activated. Full public contracts and nil-library reads remain required. Do not claim the request-skeleton byte size is fixed: addressed accepted payload/skeleton bytes are separately charged input/output terms.
