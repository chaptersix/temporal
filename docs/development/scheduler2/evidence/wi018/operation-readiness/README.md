# Bounded operation readiness adapter

This adapter implements the accepted exact `(effective ready time, sequence)` order over OperationPages. ReadyRecords maps a nonreused fixed-width sequence key to operation ID, original logical Wake and effective ReadyAt. The actual OperationState stores ReadySequence. OperationStoreControl owns PendingCount, the next unused sequence (initially1), and StartDispatch control (initial token1). The complete profile rejects missing dispatch state; the parent permits only explicitly gated pre-integration private fixtures to omit it. No public activation or migration occurs in this slice.

`newOperationReadinessEditor` receives the same detached prospective control as record/retirement preflight and owns an unpublished CountBatch plus locator overlay. It validates the persisted count against index cardinality minus the one claimed membership, rejecting active-claim count underflow, lost roots and stale prospective control. Touched locators, operation metadata, ReadySequence, decision.Wake and indexed exact time must agree. A claimed entry must be absent from the index while retaining its locator; claim expiry must be later than effective ReadyAt.

Normal record changes preserve sequence and active claim when Wake is unchanged. Entering pending or changing Wake allocates a fresh sequence; leaving pending removes the addressed locator/index or exactly matching claim. Token and armed-state reconciliation belongs to the root coordinator. Changes to another operation cannot replace the current claim. The editor returns ClaimReleased and the coordinator performs token fencing before publication.

Claim acquisition selects the current exact minimum, requires it due and before issuance Deadline, removes only its index membership and retains PendingCount/locator. Recovery validates the current claim and expiry, then uses a fresh sequence with ReadyAt=now while preserving RetryAt, Deadline and logical Wake. If issuance Deadline elapsed, recovery rejects before any mutation so the caller applies decision.Expire. Recovery's detached metadata update is included exactly once in the readiness publication plan. All editor errors poison finish; failed preparation never produces an applicable partial plan.

Publication writes captured index changes, addressed locators and recovery-only metadata. Normal record metadata and the shared prospective control publish through the parent record plan, after all rejecting checks. The parent's actual hooks run prepareStartDispatch before editor.finish, then publish the same records/control/tasks transaction. A framework panic requires abandoning the enclosing lease.

## Namespace composition

The retirement trie uses only `retirement/` and `retirement-root/` pages, with no canonical CountRoot owners. Readiness uses `sequence/`, `sequence-root/` and canonical `count/` pages. Therefore retirement and readiness captures over OperationPages have no overlapping mutation keys or reference counts. The composed pending-to-StartOnly test asserts disjoint captured key sets, publishes both orders using the real operation record preflight, and verifies exact readiness/retirement queries after CHASM reload. Another SequenceIndex can share canonical count pages even with a distinct owner, so an independently captured projection index must use a separate backend or the same batch; owner-ID separation alone is insufficient.

## Bounded work and limits

Normal record preflight handles at most3 unique changes. Each causes at most one sequence delete and one insert, plus fixed addressed old-record/locator checks. Root reconciliation adds one chronological minimum query when there is no active claim. All index operations retain the source-derived64/96-bit bounds documented under ready-index and inline-leaf; the chronological minimum ceiling is50,054 counted reads. Count queries span the full sequence interval and use the indexed aggregate, never record enumeration. Captured locator/metadata maps are bounded by this fixed change envelope. Claim/recovery performs one selected operation quantum. CPU/bytes depend on fixed key widths and touched operation metadata, not pending cardinality. This is an adapter argument, not a completed whole-Scheduler transaction bound or physical task count claim.

## Evidence

- directed01.log: initial adapter tests PASS0.703s.
- directed02.log: actual record-hook/coordinator tests PASS0.709s.
- Tests cover negative extremes and equal-time sequence ties; pending/settled transitions; claim and recovery publication discard/reload; fresh recovery sequence/effective time with unchanged logical deadline; unchanged other active claim token/expiry on metadata changes and positive settlement; lost owning root, missing locator/record, mismatched logical wake/sequence/count, active-claim underflow and missing dispatch; allocation exhaustion; future claim rejection; expired recovery before mutations; and disjoint retirement/readiness capture publication in both orders.
- lint01.log retains two findings: an unused test assignment (fixed) and an independently owned projection helper complexity finding. No readiness implementation or dispatcher finding appeared. Shared lint rerun is coordinated by the parent/framework.
- manifest.sha256 freezes this agent's adapter/tests. Root-owned coordinator/handler/hooks/schema are reviewed and frozen separately.

The real RPC dispatcher, physical task deadline conversion, late evidence and SQLite delivery remain parent-owned acceptance gates. Adapter harness results are not substituted for those witnesses.
