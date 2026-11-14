# Addressed operation storage independent review

Root reviewed the store plan, tests, moved transition publication, selected operation/request reads, effect target reads, completion callback and Describe callers. No unresolved source finding in this intermediate addressed storage slice. Public creation still selects the previous operation representation; an explicit guard rejects transitions in the incomplete keyed profile. This is not operation-runtime activation or completion of WI018.

The three-record plan clones admitted metadata/request values, rejects replacement of an existing immutable request, validates identities/representation/counts and preflights changed membership before queue/operation publication. Metadata-only changes do not load request bytes. Existing finite admission/terminal/eviction writes now use the same plan; pending tasks and public retention remain legacy paths. Root replacement preserves the new control. The internal plan is used under the same lease; it is not a durable replay token across intervening writes.

Review found that Describe could bypass operation-store format validation and silently omit running entries from malformed state. DescribePublic now validates at its error-return boundary; unknown format and mixed-representation tests reject rather than return a fabricated projection. Additional preflight tests cover overflow, underflow, duplicate IDs, exceeding the fixed change budget, and deleting metadata while retaining its request owner.

An initial test expected every node's transition metadata to remain identical after a successful CloseTransaction with no semantic change. CHASM legitimately advances transition metadata when opening a mutable component. The corrected test compares complete persisted DATA and key sets plus control instead. A separate rejected publication test still compares the entire prior serialized image. The failed fixture log is retained; it is not a production rollback defect.

Remaining gates: keyed retention/projections, blob owner deltas, readiness and fixed dispatcher tasks, actual public activation, full transaction bounds, nil-library output compatibility and physical RPC/recovery evidence. No scan fallback is accepted for the completed keyed profile.

Final root directed verification passes scheduler2 0.640s and occurrence1.147s (independent02.log.gz). The operation source hash snapshot matches the reviewed fix and corrected preflight tests.
