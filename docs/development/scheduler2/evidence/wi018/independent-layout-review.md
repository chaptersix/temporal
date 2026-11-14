# Independent descriptor/cache review (in progress)

Scope: fixed typed component topology, DATA-only collections, registration/cold validation and incremental slot cache. Default traversal remains active; this stage does not establish bounded CloseTransaction work or enforce every declared transaction task budget.

Reviewed bounded_layout.go and tests plus registration, Field type inspection, attachment, DataMap sync, passive update/deletion and snapshot call paths. Descriptor copies caller path slices; rejects undeclared component fields, raw maps, MSPointers, nonconcrete Field payloads and memo providers lacking version providers. Cold validation permits only declared typed slots, leaf DATA, DataMap collection and leaf entries, plus reserved root DATA. Serialized task arrays are checked against per-slot limits. Optional absent slots are allowed. ApplySnapshot delegates ApplyMutation, preserving its validation path.

Finding sent to author: ApplySystemMutation bypassed the new bounded validation/cache loop. Creating or recreating a declared slot can leave its cached slot absent; changing DATA to a hidden component can bypass topology rejection. Require common post-update validation/cache maintenance, or enforce a narrower system-update contract. Awaiting author correction and nested attach/delete/recreate/passive tests. Active nonempty collection-owner deletion guard is also in author progress, not reviewed as complete.

Independent command CGO_ENABLED=0 go test -tags test_dep ./chasm -run '^TestBoundedLayout' -count=1 passed 0.412s for initial snapshot. Logs and exact initial hashes retained. No acceptance claim while findings/stage followups remain.

Budget scope: Pure/SideEffect limits currently check serialized arrays on cold/attachment/passive validation. NewPerTransaction, ImmediatePerTransaction and MaxImmediateExecutions are descriptor values whose enforcement belongs to the subsequent worklist stage. Failure rollback still relies on existing engine discard/reload semantics; this cache does not introduce in-place rollback of a failed tree.


Worklist followup source review in progress: nodeWorkset removes the pending marker when popping, allowing later re-dirty to re-enqueue; detached nodes are rejected by current ancestry identity. Structure drain flushes pending DataMaps and processes only component structure entries. Serialization drains bytes, while task/metadata/time-skipping consumers enumerate fixed slots. Dirty propagation conservatively touches every fixed task slot to preserve ancestor/ParentPtr dependencies. Cleanup clears touched flags and task counters after transaction close; failure still requires discarded tree/reload. Default traversal paths remain unchanged when no descriptor is installed.

Author fixed ApplySystemMutation by sharing validateBoundedMutation with ApplyMutation, and added nested slot attach/delete/system-recreate/passive-replace/delete plus system corruption tests. Source review resolves the original finding, pending final execution/hash snapshot. Active nonempty collection-owner removal now preflights before cached-slot deletion; passive replication remains allowed to apply bulk authoritative changes.

Requested final meaningful gates: cross-slot task invalidation caused by DATA mutation, rejected serialization/publication followed by durable reload preserving prior tasks/data, and bounded fixtures exercising cold timed/side-effect RefreshTasks/EachPureTask/time-skipping consumers. Existing default-profile tests alone do not cover new iterator routing. These requests are pending author response.


## Final independent review, 2026-11-13

The prior in-progress restrictions above describe earlier source snapshots. Final reviewed source hashes are in independent-layout-final-snapshot.json. The system mutation cache/validation finding is fixed and the requested task/recovery gates are now present. No remaining blocking finding was identified in this framework capability scope.

The final review covers bounded layout registration/cold topology; shared system/passive validation; task-budget enforcement; structure/byte/touched worksets; task metadata, refresh, time-skip and lifecycle consumers; active nonempty-owner removal rejection; full snapshot/export and detached nil-handler decoding. Cross-slot DATA changes invalidate ParentPtr-dependent child tasks; attached versus detached closure matches the default profile. Serializer/structure failures reload the prior image with prior DATA and both task types. This is a rejected-candidate reconstruction witness, not actual database-fault injection.

Parent review additionally found that child receiver iteration had widened to all slots. The corrected component iterator filters fixed paths and emits receiver-relative paths. Child structure/byte drains defer sibling work and re-enqueue it after their scoped drain, preserving eventual root publication. Independent source inspection plus TestBoundedTaskChildReceiverScope and TestBoundedChildDrainPreservesSiblingWork verify sibling task isolation and later persistence. DataMap deltas and node-workset membership maps discard prior capacity rather than carrying a large constructor transaction into later hot map iteration/clears; ordinal worksets retain only length-zero backing slices. Addressed payload size and application touched-key count remain separate caller obligations.

Independent command: CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm -run 'TestBounded' -count=1. PASS0.425s, layout-independent02.log. This directed evidence complements the author's full suite/lint; it does not prove scheduler2 runtime integration or close WI018/M3.
