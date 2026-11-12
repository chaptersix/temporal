# DataMap default traversal witness

At N4/5/1000 records, a hot addressed Get/Put and full close visits37/43/6013 tree iterator nodes. Every size hydrates/copies/synchronizes zero ordinary-map keys, decodes only the addressed DATA once after true reload (zero on the second hot mutation), and serializes only the small root plus addressed record (51 complete node protobuf bytes). Both hot mutations reconstruct zero cold nodes. True reload separately constructs N+2 nodes with no DATA or collection-key hydration. This is not an apples-to-apples performance comparison with the earlier root-only raw-map witness; it demonstrates the remaining full-tree traversal independently.

These counts establish that DataMap capability alone does not satisfy the total transaction-work bound. The forthcoming opt-in worklist fixture must meet fixed maxima; this default fixture intentionally preserves the existing traversal contract. Frozen source hashes and raw operation-site counts are attached.

Initial fixture attempts incorrectly expected only one serialized node and used the wrong encoded path convention; the mutable root access also serializes its empty root, and collection paths use #. An intermediate encoder call also omitted its required Node argument. Failed logs are retained as test-authoring evidence, not runtime failures. Final test passes0.447s.

Required make lint-code-fast passes with zero issues on detached worktree /private/tmp/scheduler2-wi018-baseline-s985q4bm at6e80c11cf plus the exact new test. The first shared-worktree lint encountered unfinished bounded-layout declarations from concurrent implementation; its log is preserved and is not a baseline test failure. Isolation preserved the active implementation and original checkout.
