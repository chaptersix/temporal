# Independent memo-version capability review

Reviewer: root. Read the complete optional interface, cache helper, registration/tree diff and test file. Traced the unchanged full memo export call at service/history/visibility_queue_task_executor.go:465. No blocking behavior finding for the opt-in capability; scheduler2 has not adopted it yet.

Versioned close compares a nonzero persisted version without assembling, cloning or comparing Memo. A decreasing warm version fails rather than silently suppressing an update. Cold construction and passive ApplyMutation/ApplySnapshot refresh the cache from persisted state while preserving pending visibility tasks. Default providers retain full Memo equality/clone behavior. Version-only providers are rejected at registration. Every semantic memo change still requires the application to atomically increase the version; this interface cannot detect a dishonest unchanged version without losing the optimization.

The 1MiB fixture rejects any unexpected Memo call on creation/close/reload and verifies unrelated changes leave visibility unchanged, version changes create visibility work, detached reads can retrieve full content, and the old provider follows its existing behavior. The max-version/wrap and cold-zero tests cover failure cases. Child-only replication and ApplySnapshot tests exercise actual framework application methods.

Publication rejection is simulated by discarding the candidate mutation and reconstructing the prior committed node image. This proves framework cache behavior after a rejected publication at this boundary; it is not an actual database-fault experiment. Full visibility export is source-traced unchanged and direct full-Memo retrieval is tested; no new actual visibility-queue execution witness is claimed here. Actual scheduler version management, overflow preflight, public projection correctness and complete hot-work bounds remain integration gates.

Independent final focused memo tests PASS0.413s. Exact reviewed source hashes and command are in independent-memo-version-snapshot.json; raw log is memo-independent01.log.gz.
