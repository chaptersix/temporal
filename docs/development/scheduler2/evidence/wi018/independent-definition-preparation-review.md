# Independent persisted calendar preparation review

Reviewer: root. Reviewed the complete handwritten component and tests, narrow proto additions, library registration and regeneration scope. No blocking finding for this unselected capability slice. Scheduler and Generator do not yet use it.

Source validation/hash/paging is caller-side input-proportional work; constructor/replacement copy the addressed source, while ordinary page installation preflights at most sixteen pages and Advance reads only addressed fixed-size pages. Header and cursor lengths/identities are validated. The source hash includes revision, and replacement resets preparation without reinterpreting old pages. Nil-library registration makes the component typed-decodable without runtime configuration or handlers.

Root review identified the exported staging type's zero value as an invalid-constructor hole. The final validate method rejects nil/uninitialized staging before publication; tests cover both construction and replacement. Input/snapshot/page aliases are detached. Failed installation/advance/replacement publication leaves the prior committed tree image intact in the harness. Each successful quantum serializes/reloads real CHASM node bytes. Poisoning the unrelated source blob demonstrates that Advance does not reread it.

Scope limits: durable-node harness reload is not a SQLite persistence-fault experiment. The counters around Advance precede full CloseTransaction and do not establish the total framework work bound. Superseded page cleanup is not implemented, including the durable FIFO of old source identities and seven field page counts needed for discovery; old pages remain retained. Runtime scheduling adoption must resolve that lifetime debt, timezone preparation and candidate-evaluation refinement. No M3 acceptance follows from this capability.

Author evidence records full scheduler2 packages PASS (root11.518s), nil-library checks PASS0.663s and scoped normal/minimal lint with zero issues. Review is source-based against the frozen author fingerprint, without redundant rerunning of unchanged tests.
