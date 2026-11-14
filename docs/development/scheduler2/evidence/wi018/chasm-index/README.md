# CHASM index adapter integration gate

The private indexBackend adapts the queue index's opaque keys/values to a DATA-only collection. Each batch stages page changes and owning roots in one enclosing CHASM transaction. No independent commit, runtime Scheduler attachment, production routing or migration is introduced. The adapter is scoped to one context/epoch; framework panics require discarding the enclosing lease, as for other CHASM mutations.

TestIndexBackendPublicationAndReload exercises the actual CHASM serialization/reload path: batch publication alone does not survive a discarded enclosing transaction; a published enclosing transaction preserves all three sequence records; a later deletion survives reload. The harness models publication and discards rejected candidates; it is not an actual SQLite failure test.

TestIndexBackendSerializedFootprint builds a permuted-time sequence index at N4/5/1000/10000 and measures every persisted ChasmNode protobuf plus encoded path. It then reloads, publishes one additional point update, and verifies the full count. Whole-record wrapper fields, database row overhead and Go heap are excluded. Initial admissions are staged for fixture construction; that setup is not a claim of bounded per-transaction work. The measured subsequent point update is separate.

Exploratory01 passes2.207s. At N10000 it reports53891 nodes,6,664,895 protobuf bytes and2,863,892 encoded path bytes:9,528,787 total, still above8MiB before queue payload records or whole-record overhead. At N1000 the total is792,689. The next point update at N10000 has74 updated nodes,30 deleted nodes and14,603 updated-node/path bytes. These are measurements, not the final schema-derived worst-case bounds.

This exploratory build overlapped internal compact-encoding development, so it does not establish frozen-source acceptance. Preserve its log as a diagnostic witness; repeat against frozen source with hashes, independent adapter review and required lint before checkpoint acceptance. Storage reduction, full runtime integration and M3/M4 remain open.

Command: CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm/lib/scheduler2 -run TestIndexBackend -count=1 -v
