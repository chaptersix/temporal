# WI-010 terminal-fence review

Independent review found no blocking authority bypass. Control/Apply/internal
transition, issuance snapshots and task validators check closure before payloads.
Exact-run merge references are unchanged. Close rollback and actual History UUID
allocation/recreation are exercised independently of foreign-incarnation event tests.

D51 qualification: new/non-deduplicated controls reject; prior successful command
IDs remain successful no-ops. Snapshot authorization before close may outlive the
lock; deletion does not promise to cancel a preauthorized or issued RPC.

Test iteration notes: initial History fixture compilation used an unavailable shard
UUID method; corrected to cache owner identity. An initial assertion rejecting all
published tasks failed because no-op updates can emit SyncVersionedTransition
replication bookkeeping. Corrected assertion forbids scheduler tasks while keeping
complete replacement snapshot equality. Those iteration outputs were not saved as
files; no fabricated logs are provided. Lint iterations and final passes are saved.

Five source mutations fail named tests and are restored byte-for-byte. This reviewed
increment is not full WI-010 acceptance: source-backed anchor/idle design remains
under review, and no new L3/refinement/database claim is made.
