# Required-empty legacy DATA map capability

`BoundedComponent[T](path, limits).WithRequiredEmptyMaps(names...)` declares raw
legacy chasm.Map fields forbidden to contain entries. Names use the persisted
field name, including `name` tags. Both API and registration copy the declarations.
Registration rejects unknown/duplicate/ambiguous names, fields of other kinds,
component maps, interface payload maps and non-protobuf values. Undeclared maps
remain unsupported. This does not opt any Scheduler execution into the profile.

Validation reads fixed schema/slot metadata, reflect.Len for materialized maps,
and len(children) for existing map containers. It never enumerates map entries.
An empty persisted container is accepted; any DATA/component child is rejected.
Checks occur at attachment/cold reconstruction/replication, every bounded sync,
and immediately before serialization and transaction return. The latter two
also catch deliberately malicious read-only callbacks changing raw Go maps after
sync. No bounded error falls back to legacy traversal. Nonbounded registration
and ordinary legacy map serialization remain unchanged.

Tests cover 1 and 10,000 attempted hot entries with zero map-key copies and map
hydration, failed attachment, reload of the unchanged durable snapshot, malformed
cold topology, ordinary and system replication, deep-copy descriptor isolation,
and malicious LifecycleState / IsExecutionSkippable mutations. Tests explicitly
check those callbacks ran and rejection returned no publishable mutation.

Full CHASM tests passed after the final static ambiguity check; scoped CHASM lint
logs capture normal/minimal/vet modes. Parent supplies independent review and
aggregate required fastlint. Earlier lint01 caught an unused initial test context
binding, corrected before successful lint02/03. All logs retained. These are CHASM
transaction tests; they do not assert an external database rollback or selected
Scheduler physical delivery. Private fresh-profile library registration and task
limit evidence are separate integration work.
