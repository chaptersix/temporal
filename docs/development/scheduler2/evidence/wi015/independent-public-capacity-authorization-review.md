# Independent public capacity and authorization review

Reviewer: api_seam. Read-only review; source snapshots kept separately from authors'
passing-test manifests. No remaining concrete behavioral blocker found in reviewed scope.

## Public capacity profile

Reviewed capacity.go, internal/decision/capacity.go and phase accounting, component
publication/retention, coordinator arbitration, public completion/projection, backfill
batch accounting, tests and design/public-capacity.md against old BufferedStarts semantics.
Persisted PublicRevision selects the public profile; live namespace tweak getter is bound
by the real test runtime library. Internal zero-valued capacity remains four-slot.
Positive public MaxBufferSize limits queue + unfinished + retained completed operations;
nonpositive disables automatic dropping, matching the reference rather than silently
retaining a four-slot ceiling. Successful ALLOW_ALL acknowledgement becomes StartOnly and
removes unfinished occupancy. Tracked completion replaces unfinished occupancy with a
canonical-close membership. Admission/phase changes update preview capacity for preflight.

Automatic overflow advances its disposition cursor and increments dropped count without
allocating operation identity or consuming action budget. Accepted queued work transfers
to operation without a second vacancy requirement. Public operation admission does not
evict active authority to fit four slots. Manual/backfill capacity waits retain demand
identity and existing lease; one-second physical retry permits progress when configuration
changes even without an external callback. The unchanged per-quantum turn updates remain
in force. These are source-reviewed transition claims, not a generalized fairness proof
under permanently unavailable capacity.

Completion retention is independent from recent-start ordering. Real callback input
requires valid CloseTime, and bounded ten-member retention sorts by canonical close time.
Only terminal completed metadata is deleted when membership expires; active work is not
reclaimed. An oldest late completion may evict itself, leaving the latest-operation ID
without a map entry; late acknowledgement then no-ops and a new admission continues.
The dedicated regression covers this case. Callback identity/terminal checks occur before
new payload validation, preserving harmless duplicate/stale delivery behavior. Payload
owner GC follows live references; terminal issuance payloads were already reclaimed.

Tests cover configured2/5/1000/nonpositive profiles, failed publication, dynamic capacity
retry, queue transfer, canonical close reordering, self-eviction and reload accounting.
Author reports full scheduler2 recursive PASS and real configured2/5 capacity witnesses;
reviewer did not rerun those gates. A metadata-cardinality-independent transaction bound
is NOT established: stateFor clones queue/manual metadata, retention scans operation
metadata and public projections scan retained actions; nonpositive public limits allow
input-dependent growth. The immediate pump bounds logical quanta, not bytes/CPU or all
public-state cardinality. Internal finite proofs do not generalize automatically. Author
design/evidence explicitly retain this M4 implementation/proof obligation.

## Denial before all selected public operations

Reviewed schedule2_authorization_test.go and selected adapter interfaces. All eight
public operations are independently overridden in the counting wrapper, including the
optional mutation interface; any reached dispatch increments the counter and returns a
distinct Internal error. Each real frontend call must instead return PermissionDenied
and the total dispatch counter remain zero. Requests have sufficient valid shape to avoid
confounding input validation failures. Test authorization is restored before environment
cleanup, so cleanup failures do not masquerade as API behavior. Atomic counter/checking
state safely crosses RPC goroutines. Parent reports PASS1.176s.

The fixture is an authorization-order witness; it is not independently an allowed-operation
persistence or backend-identity witness. Those are supplied by the separate real public
API tests. No original backend authorization rule, production routing, or deployment
configuration is changed by this test. No finding in this lightweight review.
