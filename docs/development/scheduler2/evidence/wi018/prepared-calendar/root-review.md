# Independent prepared-calendar review

Root compared phase/carry/reset behavior with service/worker/scheduler/calendar.go compiledCalendar.next, including one-hour fold correction, skipped-hour handling, short-circuit day matching, explicit/wildcard years and local2100 exhaustion. No remaining semantic discrepancy identified in this standalone primitive scope. Root's conversion-count finding is fixed by counting the high-level Date/Clock/Weekday/construction calls actually made; underlying Go timezone lookup work remains unbounded by this metric.

The earlier differential tests omitted required wildcard calendar fields and could succeed only on exhaustion. Those runs are not evidence of real candidate equivalence. Final tests use explicit fields, require nonempty reference candidates where appropriate, compare single/mixed budgets with continuation roundtrips, and independently assert six known NY/London fold/gap instants. Separate horizon tests demonstrate raw NY candidate2101-01-01T00:00Z and public CompiledSpec UTC-year exhaustion. Future union/public integration must preserve that wrapper check.

Verified all manifest hashes against source and independently ran CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm/lib/scheduler2/internal/occurrence -count=1: PASS1.050s. Author full final run passes1.100s and scoped lint normal/minimal/vet zero issues.

Accepted as an unselected pure resumable-calendar prerequisite only. Named timezone resolution/refresh integration, bounded prepared TZif lookup, union/exclusion/jitter continuation, stored list previews and scheduler runtime integration remain required. A supplied time.Location does not establish input-independent timezone work.
