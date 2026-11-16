# Occurrence test fixtures

`America_New_York.tzif` was copied from the local
`/usr/share/zoneinfo/America/New_York` during WI-002. Its SHA-256 is
`e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95`.
The originating timezone database release is not asserted. Tests embed these
exact bytes and verify their digest, so later host timezone changes do not
change the selected 2024 US gap/fold expectations. The timezone conversion
primitive is shared with production; literal UTC expectations independently
check the selected gap/fold cases.

`generator-regressions/TestEnumerationProperties-20261110193611-6103.fail`
preserves the initial Rapid generator failure: period=1, phase=1 produced an
invalid interval. It is outside Rapid's automatic replay directory because it
records a generator defect, not a valid product scenario. The generator now
chooses phase in [0, period); `TestMalformedDefinitionsRejected/phase_equals_period`
retains the explicit invalid-input expectation. Initial run output is recorded
by the integrator in `/private/tmp/scheduler2-wi002-initial-tests.log`.
