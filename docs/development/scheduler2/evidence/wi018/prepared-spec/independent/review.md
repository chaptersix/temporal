# Independent prepared whole-spec review

Reviewed source against legacy spec.go GetNextTime/rawNextTime/excluded/addJitter, calendar.go next/matches, candidate.go NextTimeCandidate, occurrence Enumerator.probe and the pinned go-farm Hash32 source.

Found and reproduced an exhausted-result mismatch: the sole Jan1 midnight2024 calendar, excluded by itself, after2023-12-31T23:59:59Z returned a stale Jan1 Nominal with Found=false/Done=true; legacy returns zero Nominal/Next. Original overlay test failed0.630s. Calendar owner repaired exhaustion to clear Nominal and added a full-result regression. Independent full occurrence suite plus unchanged overlay passed1.335s on corrected manifest; overlay verifies budgets1/7/64. Original repro/source and both manifests retained. Review edited no calendar source.

No additional concrete defect found. Start subtraction1second versus automatic1nanosecond matches intended wrappers. Signed normalized interval arithmetic matches legacy operators. Exclusion uses resolved wall time, following raw lookup bypasses exclusion/end wrappers, jitter tail initialization/20byte recurrence/finalization matches the dependency. Per quantum each phase reads at most one term or seed window plus fixed calendar conversion work; whole input continuation is returned on read error. Source/zone identities fence unfinished evaluation; completed results are zone independent.

Limitations: immutable term/seed ownership validation and continuation semantic history remain trusted capability obligations. Durable codec, persisted-reader addressing, ingestion/publication, runtime continuation integration and full transaction/storage bounds are separate gates. Pure suite success does not establish those gates.

Reproduce overlay: map chasm/lib/scheduler2/internal/occurrence/independent_review_test.go to overlay-test.go.txt using go -overlay; run CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 -overlay <mapping.json> ./chasm/lib/scheduler2/internal/occurrence -count=1.
