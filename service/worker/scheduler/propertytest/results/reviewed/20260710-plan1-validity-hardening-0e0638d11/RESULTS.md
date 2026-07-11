# Plan 1 Validity And Property Hardening Results

## Campaign

- Campaign ID: 20260710-plan1-validity-hardening-0e0638d11
- Plan: plan1
- Work definition: v1 / validator-v1
- Reviewed cases: 6 curated regression cases

## Observations

### OBSERVATION: Revised validity classifications hold

- Case IDs: plan1-empty-inclusion, plan1-february-30, plan1-fully-excluded, plan1-valid-empty-query
- Result: empty inclusions and impossible components are invalid-unsatisfiable-component; a fully excluded effective set is invalid-effective-set-empty; query-local emptiness remains valid success.

### OBSERVATION: Validation budgets remain distinct

- Case ID: plan1-fully-excluded
- Result: budget 100 is indeterminate; a sufficient proof budget classifies the effective set as empty without entering matching computation.

### OBSERVATION: Civil-date iteration must not carry normalized midnight offsets

- Case ID: plan1-apia-end-boundary
- Regression failfile: testdata/rapid/TestExperimentBudgetMatrix/TestExperimentBudgetMatrix-20260710214418-23034.fail
- Result: an occurrence exactly at the schedule end in Pacific/Apia remains a valid witness.

### OBSERVATION: Lord Howe repeated-half-hour matching diverges from the oracle

- Case ID: plan1-lord-howe-repeated-half-hour
- Result: the independent oracle includes the first repeated 01:30 occurrence; copied and production matching omit it. Production remains unchanged.

## Negative Evidence

### NEGATIVE EVIDENCE: No reduced-domain differential mismatch

- Search space: constructed valid and fully excluded schedules with horizons of 1-120 seconds.
- Checks: 50,000.
- Result: no classification or witness mismatch.

### NEGATIVE EVIDENCE: No full property counterexample

- Checks: 10,000 per Rapid property.
- Result: no counterexample after retained failfiles were replayed.

### NEGATIVE EVIDENCE: Bounded fuzz campaigns completed

- Validation fuzz: 30 seconds, 9,712 executions, 2 new interesting inputs after a 28-input baseline; throughput plateaued during the finite run.
- Matching oracle fuzz: 30 seconds, 1,207 executions, no new interesting inputs after a 33-input baseline; throughput plateaued during the finite run.

## Property Lifecycle

- Valid generators changed from field-independent construction to witness-first construction.
- Query-local empty success was separated from global satisfiability.
- Eleven named mutation probes killed all planned mutations in TestMutationKillMatrix.

## Reproduction

Run the commands recorded in campaign.json from repository revision 0e0638d119313d3c185ac5ca4a25053d32a3b4c7 with the recorded dirty patch checksum.
