# Plan 3 Cancellation And Deadline Results

OBSERVATION: Campaign 20260711-plan3-operational-red-team-b960841d9-cancellation-deadline covered cancellation before validation, during component satisfiability, during effective-set proof, before the first result, after one result, during exclusion retries, at N-1, and across ten workers.

OBSERVATION: Cancellation was checked before the active phase budget at every existing work-tick boundary. Once the deterministic hook made cancellation observable, zero additional budgeted ticks were consumed; cancellation checks are separately reported.

OBSERVATION: Expired, 1 ms, 10 ms, 100 ms, 1 second, and sufficient deadlines ran against low, transition, and maximum-work cases. The recorded wall-clock outcomes are host-specific and paired with validation and matching work.

NEGATIVE EVIDENCE: No cancellation or deadline returned Complete=true, no partial prefix was reported as complete, and the simultaneous ten-worker case retained identical stopping work.

RECOMMENDATION: Preserve context.Canceled, context.DeadlineExceeded, validation indeterminacy, and matching exhaustion as distinct outcomes. Select no production polling interval or guard in Plan 3.
