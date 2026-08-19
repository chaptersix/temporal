# CHASM scheduler property-test capabilities

| Historical behavior | Detector | Minimized evidence | Fixed behavior |
| --- | --- | --- | --- |
| Deferred `BUFFER_ONE` occupancy (`cf33102272`) | `TestPropertyDirectedDeferredBufferOneOccupancy` and occurrence accounting | A deferred `BUFFER_ONE` occurrence plus one new `BUFFER_ONE` occurrence while a workflow runs must retain at most one pending occurrence. | The planner treats the deferred occurrence as pending before selecting the new buffer entry. |
| Completed retained history consumes capacity (`f3d7f01b18`) | Existing `TestCompletedHistoryDoesNotConsumeBackfillCapacity` plus property-runner capability coverage | Ten retained completions with `MaxBufferSize=20` leave positive Backfiller admission capacity. | Admission counts actionable starts, not retained completions. |
