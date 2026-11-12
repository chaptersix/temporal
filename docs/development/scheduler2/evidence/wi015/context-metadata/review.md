# Public attribution and closed-mutation evidence

Patch/Delete actual frontend trailers initially lacked workflow-type and
workflow-task-queue. The new ContextMetadata provider reads only the public
action skeleton, mirrors existing CHASM metadata keys/empty omission, and retains
closed-schedule attribution. It replaces the old nil stub in lifecycle.go. Unit
coverage verifies no payload-owner dereference and absent/empty/closed handling.
Independent api_seam review reports source parity; review artifact is separate.

The additional actual frontend comparison creates/deletes one schedule per backend,
then verifies Update and Patch return NotFound on both old CHASM and scheduler2.
The migration suite's direct SchedulerService ErrClosed/FailedPrecondition is an
internal fallback signal: frontend isSchedulerErrorLegacyRoutable routes that
signal to the V1 missing-schedule branch. The selected scheduler2 seam has no
migration fallback and returns public NotFound directly. This is explained public
parity, not a claim to implement old SchedulerService migration semantics. Sentinel
ownership combinations are excluded from this public-only port and remain
explicitly unsupported under the migration boundary.
