# Runtime integration review checkpoint

Integrator reviewed the additive internal first-run metadata transport, runtime
dependency binding, logical wake payload validation and parent callback integration
against the source paths documented in the investigations. The first-run value comes
from persisted canonical execution metadata with the existing deprecated fallback;
both callback implementations and History preserve it. External HTTP serialization
does not expose the additional internal option. Runtime dependencies bind before
service start, with explicit test selection and independent frontend registration.

Scheduled queue timestamps are physical delivery metadata and can be rounded/clamped
by shard task-key allocation. Exact logical WakeTime now travels in the task payload;
validation still checks operation/effect identity and current pending wake. Issuance
checks execution-clock readiness and deadline separately. The real transport check
exercised this difference after the original exact-physical-time implementation
dropped live start tasks. No zero-wake compatibility fallback is introduced for this
undeployed prototype.

The newer public execution constructor creates the generator itself. The old smoke
setup's same-revision typed ConfigureGeneration consequently failed as expected; the
retained constructor-transition log records this. The test now uses public Create
and Patch exclusively to issue actions and passes10.481s. Typed reads remain for
independent persisted operation/carryover observations. Both normal completion and
continue-as-new retain the held-ack ordering and second-start merge barrier. This is
a directed public transport test, not full shared-suite acceptance.

Consolidated lint passes. Earlier runtime race evidence predates public control
implementation; final controls, projections, aggregate-capacity behavior and full
shared-suite/race remain required. No WI015 or M3 acceptance is asserted here.
