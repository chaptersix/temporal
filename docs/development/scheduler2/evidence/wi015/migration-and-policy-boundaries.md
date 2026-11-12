# Explicit legacy-only coverage boundaries

This document does not mark excluded tests as passed. Scheduler2 has an isolated
component identity and selected test frontend, no sentinel ownership handoff,
no production routing percentage and no migration implementation. The retained
legacy tests continue to own those mechanisms. M4 must keep migration unsupported.

Delete/Patch context metadata subcases involving V1-only ownership, sentinels,
both stacks and neither stack assert the old frontend's ownership arbitration.
The scheduler2 public obligation (attribute to the selected action, preserve
closed public NotFound, never fall back) has actual witnesses in context-metadata,
listing-ports and the accepted public persistence/identity profile. Those do not
claim the old ownership combinations or internal SchedulerService ErrClosed
signal. The selected backend intentionally has no such routing path.

CallbacksSuite.TestScheduledCallbackTokenMigration_LegacyWriteEnvelopeRead begins
with the legacy envelope-write flag disabled, then enables it while a running
workflow retains its old bare callback token. D59 explicitly selects request-ID
envelopes for every scheduler2 admission; it never writes the legacy bare token.
That old writer/reader migration is therefore unsupported, not a scheduler2
passing test. Actual request-ID token preservation through continue-as-new/reset,
completion-before-ack and callback-after-recreation is verified in the callback,
contract and physical recreation artifacts. Importing legacy callback ownership
would require a separately authorized and verified migration contract.

The V1 namespace start reservation limiter is an existing backend-specific policy;
rate-policy-investigation.md and independent-timecontrol-rate-review.md document
CHASM APS-only behavior and scheduler2's matching actual frontend transport.
V1-only memo-update rejection likewise remains named in listing-ports/review.md;
scheduler2 supports the existing CHASM replacement behavior and tests it positively.

Legacy creation/migration controls do not select the explicitly wired scheduler2
backend. TestSchedule2ReadsWithLegacyCreationDisabled verifies actual Describe/List
and persisted identity with both controls false. It does not enable migration,
change production routing, or claim a legacy routing-state transition experiment.
