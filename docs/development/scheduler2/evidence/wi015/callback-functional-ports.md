# Callback chain functional ports

The original double-reset, reset/continue-as-new and additional-callback helpers now
accept testing.T and a backend descriptor. Existing CHASM wrappers retain their cases.
Suite Await assertions were translated to await.Require with the same timeouts and
assertions. TestSchedule2CallbackChains runs all seven variants against the explicitly
installed scheduler2 frontend; after each Create, its descriptor asserts typed
scheduler2 read identity and the actual database archetype through Admin DescribeMutableState.
No per-case skips or old scheduler fallback are introduced.

Command: `CGO_ENABLED=0 go test -tags test_dep,integration ./tests -run
'^TestSchedule2CallbackChains$' -count=1 -timeout 4m`.
Initial log: `/private/tmp/scheduler2-wi015-callback-ports01.log`; PASS (exact package timing in archived log).
Scheduled workflow continue-as-new is separately ported by proof_review in
TestSchedule2ContractContinueAsNewCompletion.

## Token migration source scope

CallbacksSuite.TestScheduledCallbackTokenMigration_LegacyWriteEnvelopeRead creates an
old scheduler action with EncodeInternalTokenWithEnvelope=false, explicitly verifies
its token has no embedded request ID, then flips the flag and observes completion.
The scheduler2 admission path instead calls GenerateNexusCallback(ref, op.ID, true)
(component.go) to freeze mandatory request identity, regardless of this legacy gate.
Thus these new-backend ports do not certify that legacy-token migration scenario.
This remains an explicit migration/legacy-compatibility boundary, not a test skip or
claim that a gate-flip with a scheduler2 envelope is equivalent evidence. No migration
or production routing was performed. The broader coverage ledger must retain that
source scenario's unproven boundary until an applicable compatibility contract exists.

Archived log SHA256: `07992b693db86368d817aa83f72eb7c86c0345e476b86356715c79029421c2e7`.
