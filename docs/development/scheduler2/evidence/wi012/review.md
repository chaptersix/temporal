# WI-012 independent setup/source review

Reviewed the descriptor implementation and changed callers against baseline
`352dafa83`. Reviewer owns the functional inventory but did not author the setup
refactor. This review does not independently approve the inventory it authored.
No tests were executed by this reviewer; parent logs own compilation, lint and local
SQLite functional results. This is source review, not scheduler2 frontend evidence.

## Finding resolved during review

`ScheduleV1Suite.TestSkipsCHASMSentinelWhenDisabled` creates a real V1 schedule and
then a real CHASM schedule through SchedulerClient. The first refactor declared
only the V1 backend, omitting the second cleanup route. It now declares both
`scheduleV1Backend` and `scheduleCHASMBackend`. Verified the changed declaration
against the actual second CreateSchedule call. This was a setup/cleanup parity bug,
not an assertion change. Intermediate undefined backend locals in IdleClose and
PausedBehavior were also corrected before the implementer's ready signal.

## Behavior reviewed

- Shared suite entrypoints pass explicit descriptors into the same shared methods;
  boolean selector and test-name-prefix worker inference are gone. Existing
  context factories remain unchanged. There is no scheduler2 descriptor alias,
  placeholder pass, or fallback implementation introduced.
- `newScheduleEnv` applies common options once; `scheduleCommonOpts` requires a
  nonempty descriptor list and adds the V1 worker once if any descriptor needs it.
  Mixed carryover comparison, creation rollout and migration fixtures declare
  both backends. Existing explicit dynamic-config options retain their values and
  override order after common setup. Audit and workflow-pause helpers carry the
  descriptor through creation and cleanup; the scheduled callback-token test
  explicitly selects existing CHASM.
- Cleanup copies the descriptor slice, runs independently of the canceled test
  context with a30s deadline per backend, and uses the descriptor's context for
  both List and Delete. It gathers all pages before deleting, uses page size1000,
  accepts NotFound, and aggregates other deletion errors while continuing through
  collected IDs. List failures stop that backend's deletion pass; the next backend
  still gets cleanup. Errors fail a previously successful test or log on an
  already-failed test, preserving previous behavior.
- Existing public assertions and the existing admin migration skip are preserved.
  Changes to previously raw NewEnv schedule fixtures add explicit cleanup/setup;
  no shared behavioral assertion was removed to accommodate the descriptor.

## Discriminating verification present in source

`TestScheduleBackendDescriptorAndCleanup` uses arbitrary subtest names, checks
backend experiment headers on both List and Delete, checks request namespace/page
size/tokens, and requires both pages to be collected before deletion. NotFound is
accepted. `TestScheduleBackendCleanupFailureAndCancellation` exercises canceled
List with no deletion and aggregate Delete failures without stopping after the first.
These are client-boundary tests, not proof of real visibility persistence behavior.

`TestScheduleDescriptorStartsLegacyWorker` deliberately lacks the former
`TestScheduleV1` prefix and executes the actual shared active-trigger scenario.
That helper requires public Create/Patch plus child execution/count assertions, so
worker setup is tested through behavior rather than merely checking `worker=true`.
Parent must record its local SQLite result plus representative V1/current-CHASM
shared scenarios and affected-package compile/lint before acceptance.

## Limits and recommendation

No remaining source-level blocker found for the WI-012 setup increment after the
mixed-backend correction. Existing V1 context is an identity wrapper, not a routing
force against arbitrary dynamic configuration; preserving that behavior is deliberate.
Scheduler2 needs actual frontend routing, typed List/Count, identity/fallback checks
and public scenario outcomes in its dependent item. These tests do not prove those.
Cleanup is deadline-bounded through the real gRPC client, not a fixed number of pages;
eventual visibility can still omit newly created entries, as before. A source review
cannot claim every functional scenario passed, nor full M3 completion.

Conditional recommendation: accept this setup change once parent compile, scoped lint
and required local functional evidence pass and the baseline inventory receives its
separate parent audit. Preserve missing scheduler2 capabilities as pending coverage.

## Reviewed file fingerprints

- `tests/schedule_backend_test.go`: `7379ea05ab53b33bb1c2d30627bae8237f47882d90eb8efecc88e624a0b48fa6`
- `tests/schedule_test.go`: `3f945dd83b9bf61de470a3a7ae72e646d10585fad8a3fddecb1554e19bdd37af`
- `tests/schedule_migration_test.go`: `12710bf9272f1aee66c8db3e2eb974ad421f25be4d522b04d7fc265ae10dc98e`
- `tests/schedule_workflow_pause_interaction_test.go`: `12119056dc5adadfe986da0cfd96762cca5ef3e1e701c4a9031f891454564a7c`
- `tests/schedule_audit_test.go`: `eadf884b797e80f7f2a1212970437e6d1789527e642bd9b1edb5719fa07c19b9`
- `tests/callbacks_test.go`: `4cc6eff04c119a6b58c0cd16123d6e7148be65816a18b18074e5c7c32cc283bd`

## Final descriptor refinements reviewed

Reviewed the final two changes after the original fingerprint snapshot. Both preserve
existing backend behavior and remove residual backend-name branching:

- `preserveResultOnEmptySuccess` is true only for the V1 descriptor and false for
  current CHASM. The no-result carryover assertion still expects the previous value
  for V1 and absence for CHASM, while explicit null expectations remain unchanged.
  This is a named baseline semantic difference, not permission to weaken scheduler2
  expectations when its descriptor is later added.
- `sdkOptions` supplies EnableCHASMSchedulerCreation=true only for the CHASM SDK
  creation fixture. V1 supplies no additional SDK options. `newScheduleEnv` still
  applies common options once and appends the supplied options afterward, preserving
  the previous creation routing configuration and assertion body. The slice is read
  and expanded into environment setup, not mutated in these paths.

No new source blocker found. Parent reports both complete existing shared suites
passed before these two refinements; focused affected cases and lint were underway
at this review. Their final pass remains execution evidence owned by parent logs.
No test was run by this reviewer.

Final reviewed fingerprints for the two changed files:
- `tests/schedule_backend_test.go`: `0eccdf2fe58eded700a35f9e655b5f11274f60003bbd46b89e791b2aecf3ed87`
- `tests/schedule_test.go`: `20c8a9feb80eb516f8e08187f6725d7703f4f39b287978f0534883003001fc65`
