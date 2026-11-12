# WI015 time-skipping integration

Scope: D59 actual execution clock/configuration support in the explicitly registered local scheduler2 service path. This evidence does not accept WI015 or authorize default routing.

## Source findings and implementation

`service/history/workflow/timeskipping.go:IsTimeSkippingFeatureEnabled` previously recognized only workflow and original scheduler identities. The new branch recognizes the generated `scheduler2.scheduler` identity only if present in the current shard's CHASM registry, then uses the existing namespace `CHASMSchedulerTimeSkippingEnabled` flag. Unknown identities, absent registry, absent registration and disabled namespace flags remain false. No registration/default configuration/routing is added here.

`chasm/lib/scheduler2/activity.go:startSnapshot` previously captured propagated duration solely for post-RPC timestamp reconstruction and discarded the configuration. It now forwards cloned configuration and propagation into the freshly decoded issuance request. The context's real `PropagateTimeSkippingToOtherExecution` implementation remains responsible for disabling propagation and stripping parent fast-forward configuration. Persisted admission request/callback bytes remain unchanged. The actual RPC observation uses the same captured offset, as in the legacy invoker.

The first real integration run exposed a second missing contract: `chasm/tree.go:closeTransactionHandleTimeSkipping` requires `TimeSkippingRuntimeGate` when enabled. Both public initial-patch and later-patch paths failed with `time skipping is not implemented for current execution type`. A separate scheduler2 `timeskipping.go` now implements that gate. It refuses advancement while closed, paused, uninitialized, with unresolved/tracked work, pending starts/effects/queues/manuals/backfills, reserved or due coordinator work, or due generator work. Future coordinator/generator deadlines remain eligible targets after internal work settles. This uses bounded metadata summaries and mirrors the legacy scheduler's status/backfill/invoker/readiness checks; it does not load workflow request payloads.

## Checks and limits

Commands run from `/private/tmp/sch-redesign-execution` with `CGO_ENABLED=0 GOMAXPROCS=4`:

- `go test -tags test_dep -p 2 ./chasm/lib/scheduler2 ./service/history/workflow -run 'TestActivity|TestMutableStateSuite/TestIsTimeSkippingFeatureEnabled' -count=1`: PASS (timeskipping03.log, scheduler2 0.962s, History 0.688s). Tests cover namespace flag/registration boundaries, clone isolation, durable request preservation, and actual StartHandler outgoing config/offset plus post-RPC snapshot-clock reconstruction.
- `go test -tags test_dep ./chasm/lib/scheduler2 -run 'TestTimeSkippingRuntimeGate|TestActivity' -count=1`: PASS (timeskipping04.log, 0.827s). Runtime-gate table includes each blocking summary, due/exact boundary, future wake and settled exhausted source.
- One intermediate compile attempt overlapped another owner's proto regeneration and failed missing generated API packages. It is not treated as a behavioral failure or a passing check.
- `go test -tags test_dep,integration -p 2 ./tests -run '^TestSchedule2TimeSkippingPropagation$' -count=1 -timeout=3m`: functional01 records the missing runtime-gate failure above. Functional02 reached actual workflow starts but failed an incorrect test assertion that parent configuration remained enabled after fast-forward completion. `service/history/workflow/timeskipping.go` intentionally disables the parent at its target. Functional03 corrects that expectation, preserves all child propagation checks, and PASSes both initial/later patch variants (9.367s).
- `make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false`: lint01 found only two other-owner `public_projection.go` revive findings. Sent to owner; lint02 found only the remaining other-owner `publicDispositionCounts` complexity finding. Own source had no findings; consolidated final lint pending.

The dedicated real integration test uses both initial and later patches, a weekly schedule with its first tick twelve hours ahead, and a two-day fast-forward target. It requires an actual child's one-day timer to complete, inspects persisted child initial skipped duration and enabled configuration, and checks parent fast-forward configuration does not propagate. This is a causal single-tick propagation test, not a claim of all shared weekly/backfill/overlap scenarios.

Raw logs are archived in `time-skipping/`. Functional assertions establish real child execution and persisted clock state, not merely an intercepted outgoing request.

Source freeze fingerprints for the seven owned implementation/test files are in `time-skipping/source-hashes.json`. No changes to component.go/tasks.go or production routing were made by this slice.
