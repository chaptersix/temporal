# Independent public backfill batch review

Non-author review of the added backfill grouping, context getter, coordinator arbitration, reducer marker, revision reset, counters, schema and tests. No blocking implementation finding within the parent-approved `design/backfill-overlap-batches.md` contract. This does not independently approve the original projection implementation by this reviewer.

Source comparison confirms the reference computes available=max(0,MaxBufferSize/2−max(0,buffered−10)−reserve), divided across active range backfillers (manual triggers excluded). New representation counts its queue and retained non-StartOnly records; its smaller caps make that input at most8. This mapping is expressly documented and does not prove arbitrary old/new full-buffer equivalence. Dynamic settings are sampled at new logical group boundaries through registered namespace getters; no persisted frozen configuration or invented positive minimum is introduced.

A group is public-only and SKIP-only. First eligible handoff samples capacity and obeys ordinary overlap/capacity arbitration; later eligible handoffs carry the persisted remaining/revision marker even if the first workflow already completed. The marker allocates no operation and debits no action budget. Definition revision change and resolved non-SKIP policy break suppression. Excluded raw values do not consume candidate membership. Finishing/rebasing/deleting a range removes its authority. This is a bounded logical batch, not a permanent whole-range one-start latch.

Demand construction retains vacancy and cyclic work arbitration, excludes operation-slot demand only for authorized skip disposition, and schedules a one-second retry if a fresh group has zero capacity. Transfer reads the newly published range pointer after root replacement, then decrements/resets group metadata and disposes the candidate within the same framework transaction. Failure/rollback must discard both. Suppression cannot produce duplicate starts; direct reducer marker acceptance is scoped to DispatchBackfill with resolved SKIP, while public coordinator owns marker production. Public overlap accounting includes these dispositions independently of whether a blocker is still present.

## Checked evidence

Read targeted source tests and archived shared log: unchanged shared SkipOverlap PASS10.249s. Tests force completion after each candidate, demonstrate default450 gives1/5 starts and configured2 gives3/5, reload and failed publication preserve membership, existing blocker suppresses the group, zero capacity advances no candidate and recovers on config change, revision/policy replacement reopens correctly, and active range count divides capacity. The directed read-only overlay disables suppression; mutant log contains expected1/actual5 failure (also configured boundary sensitivity). No shrink or exhaustive proof is claimed.

Finite coordinator/Rapid/FizzBee fixtures remain internal PublicRevision=0 per-candidate semantics. They do not prove this public counter extension. Enumeration proofs concern candidate/disposition streams, not public overlap group equivalence. Parent owns consolidated package/functional/lint evidence and acceptance. No code or tests changed by this review.

## Source snapshot

- `chasm/lib/scheduler2/backfill_batch.go`: `f824e778c80772ca7971e74b8c13bb646c6cdef773afb78d413979c4b7c5a43b`
- `chasm/lib/scheduler2/backfill_batch_test.go`: `83e7d01f03a60b53958fbb9de29679ba3d34c5257690f54ea0063f54791404b1`
- `chasm/lib/scheduler2/backfill.go`: `7a75ee0ba40f0614ea4b6207d7b65d873e0c38e455bc542a501f070d4e444d10`
- `chasm/lib/scheduler2/coordinator.go`: `a7982e1ad8f9e2217e46169c500a23463812027c9e578fc5847bef7c4de7a601`
- `chasm/lib/scheduler2/internal/decision/manual.go`: `1d1b00a401a30d0cc1baa39093913286dafe18d37bb4c013dd8b8ac8e14a5d17`
- `chasm/lib/scheduler2/internal/decision/decision.go`: `e422ebf93545031451ded07a3388e0ec383a7e264a6c83e4f520e5e1b8643691`
- `chasm/lib/scheduler2/public_projection.go`: `4ff697a6d3e0e4f6fbd6b5f6faee18cddce373a05aa18ebd3cc2edd50b6793dd`
- `chasm/lib/scheduler2/library.go`: `ae4ce93663adff667d94c6e1ed99d4b6156a83c5452786ec0e60d901a1c46c7d`
- `chasm/lib/scheduler2/public_api_test_dep.go`: `1f774a4c7cf8d4bcb31f785c7afdb04f21d66eb4f13a7ade9e115ad343277aee`
- `chasm/lib/scheduler2/proto/v1/state.proto`: `6d5a7fd647d17969b59871f6590eb7a4eee34d4bd830ce9596ca32f959d3e54a`
- `chasm/lib/scheduler2/gen/scheduler2pb/v1/state.pb.go`: `21eb8869fdaf3329e2b023ad2a376a9bd8d5cd51d94a21cfb3d6657373edf67e`

Drift from author manifest at review: none.
