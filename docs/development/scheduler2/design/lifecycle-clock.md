# WI-010 lifecycle clock and first-start evidence

Investigation/design proposal; no code or accepted compatibility claim. Source
references below are repository-relative. The starter's existing `(chain, error)`
signature can remain unchanged.

## What the reference actually measures

- `chasm/lib/scheduler/invoker_tasks.go:740–754` samples `time.Now()` **after** a
  successful StartWorkflow RPC and adds the pre-RPC propagation snapshot's
  `InitialSkippedDuration`. This is successful-start observation time, not the
  workflow's authoritative server start timestamp or scheduled/jittered time.
- `chasm/timeskipping.go:47–65` constructs that propagation duration from accumulated
  skipped time. It remains available even when propagation config is disabled.
  `chasm/context.go:233–237` exposes the snapshot through
  `GetTimeSkippingPropagateState` inside a CHASM read transaction.
- `chasm.Context.Now` is captured transaction time (`context.go:205–207`), not an
  independently advancing clock available from ordinary `context.Context` outside
  the execution lock. `Node.Now` delegates to backend time (`tree.go:1771–1775`).
- `invoker.go:215–234` accepts the first start result for an entry with empty RunId,
  computes maximum StartTime and ignores duplicates. `invoker_tasks.go:262–264`
  persists that maximum before start-only history/removal. `scheduler.go:659`
  exposes StartTime as ScheduleActionResult.ActualTime.

## Minimal clock adapter choices

**Closest reference mapping:** make the StartHandler's existing keyed ReadComponent
return a private issuance snapshot containing cloned request and skipped duration.
After successful RPC, sample an injectable wall clock and add the captured duration.
Set an explicitly present `StartObservedAt` field on the acknowledgment event.
This preserves the starter API and directly reproduces the reference formula.
Validate timestamp conversion/overflow; do not manufacture zero as missing evidence.
The snapshot is taken only after the local permit, with the same request/deadline
validation as today. No stored request bytes need changing. Snapshot time-skipping
metadata does not justify loading unrelated operation payloads.

**Virtual-time snapshot alternative:** return `virtualReadNow=ctx.Now(s)` together
with a contemporaneous `wallReadNow=clock.Now()`; after RPC compute
`virtualReadNow + (wallAfterRPC-wallReadNow)`. A Go monotonic time component prevents
wall-clock adjustment from reversing elapsed duration; test injection must supply
both samples coherently. This supports fake execution clocks with a small adapter,
but it is not exactly the reference formula: transaction clock capture predates the
wall sample, and reference uses the post-RPC wall clock plus accumulated offset.
It also cannot observe virtual jumps after the snapshot. Document this as the chosen
local clock mapping if used, rather than claiming exact wall-plus-offset parity.

Do not use the result UpdateComponent's `ctx.Now` as if it were RPC observation:
lock/queue delay becomes part of the timestamp. Do not use operation nominal,
jittered actual, admission time, or a time supplied by arbitrary completion delivery
as equivalent start-observation evidence. Time-skipping enablement/runtime gating is
a separate contract; a snapshot does not prove no virtual jumps occur during RPC.

## Early completion and terminal metadata

The old completion path does not synthesize StartTime. `completion.go:165–218`
finds an outstanding buffered entry and records completion; tracked completion
keeps that entry (`invoker.go:277–303`). If the start result arrives while its RunId
is still empty and the entry remains retained, `recordExecuteResult` can still fill
StartTime and return the anchor. An early untracked completion removes its entry,
and retention can remove tracked entries, so the old code does not establish an
unconditional eventual-observation guarantee for every callback ordering.

Current scheduler2 is different by design: completion binds the chain/counts start
once, and `decision.go:230–234` ignores all results for Completed/StartOnly records.
To accept a first later start timestamp without reopening completion accounting,
introduce a narrowly specified metadata merge:

1. Only a matching-incarnation, retained operation and matching nonempty chain can
   accept an explicitly present start observation; no unvalidated terminal payload
   is decoded. Preserve existing stale/wrong-chain terminal callback no-op behavior.
2. Persist `StartObservationRecorded` independently of completion phase. A first
   matching acknowledgment can fill it after Completed; repeated acknowledgments
   cannot move it. Do not change Started/Completed counters, carryover, pause,
   operation occupancy, terminal phase, effect state or immutable requests.
3. Advance the root anchor by max(old anchor, observation), re-evaluating idle
   scheduling atomically. Eviction must never regress the root anchor.

This is a **new explicit exception** to blanket terminal-result no-op. It needs
independent review and regression coverage. If the terminal record is already
removed, the current bounded retention contract says old messages are ignored;
this proposal does not require an unbounded observation ledger.

Completion alone proves the workflow ran and carries status/result/chain evidence,
but the current reducer event has no trusted start-observation timestamp. There is
no source-backed exact timestamp to reconstruct. Choose and record one policy:
retain the create/update anchor until start evidence arrives; or conservatively
advance once on first completion observation (documented retention extension,
not reference parity). Neither repeated callbacks nor late duplicate metadata may
refresh retention indefinitely. If closure must await unresolved start evidence,
that is a separate idle-eligibility/retention decision, including missing/evicted ack.

## Discriminating tests

- Controlled wall/virtual samples: RPC elapsed time contributes; waiting for the
  result transaction does not. Offset/time-skipping and timestamp limits are explicit.
- First acknowledgment, repeated acknowledgment with later timestamp, early callback
  followed by acknowledgment, wrong-chain terminal acknowledgment, and evicted result.
- No counter/carryover/phase changes from terminal metadata merge; failed publication
  rolls back both observation marker and root anchor/timer.
- Historical backfill and future manual scheduled identity do not determine anchor.
- Retention removes the latest observed record; root anchor and idle deadline persist.

No tests were run for this investigation. No lifecycle contract is accepted here.

## D52 implementation mapping

The activity/idle increment implements the closest reference mapping above: a
locked snapshot supplies the skipped duration, and StartHandler samples its
injectable wall clock immediately after a successful RPC. The constructor has a
local, documented forbidigo exception for this post-RPC observation; transaction
clock reads continue to use ctx.Now. A timestamp outside int64 Unix nanoseconds
leaves the durable pending intent and immutable retry request intact.

ConfigureIdle changes retention duration without refreshing an existing activity
anchor. It initializes an absent anchor only for low-level roots; NewExecution
initializes the anchor from the execution clock when History allocates the root.
Completion alone retains the known anchor, as specified by D52–D54.
