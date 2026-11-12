# WI-015 live projection follow-up investigation

Read current public.go/component.go, decision acceptResult/activity semantics, the
legacy CHASM Describe/List projections and shared ScheduleSuite assertions. PublicInfo
and PublicList currently preserve mostly creation-time data; Describe updates state
controls and state-size/time-skipping, while Memo only overlays Paused. Live actions,
running workflow views, recent history, future times and counters remain required.

Source-backed implementation constraints:

- Root Started increments once when a previously unbound operation first gains
  ChainID via positive acknowledgment/completion. It is the natural lifetime action
  count source, not Operations map length or current admission count.
- RunningWorkflows should select positively started tracked operations and exclude
  StartOnly/Completed records; the accepted ALLOW_ALL contract reports recent starts
  without tracking them as running. Pending/Ambiguous intent is not proof of a start.
- Legacy recent results retain scheduled actual time, observed start time, workflow
  ID/run ID and status, sorted by observed actual time, with10 Describe/5 List entries.
  Current Operation metadata retains nominal/chain identity and observed-start metadata,
  but not all of these projection values. A first-execution chain fence must not be
  silently substituted for the actual run ID in a public recent result.
- Four terminal-operation retention slots cannot reconstruct a ten-entry public recent
  history after eviction. Store bounded public history separately (the PublicInfo DATA
  partition can hold ScheduleActionResult), updating it atomically with accepted
  lifecycle events. Do not materialize immutable request/carryover payloads merely to
  construct compact List/Memo views.
- Future times must honor current spec/update boundary/remaining action count, retain
  read-only Describe behavior, and publish a compact List projection in the same
  mutation. OverlapSkipped/MissedCatchupWindow must follow actual disposition decisions;
  do not simply relabel BufferDropped/CapacityDropped/OverlapSuperseded counters without
  establishing that accounting matches the accepted public definition.

A disjoint subsequent implementation area is a new public_projection.go and dedicated
tests for detached read-only projection and bounded recent-action/list helpers. Parent
keeps public.go/component/schema transition wiring; the helper receives explicit
accepted lifecycle metadata rather than guessing missing IDs/times. Agree the small
metadata hook before coding. Shared ScheduleSuite running/ALLOW_ALL, recent-action,
buffer-size and future-time tests provide concrete expected behavior. This is a next
area recommendation, not implementation or a narrowed WI-015 acceptance proposal.

The immediate assigned next task is instead removal of prototype byte ceilings in
completion/effects/generator code; those files are disjoint from parent counter/schema
work and do not change these projection requirements.
