# Independent visibility index review

Reviewer: api_seam. Reviewed public_visibility.go, public_visibility_test.go, library.go,
public projection retention/callers, and old scheduler.SearchAttributes plus Invoker
runningWorkflowExecutions/bufferedStartsCount. No source edits by reviewer.

The three original indexes use the same logical names and typed storage slots as the
old scheduler. Next time reads the compact published list preview. Running counts only
acknowledged PHASE_STARTED operations with public run identity; StartOnly ALLOW_ALL
records are excluded, matching the old completion-tracked running list. Active operations
are retained independently of recent-action truncation, so the bounded recent history
cannot hide a running workflow. Buffered count is queue plus unconfirmed starts; this
matches the old durable successful-acknowledgement boundary. Counts are emitted even
at zero, and closed roots omit them. Native execution status remains a framework field,
not the old scheduler custom execution-status search attribute.

## Resolved scope gap: idle-close index

Initial source omitted ScheduleIdleCloseTime, which the old scheduler publishes. Parent
added publicIdleCloseTime in DateTime02 and proof_review registered it on the root.
The source correctly gates it on !Closed && IdleArmed: reconcileIdle clears IdleArmed
when disabled/ineligible but deliberately retains IdleAt, so timestamp-only projection
would emit a stale deadline. Added unit assertions distinguish armed/unarmed/closed.
This resolves the source-level index gap. No remaining behavioral finding in these
indexes. Exact physical idle-close behavior remains the parent's separately owned gate.

The unit regression covers pending→acknowledged→completed counts and closed omission.
Oracle reports five actual visibility ports PASS5.315s; this reviewer did not execute
those tests and does not promote that report to independent physical evidence. Snapshot
hashes below capture the reviewed idle addition, preserving earlier author manifests.

`chasm/lib/scheduler2/public_visibility.go`: `318b7a13e018097fb824aacf45c43b53cf9778b9bf8f972ec5acba1866ce8652`

`chasm/lib/scheduler2/public_visibility_test.go`: `c01be36099ab2f3498e08709ec6019b24486ff7bd79c23c6c75d7bbe760a5d96`

`chasm/lib/scheduler2/library.go`: `50d314c98dd741a4537cd9337190191157f5261747722edfbfbf6daf67f1ab4f`
