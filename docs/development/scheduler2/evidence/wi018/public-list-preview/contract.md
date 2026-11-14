# Durable bounded list preview job

Add `PublicListPreview` message and Scheduler field `ListPreview chasm.Field[*scheduler2pb.PublicListPreview]` (no activation):

```
bytes owner_id = 1; //32-byte DefinitionPreparation owner
uint64 stamp = 2; //exact ListControl.pending_stamp
 google.protobuf.Timestamp original_after = 3;
 google.protobuf.Timestamp after = 4;
bool limited = 5;
int64 remaining = 6;
uint32 target_count = 7; //0..5, checked against limited/remaining
repeated google.protobuf.Timestamp future_action_times = 8; //<=target_count
bytes zone_fingerprint = 9; //empty before first capability bind or32 bytes
bytes work = 10; //empty or exactly PreparedSpecStateBytes=328
bool policy_captured = 11;
int64 warn_iterations = 12;
int64 max_iterations = 13;
bool done = 14;
```

`newListPreview(owner,stamp,after,limited,remaining)` validates and captures `after=max(now,cursor,updateTime)` supplied by root. The helper does not read definition/spec or dynamic configuration. Root preflights owner+stamp with refresh plan and publishes job/control together.

`advanceListPreview(ctx, definition *DefinitionPreparation, job *PublicListPreview, owner [32]byte, stamp uint64, limits func()(warn,max int), budget int) (listPreviewStep,error)` is read-only/capture-only. `listPreviewStep` contains detached next job, PreparedSpecStats and Stale/Reset flags. It validates fixed cardinalities/codec size/owner/stamp before any source-page reads, uses actual DefinitionPreparation.Prepared/PreparedTimezone capabilities, and never reads Source. Budget1..64. One call invokes AdvancePreparedSpec at most once and completes at most one GetNextTime output. Every new logical output captures live warning/maximum once; quantum continuation uses the captured values. Source policy identity includes these values. GetNextTime uses previous ACTUAL as next after; completing five outputs returns Done. Semantic exhaustion/compute-limit ends with already completed prefix, exactly publicFuture. Other read/codec errors return error without replacement/publication.

Definition owner or ListControl pending stamp mismatch yields Stale before page/timezone reads. Root supplies the current ListControl fence, never an unvalidated request fence. Original snapshot of limited/remaining remains fixed; changes that alter preview inputs must create a new pending stamp. Recent/notes-only callbacks use prepareListDynamic, preserving job and preventing starvation.

A zone fingerprint replacement restarts the entire unpublished window from original_after, clears completed prefix and current raw work, recaptures policy for new logical calls. Old publicFuture compiles once and pins one zone across five GetNextTime calls; retaining an old-zone prefix would mix one window across snapshots. Published prior window stays untouched. Reset is bounded and may consume the current one-quantum budget. Root outside-mutation source service owns refresh policy and resolves before jobs; no loader/cache/clock configuration access is added here.

On step.Done, root calls prepareListWindow(owner,stamp,job.FutureActionTimes), reserves MemoVersion before any publication, then commits the selected list slot/window and clears/completes ListPreview atomically. No unfinished prefix is exposed. Root scheduler owns durable continuation token/timer/recovery, fair progress while paused, refresh after published first future expires, definition/source changes and close fencing. This helper alone is not actual runtime activation or a complete transaction bound.
