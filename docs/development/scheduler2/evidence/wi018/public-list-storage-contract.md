# Selected public list partition schema/API

Additive message `PublicListControl`:

```
uint32 published_slot = 1; // 0 none, 1 StaticA, 2 StaticB
uint32 pending_slot = 2; // 0 no pending preview, otherwise 1 or 2
bytes slot_a_owner = 3; // absent or exactly32 bytes, SHA256(contentID || revision)
bytes slot_b_owner = 4;
uint64 published_stamp = 5;
uint64 pending_stamp = 6;
int64 published_list_limit = 7;
int64 pending_list_limit = 8;
repeated temporal.api.schedule.v1.ScheduleActionResult recent_actions = 9; // <=5
repeated google.protobuf.Timestamp future_action_times = 10; // <=5
```

Scheduler fields:
- `ListControl chasm.Field[*scheduler2pb.PublicListControl]`
- `ListStaticA chasm.Field[*schedulepb.ScheduleListInfo]`
- `ListStaticB chasm.Field[*schedulepb.ScheduleListInfo]`
- `ListNotes chasm.Field[*scheduler2pb.Payload]` (`Data` exact UTF8 Notes bytes)

ListControl presence selects new profile. Root chooses constructor activation only with all real preview/staging hooks; legacy PublicList path remains unchanged. Initialized control may have no published owner/window while initial eventual visibility is pending. Static slot values contain only cleaned/truncated Spec and WorkflowType, never dynamic fields. Slots are double buffered: incoming definition writes nonpublished slot; repeated updates overwrite pending slot. Completion swaps selector/window and empties old slot without decoding it. Same definition/limit refresh reuses published static slot. Owner identifiers are fixed32 bytes; zero invalid. Both owners are represented in control; IsSet membership validates without static decode. Output strings in <=5 recent results and user Notes remain explicit variable-byte terms, not claimed fixed bytes.

List-limit settings: capture nonnegative limit outside mutation together with static staging. A changed setting requires restaging from canonical source outside mutation even when definition owner unchanged; otherwise refresh preflight rejects the limit mismatch. Keep separate published/pending limits so Memo retains old coherent projection while replacement preview is unfinished. Neither Memo nor control validation calls dynamic configuration.

Monotone preview stamp allocates max(published_stamp,pending_stamp)+1 with overflow rejection before writes. Preview completed-result publication requires exact pending owner+stamp. Stale job returns false/no writes. Definition owner derives from canonical-content+seed identity and positive revision; hashing fixed identity/revision may occur in mutation after preflight. Calendar agent confirms identity compatibility.

Helpers in new public_list_storage.go only:
- stagePublicListStatic(schedule,limit) (*ScheduleListInfo,error): input-proportional clone/CleanSpec/TZ-data removal and existing per-top-level-list truncation, outside mutation.
- prepareListRefresh(ctx,owner,staticOrNil,limit,recent,notes) (publicListPlan,error): capture-only plan with stamp, validated ownership, detached fixed dynamic values and optional staged static/notes. Nil static requires reusable published owner AND same captured limit. Notes pointer nil means unchanged. Changed recent/notes require reserved MemoVersion; pending-only refresh does not expose new spec/futures.
- publishListRefresh(ctx,plan): fixed slot/control/notes writes and MemoVersion consume for exposed changes.
- prepareListWindow(ctx,owner,stamp,future) (publicListPlan,bool,error): stale=false; validates all cursors/lengths before writes, captures selector/window publication and old-slot clearing. Publication uses publishListWindow and reserved MemoVersion.
- listMemo(ctx): assemble published static+current dynamic+Notes+root Paused only at export/read.
- listFirstFuture(ctx): fixed control read only.

Root replaces synchronous selected mutation callsites: publishPublicTransition's PublicSchedule.TryGet+publishPublicList; publishPublicMutation's Get+publishPublicList; applyPublicPatch final publishPublicList; projectPublicPause final publishPublicList; initial create list publication. publicListProjection currently invokes publicFuture(5), and publishPublicList clones/CleanSpec every call. Those selected paths must request/advance durable preview and update dynamic metadata instead. Describe publicInfoProjection->publicFuture(10) and MatchingTimesPublic stay immediate read-only but use ephemeral prepared driver over consistent source snapshot. Definition output/read work is separately measured. Existing legacy helper tests and legacy runtime branch remain intact.

## Initial publication readiness and source lifetime (D60–D61)

An unfinished preview is an immutable lookup window. Its source is resolved once for the entire window, matching publicFuture's one NewCompiledSpec before the output loop; per-output iteration settings remain sampled. Expiry of an already produced time does not discard unfinished work. Completion publishes the coherent window, after which normal expiry requests another window. A maintenance quantum may advance multiple outputs but spends at most64 total elementary steps.

A prepared root whose ListControl.PublishedStamp is zero reports VisibilityReady=false. The actual History visibility executor retries incomplete running projections with ErrDependencyTaskNotCompleted before exporting a row. Publication advances the memo revision and creates the latest visibility task as before. Closed execution export bypasses readiness to preserve close/delete progress. This chooses storage publication deferral over List-only filtering, which could expose mismatched Count/List contents, and over partial initial memo publication, which would violate the coherent-window contract. Default roots without the optional provider retain existing behavior. Unit history/reload evidence is in ../visibility-readiness/; shared public deadline acceptance remains required.
