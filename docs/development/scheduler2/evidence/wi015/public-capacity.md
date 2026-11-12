# Public configured capacity verification

Implementation complete, independent review pending; no milestone acceptance. See `../../design/public-capacity.md` for reference accounting and explicit cardinality/work-bound limitations. All evidence is local.

## Behavioral regression and results

- `scheduler2-capacity-before.log`: actual-tree regression FAIL 0.720s against previous production: limit2 over-admitted; ALLOW_ALL limited to4 for larger/disabled values; BUFFER_ALL total retained limited to5. A preceding test compile typo was fixed before this behavioral baseline, not counted as defect detection.
- `scheduler2-capacity-after04.log`: focused capacity/close-time cases PASS0.677s. Covers2,5,1000,0,-1 with BUFFER_ALL/ALLOW_ALL; live reduction; acknowledged ALLOW_ALL vs tracked running/completed occupancy; queue transfer at full capacity; ten-completion canonical ordering; oldest late completion self-eviction and later admission; accepted manual wait/config retry/failed publication; missing/invalid canonical close time rejection.
- `scheduler2-capacity-package04.log`: all `./chasm/lib/scheduler2/...` PASS(root16.141s, decision0.445s, occurrence1.016s). Internal finite domain remains capacity4. Package01 exposed a stale test setup relying on public4 and a BUFFER_ONE public counter regression; both corrected. Package03 exposed a competing automatic scan in a capacity-only fixture and the timestamp owner's changed internal replay key; fixture now explicitly disarms unrelated generation, timestamp owner restored original internal key. Package02 was attempted during another owner's proto regeneration and failed setup; it is not semantic evidence.
- `scheduler2-capacity-functional02.log`: actual frontend `TestSchedule2ConfiguredCapacityNeighbors` limits2/5 plus existing `TestSchedule2BufferOverrunDropsActions` PASS7.397s. Each neighbor observes a running workflow and exactly limit−1 queued starts after overflow; completes it, observes the next start, then requires another overflow and exactly limit−2 queued starts. This tests retained completed occupancy rather than merely positive drop counters.
- Functional01 PASS4.265s ran a temporary duplicate original wrapper; removed in favor of the final stronger test and the parent's existing wrapper. It is not a distinct retained contract test.
- Proto generation passed; the timestamp owner subsequently regenerated the combined schema. The source hash manifest identifies final combined source, not exclusively authored portions.

## Exact commands

From `/private/tmp/sch-redesign-execution`:

```
CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 -parallel 4 ./chasm/lib/scheduler2 -run '^TestPublicConfiguredAutomaticCapacity$' -count=1
CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 -parallel 4 ./chasm/lib/scheduler2 -run '^TestPublic(Capacity|ConfiguredAutomaticCapacity|CompletionRequiresCanonicalCloseTime)' -count=1
CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 -parallel 4 ./chasm/lib/scheduler2/... -count=1
CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep,integration -p 2 -parallel 4 ./tests -run '^TestSchedule2(ConfiguredCapacityNeighbors|BufferOverrunDropsActions)$' -count=1 -timeout=2m
make proto
```

Logs and SHA256 manifest: `public-capacity/`. Parent consolidated lint initially identified import ordering/unused test assignment, now corrected without runtime changes; final lint result is parent-owned. The fixed synthetic lease regression retains the same lease/wait/recovery assertions but models configured full capacity instead of the removed public four-operation limit.

## Limits

Default1000 and disabled-capacity cases test crossing the former4 boundary; they do not materialize1000 workflows. Wider configured storage and retained active authority are not covered by the four-slot finite model. The two-quantum immediate pump and one-quantum fallback remain, but queue copies, payload garbage collection, active-operation retention scans, and public projection sorting depend on retained cardinality. M4's stronger work-bound obligation remains unresolved. No source-level capacity mutation campaign or production rollout is claimed.
