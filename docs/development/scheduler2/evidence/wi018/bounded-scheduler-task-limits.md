# Task limits for the private fresh addressed Scheduler registry

Source audit 2026-11-13. Scope: one supported public ingress call or one CHASM
physical task delivery transaction, with the complete addressed runtime selected.
This does not promise that arbitrary application code may call public ingress an
unbounded number of times within one transaction. No source registration changed
by this audit. Limits are finite upper bounds, not claims that every summand can
be simultaneously saturated.

## Descriptor

| Fixed component slot | Pure persisted | Side effect persisted | New regular per transaction | Immediate enqueues per transaction |
| --- | ---: | ---: | ---: | ---: |
| Scheduler root | 3 | 3 | 25 | 103 |
| Generator | 0 | 0 | 0 | 0 |
| Visibility | 0 | 1 | 4 | 0 |

Layout MaxImmediateExecutions = 103. Required-empty maps on Scheduler:
Operations, Requests, PayloadBlobs, EffectRequests.

Use exactly the same descriptor in private live and private nil/offline library
variants, keeping the same names/type IDs and task registration list. Default
libraries remain unchanged. Root constructor must use the complete fresh
addressed stores. Work quanta can be zero offline and two live; descriptor remains
identical. Components are root nil path, `Generator`, and `Visibility`. Register
CoreLibrary so the Visibility component type resolves. CalendarPreparation and
TimezonePreparation are not currently fields of Scheduler/Generator and must not
be declared as fictitious paths. When attached, add the actual fixed preparation
field path and its `Timezone` child; both currently issue zero tasks.

## Persisted obligations

- Scheduler pure tasks: one Work fence, one Idle fence, one StartRecovery fence.
  They need not all be simultaneously valid; sum three is a safe category bound.
- Scheduler side effects: one StartDispatch fence plus at most two cancel/terminate
  effects (validateEffects bounds unique kinds/IDs). Dispatch and Recovery share
  one control and are mutually exclusive, but the framework limits categories
  separately. Legacy StartTask/ManualTask/DrainTask are not issued in this profile;
  the old validators reject legacy manual/drain tasks and empty PendingStarts
  fences old starts.
- Generator.arm only updates its token/armed/wake scalars. GenerateHandler rejects
  legacy tasks; no AddTask call targets Generator.
- Visibility.generateTask increments one transition count, invalidating every old
  visibility task. Final framework task validation retains at most one.

These are post-validation persisted bounds. chasm.closeTransactionUpdateComponent-
Tasks invokes checkBoundedTasks only after old/new validity filtering. Stale
physical queue envelopes are outside persisted logical cardinality.

## Immediate count: 103, not 2

coordinatorTransactionQuanta=2 bounds valid work steps in an immediate chain.
It does not bound enqueue attempts. reconcileCoordinator deliberately promotes an
already-due unchanged fence whenever remainingWork>0. A public patch accepts at
most100 total manual/backfill commands (`checkPublicCommandCapacity`). Each
acceptance reconciles at most once. Creation additionally reconciles once during
ConfigureGeneration, and applyPublicPatch reconciles once after all commands.
Thus at most 1+100+1=102 initial immediate enqueues. Some can carry the same fence.

WorkHandler consumes one unit, sets planned.Armed=false and reconciles a new
fence (or disarms). Consequently the first valid queued work item invalidates its
old-fence duplicates. It may enqueue one successor with remaining=1; that valid
successor has remaining=0 and yields through a regular physical task. At most
one additional immediate enqueue, total103. Ordinary public mutations/claims/
callbacks and the maximum three due persisted pure tasks enqueue fewer initial
items than this construction/patch bound.

chasm.executeImmediatePureTasks calls countBoundedImmediateExecution before
ExecutePureTask validates each queued item. It counts stale duplicate attempts,
not merely the at-most-two valid Work steps. Therefore both the enqueue budget
and total execution-attempt budget must admit103. A physical Work task delivered
before Close can itself perform a work step in addition to the final immediate
chain; the regular-task formula below includes those outer callbacks separately.

## New regular count: 25 from source fanout

A single state-changing callback has at most these AddTask producers:

1. publishStartDispatch: one dispatch OR recovery task.
2. publishEffects: at most two effects, each once if its wake changed.
3. reconcileIdle: at most one new idle fence.
4. reconcileCoordinator/addWorkTask: at most one regular Work fence.

This totals5 regular tasks per callback, conservatively including incompatible
branches. No internal action taken by WorkHandler calls reconcileWork twice:
scan returns state only; manual/drain use applyTransition; transfer uses
applyTransition and the WorkHandler performs the final reconcile.

EachPureTask may invoke all three persisted root pure obligations in one
transaction. They are not separate transactions, so the bound must include all
three, even though Idle closure and several wake predicates make the worst sum
unreachable. There are at most two valid immediate Work steps afterward. Hence
(3+2)*5=25 regular enqueues. A side-effect result or single public transition has
one outer callback, giving the smaller (1+2)*5=15 bound.

The100-command patch is a different ingress path, not100 admission callbacks.
AcceptBackfill only adds command metadata and reconciles. Its first accepted
command makes Work due; subsequent commands cannot postpone that fence, and the
unchanged-due branch emits no regular duplicate. AcceptManual exists at most once
per patch, and accepting a command is not operation admission. Fresh construction
can emit its original regular Work fence and one earlier command fence, plus an
initial Idle fence. Two immediate steps add at most2*5 tasks: 3+10=13<25. Existing
patches have a smaller baseline. The remaining immediate promotion attempts are
counted separately above, not included in NewPerTransaction.

This derivation relies on supported entrypoint structure. Future code that adds
another task producer, increases effect kinds or initial command capacity, or
calls ingress repeatedly under one transaction must rederive these constants.
The framework rejection remains a guard, never a fallback to a scan.

## Visibility new count: 4

Creation can call NewVisibility (1), MergeCustomSearchAttributes (1),
MergeCustomMemo (1), and CloseTransaction's forced visibility refresh (1):4.
Each method emits once regardless of custom field cardinality. UpdatePublic can
replace SA and memo once each and receives at most one final refresh:3. Public
list changes inside100 commands do not call Visibility.generateTask directly;
CloseTransaction coalesces their memo/version and search-attribute changes into
one refresh. All four tasks use distinct visibility transition counters, so only
the last remains logically valid.

## Required integration witnesses

Use this private registry with fresh selected constructor, including a100-command
initial patch, a100-command subsequent patch, a live quanta2 chain, due Work plus
Recovery delivered in one transaction, callbacks that bind effects, and custom SA
plus memo on creation/update. Record actual enqueues/attempts separately from
persisted and physical tasks. Existing physical tests prove runtime semantics but
do not yet establish these actual bounded-registry maxima or full transaction
cost; parent owns those integration gates.

## Source task integration increment

The registered source task contributes at most one persisted side-effect obligation, fenced by fixed SourcePreparationRequest owner/token/pending and closed-state validation. Root side-effect allowance is now4. A successful source response emits at most one successor source task; a completing response instead reconciles existing Work/Idle. The provisional regular allowance is26 (previous25 plus one source obligation). The handler does not enqueue one task per page and creates no new immediate chain. Current constructor paths do not yet invoke source preparation; staged ingress and per-evaluation checks require producer re-audit before activation acceptance. Fixed preparation child slots remain zero-task components.

## Per-lookup source checks and prepared maintenance

The prepared Work branch can now issue one source-check successor in addition to the previous conservative five producers. It never emits one task per page or per candidate in a single quantum. Root regular allowance therefore becomes (three persisted pure callbacks + two valid immediate Work quanta) * six producers =30. A single source response plus two immediate quanta remains below that. Prepared construction adds one initial source task to the former three-task ingress baseline:4+2*6=16<30. Maintenance is a fifth logical work class inside the same Work task, not another persisted pure obligation; pure3, side-effect4, immediate103 and Visibility limits stay unchanged. Root fixed preparation child slots still emit no tasks. Actual worst-case prepared-profile witnesses remain required before accepting the complete handler bound.
