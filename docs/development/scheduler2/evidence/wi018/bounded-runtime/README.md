# Actual Scheduler private bounded registry

newBoundedLibrary / newBoundedNilLibrary opt only private fresh addressed roots
into the descriptor derived in ../bounded-scheduler-task-limits.md. Default live
and nil libraries remain unchanged. Fixed slots are Scheduler, Generator and
Visibility, with the four old DATA maps explicitly required empty. Offline reload
uses the same descriptor. No preparation child is declared before it is attached.

The new tests use actual Scheduler roots, actual complete private construction,
coordinator pure tasks, and framework CloseTransaction. Unlike the earlier DATA
collection fixture, this includes the actual component topology and visibility.
Operation-site counters at AddTask count regular/immediate enqueues; the existing
immediate attempt guard records attempted execution including invalid duplicate
fences. Initial bounded-layout seeding does not double count them. Production
builds retain the existing no-op observation implementation.

Frozen measured witnesses (final verbose log03):

| Transaction | Regular enqueue total | Immediate enqueues / attempts |
| --- | ---: | ---: |
| Fresh creation with100 backfills + custom SA/memo | 5 | 103 / 103 |
| Patch with100 backfills | 3 | 102 / 102 |
| Following physical Work delivery | 2 | 1 / 1 |
| Physical Work + claim Recovery in one transaction | 3 | 1 / 1 |
| Ack binding cancel effect | 2 | 0 / 0 |
| Update custom SA and memo | 4 | 1 / 1 |

Creation verifies four visibility transition increments and one retained logical
visibility task. Update verifies three increments. Combined due-task test checks
that two physical pure callbacks actually ran in one transaction. The next Work
transaction checks exactly two coordinator token advances (physical then
immediate).100-command tests verify command occupancy and limited admissions,
not merely absence of errors. A deliberate legacy map insertion is rejected with
zero map-key copies and no publishable mutation, retaining the durable snapshot.

These witnesses hit the immediate ceiling exactly; regular25 is the conservative
source-derived fanout bound, not an observed worst-case25. Persisted limits are
checked separately per slot. No bound was increased in response to test failures.

Validation: full CHASM0.474s and scheduler222.759s passed; final directed bounded
witnesses0.634s passed. Scoped CHASM+scheduler2 lint normal/minimal/vet all zero.
The parent supplies independent review, required aggregate fastlint and actual
SQLite/public suite results. These local tree tests do not alone establish SQL
commit failures, final backlog-size work bounds, or prepared-definition runtime
selection. The tree.go manifest supersedes its earlier capability-only hash by
adding the task enqueue observation sites; capability behavior remains covered by
the full CHASM test run.
