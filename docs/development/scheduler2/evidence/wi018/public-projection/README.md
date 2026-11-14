# Bounded public action and running projections

Implements `../public-projection-integration.md` behind the explicit fresh-state
ProjectionStore selector. The public constructor is not activated by this slice.
The parent owns dispatcher integration and final complete-profile activation.

Actions use addressed DATA, immutable admission sequence and explicit operation
ownership. Recent results retain the greatest ten `(actual time, admission
sequence)` records; List takes the last five. Completion occupancy stays at ten
with stable close-time ordering, including late completion self-eviction. A
fixed mutation plan covers addressed operation changes and one recent victim,
preflights index changes, then publishes without new rejection. Old DATA values
are cloned before result updates, so failed preparation cannot mutate them.

Running membership has its own sequence index in ProjectionPages. It is separate
from readiness OperationPages: independently captured CountBatches must not share
canonical count-page reference ownership. Visibility reads the checked scalar;
Describe traverses only returned RunningRecords, with exact admission order and
no pagination/truncation. Its cost is output proportional, not constant. Each
SequenceAtOrAfter query has at most 258 index reads; mutation work is bounded by
fixed changes and trie depth. The test with 25 started records exercises the
storage/output seam, not reachability under one configured overlap policy.

Validation rejects unsupported/mixed formats, invalid fixed windows, missing
post-admission runtime, index/count disagreement and malformed running records.
Describe and public mutations propagate errors. SearchAttributes has no error
result, so invariant violations use the framework caller's panic boundary like
DATA decoding; this evidence does not claim a new local panic recovery boundary.

Evidence:

- Existing lifecycle assertions run for both finite and indexed projections:
  ack/callback reordering, late/duplicate ack, detached output, retained history,
  List/Describe windows and completion retention/self-eviction.
- Fixed-window test reverses acknowledgment order and tests actual-time ties and
  an older late observation; action preflight test checks original DATA identity.
- Running output test verifies full 25-result order and no ActionRecords,
  Operations or Requests materialization, plus corrupt DATA rejection.
- Sequence lower-bound Rapid reference covers staged and persisted views,
  arbitrary uint64 keys and maximum sequence; every backend read fault poisons
  commit and leaves durable backend unchanged.
- Full scheduler2 and queueindex passed (18.923s / 4.443s); final directed/lint
  logs cover subsequent fail-closed validation tightening. Parent supplies the
  independent review and required aggregate fast-lint acceptance.

Retained failures are fixture failures, not implementation counterexamples:
`projection-index01` blindly reinserted a generated duplicate sequence (minimal
witness 0,0), correctly rejected by InsertSequence. The fixture now toggles
insert/delete; the original Rapid failfile remains in queueindex/testdata and is
replayed by the successful full and directed runs. `projection03` omitted Step's
error return at compile time. `projection04` assumed PublicRuntime already existed
on an empty fixture; the helper now explicitly creates it. All original logs are
kept. No SQL commit/rollback, physical task delivery or public activation is
claimed by this projection slice.
