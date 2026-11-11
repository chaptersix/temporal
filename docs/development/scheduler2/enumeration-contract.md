# WI-002 occurrence contract

D22: user removed human-review checkpoints. ALC7 remains an independent agent
review; acceptance is autonomous after required evidence passes. Local-only/no
push/no production routing/rollout/migration boundaries continue.

D23 (SC1-SC5): prototype instants are UTC in [2000-01-01,2101-01-01), with a
one-nanosecond-before-lower-bound initial cursor sentinel. Inputs outside this
supported range return errors. Nominal calendar/interval matches have integer
second resolution; actual jitter has millisecond resolution. Effective spec
StartTime and nominal EndTime are inclusive. Search is strictly after the nominal
cursor; an output window includes actual <= through. Jitter may exceed nominal
EndTime but must remain inside the prototype range or return an explicit error.
Fractional StartTime excludes the preceding integer second. Existing legacy
GetNextTime behavior is preserved; its fractional-start discrepancy is DIV2.

One probe evaluates one raw candidate and its exclusion, with at most two raw
calendar lookups (second for jitter lookahead). Maximum64 inclusion/exclusion
terms combined and maximum1024 ranges, bounded at compilation for this adapter.
This bounds candidate calls and calendar input size, not wall-clock CPU; legacy
calendar search itself scans fields/years through2100. Sparse/excluded work yields
with an exact continuation, not global exhaustion (DIV3 versus current CHASM's
compute-limit suppression). Limits: routine8 probes/4 outputs; allowed1..1024
probes and1..128 outputs per call. No new production dependency.

SearchAfter records examined nominal progress, DisposedThrough records completed
include/exclude dispositions, Pending holds a fully determined eligible candidate
with nominal, actual and definition revision. A candidate beyond the current
actual-time window stays pending. Budget/output boundaries do not discard it.
Source exhaustion is distinct from budget exhaustion. Every value returned is
owned by the caller; input continuations/specs must remain unchanged.

Empty means no configured inclusion terms. Impossible is reported only after
scanning the complete effective definition range from its lower boundary without
finding any eligible candidate. A partial-origin scan with no remaining matches
is Exhausted. Yield means the probe budget ended; Limited means output capacity
ended; Waiting means a retained candidate's actual time is beyond the window.
None of the latter three imply impossibility. Completion state is persisted in
the continuation, so repeated terminal queries do no work.

Jitter reuses existing Farm32(seed+nominal binary representation), millisecond
quantization and bound below next RAW nominal (including excluded matches).
Duplicate inclusion expressions coalesce at one nominal instant. Exclusion wins.
Nominal order determines enumeration; unchanged-definition actual times cannot
cross the next raw nominal. Seed is fixed in each immutable revision.

D24 (SC6): explicit definition update discards any unadmitted pending candidate
with a returned Superseded disposition. Search restarts strictly after
max(update boundary, prior DisposedThrough); a discarded future nominal must not
advance this boundary. New revision/seed controls subsequent candidates. Already
admitted WI-001 operations retain their frozen requests. This follows the full
legacy generator's UpdateTime reset (generator_tasks.go:76-82), rather than the
contradictory isolated processor preservation test; name that difference DIV4.
Ordinary Run rejects mismatched revision; callers must use explicit update.

D25 (SC7): reuse existing canonicalization/calendar/timezone primitives, including
DST gap skip and both repeated-hour instants. Tests pin embedded TZif bytes and
record their SHA256; named host zones alone are not reproducible evidence. General
historical non-one-hour transitions remain an unproved library boundary. We test
selected US DST cases and independently filter bounded instants; tests never use
production next-time search to derive expected occurrences.

D26: Lean draft defines the finite-stream abstraction independently and names
completeness/uniqueness/resumption/batching propositions. No unproved theorem is
presented as proved. Compilation and examples check the executable draft; formal
proofs and full Go correspondence gate M3. Source candidate ordering, timezone
and hash primitives remain explicit trusted assumptions.

Sources at00b65430a: service/worker/scheduler/spec.go:291-406;
calendar.go:31-35,93-214; chasm/lib/scheduler/spec_processor.go:132-245;
generator_tasks.go:76-82; service/frontend/workflow_handler.go:7188-7201.
API schedule message comments define inclusive spec boundaries and (start,end]
backfills. Legacy full API compatibility is not claimed by the new local core.
