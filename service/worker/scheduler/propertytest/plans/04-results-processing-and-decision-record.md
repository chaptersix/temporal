# Plan 4: Results Processing And Decision Record

## Objective

Make every material observation, counterexample, negative result, property revision,
and guard decision durable, reproducible, machine-readable, and clearly separated from
interpretation and recommendation.

This is a continuous execution requirement for Plans 1 through 3, not only a final
reporting phase.

## Storage Model

Use three storage tiers:

### Checked-In Contract

Keep these files in the repository:

```text
propertytest/results/README.md
propertytest/results/campaign.schema.json
propertytest/results/case.schema.json
propertytest/results/RESULTS.template.md
propertytest/results/DECISIONS.template.md
```

Schemas and templates are versioned with the analysis code.

### Bulk Campaign Output

Write raw JSONL, profiles, and large corpora to a configurable directory outside the
repository. Every campaign manifest records paths and SHA-256 checksums. Bulk output is
immutable once referenced by a reviewed result.

### Reviewed Evidence

Commit only:

- Minimized Rapid failfiles selected as regressions.
- Small original high-work cases when minimization materially changes cost.
- Stable JSON case records needed for decisions.
- Human-readable findings and decisions.

## Required Result Bundle

Every campaign produces:

```text
campaign.json
cases.jsonl
summary.json
RESULTS.md
DECISIONS.md (when a decision is evaluated)
artifacts.sha256
```

Optional artifacts include benchmark text, benchstat output, CPU/heap profiles, fuzz
corpora, mutation reports, and plots.

The campaign directory name must include UTC date, plan, campaign name, and a short Git
revision:

```text
20260710-plan2-dense-sparse-search-23b933507/
```

## Campaign Manifest

`campaign.json` must validate against `campaign.schema.json` and record:

- Schema version and campaign ID.
- Plan and campaign names.
- Start/end timestamps.
- Git revision and dirty-worktree state.
- Patch checksum when dirty.
- Go, Rapid, Temporal API, and timezone-data versions.
- Iteration/work-definition version.
- Generator, semantic-model, validator, and query-profile versions.
- Commands, flags, environment variables, and seeds.
- Host OS, architecture, CPU count/model, and memory.
- Race, cache, profiling, and instrumentation settings.
- Input population classification.
- Planned and completed case counts.
- Artifact names, sizes, and SHA-256 checksums.

Never compare aggregate results from different work-definition versions without an
explicit conversion or rerun.

## Per-Case Record

Every material case validates against `case.schema.json` and records:

- Stable case and campaign IDs.
- Source population.
- Original and minimized forms.
- Canonical protobuf JSON and serialized size.
- Semantic model.
- Query range, result limit, jitter seed, and timezone.
- Structural and satisfiability classification.
- Witness or proof category.
- Reachability through current production validation.
- Typed outcome and error.
- Complete validation and matching work breakdowns.
- Partial, final, first, and last result information.
- Exact budget transitions.
- CPU, latency, allocation, and memory observations when measured.
- Property hypothesis and lifecycle classification.
- Reproduction command, seed/failfile, and required artifact checksums.

Retain both the original cost-maximizing case and minimized semantic counterexample when
shrinking changes total work, serialized size, or classification.

## Evidence Labels

Every human-readable claim must be labeled:

```text
OBSERVATION
INFERENCE
RECOMMENDATION
OPEN QUESTION
NEGATIVE EVIDENCE
```

Examples:

```text
OBSERVATION: 119/1,000 adversarial generated cases crossed 10,000 work.

INFERENCE: Dense interval results repeatedly pay sparse union-member search cost.

RECOMMENDATION: Do not adopt 10,000 as a universal matching-work limit.
```

Do not turn a generator frequency into a customer-frequency statement.

## Property Lifecycle Record

For each property record:

- Property ID and exact wording.
- Generator and population.
- Independent oracle or metamorphic basis.
- Counterexample and classification.
- Whether the implementation, model, generator, or property changed.
- Revised wording.
- Regression fixture.
- Mutations assigned to the property.
- Mutations actually killed.

Properties that fail to kill assigned mutations are recorded as weak evidence.

## Negative Evidence

Record results that are easy to lose because nothing failed:

- Campaigns and search spaces with no counterexamples.
- Fuzz executions, duration, corpus size, and coverage plateau.
- Mutations not killed.
- Cases indeterminate at maximum validation budget.
- Environment-sensitive outcomes.
- Candidate guards rejected by valid counterexamples.
- Profiles where higher budgets did not change outcomes.

Absence of a failure is always tied to a finite population, duration, and version.

## Aggregate Processing

Every summary separates:

- Curated realistic cases.
- Uniform property-generated cases.
- Coverage-guided fuzz cases.
- Evolutionary/adversarial cases.
- Production-derived cases, if later available.
- Invalid and validation-abuse cases.

For each population report denominators, result-limit and query-window buckets, validity
classes, budget transitions, work percentiles, CPU/allocations, and top retained cases.

Do not aggregate validation work and matching work into one unlabeled number.

## Decision Record

Every candidate guard or behavior change gets a durable entry in `DECISIONS.md`:

- Decision ID and status: proposed, accepted, rejected, deferred, or superseded.
- Exact question.
- Relevant case IDs and campaigns.
- Observed benefits.
- Valid cases rejected or behavior changed.
- False-positive and compatibility risks.
- Client actionability and retry behavior.
- Legacy/CHASM implications.
- Tenfold-load evidence.
- Confidence and unresolved questions.
- Follow-up owner/project when known.

Rejected guards remain in the log. Never delete a rejected decision when new work
supersedes it; link the superseding decision.

## Automation

Add a test-only result writer that:

1. Creates a campaign manifest before execution.
2. Writes case records as JSONL through an atomic temp file.
3. Finalizes summary and checksums only after clean completion.
4. Marks interrupted campaigns incomplete rather than presenting partial aggregates as
   final.
5. Validates every record against the versioned schema.
6. Redacts or bounds timezone data and diagnostic strings.

Add a verification command that checks schema validity, referenced artifacts, and
checksums for any reviewed result bundle.

## Review Workflow

Before a finding is used in a recommendation:

1. Reproduce it from the recorded command and seed/failfile.
2. Confirm Git and work-definition versions.
3. Confirm population labeling and denominator.
4. Compare original and minimized cases.
5. Verify independent oracle or property basis.
6. Check whether a later campaign supersedes it.
7. Add case IDs to `FINDINGS.md` and any decision record.

## Completion Gate

A campaign is incomplete until:

- Manifest and case records validate.
- Checksums pass.
- Commands and seeds reproduce selected cases.
- Original and minimized performance-sensitive cases are retained.
- Negative evidence is recorded.
- Findings distinguish observations, inferences, and recommendations.
- Any evaluated guard has a decision-log entry.

## Acceptance Criteria

- Schemas and templates remain checked in and versioned.
- Every future campaign produces the required result bundle.
- Every durable finding references a case or aggregate campaign ID.
- Every decision references evidence and preserves rejected alternatives.
- Work-definition, environment, and population differences are explicit.
- Interrupted or partial campaigns cannot be mistaken for completed evidence.
- Reviewed results can be reproduced without relying on this conversation.
