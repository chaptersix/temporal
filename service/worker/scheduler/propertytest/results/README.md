# Schedule Property Analysis Result Contract

This directory contains the durable contract for processing property-analysis and
red-team results. It intentionally does not contain bulk generated output.

## Files

- `campaign.schema.json`: required campaign provenance and artifact manifest.
- `case.schema.json`: required material per-case record.
- `RESULTS.template.md`: human-readable findings template.
- `DECISIONS.template.md`: guard and behavior decision template.

## Durability Rules

1. Bulk output is written outside the repository.
2. Every bulk artifact referenced by reviewed evidence has a SHA-256 checksum.
3. Reviewed minimized cases and decision-critical original cases are committed.
4. Findings reference stable campaign and case IDs.
5. Every claim is labeled observation, inference, recommendation, open question, or
   negative evidence.
6. Population type and denominator accompany every aggregate.
7. Validation work and matching work remain separate.
8. Original and minimized cases are both retained when shrinking changes cost.
9. Schema versions are never changed in place; incompatible changes create a new
   version.
10. No result depends solely on conversation history.

## Bundle Layout

```text
<campaign-id>/
    campaign.json
    cases.jsonl
    summary.json
    RESULTS.md
    DECISIONS.md
    artifacts.sha256
    artifacts/
```

Campaign bundles are incomplete until schemas and checksums validate.

## Test-Only Writer And Verification

`result_writer_test.go` implements the Plan 4 lifecycle for analysis campaigns:

1. Write an incomplete `campaign.json` before case output.
2. Stream validated case records to a temporary JSONL file.
3. Atomically publish cases, summary, results, and optional decisions.
4. Finalize the manifest and artifact checksums only after clean completion.
5. Re-read every case and verify every referenced checksum.

`TestCampaignWriterLeavesInterruptedCampaignIncomplete` verifies that an interrupted
campaign cannot pass bundle verification. `TestCampaignWriterRoundTrip` verifies a
complete bundle. `TestWritePlan1ReviewedCampaign` writes the reviewed Plan 1 bundle
when `SCHEDULE_PROPERTY_RESULTS_DIR` and campaign provenance variables are supplied.

## Reviewed Campaigns

- `reviewed/20260710-plan1-validity-hardening-0e0638d11/`: Plan 1 validity contract,
  validation-budget, minimized timezone-boundary, property, fuzz, mutation, and package
  verification evidence.
