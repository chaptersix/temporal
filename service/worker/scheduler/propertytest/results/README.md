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
