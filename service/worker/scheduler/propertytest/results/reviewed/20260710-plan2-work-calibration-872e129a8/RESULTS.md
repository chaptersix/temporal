# Plan 2 Work Calibration Results

OBSERVATION: Campaign 20260710-plan2-work-calibration-872e129a8 recorded ns/op, B/op, and allocs/op for all nine required work categories and the low, median, budget-transition, P99-like, and maximum-work candidate classes. Raw output is retained in benchmark.txt.

OBSERVATION: Raw iteration-v1 vectors and work/CPU correlations are retained in summary.json. Validation and matching work are separate.

INFERENCE: Benchmark ns/op is a host-specific CPU proxy and is not a deterministic request budget.

RECOMMENDATION: Keep weighted model cpu-correlation-v1 separate from iteration definition v1 and do not tune or enforce it as a production guard in Plan 2.

NEGATIVE EVIDENCE: All bounded benchmark repetitions completed under the five-minute subprocess deadline without panic or budget exhaustion.
