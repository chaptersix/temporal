#!/usr/bin/env python3
"""Check the independent buffer model and four named sensitivity defects."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--fizz", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
source = Path(__file__).with_name("buffer.fizz").read_text()
args.output.mkdir(parents=True, exist_ok=True)
cases = [("baseline-100", 100, None), ("baseline-200", 200, None)]
mutations = {
    "DEBIT_ON_ENQUEUE": ["NoEnqueueBudgetDebit"],
    "LOSE_COMPLETION_WAKE": ["DrainObligation"],
    "STALE_DRAIN_CONSUMES_QUEUE": ["FIFOConservation"],
    "REUSE_QUEUED_TEMPLATE": ["FrozenAdoption"],
}
cases += [(name.lower(), 100, name) for name in mutations]
results = []
for name, bound, mutation in cases:
    case = args.output / name
    case.mkdir(parents=True, exist_ok=True)
    spec = source.replace("max_actions: 100", f"max_actions: {bound}")
    if mutation:
        spec = spec.replace(f"{mutation} = False", f"{mutation} = True")
    path = case / "buffer.fizz"
    path.write_text(spec)
    command = [str(args.fizz.resolve()), "--output-dir", str((case / "graph").resolve()), str(path.resolve())]
    try:
        run = subprocess.run(command, capture_output=True, text=True, timeout=300)
        output, code = run.stdout + run.stderr, run.returncode
    except (OSError, subprocess.TimeoutExpired) as exc:
        output, code = str(exc), None
    (case / "run.log").write_text(output)
    passed = code == 0 and "PASSED: Model checker completed successfully" in output
    expected = mutations.get(mutation)
    detected = expected is not None and any(f"FAILED: Model checker failed. Invariant:  {name}" in output for name in expected)
    results.append({"case": name, "command": command, "exit_code": code,
        "bound": bound, "mutation": mutation, "expected_invariant": expected,
        "sha256": hashlib.sha256(spec.encode()).hexdigest(), "passed": passed,
        "detected": detected, "counts": re.findall(r"Nodes: .*|Valid Nodes: .*", output)})
    print(name, "passed=" + str(passed), "detected=" + str(detected), flush=True)
(args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
if not all(r["detected"] if r["mutation"] else r["passed"] for r in results):
    sys.exit("Required baseline or named mutation detection missing; inspect run.log")
