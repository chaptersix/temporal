#!/usr/bin/env python3
"""Run the pinned model and one-fault-at-a-time sensitivity checks locally."""
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
root = Path(__file__).resolve().parent
source = (root / "start.fizz").read_text()
args.output.mkdir(parents=True, exist_ok=True)
cases = [("baseline-100", 100, None), ("baseline-200", 200, None)]
cases += [(name.lower(), 100, name) for name in (
    "LOST_WAKEUP", "CURSOR_LOSS", "DUPLICATE_ACCOUNTING", "RETRY_IDENTITY", "RETRY_INPUT"
)]
results = []
for name, bound, mutation in cases:
    case = args.output / name
    case.mkdir(parents=True, exist_ok=True)
    text = source.replace("max_actions: 100", f"max_actions: {bound}")
    if mutation:
        text = text.replace(f"{mutation} = False", f"{mutation} = True")
    spec = case / "start.fizz"
    spec.write_text(text)
    command = [str(args.fizz.resolve()), "--output-dir", str((case / "graph").resolve()), str(spec.resolve())]
    result = subprocess.run(command, capture_output=True, text=True, timeout=300)
    output = result.stdout + result.stderr
    (case / "run.log").write_text(output)
    passed = "PASSED: Model checker completed successfully" in output
    # A parser error or timeout does not count as a detected invariant mutation.
    expected = {
        "LOST_WAKEUP": "DurableObligation", "CURSOR_LOSS": "AdmissionAtomicity",
        "DUPLICATE_ACCOUNTING": "AccountingOnce", "RETRY_IDENTITY": "StableRequest",
        "RETRY_INPUT": "StableRequest",
    }.get(mutation)
    detected = expected is not None and f"FAILED: Model checker failed. Invariant:  {expected}" in output
    record = {
        "case": name, "command": command, "exit_code": result.returncode,
        "bound": bound, "mutation": mutation,
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "passed": passed, "detected": detected,
        "counts": re.findall(r"Valid Nodes: .*|Nodes: .*|Unique states: .*", output),
    }
    results.append(record)
    print(name, "passed=" + str(passed), "detected=" + str(detected), flush=True)
(args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
if not all(r["detected"] if r["mutation"] else r["passed"] for r in results):
    sys.exit("At least one required result is missing; inspect run.log")
